"""通过 wecom-cli 访问企业微信智能表格的真实适配器。"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import date, datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings
from app.core.failures import PermanentTaskFailure, RetryableTaskFailure
from app.core.logging import LOG_CONTEXT
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapterConfigurationError,
    SmartTablePermissionError,
    SmartTableRecordNotFoundError,
)
from app.smart_table.enums import BUSINESS_ENUM_FIELDS, EnumSnapshotService
from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableOption,
    SmartTablePermissions,
    SmartTableRecord,
    SmartTableSchema,
)
from app.smart_table.registry import (
    CRM_BUSINESS_FIELD_NAMES,
    READ_ONLY_SYSTEM_TIME_FIELDS,
    REQUIRED_SMART_TABLE_FIELDS,
    writable_smart_table_fields,
)

logger = logging.getLogger(__name__)
_CLI_MIN_INTERVAL_SECONDS = 0.25

CliRunner = Callable[[Sequence[str]], Mapping[str, object]]
ReadResource = Literal["fields", "records"]
WriteAction = Literal["list", "add", "update"]

_FIELD_TYPES = {
    "text": SmartTableFieldType.TEXT,
    "phone_number": SmartTableFieldType.PHONE_NUMBER,
    "email": SmartTableFieldType.EMAIL,
    "single_select": SmartTableFieldType.SINGLE_SELECT,
    "select": SmartTableFieldType.MULTI_SELECT,
    "date_time": SmartTableFieldType.DATE,
    "attachment": SmartTableFieldType.ATTACHMENT,
    "location": SmartTableFieldType.LOCATION,
    "user": SmartTableFieldType.MEMBER,
    "reference": SmartTableFieldType.REFERENCE,
}
_RECENT_WRITE_VISIBILITY_SECONDS = 30.0
_WRITE_VERIFICATION_DELAYS = (0.0, 0.3, 1.0, 2.0)
_SAFE_PROCESS_ERROR_TYPES = frozenset(
    {
        "ApiError",
        "AuthError",
        "AuthenticationError",
        "CryptoError",
        "HttpError",
        "NetworkError",
        "ParseError",
        "TimeoutError",
        "TransportConfigError",
    }
)


class WecomCliSmartTableAdapterError(RuntimeError):
    """表示 wecom-cli 返回了不能安全继续处理的响应。"""


class WecomCliTransportError(WecomCliSmartTableAdapterError, RetryableTaskFailure):
    """表示 wecom-cli 网络或进程传输失败，可安全重试同一请求。"""

    @property
    def error_code(self) -> str | None:
        """暴露底层受控错误分类，供上层审计保留安全诊断。"""
        value = getattr(self.__cause__, "error_code", None)
        return value if isinstance(value, str) else None

    @property
    def external_error_code(self) -> int | None:
        """暴露底层远端数字错误码，不传播远端正文。"""
        value = getattr(self.__cause__, "external_error_code", None)
        return value if type(value) is int else None

    @property
    def external_error_type(self) -> str | None:
        """暴露底层白名单错误类型，供结构化日志和审计使用。"""
        value = getattr(self.__cause__, "external_error_type", None)
        return value if isinstance(value, str) else None

    @property
    def http_status(self) -> int | None:
        """暴露底层 HTTP 状态码，供安全重试诊断使用。"""
        value = getattr(self.__cause__, "http_status", None)
        return value if type(value) is int else None


class WecomCliProtocolError(WecomCliSmartTableAdapterError, PermanentTaskFailure):
    """表示 wecom-cli 返回结构、参数或权限业务失败，不应自动重试。"""


class SmartTableWriteVerificationError(WecomCliSmartTableAdapterError, RetryableTaskFailure):
    """表示写入已获 ACK 但远端事实暂未在有限窗口内可核实。"""

    def __init__(
        self,
        missing_or_mismatched_fields: Sequence[str],
        *,
        remote_record_id: str | None = None,
    ) -> None:
        """构造不含字段值的写入核实失败。

        参数：missing_or_mismatched_fields 为本次写入中未能远端核实的规范字段名；
        remote_record_id 为服务端已确认创建的记录标识，供上层安全恢复使用。
        返回值：无。
        异常：无。
        副作用：仅在异常对象中保存字段名和远端记录标识，标记为可恢复核实等待。
        """
        self.missing_or_mismatched_fields = tuple(missing_or_mismatched_fields)
        self.remote_record_id = remote_record_id
        self.error_code = "write_verification_pending"
        super().__init__("智能表格写入未通过远端字段核实")


class WecomCliProcessError(WecomCliProtocolError):
    """表示 wecom-cli 子进程非零退出，具体动作由调用层判断是否可重试。"""

    def __init__(
        self,
        message: str,
        *,
        error_code: str = "process_exit",
        external_error_code: int | None = None,
        external_error_type: str | None = None,
        http_status: int | None = None,
        rejected_field_candidates: Sequence[str] = (),
    ) -> None:
        """保存不含原始输出的受控 CLI 错误诊断字段。

        参数：message 为安全异常摘要；其余字段为内部错误分类和白名单结构字段。
        返回值：无。
        异常：无。
        副作用：仅保存异常对象状态，不记录 stdout、stderr 或远端正文。
        """
        super().__init__(message)
        self.error_code = error_code
        self.external_error_code = external_error_code
        self.external_error_type = external_error_type
        self.http_status = http_status
        self.rejected_field_candidates = tuple(rejected_field_candidates)

    @property
    def retryable(self) -> bool:
        """仅允许明确网络、超时或可重试 HTTP 状态触发幂等重试。"""
        return self.error_code in {"network_error", "timeout", "rate_limited"} or (
            self.error_code == "http_error"
            and self.http_status is not None
            and (self.http_status in {408, 425, 429} or self.http_status >= 500)
        )


class WecomCliSmartTableAdapter:
    """以 wecom-cli 实现冻结的智能表格读写契约。"""

    def __init__(
        self,
        *,
        doc_id: str,
        sheet_id: str,
        sheet_title: str | None = None,
        sales_can_create_records: bool | None = None,
        sales_can_delete_records: bool | None = None,
        command: str = "wecom-cli",
        timeout_seconds: float = 120.0,
        retry_count: int = 1,
        require_owner_field: bool = True,
        include_entry_time: bool = True,
        runner: CliRunner | None = None,
    ) -> None:
        """初始化固定文档、子表和可替换的 CLI 执行入口。

        参数：doc_id、sheet_id 为管理员配置的目标表标识；sheet_title 为完整查询接口使用的子表名称；
        两个 sales 参数仅接受管理员已核验的权限快照；require_owner_field 控制机器人是否必须
        写入“负责人”；include_entry_time 控制记录回读是否查询“录入时间”；command、timeout_seconds、
        retry_count 控制 CLI 调用；runner 供测试替换。
        异常：标识为空或重试次数为负数时抛出 SmartTableAdapterConfigurationError。
        副作用：不访问网络，仅保存不可变配置。
        """
        if not doc_id or not sheet_id:
            raise SmartTableAdapterConfigurationError(
                "缺少 WECOM_SMART_TABLE_DOC_ID 或 WECOM_SMART_TABLE_SHEET_ID"
            )
        if retry_count < 0:
            raise SmartTableAdapterConfigurationError("WECOM_CLI_RETRY_COUNT 不能小于 0")

        self._doc_id = doc_id
        self._sheet_id = sheet_id
        self._sheet_title = sheet_title.strip() if isinstance(sheet_title, str) else None
        self._sales_can_create_records = sales_can_create_records
        self._sales_can_delete_records = sales_can_delete_records
        self._command = command
        self._timeout_seconds = timeout_seconds
        self._retry_count = retry_count
        self._require_owner_field = require_owner_field
        self._include_entry_time = include_entry_time
        self._runner = runner or self._run_subprocess
        self._pace_cli_calls = runner is None
        # 只缓存本进程已通过通讯录唯一确认的成员映射，失败结果不缓存。
        self._member_user_id_cache: dict[str, str] = {}
        self._member_name_cache: dict[str, str] = {}
        self._rate_limit_redis: Any = (
            Redis.from_url(
                get_settings().redis_url,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            if self._pace_cli_calls
            else None
        )
        self._schema: SmartTableSchema | None = None
        # 单一 TTL 由快照服务负责，写入和 AI 可固定同一处理版本。
        self._enum_snapshots = EnumSnapshotService(
            self._load_schema, get_settings().smart_table_enum_cache_seconds
        )
        self._recent_written_records: dict[str, tuple[float, SmartTableRecord]] = {}

    def get_schema(self) -> SmartTableSchema:
        """分页读取真实字段、类型和枚举选项。

        返回：保留可写字段标识和选项标识的结构快照；系统时间列不参与类型绑定。
        异常：字段类型不受冻结契约支持或 CLI 调用失败时抛出异常。
        副作用：调用 wecom-cli 的 fields list 接口。
        """
        self._schema = self._enum_snapshots.get_schema()
        return self._schema

    def _load_schema(self) -> SmartTableSchema:
        """分页重新读取真实结构；返回不可变快照，CLI 异常传播，不写记录或修改管理员配置。"""
        fields: list[SmartTableField] = []
        for item in self._list_pages("fields"):
            title = item.get("field_title")
            if isinstance(title, str) and title.removeprefix("*") in READ_ONLY_SYSTEM_TIME_FIELDS:
                # 原生只读列的真实类型尚未在 CLI 契约确认，按管理员确认名称排除，不猜测 DATE。
                continue
            fields.append(self._parse_field(item))
        return SmartTableSchema(fields=tuple(fields))

    def get_permissions(self) -> SmartTablePermissions:
        """返回管理员已核验并通过环境变量注入的销售权限快照。

        返回：普通销售新增和删除权限。
        异常：权限未注入时抛出 SmartTableAdapterConfigurationError；当前 CLI 无读取该配置的接口。
        副作用：无；避免把历史 PoC 误报为实时权限读取结果。
        """
        if self._sales_can_create_records is None or self._sales_can_delete_records is None:
            raise SmartTableAdapterConfigurationError(
                "wecom-cli 未提供记录新增/删除权限读取接口；请注入已核验的销售权限快照"
            )
        return SmartTablePermissions(
            sales_can_create_records=self._sales_can_create_records,
            sales_can_delete_records=self._sales_can_delete_records,
        )

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """按记录标识读取目标记录，并在配置完整子表名称时使用服务端单行查询。

        参数：record_id 为企业微信返回的记录标识。
        返回：记录快照；不存在时返回 None。
        异常：CLI 调用或响应结构异常时抛出异常。
        副作用：调用 wecom-cli 的 records query 或 records list 接口。
        """
        schema = self.get_schema()
        if self._sheet_title:
            # 完整查询可读取机器人在列表接口中不可见的受限记录，且服务端只返回目标行。
            return self._query_record_by_id(record_id, schema)
        for item in self._list_pages("records"):
            # records list 没有按记录标识读取的独立接口，先按原始标识定位。
            # 这样历史异常行不会阻断目标行回读。
            if item.get("record_id") == record_id:
                record = self._parse_record(item, schema)
                self._recent_written_records.pop(record_id, None)
                return record
        # 企业微信写入响应已确认成功但列表可能短暂未反映；仅在短窗口使用本进程快照，
        # 防止 T09 把刚创建的行误判为已删除并重复新增。
        recent = self._recent_written_records.get(record_id)
        if recent is not None and time.monotonic() - recent[0] <= _RECENT_WRITE_VISIBILITY_SECONDS:
            return recent[1]
        self._recent_written_records.pop(record_id, None)
        return None

    def _get_record_remote_uncached(
        self, record_id: str, schema: SmartTableSchema
    ) -> SmartTableRecord | None:
        """直接从企业微信读取记录，完全绕过本地近期写入缓存。

        参数：record_id 为目标记录标识；schema 为已读取的字段定义。
        返回值：远端记录快照；远端暂不可见或不存在时返回 None。
        异常：CLI 传输、权限和响应协议错误向调用方传播。
        副作用：配置 sheet_title 时按 RECORD_ID 执行单行 records query，否则分页执行 records list。
        """
        if self._sheet_title:
            # 写后核实必须绕过近期缓存，并通过服务端过滤读取真实目标行。
            return self._query_record_by_id(record_id, schema)
        # list 分页是该配置下唯一可用的远端读路径；本方法刻意不查 _recent_written_records。
        for item in self._list_pages("records"):
            if item.get("record_id") == record_id:
                return self._parse_record(item, schema)
        return None

    def find_records(self, filters: Mapping[str, object]) -> list[SmartTableRecord]:
        """以冻结契约定义的全部字段精确匹配筛选记录。

        参数：filters 为字段展示名到期望原始值的映射。
        返回：满足所有条件的记录快照。
        异常：CLI 调用或响应结构异常时抛出异常。
        副作用：分页读取当前子表；T03 契约未定义可无损映射的服务端过滤表达式。
        """
        return [
            record
            for record in self.get_records()
            if all(record.fields.get(name) == value for name, value in filters.items())
        ]

    def find_records_by_field_values(
        self, field_name: str, values: Collection[str]
    ) -> list[SmartTableRecord]:
        """通过一条服务端查询匹配字段值集合，避免批量幂等检查逐条扫描。

        参数：field_name 为规范文本字段名；values 为待精确匹配的字符串集合。
        返回值：服务端返回且字段值确实属于集合的记录快照。
        异常：缺少子表名、字段不为文本类型或 CLI 查询失败时抛出异常。
        副作用：执行一次只读 records query。
        """
        requested_values = tuple(dict.fromkeys(values))
        if not requested_values:
            return []
        if not self._sheet_title:
            raise SmartTableAdapterConfigurationError(
                "字段值批量查重需要配置 WECOM_SMART_TABLE_SHEET_TITLE"
            )
        schema = self.get_schema()
        field = schema.get_field(field_name)
        if field is None or field.field_type not in {
            SmartTableFieldType.TEXT,
            SmartTableFieldType.LONG_TEXT,
        }:
            raise SmartTableAdapterConfigurationError(
                f"字段值批量查重要求字段“{field_name}”为文本类型"
            )
        identifier = self._quote_sql_identifier(field.name)
        where = " OR ".join(
            f"{identifier} = {self._quote_sql_string_literal(value)}" for value in requested_values
        )
        rows = self._query_rows(
            self._build_query_sql(
                schema,
                where=where,
                limit=1000,
            )
        )
        requested = set(requested_values)
        canonical_name = field.name.removeprefix("*")
        records = [self._parse_query_record(row, schema) for row in rows]
        return [record for record in records if record.fields.get(canonical_name) in requested]

    def get_records(
        self,
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: Collection[str] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """读取全部记录、指定记录或指定负责人范围的快照。

        参数：记录标识与负责人过滤最多提供一种；显示名仅用于负责人服务端预筛；
        submission_status 和 capture_date 会与负责人条件一起写入服务端查询。
        返回：按 CLI 页顺序合并的记录列表。
        异常：过滤配置冲突、缺少服务端过滤所需子表名称或 CLI 响应无效时抛出异常。
        副作用：只读指定记录或负责人范围；未过滤调用才读取整个子表。
        """
        requested = tuple(dict.fromkeys(record_ids)) if record_ids is not None else None
        owners = tuple(dict.fromkeys(owner_user_ids)) if owner_user_ids is not None else None
        owner_names = dict(owner_display_names) if owner_display_names is not None else None
        scoped_filters = submission_status is not None or capture_date is not None
        if requested is not None and (
            owners is not None or owner_names is not None or scoped_filters
        ):
            raise ValueError("记录标识与负责人范围不能同时过滤")
        if owner_names is not None and (
            owners is None
            or not set(owner_names).issubset(owners)
            or any(not name for name in owner_names.values())
        ):
            raise ValueError("负责人显示名必须与负责人 user_id 一一对应")
        if requested == () or owners == ():
            return []
        if owners is not None:
            source_owners = owners
            owners = tuple(
                self.resolve_member_user_id(user_id, (owner_names or {}).get(user_id))
                for user_id in source_owners
            )
            if len(set(owners)) != len(owners):
                raise SmartTableAdapterConfigurationError("多个回调身份映射到了同一表格负责人")
            if owner_names is not None or scoped_filters:
                # 新回调没有姓名时直接复用刚确认的通讯录姓名，避免依赖后台预登记。
                owner_names = {
                    resolved_id: (owner_names or {}).get(source_id)
                    or self._member_name_cache[resolved_id]
                    for source_id, resolved_id in zip(source_owners, owners, strict=True)
                }
                # 表格可能只显示本名；姓名仅作预筛，最终归属仍由成员标识精确校验。
                for owner_id, name in owner_names.items():
                    combined = re.fullmatch(r"(.+?)[(（]([^()（）]+)[)）]", name.strip())
                    if combined is not None:
                        owner_names[owner_id] = combined.group(1).strip()
        if scoped_filters and (owners is None or owner_names is None):
            raise SmartTableAdapterConfigurationError(
                "提交状态/日期服务端筛选需要负责人 user_id 和显示名"
            )
        if (
            (requested is not None or owners is not None or owner_names is not None)
            and not self._sheet_title
        ):
            # 没有完整子表名称时只能分页扫描全表，提交路径必须失败闭合，不能读取其他负责人的行。
            raise SmartTableAdapterConfigurationError(
                "负责人/记录过滤读取需要配置 WECOM_SMART_TABLE_SHEET_TITLE"
            )
        schema = self.get_schema()
        if self._sheet_title:
            # 查询条件在企微服务端执行，提交只会读取目标记录 ID 或负责人范围。
            return self._query_records(
                schema,
                record_ids=requested,
                owner_user_ids=owners,
                owner_display_names=owner_names,
                submission_status=submission_status,
                capture_date=capture_date,
            )
        return [self._parse_record(item, schema) for item in self._list_pages("records")]

    def resolve_member_user_id(self, user_id: str, display_name: str | None = None) -> str:
        """通过已授权通讯录把回调成员标识转换为表格可接受的 userid。

        参数：user_id 为回调或通讯录成员标识；display_name 仅用于回查已知通讯录标识。
        返回值：通讯录搜索得到的唯一成员 userid。
        异常：通讯录查询失败、结果截断、成员缺失或命中不唯一时抛出协议异常。
        副作用：只读查询通讯录；已知标识无法直接搜索时可按姓名再核验一次；成功结果进程内缓存。
        """
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("智能表格成员标识不能为空")
        source_id = user_id.strip()
        cached = self._member_user_id_cache.get(source_id)
        if cached is not None:
            return cached

        keywords = [source_id]
        if display_name and display_name.strip() != source_id:
            keywords.append(display_name.strip())
        for keyword in keywords:
            # 完整结果才能证明身份唯一；姓名兜底只接受与已知标识完全相同的成员。
            response = self._run_cli((
                self._command, "contact", "users", "search", "--json",
                json.dumps(
                    {"keywords": [keyword], "search_mode": "list"},
                    ensure_ascii=False, separators=(",", ":"),
                ),
            ))
            self._raise_for_error(response)
            users = response.get("users")
            # CLI 未命中时省略 users，仅返回 users_count=0；仍允许按已知姓名核验身份。
            if users is None and response.get("users_count") == 0:
                users = []
            if response.get("hint") or not isinstance(users, list):
                raise WecomCliProtocolError("企微通讯录结果不完整，不能确认智能表格成员身份")
            matches: dict[str, str] = {}
            for user in users:
                if not isinstance(user, Mapping):
                    raise WecomCliProtocolError("企微通讯录成员结构无效")
                member_id, name = user.get("userid"), user.get("name")
                if (
                    not isinstance(member_id, str) or not member_id.strip()
                    or not isinstance(name, str) or not name.strip()
                ):
                    raise WecomCliProtocolError("企微通讯录成员缺少 userid 或姓名")
                matches[member_id.strip()] = name.strip()
            if not matches:
                continue
            if source_id in matches:
                resolved_id = source_id
            elif keyword != source_id:
                raise WecomCliProtocolError("通讯录姓名对应成员与已知负责人身份不一致")
            elif len(matches) == 1:
                resolved_id = next(iter(matches))
            else:
                raise WecomCliProtocolError("企微通讯录未返回唯一智能表格成员")
            # 同时记住输入和已确认标识，避免后续校验再次搜索不可检索的表格标识。
            self._member_user_id_cache[source_id] = resolved_id
            self._member_user_id_cache[resolved_id] = resolved_id
            self._member_name_cache[resolved_id] = matches[resolved_id]
            return resolved_id
        raise WecomCliProtocolError("企微通讯录未返回唯一智能表格成员")

    def create_record(
        self,
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
    ) -> SmartTableRecord:
        """以机器人身份创建一条记录，并转换规范字段名和成员字段值。

        参数：fields 的键为规范字段名；actor 为调用主体。
        返回：CLI 返回的创建记录快照。
        异常：销售主体、机器人缺少负责人或 CLI 写入失败时抛出异常。
        副作用：在目标智能表格新增一条记录。
        """
        if actor is SmartTableActor.SALES:
            raise SmartTablePermissionError("wecom-cli 使用机器人凭据，不能模拟销售直接新增记录")
        if (
            actor is SmartTableActor.ROBOT
            and self._require_owner_field
            and not fields.get("负责人")
        ):
            raise ValueError("机器人新增智能表格记录时必须写入负责人")

        schema = self.get_schema()
        cli_fields = self._to_cli_fields(fields, schema)
        # 只核实本次实际发出的字段；无效日期和不可写位置已由编码阶段跳过。
        fields_to_verify = {
            name: value
            for name, value in fields.items()
            if (field := schema.get_field(name)) is not None and field.name in cli_fields
        }
        response = self._call(
            "records", "add", {"records": [{"values": cli_fields}]}
        )
        record_id = self._parse_written_record_id(response)
        # 新增没有可安全重放的幂等键；只有远端逐字段回读匹配后才缓存并返回成功。
        return self._verify_remote_write(
            record_id, fields_to_verify, schema, operation="create"
        )

    def create_records(
        self, records: Sequence[Mapping[str, object]], *, actor: SmartTableActor
    ) -> list[SmartTableRecord]:
        """一次新增多条记录，并通过一次批量回读核实实际写入结果。

        参数：records 为按请求顺序排列的规范字段映射；actor 为调用主体。
        返回值：按输入顺序排列的远端记录快照。
        异常：权限、CLI 响应异常或远端字段未通过有限核实时抛出适配器异常。
        副作用：最多发起一次批量新增和有限次批量只读核实。
        """
        if not records:
            return []
        if len(records) > 2000:
            raise ValueError("智能表格单次批量新增最多支持 2000 条记录")
        if actor is SmartTableActor.SALES:
            raise SmartTablePermissionError("wecom-cli 使用机器人凭据，不能模拟销售直接新增记录")
        if (
            actor is SmartTableActor.ROBOT
            and self._require_owner_field
            and any(not fields.get("负责人") for fields in records)
        ):
            raise ValueError("机器人新增智能表格记录时必须写入负责人")

        schema = self.get_schema()
        cli_records: list[dict[str, object]] = []
        fields_to_verify: list[dict[str, object]] = []
        for fields in records:
            cli_fields = self._to_cli_fields(fields, schema)
            cli_records.append({"values": cli_fields})
            # 只核实本次实际发出的字段；未映射的管理员字段不会被静默认作成功。
            fields_to_verify.append(
                {
                    name: value
                    for name, value in fields.items()
                    if (field := schema.get_field(name)) is not None and field.name in cli_fields
                }
            )
        response = self._call("records", "add", {"records": cli_records}, idempotent_add=True)
        record_ids = self._parse_written_record_ids(response, len(records))

        mismatched_fields: tuple[str, ...] = ()
        mismatched_record_id: str | None = None
        for delay in _WRITE_VERIFICATION_DELAYS:
            if delay:
                time.sleep(delay)
            try:
                remote_records = self.get_records(record_ids=record_ids)
            except RetryableTaskFailure as error:
                logger.error(
                    "wecom_cli_batch_create_verification_read_failed",
                    extra={"error_code": getattr(error, "error_code", type(error).__name__)},
                )
                continue
            by_id = {record.record_id: record for record in remote_records}
            mismatched_fields = ()
            mismatched_record_id = None
            for record_id, expected_fields in zip(record_ids, fields_to_verify, strict=True):
                remote_record = by_id.get(record_id)
                current_mismatches = (
                    self._mismatched_write_fields(expected_fields, remote_record, schema)
                    if remote_record is not None
                    else tuple(expected_fields)
                )
                if current_mismatches:
                    mismatched_fields = current_mismatches
                    mismatched_record_id = record_id
                    break
            if mismatched_record_id is None:
                for record in remote_records:
                    self._remember_written_record(record.record_id, record.fields)
                return [by_id[record_id] for record_id in record_ids]

        logger.error(
            "wecom_cli_batch_create_verification_failed",
            extra={"missing_or_mismatched_fields": list(mismatched_fields)},
        )
        raise SmartTableWriteVerificationError(
            mismatched_fields, remote_record_id=mismatched_record_id
        )

    def update_record(
        self,
        record_id: str,
        fields: Mapping[str, object],
        *,
        skip_preflight: bool = False,
    ) -> SmartTableRecord:
        """更新指定记录并在返回成功前远端回读核实每个补丁字段。

        参数：record_id 为目标记录标识；fields 只包含本次变更的字段和值；skip_preflight 表示调用方
        已持有刚读取或刚创建的记录快照，可跳过重复预读。
        返回：远端回读且逐字段匹配后的记录快照。
        异常：记录不存在、CLI 写入失败或有限核实窗口后仍有字段不匹配时抛出异常。
        副作用：只修改目标记录的传入字段，并在 ACK 后执行有上限的只读回查。
        """
        if not skip_preflight:
            current_record = self.get_record(record_id)
            if current_record is None:
                raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{record_id}")

        schema = self.get_schema()
        cli_fields = self._to_cli_fields(fields, schema)
        # 仅将实际进入本次 request 的字段纳入远端核实。
        fields_to_verify = {
            name: value
            for name, value in fields.items()
            if (field := schema.get_field(name)) is not None and field.name in cli_fields
        }
        planned_fields = [
            field.name.removeprefix("*")
            for name in fields
            if (field := schema.get_field(name)) is not None
        ]
        logger.info("wecom_cli_write_planned", extra={"planned_fields": planned_fields})
        self._call(
            "records",
            "update",
            {"records": [{"record_id": record_id, "values": cli_fields}]},
        )
        # CLI ACK 只代表请求被接受；有限 uncached 回读以远端实际字段为唯一成功证据。
        return self._verify_remote_write(
            record_id, fields_to_verify, schema, operation="update"
        )

    def update_records(
        self, records: Sequence[tuple[str, Mapping[str, object]]]
    ) -> list[SmartTableRecord]:
        """一次批量更新多条记录，并通过批量回读核实写入结果。

        参数：records 为记录标识与增量字段补丁组成的序列。
        返回值：按输入顺序排列且经远端核实的记录快照。
        异常：记录缺失、CLI 写入失败或有限核实窗口后仍有字段不匹配时抛出异常。
        副作用：先批量确认记录存在，再发起一次批量更新及有限次批量只读核实。
        """
        if not records:
            return []
        if len(records) > 2000:
            raise ValueError("智能表格单次批量更新最多支持 2000 条记录")
        record_ids = [record_id for record_id, _ in records]
        if any(not isinstance(record_id, str) or not record_id for record_id in record_ids) or len(
            set(record_ids)
        ) != len(record_ids):
            raise ValueError("智能表格批量更新需要非空且不重复的记录标识")

        schema = self.get_schema()
        existing_ids = {
            record.record_id for record in self.get_records(record_ids=record_ids)
        }
        missing_ids = [record_id for record_id in record_ids if record_id not in existing_ids]
        if missing_ids:
            raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{missing_ids[0]}")

        cli_records: list[dict[str, object]] = []
        fields_to_verify: list[Mapping[str, object]] = []
        for record_id, fields in records:
            cli_fields = self._to_cli_fields(fields, schema)
            expected_fields = {
                name: value
                for name, value in fields.items()
                if (field := schema.get_field(name)) is not None and field.name in cli_fields
            }
            unmapped_fields = set(fields) - set(expected_fields)
            if unmapped_fields:
                raise SmartTableAdapterConfigurationError(
                    f"智能表格批量更新字段不存在或不可写：{sorted(unmapped_fields)}"
                )
            cli_records.append({"record_id": record_id, "values": cli_fields})
            fields_to_verify.append(expected_fields)
        planned_fields = sorted(
            {
                field.name.removeprefix("*")
                for _, fields in records
                for name in fields
                if (field := schema.get_field(name)) is not None
            }
        )
        logger.info("wecom_cli_batch_write_planned", extra={"planned_fields": planned_fields})
        self._call("records", "update", {"records": cli_records})

        mismatched_fields: tuple[str, ...] = ()
        mismatched_record_id: str | None = None
        for delay in _WRITE_VERIFICATION_DELAYS:
            if delay:
                time.sleep(delay)
            try:
                remote_records = self.get_records(record_ids=record_ids)
            except RetryableTaskFailure as error:
                logger.error(
                    "wecom_cli_batch_update_verification_read_failed",
                    extra={"error_code": getattr(error, "error_code", type(error).__name__)},
                )
                continue
            by_id = {record.record_id: record for record in remote_records}
            mismatched_fields = ()
            mismatched_record_id = None
            for (record_id, _), expected_fields in zip(records, fields_to_verify, strict=True):
                remote_record = by_id.get(record_id)
                current_mismatches = (
                    self._mismatched_write_fields(expected_fields, remote_record, schema)
                    if remote_record is not None
                    else tuple(expected_fields) or ("record_id",)
                )
                if current_mismatches:
                    mismatched_fields = current_mismatches
                    mismatched_record_id = record_id
                    break
            if mismatched_record_id is None:
                for record in remote_records:
                    self._remember_written_record(record.record_id, record.fields)
                return [by_id[record_id] for record_id in record_ids]

        logger.error(
            "wecom_cli_batch_update_verification_failed",
            extra={"missing_or_mismatched_fields": list(mismatched_fields)},
        )
        raise SmartTableWriteVerificationError(
            mismatched_fields, remote_record_id=mismatched_record_id
        )

    def _verify_remote_write(
        self,
        record_id: str,
        fields: Mapping[str, object],
        schema: SmartTableSchema,
        *,
        operation: Literal["create", "update"],
    ) -> SmartTableRecord:
        """在有限窗口内绕过缓存回读并逐项核实一次远端写入。

        参数：record_id 为首次写入响应确认的目标；fields 为本次实际发送字段；schema 为字段结构；
        operation 区分新增与更新的安全诊断事件。
        返回值：全部字段匹配后的远端记录快照。
        异常：明确永久读错误原样传播；有限窗口后仍未核实时抛出可恢复核实异常。
        副作用：执行有限只读回查；仅验证成功后更新进程内近期记录缓存。
        """
        mismatched_fields = tuple(fields)
        for delay in _WRITE_VERIFICATION_DELAYS:
            if delay:
                time.sleep(delay)
            try:
                remote_record = self._get_record_remote_uncached(record_id, schema)
            except RetryableTaskFailure as error:
                # 回读暂态失败只消耗本次核实窗口，不把已获 ACK 的写入误判为永久失败。
                logger.error(
                    f"wecom_cli_{operation}_verification_read_failed",
                    extra={
                        "error_code": getattr(error, "error_code", type(error).__name__),
                        "missing_or_mismatched_fields": list(fields),
                    },
                )
                continue
            except PermanentTaskFailure:
                # 权限、认证、业务拒绝和协议错误必须保留原始安全诊断，不能洗成核实等待。
                raise
            if remote_record is None:
                continue
            mismatched_fields = self._mismatched_write_fields(fields, remote_record, schema)
            if not mismatched_fields:
                logger.info(
                    "wecom_cli_write_verified",
                    extra={"persisted_fields": list(fields)},
                )
                self._remember_written_record(record_id, remote_record.fields)
                return remote_record

        logger.error(
            f"wecom_cli_{operation}_verification_failed",
            extra={"missing_or_mismatched_fields": list(mismatched_fields)},
        )
        raise SmartTableWriteVerificationError(
            mismatched_fields, remote_record_id=record_id
        )

    def _mismatched_write_fields(
        self,
        fields: Mapping[str, object],
        remote_record: SmartTableRecord,
        schema: SmartTableSchema,
    ) -> tuple[str, ...]:
        """逐项比较本次补丁字段与远端记录中的规范领域值。

        参数：fields 为本次 SafePatchPlan 字段补丁；remote_record 为 uncached 远端快照；
        schema 为目标智能表格结构。
        返回值：缺失或不匹配的规范字段名元组，不包含任何字段值。
        异常：字段编码无法解析时传播受控 CLI 协议异常。
        副作用：无。
        """
        mismatched: list[str] = []
        for canonical_name, target in fields.items():
            # 只核实本次计划字段，不把缺少于计划之外的 CRM 字段当作写入失败。
            field = schema.get_field(canonical_name)
            if field is None:
                mismatched.append(canonical_name)
                continue
            expected = self._to_cli_value(canonical_name, field, target)
            if expected is None:
                mismatched.append(canonical_name)
                continue
            # 反向恢复 CLI option/member 编码，使远端读值与领域目标值可直接比较。
            if (
                field.field_type
                in {SmartTableFieldType.SINGLE_SELECT, SmartTableFieldType.MULTI_SELECT}
                or canonical_name == "AI待确认"
            ):
                if isinstance(expected, list):
                    if expected and isinstance(expected[0], Mapping):
                        expected = [item.get("text") for item in expected]
                    # AI待确认字段在远端回读时使用不含管理员必填前缀的规范名称。
                    if canonical_name == "AI待确认":
                        expected = [item.removeprefix("*") for item in expected]
                    if field.field_type is SmartTableFieldType.SINGLE_SELECT:
                        expected = expected[0] if expected else None
            elif field.field_type is SmartTableFieldType.MEMBER:
                expected = (
                    expected[0].get("userId")
                    if isinstance(expected, list) and expected and isinstance(expected[0], Mapping)
                    else None
                )

            actual = remote_record.fields.get(canonical_name)
            if field.field_type is SmartTableFieldType.MULTI_SELECT and actual is None:
                actual = []
            # 企微把清空后的文本单元格回读为 null，与本次写入的空字符串语义相同。
            if (
                field.field_type in {SmartTableFieldType.TEXT, SmartTableFieldType.LONG_TEXT}
                and target == ""
                and actual is None
            ):
                continue
            if actual != expected:
                mismatched.append(canonical_name)
        return tuple(mismatched)

    def _remember_written_record(self, record_id: str, fields: Mapping[str, object]) -> None:
        """暂存刚由本进程确认写入的记录，覆盖企业微信列表的短暂可见性延迟。

        参数：record_id 为写入响应确认的记录标识；fields 为本次请求已明确写入的规范字段。
        返回值：无。
        异常：无。
        副作用：写入仅存活于当前适配器进程的短期快照。
        """
        self._recent_written_records[record_id] = (
            time.monotonic(),
            SmartTableRecord(record_id=record_id, fields=dict(fields)),
        )

    def _query_record_by_id(
        self, record_id: str, schema: SmartTableSchema
    ) -> SmartTableRecord | None:
        """使用 RECORD_ID 服务端过滤读取单条智能表格记录。

        参数：record_id 为目标记录标识；schema 为启动时校验过的字段结构。
        返回值：命中的记录快照；服务端返回空行时为 None。
        异常：查询协议、权限或字段结构异常时抛出适配器异常。
        副作用：启动一次带 WHERE 和 LIMIT 1 的只读 records query，不修改智能表格。
        """
        sql = self._build_query_sql(
            schema,
            where=f"RECORD_ID = {self._quote_sql_string_literal(record_id)}",
            limit=1,
        )
        rows = self._query_rows(sql)
        return self._parse_query_record(rows[0], schema) if rows else None

    def _query_records(
        self,
        schema: SmartTableSchema,
        *,
        record_ids: tuple[str, ...] | None = None,
        owner_user_ids: tuple[str, ...] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """使用服务端 SQL 过滤读取全部记录、目标记录或负责人记录。

        参数：schema 为启动时校验过的字段结构；record_ids 或负责人范围为可选过滤项；
        提交状态和录入日期仅与负责人范围组合查询。
        返回值：按查询结果顺序转换后的记录快照。
        异常：过滤冲突、查询协议或读取上限不确定时抛出适配器异常。
        副作用：发起一项或多项只读 records query，不修改智能表格。
        """
        if record_ids is not None and (
            owner_user_ids is not None
            or owner_display_names is not None
            or submission_status is not None
            or capture_date is not None
        ):
            raise ValueError("记录标识与负责人范围不能同时过滤")
        if owner_display_names is not None:
            if (
                owner_user_ids is None
                or set(owner_user_ids) != set(owner_display_names)
                or any(
                    not name or "%" in name or "_" in name
                    for name in owner_display_names.values()
                )
            ):
                raise ValueError("负责人显示名必须与负责人 user_id 一一对应且不含 LIKE 通配符")
            owner_field = schema.get_field("负责人")
            if owner_field is None:
                raise SmartTableAdapterConfigurationError("负责人范围查询缺少负责人字段")
            # 当前服务端未按 user filter_spec 返回记录，改用姓名预筛再严格核对 user_id。
            owner_identifier = self._quote_sql_identifier(owner_field.name)
            owner_where = " OR ".join(
                f"{owner_identifier} LIKE "
                f"{self._quote_sql_string_literal('%' + name + '%')}"
                for name in owner_display_names.values()
            )
            if capture_date is not None:
                # 日期查询允许业务层核对负责人为空的人工行，其他查询仍严格按负责人隔离。
                owner_where = (
                    f"({owner_where} OR {owner_identifier} IS NULL OR {owner_identifier} = '')"
                )
            conditions = [f"({owner_where})"]
            if submission_status is not None:
                status_field = schema.get_field("提交状态")
                if status_field is None:
                    raise SmartTableAdapterConfigurationError("提交状态服务端筛选缺少提交状态字段")
                status_identifier = self._quote_sql_identifier(status_field.name)
                if submission_status == "未提交":
                    # 人工录入行的状态可能为空；无成功标记的行仍可进入提交候选。
                    conditions.append(
                        f"({status_identifier} = "
                        f"{self._quote_sql_string_literal(submission_status)} "
                        f"OR {status_identifier} IS NULL OR {status_identifier} = '')"
                    )
                else:
                    conditions.append(
                        f"{status_identifier} = "
                        f"{self._quote_sql_string_literal(submission_status)}"
                    )
            if capture_date is not None:
                entry_time = self._quote_sql_identifier("录入时间")
                entry_time_value = f"NULLIF({entry_time}, '')"
                date_value = self._quote_sql_string_literal(capture_date.isoformat())
                conditions.append(
                    f'(DATE_FORMAT({entry_time_value}, "%Y-%m-%d") = {date_value} '
                    f"OR {entry_time_value} IS NULL)"
                )
            where = " AND ".join(conditions)
            sql = self._build_query_sql(
                schema, where=where, limit=1000, include_entry_time=True
            )
            rows = self._query_rows(sql)
            if len(rows) >= 1000:
                raise WecomCliProtocolError("负责人范围查询达到读取上限，结果可能被截断")
            records = [self._parse_query_record(row, schema) for row in rows]
            allowed_owners = set(owner_user_ids)
            # 姓名可能重名或互为子串；服务端预筛后仍只向业务层返回精确 user_id 命中项。
            return [
                record
                for record in records
                if (
                    record.fields.get("负责人") in allowed_owners
                    or (
                        capture_date is not None
                        and record.fields.get("负责人") in (None, "")
                    )
                )
                and (
                    submission_status is None
                    or record.fields.get("提交状态") == submission_status
                    or submission_status == "未提交"
                    and record.fields.get("提交状态") in (None, "")
                )
            ]
        if submission_status is not None or capture_date is not None:
            raise SmartTableAdapterConfigurationError("提交状态/日期服务端筛选需要负责人显示名")
        where: str | None = None
        if record_ids is not None:
            records: list[SmartTableRecord] = []
            # 分块规避单条 SQL 长度及 CLI 的 1000 行上限，避免大批量勾选静默截断。
            for start in range(0, len(record_ids), 500):
                chunk = record_ids[start : start + 500]
                where = " OR ".join(
                    f"RECORD_ID = {self._quote_sql_string_literal(record_id)}"
                    for record_id in chunk
                )
                sql = self._build_query_sql(
                    schema,
                    where=where,
                    limit=len(chunk),
                    include_entry_time=self._include_entry_time,
                )
                records.extend(
                    self._parse_query_record(row, schema) for row in self._query_rows(sql)
                )
            return records

        if owner_user_ids is not None:
            owner_field = schema.get_field("负责人")
            if owner_field is None:
                raise SmartTableAdapterConfigurationError("负责人范围查询缺少负责人字段")
            field_titles = [field.name for field in schema.fields]
            filter_spec: dict[str, object] = {
                "conjunction": "or",
                "conditions": [
                    {
                        "field_title": owner_field.name,
                        "field_type": "user",
                        "operator": "is",
                        "user_value": {"value": [owner_user_id]},
                    }
                    for owner_user_id in owner_user_ids
                ],
            }
            # records list 对成员字段支持按 userid 精确过滤并分页，不读取相似 ID 的其他负责人记录。
            page_limit = max(1, min(1000, 9999 // max(1, len(field_titles))))
            rows = self._list_pages(
                "records",
                filter_spec=filter_spec,
                field_titles=field_titles,
                limit=page_limit,
            )
            records = [self._parse_record(row, schema) for row in rows]
            allowed_owners = set(owner_user_ids)
            if any(record.fields.get("负责人") not in allowed_owners for record in records):
                raise WecomCliProtocolError("负责人精确过滤返回了范围外记录")
            return records

        sql = self._build_query_sql(schema, where=where, limit=1000)
        return [self._parse_query_record(row, schema) for row in self._query_rows(sql)]

    def _build_query_sql(
        self,
        schema: SmartTableSchema,
        *,
        limit: int,
        where: str | None = None,
        include_entry_time: bool = False,
    ) -> str:
        """根据字段结构构造只读 records query SQL。

        参数：schema 为字段结构；limit 为服务端结果上限；where 为已安全编码的筛选子句；
        include_entry_time 表示同时返回只读系统“录入时间”。
        返回值：供 wecom-cli records query 使用的 SELECT 语句。
        异常：未配置子表名称时抛出 SmartTableAdapterConfigurationError。
        副作用：无；不执行查询。
        """
        if not self._sheet_title:
            raise SmartTableAdapterConfigurationError("完整查询缺少 WECOM_SMART_TABLE_SHEET_TITLE")
        columns = ["RECORD_ID"]
        for field in schema.fields:
            quoted_name = self._quote_sql_identifier(field.name)
            if field.field_type is SmartTableFieldType.DATE:
                # records query 默认把日期返回为 Excel 序列号；格式化后与写入值及业务快照统一。
                columns.append(
                    f'DATE_FORMAT({quoted_name}, "%Y-%m-%d %H:%i:%s") AS {quoted_name}'
                )
            elif field.field_type in {SmartTableFieldType.TEXT, SmartTableFieldType.LONG_TEXT}:
                # 直接选取富文本时 SQL 可能只返回首个超链接；幂等替换强制输出全文且保留换行。
                columns.append(f'REPLACE({quoted_name}, "\\n", "\\n") AS {quoted_name}')
            else:
                columns.append(quoted_name)
        if include_entry_time:
            # 当前表格的“录入时间”就是原生创建时间字段。
            schema_names = {field.name.removeprefix("*") for field in schema.fields}
            if "录入时间" not in schema_names:
                quoted_time = self._quote_sql_identifier("录入时间")
                columns.append(
                    f'DATE_FORMAT({quoted_time}, "%Y-%m-%d %H:%i:%s") AS {quoted_time}'
                )
        query = (
            f"SELECT {', '.join(columns)} FROM "
            f"{self._quote_sql_identifier(self._sheet_title)}"
        )
        if where:
            query += f" WHERE {where}"
        return f"{query} LIMIT {limit}"

    def _query_rows(self, sql: str) -> list[Mapping[str, object]]:
        """执行 records query 并解析其 values/rows 外层协议。

        参数：sql 为已经完成安全编码的只读 SELECT 语句。
        返回值：查询返回的结构化行列表。
        异常：CLI 调用失败、业务错误或响应结构异常时抛出受控适配器异常。
        副作用：启动一次只读 wecom-cli records query，不修改智能表格。
        """
        response = self._call_query(sql)
        values = response.get("values")
        if not isinstance(values, list) or len(values) != 1:
            raise WecomCliProtocolError("wecom-cli records query 缺少唯一 values 结果")
        result = values[0]
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError as error:
                raise WecomCliProtocolError("wecom-cli records query 结果不是 JSON") from error
        if not isinstance(result, Mapping):
            raise WecomCliProtocolError("wecom-cli records query 结果结构异常")
        inner_error = result.get("errcode")
        if inner_error not in (None, 0):
            raise WecomCliProtocolError("wecom-cli records query 返回业务错误")
        rows = result.get("rows", [])
        if not isinstance(rows, list):
            raise WecomCliProtocolError("wecom-cli records query 的 rows 不是列表")
        return [self._as_mapping(row, "查询记录") for row in rows]

    @staticmethod
    def _quote_sql_identifier(value: str) -> str:
        """为智能表格查询安全引用字段或子表名称。"""
        return f"`{value.replace('`', '``')}`"

    @staticmethod
    def _quote_sql_string_literal(value: str) -> str:
        """按 records query 的双引号字符串语法安全引用记录标识。

        参数：value 为待放入 SQL WHERE 子句的记录标识。
        返回值：已转义的双引号字符串字面量。
        异常：无。
        副作用：无；不执行 SQL。
        """
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def _call_query(self, sql: str) -> Mapping[str, object]:
        """调用 records query 并按底层错误的 retryable 属性执行有限重试。

        参数：sql 为已经安全构造的只读 SELECT 语句。
        返回值：CLI 成功响应映射。
        异常：永久 WecomCliProcessError 原样抛出；暂态错误耗尽后包装为传输错误。
        副作用：启动只读 wecom-cli 子进程，并可能按配置短暂重试。
        """
        arguments = (
            self._command,
            "smartsheet",
            "records",
            "query",
            "--docid",
            self._doc_id,
            "--sql",
            sql,
        )
        for attempt in range(self._retry_count + 1):
            try:
                response = self._run_cli(arguments)
            except WecomCliProcessError as error:
                if not error.retryable:
                    self._log_final_failure(
                        "records",
                        "query",
                        error,
                        attempt + 1,
                        fallback_error_code="remote_business_error",
                    )
                    raise
                if attempt == self._retry_count:
                    self._log_final_failure(
                        "records",
                        "query",
                        error,
                        attempt + 1,
                        fallback_error_code="transport_error",
                    )
                    raise WecomCliTransportError("wecom-cli 查询进程调用失败") from error
                self._log_retry("records", "query", attempt, error.error_code)
                time.sleep(self._retry_delay_seconds(error.error_code, attempt))
                continue
            except (OSError, subprocess.TimeoutExpired) as error:
                if attempt == self._retry_count:
                    fallback_code = (
                        "timeout"
                        if isinstance(error, subprocess.TimeoutExpired)
                        else "process_error"
                    )
                    self._log_final_failure(
                        "records", "query", error, attempt + 1, fallback_error_code=fallback_code
                    )
                    raise WecomCliTransportError("wecom-cli 查询调用失败") from error
                self._log_retry("records", "query", attempt, type(error).__name__)
                time.sleep(self._retry_delay_seconds(type(error).__name__, attempt))
                continue
            if self._is_transient_network_error(response):
                if attempt == self._retry_count:
                    self._log_final_failure(
                        "records",
                        "query",
                        None,
                        attempt + 1,
                        fallback_error_code="network_error",
                    )
                    raise WecomCliTransportError("wecom-cli 查询网络调用失败")
                self._log_retry("records", "query", attempt, "NetworkError")
                time.sleep(self._retry_delay_seconds("network_error", attempt))
                continue
            try:
                self._raise_for_error(response)
            except WecomCliSmartTableAdapterError as error:
                self._log_final_failure(
                    "records",
                    "query",
                    error,
                    attempt + 1,
                    fallback_error_code="remote_business_error",
                )
                raise
            return response
        raise AssertionError("已覆盖全部 CLI 查询重试分支")

    def _parse_query_record(
        self, row: Mapping[str, object], schema: SmartTableSchema
    ) -> SmartTableRecord:
        """把 records query 的字段名称和值转换为统一领域快照。"""
        record_id = row.get("RECORD_ID")
        if not isinstance(record_id, str) or not record_id:
            raise WecomCliProtocolError("wecom-cli 查询记录缺少 RECORD_ID")
        normalized: dict[str, object] = {}
        member_names: dict[str, str] = {}
        for field in schema.fields:
            raw = row.get(field.name)
            if raw is None and field.name.startswith("*"):
                raw = row.get(field.name.removeprefix("*"))
            canonical_name = field.name.removeprefix("*")
            # 文本投影必须返回完整字符串；链接对象仅能证明局部内容，不能当成整段备注。
            if raw is not None and field.field_type in {
                SmartTableFieldType.TEXT,
                SmartTableFieldType.LONG_TEXT,
                SmartTableFieldType.PHONE_NUMBER,
                SmartTableFieldType.EMAIL,
                SmartTableFieldType.SINGLE_SELECT,
            } and not isinstance(raw, str):
                raise WecomCliProtocolError("SQL 查询文本或单选字段类型不符合 CLI 契约")
            if field.field_type is SmartTableFieldType.MULTI_SELECT and raw is not None:
                if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
                    raise WecomCliProtocolError("SQL 查询多选字段类型不符合 CLI 契约")
            if field.field_type is SmartTableFieldType.MEMBER and raw not in (None, []):
                if not isinstance(raw, list) or any(not isinstance(item, Mapping) for item in raw):
                    raise WecomCliProtocolError("SQL 查询人员字段类型不符合 CLI 契约")
            if field.field_type is SmartTableFieldType.DATE and raw is not None:
                is_number = isinstance(raw, (int, float)) and not isinstance(raw, bool)
                if not is_number and not isinstance(raw, str):
                    raise WecomCliProtocolError("SQL 查询日期字段类型不符合 CLI 契约")
            if field.field_type is SmartTableFieldType.MEMBER and raw in (None, []):
                # 查询接口对未设置成员字段返回 null/空列表；不应让无关字段阻断整行读取。
                normalized[canonical_name] = None
                continue
            if field.field_type is SmartTableFieldType.MULTI_SELECT and raw is None:
                # records query 对未填多选返回 null；领域层统一使用空列表表示未选择。
                raw = []
            if field.field_type is SmartTableFieldType.MEMBER and isinstance(raw, list):
                # query 返回成员对象使用 id；同时保留服务端提供的可读姓名，供提交边界解析员工目录。
                raw = [
                    {
                        "userId": item.get("id") or item.get("userId"),
                        "userName": item.get("name") or item.get("userName"),
                    }
                    for item in raw
                    if isinstance(item, Mapping)
                ]
            normalized[canonical_name] = self._from_cli_value(canonical_name, field, raw)
            member_name = self._member_display_name(field, raw)
            if member_name is not None:
                member_names[canonical_name] = member_name
        # “录入时间”是当前表格的原生创建时间字段，不属于管理员维护的业务 Schema。
        if row.get("录入时间") is not None:
            normalized["录入时间"] = row["录入时间"]
        return SmartTableRecord(record_id=record_id, fields=normalized, member_names=member_names)

    def _list_pages(
        self,
        resource: ReadResource,
        *,
        filter_spec: Mapping[str, object] | None = None,
        field_titles: Sequence[str] | None = None,
        limit: int = 1000,
    ) -> list[Mapping[str, object]]:
        """按 CLI next_cursor 读取 fields 或 records 的全部匹配分页响应。

        参数：resource 只能是 fields 或 records；filter_spec、field_titles、limit 供范围查询使用。
        返回：合并后的原始字段或记录对象列表。
        异常：游标循环、响应列表类型错误或 CLI 调用失败时抛出异常。
        副作用：对每一页调用一次对应的 wecom-cli list 接口。
        """
        # 阅读资源与响应数组键一一对应，类型约束避免未知字符串被错误视作 records。
        result_key = "fields" if resource == "fields" else "records"
        cursor: str | None = None
        seen_cursors: set[str] = set()
        items: list[Mapping[str, object]] = []

        # CLI 仅返回当前页，直到响应不再给出 next_cursor 才能形成完整快照。
        while True:
            payload: dict[str, object] = {"limit": limit}
            if filter_spec is not None:
                payload["filter_spec"] = filter_spec
            if field_titles is not None:
                payload["field_titles"] = list(field_titles)
            if cursor is not None:
                # 续页必须回传服务端游标，不能使用本地偏移推算记录位置。
                payload["cursor"] = cursor
            # 每一页都经统一错误转换和网络重试，避免分页路径绕过容错策略。
            response = self._call(resource, "list", payload)
            raw_items = response.get(result_key, [])
            if not isinstance(raw_items, list):
                raise WecomCliProtocolError(
                    f"wecom-cli {resource} list 返回的 {result_key} 不是列表"
                )
            # 外部 JSON 数组逐项收窄为对象，拒绝异常条目而不是静默丢失字段或记录。
            items.extend(self._as_mapping(item, result_key) for item in raw_items)

            next_cursor = response.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                # 没有后续游标即表明当前快照读取完整。
                return items
            if next_cursor in seen_cursors:
                raise WecomCliProtocolError(f"wecom-cli {resource} list 返回了循环分页游标")
            # 记录已消费游标，防止外部服务异常导致无限分页循环。
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def _call(
        self,
        resource: ReadResource,
        action: WriteAction,
        payload: Mapping[str, object],
        *,
        idempotent_add: bool = False,
    ) -> Mapping[str, object]:
        """调用一个 CLI JSON 接口，并仅为临时网络错误执行有限重试。

        参数：resource、action 构成 smartsheet 子命令；payload 为 CLI JSON 请求体；
        idempotent_add 表示调用方会在重试前用稳定键核对未知新增结果。
        返回：已验证成功状态的 JSON 响应对象。
        异常：超时、网络失败、非零业务错误或非 JSON 响应时抛出异常。
        副作用：启动 wecom-cli 子进程；限流和网络错误按递增间隔有限重试。
        """
        # 文档和子表标识只在进程参数中流转，日志和异常均不输出其原值。
        request = {"docid": self._doc_id, "sheet_id": self._sheet_id, **payload}
        if action == "update":
            # 更新契约显式固定字段标题键，避免 CLI 默认 key_type 随版本或环境变化。
            request["key_type"] = "CELL_VALUE_KEY_TYPE_FIELD_TITLE"
        arguments = (
            self._command,
            "smartsheet",
            resource,
            action,
            "--json",
            json.dumps(request, ensure_ascii=False, separators=(",", ":")),
        )

        # 仅网络层和进程层暂态失败重试；业务错误必须立即返回给调用方处理。
        for attempt in range(self._retry_count + 1):
            try:
                response = self._run_cli(arguments)
            except WecomCliProcessError as error:
                # records list/update 是幂等操作，进程异常可以安全重放；records add
                # 可能已经在服务端成功，不能因客户端退出异常再次创建重复记录。
                safe_rate_limit_replay = (
                    action == "add"
                    and error.error_code == "rate_limited"
                    and error.external_error_code == 850005
                )
                if action == "add" and not safe_rate_limit_replay:
                    self._log_final_failure(
                        resource,
                        action,
                        error,
                        attempt + 1,
                        fallback_error_code="remote_business_error",
                    )
                    if error.retryable:
                        if idempotent_add:
                            raise WecomCliTransportError(
                                "wecom-cli 批量新增结果待稳定键核对"
                            ) from error
                        raise WecomCliProtocolError(
                            "wecom-cli 新增结果无法确认，禁止自动重放"
                        ) from None
                    raise
                if not error.retryable:
                    self._log_final_failure(
                        resource,
                        action,
                        error,
                        attempt + 1,
                        fallback_error_code="remote_business_error",
                    )
                    raise
                if attempt == self._retry_count:
                    self._log_final_failure(
                        resource,
                        action,
                        error,
                        attempt + 1,
                        fallback_error_code="transport_error",
                    )
                    raise WecomCliTransportError(
                        f"wecom-cli 进程调用失败：{error.error_code}"
                    ) from error
                self._log_retry(resource, action, attempt, error.error_code)
                time.sleep(self._retry_delay_seconds(error.error_code, attempt))
                continue
            except (OSError, subprocess.TimeoutExpired) as error:
                if action == "add":
                    # 新增请求超时或进程启动结果不明时，禁止自动重放以免产生重复行。
                    fallback_code = (
                        "timeout"
                        if isinstance(error, subprocess.TimeoutExpired)
                        else "process_error"
                    )
                    self._log_final_failure(
                        resource,
                        action,
                        error,
                        attempt + 1,
                        fallback_error_code=fallback_code,
                    )
                    if idempotent_add:
                        raise WecomCliTransportError(
                            "wecom-cli 批量新增结果待稳定键核对"
                        ) from error
                    raise WecomCliProtocolError(
                        "wecom-cli 新增结果无法确认，禁止自动重放"
                    ) from None
                if attempt == self._retry_count:
                    # 最后一次仍失败时隐藏底层请求和响应，避免异常泄露表格数据。
                    fallback_code = (
                        "timeout"
                        if isinstance(error, subprocess.TimeoutExpired)
                        else "process_error"
                    )
                    self._log_final_failure(
                        resource,
                        action,
                        error,
                        attempt + 1,
                        fallback_error_code=fallback_code,
                    )
                    raise WecomCliTransportError("wecom-cli 调用失败") from error
                self._log_retry(resource, action, attempt, type(error).__name__)
                time.sleep(self._retry_delay_seconds(type(error).__name__, attempt))
                continue

            if self._is_transient_network_error(response):
                # CLI 明确标记的 NetworkError 才允许重放同一个请求。
                if action == "add":
                    # 服务端网络错误不能证明新增未执行；该不确定结果转入人工检查点。
                    self._log_final_failure(
                        resource,
                        action,
                        None,
                        attempt + 1,
                        fallback_error_code="network_error",
                    )
                    if idempotent_add:
                        raise WecomCliTransportError("wecom-cli 批量新增结果待稳定键核对")
                    raise WecomCliProtocolError("wecom-cli 新增结果无法确认，禁止自动重放")
                if attempt == self._retry_count:
                    self._log_final_failure(
                        resource,
                        action,
                        None,
                        attempt + 1,
                        fallback_error_code="network_error",
                    )
                    raise WecomCliTransportError("wecom-cli 网络调用失败")
                self._log_retry(resource, action, attempt, "NetworkError")
                time.sleep(self._retry_delay_seconds("network_error", attempt))
                continue
            # 非暂态响应先做统一错误码校验，再交给具体解析器处理结构。
            try:
                self._raise_for_error(response)
            except WecomCliSmartTableAdapterError as error:
                self._log_final_failure(
                    resource,
                    action,
                    error,
                    attempt + 1,
                    fallback_error_code="remote_business_error",
                )
                raise
            return response

        raise AssertionError("已覆盖全部 CLI 重试分支")

    def _run_cli(self, arguments: Sequence[str]) -> Mapping[str, object]:
        """通过 Redis 共享锁串行并间隔执行生产 CLI 请求。

        参数：arguments 为不经 shell 拼接的 CLI 参数序列。
        返回值：CLI 返回的 JSON 映射。
        异常：Redis 限流器不可用或排队超时时抛出可重试的适配器传输异常；CLI 异常原样传播。
        副作用：所有 app/worker/scheduler 共享至少 250 毫秒请求间隔；注入 runner 不等待。
        """
        if not self._pace_cli_calls:
            return self._runner(arguments)
        client = self._rate_limit_redis
        if client is None:
            raise WecomCliTransportError("wecom-cli 共享限流器未初始化")
        lock = None
        lock_acquired = False
        try:
            # 同一部署的 app、worker 和 scheduler 用同一 Redis 键协调 CLI 调用。
            lock = client.lock(
                "crm-tool:wecom-cli:smart-table:request-lock",
                timeout=max(30, self._timeout_seconds + 10),
                blocking_timeout=max(30, self._timeout_seconds + 10),
            )
            if not lock.acquire():
                raise WecomCliTransportError("wecom-cli 共享限流器排队超时")
            lock_acquired = True
            now = self._redis_timestamp(client)
            previous = client.get("crm-tool:wecom-cli:smart-table:last-request-at")
            if previous is not None:
                delay = _CLI_MIN_INTERVAL_SECONDS - (now - float(previous))
                if delay > 0:
                    time.sleep(delay)
            # 在锁内记录 Redis 服务端时间，容器时钟偏差不会破坏全局请求间隔。
            client.set(
                "crm-tool:wecom-cli:smart-table:last-request-at",
                str(self._redis_timestamp(client)),
                ex=60,
            )
        except RedisError as error:
            raise WecomCliTransportError("wecom-cli 共享限流器不可用") from error
        finally:
            if lock is not None and lock_acquired:
                try:
                    lock.release()
                except RedisError:
                    # CLI 可能已成功；不要把释放锁故障改写成结果未知并诱发重复远端操作。
                    logger.error("wecom_cli_shared_rate_limiter_release_failed")
        # 锁只协调请求起始间隔；等待远端 CLI 完成时释放锁，避免把并发压成串行。
        return self._runner(arguments)

    @staticmethod
    def _redis_timestamp(client: Any) -> float:
        """读取 Redis 服务端时间，供跨容器节流计算使用。

        参数：client 为已连接的共享 Redis 客户端。
        返回值：以秒为单位的服务端 Unix 时间戳。
        异常：Redis 连接或响应失败时传播 RedisError。
        副作用：执行一次只读 TIME 命令。
        """
        timestamp = client.time()
        seconds = int(timestamp[0])
        microseconds = int(timestamp[1])
        return float(seconds) + microseconds / 1_000_000

    @staticmethod
    def _retry_delay_seconds(error_code: str, attempt: int) -> float:
        """为限流和网络暂态故障计算有上限的指数退避。

        参数：error_code 为安全内部错误分类；attempt 为从零开始的重试序号。
        返回值：限流、网络和超时错误使用 1、2、4 秒并封顶 8 秒，其它暂态保持短退避。
        异常：无。
        副作用：无，不等待也不访问外部服务。
        """
        if error_code in {
            "rate_limited",
            "network_error",
            "NetworkError",
            "ConnectionError",
            "TimeoutExpired",
        }:
            return float(min(8, 2**attempt))
        return 0.2 * (attempt + 1)

    def _run_subprocess(self, arguments: Sequence[str]) -> Mapping[str, object]:
        """运行 wecom-cli 并把 JSON 标准输出转换为映射。

        参数：arguments 为不经 shell 拼接的 CLI 参数序列。
        返回：CLI JSON 响应。
        异常：超时、非零退出或输出不是 JSON 对象时抛出 WecomCliSmartTableAdapterError。
        副作用：启动外部 wecom-cli 进程且不会记录请求体，避免泄露标识或数据。
        """
        # Windows 的 npm 默认暴露 .cmd shim；shutil.which 会解析它，而 Linux 仍保留原命令。
        executable = shutil.which(arguments[0]) or arguments[0]
        # 通过参数序列而非 shell 拼接启动进程，避免字段值被解释为 shell 指令。
        completed = subprocess.run(
            (executable, *arguments[1:]),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=self._timeout_seconds,
        )
        if completed.returncode != 0:
            # CLI 1.3.x 把结构化错误放在 stdout；只有空 stdout 或非法 JSON 才检查 stderr。
            parsed_error = self._structured_process_error(completed.stdout)
            if parsed_error is None:
                error_code = self._process_error_code(completed.stderr)
                external_error_code = None
                external_error_type = None
                http_status = None
                rejected_field_candidates: tuple[str, ...] = ()
            else:
                (
                    error_code,
                    external_error_code,
                    external_error_type,
                    http_status,
                    rejected_field_candidates,
                ) = parsed_error
            logger.error(
                "wecom_cli_process_failed error_code=%s external_error_code=%s "
                "external_error_type=%s http_status=%s",
                error_code,
                external_error_code,
                external_error_type,
                http_status,
                extra={
                    "returncode": completed.returncode,
                    "error_code": error_code,
                    "external_error_code": external_error_code,
                    "external_error_type": external_error_type,
                    "http_status": http_status,
                    "remote_rejected_field_candidates": list(rejected_field_candidates),
                },
            )
            raise WecomCliProcessError(
                f"wecom-cli 退出失败，退出码：{completed.returncode}",
                error_code=error_code,
                external_error_code=external_error_code,
                external_error_type=external_error_type,
                http_status=http_status,
                rejected_field_candidates=rejected_field_candidates,
            )
        try:
            parsed: Any = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            # CLI 成功退出但协议异常时，拒绝将非 JSON 文本当作业务数据继续处理。
            raise WecomCliProtocolError("wecom-cli 未返回 JSON 对象") from error
        return self._as_mapping(parsed, "CLI 响应")

    def _structured_process_error(
        self, stdout: str
    ) -> tuple[str, int | None, str | None, int | None, tuple[str, ...]] | None:
        """从非零退出 stdout 提取受控错误码、错误类型和 HTTP 状态。

        参数：stdout 为 CLI 输出，仅在内存中解析。
        返回值：内部分类及安全结构化诊断；不是 JSON 对象时返回 None。
        异常：无；非法字段和非白名单文本会被忽略。
        副作用：仅对 640027 在内存中按已缓存 schema 精确筛选字段标题，返回规范字段名；
        不记录或返回其他远端消息、响应体或客户字段。
        """
        try:
            parsed: object = json.loads(stdout)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(parsed, Mapping):
            return None

        # 先提取结构化白名单字段；只有 640027 后续会在内存中精确筛选 schema 标题。
        nested_error = parsed.get("error")
        error = nested_error if isinstance(nested_error, Mapping) else {}
        external_error_code = WecomCliSmartTableAdapter._safe_integer(parsed.get("errcode"))
        if external_error_code is None:
            external_error_code = WecomCliSmartTableAdapter._safe_integer(error.get("code"))

        raw_type = error.get("type")
        external_error_type = (
            raw_type
            if isinstance(raw_type, str) and raw_type in _SAFE_PROCESS_ERROR_TYPES
            else None
        )
        http_status = None
        for source in (error, parsed):
            for key in ("http_status", "httpStatus", "status_code", "statusCode", "status"):
                candidate = WecomCliSmartTableAdapter._safe_integer(source.get(key))
                if candidate is not None and 100 <= candidate <= 599:
                    http_status = candidate
                    break
            if http_status is not None:
                break

        error_code = WecomCliSmartTableAdapter._structured_error_category(
            external_error_code, external_error_type, http_status
        )
        # 仅对远端参数错误在内存中匹配 schema 标题，绝不保留或记录 errmsg 原文。
        rejected_field_candidates: list[str] = []
        if external_error_code == 640027 and self._schema is not None:
            raw_message = parsed.get("errmsg")
            if not isinstance(raw_message, str):
                raw_message = error.get("message", error.get("errmsg"))
            if isinstance(raw_message, str):
                for field in self._schema.fields:
                    title = field.name.removeprefix("*")
                    exact_title = (
                        re.search(rf"(?<!\w){re.escape(title)}(?!\w)", raw_message)
                        if title
                        else None
                    )
                    if exact_title:
                        if title not in rejected_field_candidates:
                            rejected_field_candidates.append(title)
        return (
            error_code,
            external_error_code,
            external_error_type,
            http_status,
            tuple(rejected_field_candidates),
        )

    @staticmethod
    def _safe_integer(value: object) -> int | None:
        """只接纳非布尔整数或纯数字文本，拒绝任意外部描述。"""
        if type(value) is int:
            return value
        if isinstance(value, str) and value.isdecimal():
            try:
                return int(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _structured_error_category(
        error_code: int | None,
        error_type: str | None,
        http_status: int | None,
    ) -> str:
        """把已筛选的 CLI 错误码映射为有限内部类别。"""
        code_categories = {
            850005: "rate_limited",
            851003: "permission_denied",
            853004: "authentication",
            893003: "local_io_error",
            893101: "network_error",
            893102: "http_error",
            893103: "protocol_parse",
            893106: "transport_config",
            893201: "authentication",
            893203: "crypto_error",
            893999: "other",
        }
        if error_code in code_categories:
            return code_categories[error_code]
        if error_code is not None and error_code > 0:
            # 850003 当前没有经验证的外部语义；与其他未知远端码一样保留通用分类且不重试。
            # 893xxx 是 CLI 自身分类，其余正数按远端业务码 fail closed。
            return "other" if 893000 <= error_code <= 893999 else "remote_business_error"
        if error_type == "NetworkError":
            return "network_error"
        if error_type == "TimeoutError":
            return "timeout"
        if error_type == "HttpError" or http_status is not None:
            return "http_error"
        if error_type == "ParseError":
            return "protocol_parse"
        if error_type in {"AuthError", "AuthenticationError"}:
            return "authentication"
        if error_type == "TransportConfigError":
            return "transport_config"
        if error_type == "CryptoError":
            return "crypto_error"
        if error_type == "ApiError":
            return "remote_business_error"
        return "process_exit"

    @staticmethod
    def _process_error_code(stderr: str) -> str:
        """将 CLI stderr 转换为不含原文的稳定错误分类。

        参数：stderr 为外部 wecom-cli 的标准错误输出。
        返回值：permission_denied、record_not_found、invalid_request、timeout、network_error
        或 process_exit 之一。
        异常：无；无法识别时保守返回 process_exit。
        副作用：无，不返回或记录 stderr 原文。
        """
        normalized = stderr.casefold()
        if any(
            token in normalized
            for token in (
                "permission",
                "forbidden",
                "unauthorized",
                "no authority",
                "851003",
                "无权限",
                "权限",
            )
        ):
            return "permission_denied"
        if "not found" in normalized or "不存在" in normalized:
            return "record_not_found"
        if any(token in normalized for token in ("invalid", "validation", "参数", "字段校验")):
            return "invalid_request"
        if "timeout" in normalized or "timed out" in normalized or "超时" in normalized:
            return "timeout"
        if any(token in normalized for token in ("network", "connect", "connection", "连接")):
            return "network_error"
        return "process_exit"

    @staticmethod
    def _is_transient_network_error(response: Mapping[str, object]) -> bool:
        """判断响应是否为可安全重试的 CLI 网络错误。

        参数：response 为 CLI JSON 响应。
        返回：仅在 error.type 为 NetworkError 时返回 true。
        副作用：无。
        """
        error = response.get("error")
        # 仅 CLI 明确声明的 NetworkError 是可重试类别，其他 error 可能代表参数或权限问题。
        return isinstance(error, Mapping) and error.get("type") == "NetworkError"

    @staticmethod
    def _raise_for_error(response: Mapping[str, object]) -> None:
        """把 CLI 的错误响应转换为不含响应明文的适配器异常。

        参数：response 为 CLI JSON 响应。
        异常：errcode 非零或 error 对象存在时抛出 WecomCliSmartTableAdapterError。
        副作用：无，避免异常消息携带客户字段或内部标识。
        """
        if "error" in response:
            # 外部 error 对象可能带有敏感上下文，因此只转换为稳定错误类型。
            raise WecomCliProtocolError("wecom-cli 返回外部服务错误")
        errcode = response.get("errcode")
        if errcode not in (None, 0):
            # errcode 非零属于业务失败，不应走网络重试或继续解析写入结果。
            raise WecomCliProtocolError("wecom-cli 返回业务错误")

    @staticmethod
    def _as_mapping(value: object, name: str) -> Mapping[str, object]:
        """校验一个外部 JSON 值是字符串键对象。

        参数：value 为待校验的 JSON 值；name 用于错误定位。
        返回：类型收窄后的映射。
        异常：值不是对象或含非字符串键时抛出 WecomCliSmartTableAdapterError。
        副作用：无。
        """
        if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
            # 字段和记录键必须能安全映射到冻结契约的字符串名称。
            raise WecomCliProtocolError(f"wecom-cli {name} 返回的对象结构无效")
        return value

    def _parse_field(self, item: Mapping[str, object]) -> SmartTableField:
        """把真实字段定义转换为冻结领域字段模型。

        参数：item 为 fields list 返回的单个字段对象。
        返回：包含真实字段与选项标识的 SmartTableField。
        异常：字段必要属性缺失或类型不受契约支持时抛出异常。
        副作用：无。
        """
        field_id = item.get("field_id")
        name = item.get("field_title")
        raw_type = item.get("field_type")
        if (
            not isinstance(field_id, str)
            or not isinstance(name, str)
            or not isinstance(raw_type, str)
        ):
            raise WecomCliProtocolError("wecom-cli 字段定义缺少标识、名称或类型")
        # 只将冻结契约中声明的 CLI 类型映射为领域类型，未知类型必须阻断 readiness。
        field_type = _FIELD_TYPES.get(raw_type)
        if field_type is None:
            raise WecomCliProtocolError(f"冻结契约不支持智能表格字段类型：{raw_type}")

        property_name = "property_select" if raw_type == "select" else f"property_{raw_type}"
        # 枚举选项只存在于选择字段属性中，其他字段统一保留空选项列表。
        property_value = item.get(property_name, {})
        options = self._parse_options(property_value)
        return SmartTableField(field_id=field_id, name=name, field_type=field_type, options=options)

    def _parse_options(self, property_value: object) -> tuple[SmartTableOption, ...]:
        """从单选或多选属性中提取服务端选项标识。

        参数：property_value 为 CLI 返回的字段属性对象。
        返回：按服务端顺序排列的选项元组；非选择字段返回空元组。
        异常：选项结构不合法时抛出异常。
        副作用：无。
        """
        if not isinstance(property_value, Mapping):
            raise WecomCliProtocolError("wecom-cli 字段属性不是对象")
        raw_options = property_value.get("options", [])
        if not isinstance(raw_options, list):
            raise WecomCliProtocolError("wecom-cli 字段选项不是列表")
        options: list[SmartTableOption] = []
        for raw_option in raw_options:
            # 服务端 option ID 是后续真实写入必需的值，不能用展示文本替代或自行生成。
            option = self._as_mapping(raw_option, "字段选项")
            option_id = option.get("id")
            name = option.get("text")
            if not isinstance(option_id, str) or not isinstance(name, str):
                raise WecomCliProtocolError("wecom-cli 字段选项缺少标识或名称")
            options.append(SmartTableOption(option_id=option_id, name=name))
        return tuple(options)

    def _to_cli_fields(
        self, fields: Mapping[str, object], schema: SmartTableSchema
    ) -> dict[str, object]:
        """把业务字段转换为标题键及 CLI 1.3.x 要求的原生 JSON 值。

        参数：fields 为业务层字段补丁；schema 为当前真实字段快照。
        返回：键为真实字段标题、值为字符串、列表或对象等原生 JSON 类型的映射。
        异常：字段未配置、成员身份或 AI待确认选项不合法时抛出异常。
        副作用：无。
        """
        converted: dict[str, object] = {}
        for canonical_name, value in writable_smart_table_fields(fields).items():
            # 规范名称冲突不得按顺序选择列，防止写错管理员重建的同名字段。
            if sum(
                field.name.removeprefix("*") == canonical_name.removeprefix("*")
                for field in schema.fields
            ) > 1:
                raise ValueError(f"字段同名冲突：{canonical_name}")
            field = schema.get_field(canonical_name)
            if field is None:
                raise WecomCliProtocolError(f"智能表格未配置字段：{canonical_name}")
            if canonical_name in BUSINESS_ENUM_FIELDS:
                # 六个业务列的类型固定，管理员改成文本不能绕过选择值校验。
                expected = next(
                    item.field_type for item in REQUIRED_SMART_TABLE_FIELDS
                    if item.name == canonical_name
                )
                if field.field_type is not expected:
                    raise ValueError(f"字段类型不匹配：{canonical_name}")
            # 地理位置必须包含企业微信地图对象；普通地址字符串不能伪造地图标识。
            # 无法构造地图对象时跳过该字段，留给人工补充。
            if field.field_type is SmartTableFieldType.LOCATION and not self._is_writable_location(
                value
            ):
                logger.warning("wecom_cli_location_value_skipped")
                continue
            # 字段名称唯一由 schema 解析，业务层绝不拼接管理员维护的必填前缀。
            cli_value = self._to_cli_value(canonical_name, field, value)
            if cli_value is not None:
                converted[field.name] = cli_value
        return converted

    @staticmethod
    def _is_writable_location(value: object) -> bool:
        """判断值是否符合智能表格地理位置列的最小写入契约。

        参数：value 为业务层准备写入的地理位置值。
        返回：值为包含地图 UID 和来源类型的非空对象列表时返回 True，否则返回 False。
        异常：不抛出异常；无法确认合法性时返回 False，避免阻断整条线索写入。
        副作用：无。
        """
        if not isinstance(value, list) or not value:
            return False
        # 只有企业微信地图对象才能被 LOCATION 列接受，普通地址文本不具备可写入的地图 UID。
        return all(
            isinstance(item, Mapping)
            and isinstance(item.get("id"), str)
            and bool(item["id"])
            and item.get("source_type") == 1
            for item in value
        )

    def _to_cli_value(
        self, canonical_name: str, field: SmartTableField, value: object
    ) -> object | None:
        """按字段类型构造 CLI 1.3.4 接受的原生 JSON 单元格值。

        参数：canonical_name 为业务规范字段名；field 为真实字段定义；value 为业务层值。
        返回：CLI `values` map 所需的原生值；不合法日期返回 None 并跳过该字段。
        异常：成员身份或 AI待确认选项格式不合法时抛出异常。
        副作用：无。
        """
        cli_value = value
        if field.field_type is SmartTableFieldType.DATE:
            if not isinstance(value, str):
                logger.warning("wecom_cli_date_value_skipped")
                return None
            try:
                # wecom-cli 1.3.4 的 date_time 写入契约要求东八区年月日时分秒。
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                # 不猜自然语言日期；单个可选日期无效时不应阻断同批可靠字段。
                logger.warning("wecom_cli_date_value_skipped")
                return None
            shanghai = ZoneInfo("Asia/Shanghai")
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=shanghai)
            cli_value = parsed.astimezone(shanghai).strftime("%Y-%m-%d %H:%M:%S")
        elif field.field_type is SmartTableFieldType.MEMBER:
            if not isinstance(value, str) or not value:
                raise ValueError(f"MEMBER 字段必须传入非空 sales_user_id：{canonical_name}")
            # 回调身份可能不是文档成员 userid，先用通讯录结果统一两种身份空间。
            cli_value = [{"userId": self.resolve_member_user_id(value)}]
        elif field.field_type in {
            SmartTableFieldType.TEXT,
            SmartTableFieldType.LONG_TEXT,
            SmartTableFieldType.EMAIL,
        }:
            if not isinstance(value, str):
                raise ValueError(f"文本类字段必须传入字符串：{canonical_name}")
        elif field.field_type is SmartTableFieldType.PHONE_NUMBER:
            if not isinstance(value, str):
                raise ValueError(f"手机号字段必须传入字符串：{canonical_name}")
            # 企微电话列接受标准字符串；去除常见空格、短横线和括号，保留号码及国际区号加号。
            cli_value = re.sub(r"[\s()\-]+", "", value.strip())
        elif canonical_name == "AI待确认" or field.field_type in {
            SmartTableFieldType.SINGLE_SELECT,
            SmartTableFieldType.MULTI_SELECT,
        }:
            if canonical_name == "AI待确认" and not isinstance(value, list):
                raise ValueError("AI待确认必须传入规范字段名列表")
            # CRM 线索表的单选/多选写入都必须使用管理员已配置的 option ID。
            raw_values = value if isinstance(value, list) else [value]
            values: list[str] = []
            for item in raw_values:
                if not isinstance(item, str) or not item:
                    raise ValueError(f"选择字段必须传入非空文本列表：{canonical_name}")
                values.append(item)
            if canonical_name == "AI待确认" and any(
                item not in CRM_BUSINESS_FIELD_NAMES for item in values
            ):
                raise ValueError("AI待确认只能包含固定 CRM 字段名称")
            if field.field_type is SmartTableFieldType.SINGLE_SELECT and len(values) != 1:
                raise ValueError(f"单选字段必须恰好传入一个合法选项：{canonical_name}")
            if not field.options:
                # 真实写入不能以测试兼容为由绕过合法选项校验。
                raise ValueError(f"字段枚举选项为空：{canonical_name}")
            else:
                options = {option.name.removeprefix("*"): option for option in field.options}
                if len(options) != len(field.options):
                    raise ValueError(f"字段枚举同名冲突：{canonical_name}")
                if any(
                    not name.strip() or not option.option_id for name, option in options.items()
                ):
                    raise ValueError(f"字段枚举选项为空：{canonical_name}")
                try:
                    cli_value = [
                        {"id": options[item].option_id, "text": options[item].name}
                        for item in values
                    ]
                except KeyError as error:
                    raise ValueError(
                        f"选择字段缺少选项：{canonical_name}={error.args[0]}"
                    ) from error
        return cli_value

    def _parse_record(
        self, item: Mapping[str, object], schema: SmartTableSchema
    ) -> SmartTableRecord:
        """把 CLI 行记录转换为使用规范字段和值的领域快照。

        参数：item 为 records list 或写入接口返回的单条记录。
        返回：记录标识和规范字段名到业务层值的映射。
        异常：记录标识或 values 对象缺失时抛出异常。
        副作用：无。
        """
        record_id = item.get("record_id")
        fields = item.get("values")
        if not isinstance(record_id, str) or not isinstance(fields, Mapping):
            raise WecomCliProtocolError("wecom-cli 记录缺少 record_id 或 values")
        normalized: dict[str, object] = {}
        member_names: dict[str, str] = {}
        for display_name, value in self._as_mapping(fields, "记录 values").items():
            # 真实记录可能包含管理员后续新增字段；未知字段保留原名以避免读数据丢失。
            field = schema.get_field(display_name)
            canonical_name = field.name.removeprefix("*") if field is not None else display_name
            normalized[canonical_name] = self._from_cli_value(canonical_name, field, value)
            member_name = self._member_display_name(field, value)
            if member_name is not None:
                member_names[canonical_name] = member_name
        return SmartTableRecord(record_id=record_id, fields=normalized, member_names=member_names)

    @staticmethod
    def _member_display_name(field: SmartTableField | None, value: object) -> str | None:
        """提取企业微信成员单元格中的可读姓名，不把姓名替代 userId。

        参数：field 为真实字段定义；value 为成员字段原始值。
        返回值：唯一非空 userName，缺失或结构不适配时返回 None。
        异常：无；成员 userId 的严格校验仍由 ``_from_cli_value`` 执行。
        副作用：无。
        """
        if field is None or field.field_type is not SmartTableFieldType.MEMBER:
            return None
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], Mapping):
            return None
        name = value[0].get("userName", value[0].get("name"))
        if not isinstance(name, str):
            return None
        normalized = name.strip()
        return normalized or None

    @staticmethod
    def _from_cli_value(
        canonical_name: str, field: SmartTableField | None, value: object
    ) -> object:
        """将成员、文本和选择字段的真实返回值恢复为业务层规范值。

        参数：canonical_name 为规范字段名；field 为可选真实字段定义；value 为 CLI 返回值。
        返回：业务层可直接比较和持久化的值。
        异常：成员、文本或选择字段返回结构不符合 CLI 契约时抛出异常。
        副作用：无。
        """
        if field is not None and field.field_type is SmartTableFieldType.MEMBER:
            # 创建人和负责人属于权限关键字段，业务层只接受唯一且可审计的销售身份。
            member = value[0] if isinstance(value, list) and len(value) == 1 else None
            member_id = (
                member.get("userId", member.get("id"))
                if isinstance(member, Mapping)
                else None
            )
            if (
                not isinstance(member_id, str)
                or not member_id
            ):
                raise WecomCliProtocolError("MEMBER 字段返回值不符合单成员 CLI 契约")
            return member_id
        if field is not None and field.field_type is SmartTableFieldType.MULTI_SELECT:
            # wecom-cli 对未填多选有时返回空字符串；它与 null/空数组同义，不能误报协议损坏。
            if value is None or value == "":
                return []
            if not isinstance(value, list):
                raise WecomCliProtocolError("多选字段返回值不是选项列表")
            values = [WecomCliSmartTableAdapter._cell_text(item) for item in value]
            # 读回选项文本同样去除管理员必填前缀，使 T09 永远与规范字段名比较。
            return (
                [item.removeprefix("*") for item in values]
                if canonical_name == "AI待确认"
                else values
            )
        if field is not None and field.field_type in {
            SmartTableFieldType.TEXT,
            SmartTableFieldType.LONG_TEXT,
            SmartTableFieldType.PHONE_NUMBER,
            SmartTableFieldType.EMAIL,
            SmartTableFieldType.SINGLE_SELECT,
        }:
            # CLI 对未填文本或单选字段可能返回 null 或空 CellValue 数组；两者都表示领域空值。
            if value is None:
                return None
            if isinstance(value, list):
                if not value:
                    return None
                if field.field_type in {
                    SmartTableFieldType.TEXT,
                    SmartTableFieldType.LONG_TEXT,
                }:
                    # 文本字段可能按富文本片段返回多个单元格，按服务端顺序拼接还原原文。
                    return "".join(WecomCliSmartTableAdapter._cell_text(item) for item in value)
                if len(value) != 1:
                    # 多个候选无法无损收敛为单一领域值，禁止拼接、任选或猜测。
                    raise WecomCliProtocolError("文本或单选字段返回值不是唯一单元格")
                value = value[0]
            return WecomCliSmartTableAdapter._cell_text(value)
        return value

    @staticmethod
    def _cell_text(value: object) -> str:
        """提取 CLI CellValue 中唯一的文本，隔离原始 `{text: ...}` 结构。

        参数：value 为 CLI 返回的单元格值或已规范化文本。
        返回值：领域层可直接比较的文本值。
        异常：CellValue 缺少文本时抛出 WecomCliSmartTableAdapterError。
        副作用：无。
        """
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            text = value.get("text")
            if isinstance(text, str):
                return text
        raise WecomCliProtocolError("CLI CellValue 缺少文本")

    def _parse_written_record_id(self, response: Mapping[str, object]) -> str:
        """从 records add 响应中取得唯一新记录的服务端标识。

        参数：response 为已验证成功的 CLI 写入响应。
        返回：唯一新记录的 record_id。
        异常：响应未返回恰好一条带有效标识的记录时抛出异常。
        副作用：无。
        """
        records = response.get("records")
        if not isinstance(records, list) or len(records) != 1:
            # 单条写入必须返回唯一结果，批量或空结果会导致调用方无法确定真实记录。
            raise WecomCliProtocolError("wecom-cli 写入响应未返回唯一记录")
        record_id = self._as_mapping(records[0], "写入记录").get("record_id")
        if not isinstance(record_id, str) or not record_id:
            raise WecomCliProtocolError("wecom-cli 新增响应缺少 record_id")
        return record_id

    def _parse_written_record_ids(
        self, response: Mapping[str, object], expected_count: int
    ) -> list[str]:
        """校验批量新增响应的数量并提取不重复的服务端记录标识。

        参数：response 为 CLI 返回的批量新增响应；expected_count 为请求记录数。
        返回值：按响应顺序排列的唯一记录标识。
        异常：响应数量不匹配、记录结构错误或记录标识重复时交给稳定键重试核对。
        副作用：无。
        """
        raw_records = response.get("records")
        if not isinstance(raw_records, list) or len(raw_records) != expected_count:
            raise SmartTableWriteVerificationError(("批量新增结果",))
        record_ids: list[str] = []
        for raw_record in raw_records:
            record_id = (
                raw_record.get("record_id") if isinstance(raw_record, Mapping) else None
            )
            if not isinstance(record_id, str) or not record_id:
                raise SmartTableWriteVerificationError(("批量新增结果",))
            record_ids.append(record_id)
        if len(set(record_ids)) != len(record_ids):
            raise SmartTableWriteVerificationError(("批量新增结果",))
        return record_ids

    @staticmethod
    def _log_final_failure(
        resource: str,
        action: str,
        error: BaseException | None,
        retry_attempt: int,
        *,
        fallback_error_code: str,
    ) -> None:
        """记录最终 CLI 操作失败的安全分类，不输出请求或响应内容。

        参数：resource、action 为 CLI 操作类别；error 为可选受控错误；retry_attempt 为本次调用序号；
        fallback_error_code 为无结构化进程错误时使用的内部分类。
        返回值：无。
        异常：无；错误属性只接受稳定字符串或整数类型。
        副作用：写入不含文档、子表、记录、字段值或正文的结构化错误日志。
        """
        process_error = error if isinstance(error, WecomCliProcessError) else None
        error_code = process_error.error_code if process_error is not None else fallback_error_code
        external_error_type = (
            process_error.external_error_type
            if process_error is not None
            and process_error.external_error_type in _SAFE_PROCESS_ERROR_TYPES
            else None
        )
        # 通用日志上下文可能绑定写入后的 record_id；操作诊断必须主动去除该字段。
        context = LOG_CONTEXT.get()
        safe_context_token = LOG_CONTEXT.set(
            {key: value for key, value in context.items() if key != "record_id"}
        )
        try:
            logger.error(
                "wecom_cli_smart_table_failed resource=%s action=%s error_code=%s retry_attempt=%s",
                resource,
                action,
                error_code,
                retry_attempt,
                extra={
                    "smart_table_resource": resource,
                    "smart_table_action": action,
                    "error_code": error_code,
                    "external_error_code": (
                        process_error.external_error_code if process_error is not None else None
                    ),
                    "external_error_type": external_error_type,
                    "http_status": process_error.http_status if process_error is not None else None,
                    "retry_attempt": retry_attempt,
                },
            )
        finally:
            LOG_CONTEXT.reset(safe_context_token)

    @staticmethod
    def _log_retry(resource: str, action: str, attempt: int, error_code: str) -> None:
        """记录不包含请求数据、内部标识或 CLI 响应的重试日志。

        参数：resource、action 标识调用；attempt 为从零开始的重试次数；error_code 为安全内部分类。
        副作用：写入结构化警告日志。
        """
        logger.warning(
            "wecom_cli_smart_table_retry resource=%s action=%s retry_attempt=%s error_code=%s",
            resource,
            action,
            attempt + 1,
            error_code,
            extra={
                "smart_table_resource": resource,
                "smart_table_action": action,
                "retry_attempt": attempt + 1,
                "error_code": error_code,
            },
        )
