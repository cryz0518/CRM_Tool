"""通过 wecom-cli 访问企业微信智能表格的真实适配器。"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from app.core.failures import PermanentTaskFailure, RetryableTaskFailure
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapterConfigurationError,
    SmartTablePermissionError,
    SmartTableRecordNotFoundError,
)
from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableOption,
    SmartTablePermissions,
    SmartTableRecord,
    SmartTableSchema,
)

logger = logging.getLogger(__name__)

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


class WecomCliProtocolError(WecomCliSmartTableAdapterError, PermanentTaskFailure):
    """表示 wecom-cli 返回结构、参数或权限业务失败，不应自动重试。"""


class SmartTableWriteVerificationError(WecomCliProtocolError):
    """表示远端写入未能在有限回读窗口内逐字段核实。"""

    def __init__(self, missing_or_mismatched_fields: Sequence[str]) -> None:
        """构造不含字段值的写入核实失败。

        参数：missing_or_mismatched_fields 为本次补丁中未能远端核实的规范字段名。
        返回值：无。
        异常：无。
        副作用：仅在异常对象中保存字段名元组，继承永久失败分类以阻止自动重放。
        """
        self.missing_or_mismatched_fields = tuple(missing_or_mismatched_fields)
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
        return self.error_code in {"network_error", "timeout"} or (
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
        timeout_seconds: float = 20.0,
        retry_count: int = 1,
        runner: CliRunner | None = None,
    ) -> None:
        """初始化固定文档、子表和可替换的 CLI 执行入口。

        参数：doc_id、sheet_id 为管理员配置的目标表标识；sheet_title 为完整查询接口使用的子表名称；
        两个 sales 参数仅接受管理员
        已核验的权限快照；command、timeout_seconds、retry_count 控制 CLI 调用；runner 供测试替换。
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
        self._runner = runner or self._run_subprocess
        self._schema: SmartTableSchema | None = None
        self._recent_written_records: dict[str, tuple[float, SmartTableRecord]] = {}

    def get_schema(self) -> SmartTableSchema:
        """分页读取真实字段、类型和枚举选项。

        返回：保留企业微信字段标识和选项标识的结构快照。
        异常：字段类型不受冻结契约支持或 CLI 调用失败时抛出异常。
        副作用：调用 wecom-cli 的 fields list 接口。
        """
        if self._schema is not None:
            # 字段绑定在进程生命周期内保持同一快照，避免一次读写的显示名映射发生漂移。
            return self._schema
        fields: list[SmartTableField] = []
        for item in self._list_pages("fields"):
            fields.append(self._parse_field(item))
        self._schema = SmartTableSchema(fields=tuple(fields))
        return self._schema

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
        """在当前子表分页读取记录并按标识返回目标记录。

        参数：record_id 为企业微信返回的记录标识。
        返回：记录快照；不存在时返回 None。
        异常：CLI 调用或响应结构异常时抛出异常。
        副作用：调用 wecom-cli 的 records list 接口。
        """
        schema = self.get_schema()
        if self._sheet_title:
            # 完整查询可读取机器人在列表接口中不可见的受限记录，避免按负责人提交时漏行。
            return next(
                (record for record in self._query_records(schema) if record.record_id == record_id),
                None,
            )
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
        副作用：配置 sheet_title 时执行 records query，否则分页执行 records list。
        """
        if self._sheet_title:
            # 有完整查询配置时绕过权限受限的 list，并直接按远端查询结果定位记录。
            return next(
                (record for record in self._query_records(schema) if record.record_id == record_id),
                None,
            )
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

    def get_records(self) -> list[SmartTableRecord]:
        """分页读取当前子表中的全部记录快照。

        返回：按 CLI 页顺序合并的记录列表。
        异常：记录响应缺少标识或字段值时抛出异常。
        副作用：调用 wecom-cli 的 records list 接口。
        """
        schema = self.get_schema()
        if self._sheet_title:
            # 记录列表接口会按当前可见范围返回子集；配置子表名称后优先使用完整 SQL 读取。
            return self._query_records(schema)
        return [self._parse_record(item, schema) for item in self._list_pages("records")]

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
        if actor is SmartTableActor.ROBOT and not fields.get("负责人"):
            raise ValueError("机器人新增智能表格记录时必须写入负责人")

        schema = self.get_schema()
        response = self._call(
            "records", "add", {"records": [{"values": self._to_cli_fields(fields, schema)}]}
        )
        record = self._parse_written_record(response, schema)
        # 新增响应的 values 在短暂最终一致性窗口内可能是旧行快照；只缓存本次确认提交的字段。
        self._remember_written_record(record.record_id, fields)
        return record

    def update_record(self, record_id: str, fields: Mapping[str, object]) -> SmartTableRecord:
        """更新指定记录并在返回成功前远端回读核实每个补丁字段。

        参数：record_id 为目标记录标识；fields 只包含本次变更的字段和值。
        返回：远端回读且逐字段匹配后的记录快照。
        异常：记录不存在、CLI 写入失败或有限核实窗口后仍有字段不匹配时抛出异常。
        副作用：只修改目标记录的传入字段，并在 ACK 后执行有上限的只读回查。
        """
        current_record = self.get_record(record_id)
        if current_record is None:
            raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{record_id}")

        schema = self.get_schema()
        planned_fields = [
            field.name.removeprefix("*")
            for name in fields
            if (field := schema.get_field(name)) is not None
        ]
        logger.info("wecom_cli_write_planned", extra={"planned_fields": planned_fields})
        self._call(
            "records",
            "update",
            {"records": [{"record_id": record_id, "values": self._to_cli_fields(fields, schema)}]},
        )
        # CLI ACK 只代表请求被接受；有限回读以远端实际字段为唯一成功证据。
        mismatched_fields = tuple(fields)
        for delay in _WRITE_VERIFICATION_DELAYS:
            if delay:
                time.sleep(delay)
            try:
                remote_record = self._get_record_remote_uncached(record_id, schema)
            except Exception as error:
                # ACK 已成功后读回失败也不能重放写请求；以永久核实失败交给人工恢复。
                logger.error(
                    "wecom_cli_write_verification_read_failed",
                    extra={
                        "error_type": type(error).__name__,
                        "missing_or_mismatched_fields": list(fields),
                    },
                )
                raise SmartTableWriteVerificationError(tuple(fields)) from None
            if remote_record is None:
                continue
            mismatched_fields = self._mismatched_write_fields(fields, remote_record, schema)
            if not mismatched_fields:
                logger.info(
                    "wecom_cli_write_verified",
                    extra={"persisted_fields": planned_fields},
                )
                self._remember_written_record(record_id, remote_record.fields)
                return remote_record

        logger.error(
            "wecom_cli_write_verification_failed",
            extra={"missing_or_mismatched_fields": list(mismatched_fields)},
        )
        raise SmartTableWriteVerificationError(mismatched_fields)

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
            encoded = self._to_cli_value(canonical_name, field, target)
            if encoded is None:
                mismatched.append(canonical_name)
                continue
            expected = json.loads(encoded)
            # 反向恢复 CLI option/member 编码，使远端读值与领域目标值可直接比较。
            if (
                field.field_type
                in {SmartTableFieldType.SINGLE_SELECT, SmartTableFieldType.MULTI_SELECT}
                or canonical_name == "AI待确认"
            ):
                if isinstance(expected, list):
                    if expected and isinstance(expected[0], Mapping):
                        expected = [item.get("text") for item in expected]
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

    def _query_records(self, schema: SmartTableSchema) -> list[SmartTableRecord]:
        """使用完整查询接口读取目标子表的全部记录。

        参数：schema 为启动时校验过的字段结构。
        返回值：按查询结果顺序转换后的记录快照。
        异常：查询协议、权限或字段结构异常时抛出适配器异常。
        副作用：启动一次只读的 wecom-cli records query，不修改智能表格。
        """
        if not self._sheet_title:
            raise SmartTableAdapterConfigurationError("完整查询缺少 WECOM_SMART_TABLE_SHEET_TITLE")
        columns = ["RECORD_ID"] + [
            self._quote_sql_identifier(field.name) for field in schema.fields
        ]
        sql = (
            f"SELECT {', '.join(columns)} FROM "
            f"{self._quote_sql_identifier(self._sheet_title)} LIMIT 1000"
        )
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
        return [self._parse_query_record(self._as_mapping(row, "查询记录"), schema) for row in rows]

    @staticmethod
    def _quote_sql_identifier(value: str) -> str:
        """为智能表格查询安全引用字段或子表名称。"""
        return f"`{value.replace('`', '``')}`"

    def _call_query(self, sql: str) -> Mapping[str, object]:
        """调用 records query 并复用 CLI 的有限重试与错误转换。"""
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
                response = self._runner(arguments)
            except WecomCliProcessError as error:
                if attempt == self._retry_count:
                    raise WecomCliTransportError("wecom-cli 查询进程调用失败") from error
                self._log_retry("records", "query", attempt, "ProcessExit")
                time.sleep(0.2 * (attempt + 1))
                continue
            except (OSError, subprocess.TimeoutExpired) as error:
                if attempt == self._retry_count:
                    raise WecomCliTransportError("wecom-cli 查询调用失败") from error
                self._log_retry("records", "query", attempt, type(error).__name__)
                time.sleep(0.2 * (attempt + 1))
                continue
            if self._is_transient_network_error(response):
                if attempt == self._retry_count:
                    raise WecomCliTransportError("wecom-cli 查询网络调用失败")
                self._log_retry("records", "query", attempt, "NetworkError")
                time.sleep(0.2 * (attempt + 1))
                continue
            self._raise_for_error(response)
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
        return SmartTableRecord(record_id=record_id, fields=normalized, member_names=member_names)

    def _list_pages(self, resource: ReadResource) -> list[Mapping[str, object]]:
        """按 CLI next_cursor 读取 fields 或 records 的所有分页响应。

        参数：resource 只能是 fields 或 records。
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
            payload: dict[str, object] = {"limit": 1000}
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
    ) -> Mapping[str, object]:
        """调用一个 CLI JSON 接口，并仅为临时网络错误执行有限重试。

        参数：resource、action 构成 smartsheet 子命令；payload 为 CLI JSON 请求体。
        返回：已验证成功状态的 JSON 响应对象。
        异常：超时、网络失败、非零业务错误或非 JSON 响应时抛出异常。
        副作用：启动 wecom-cli 子进程；重试间隔固定为短暂退避以避免风暴。
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
                response = self._runner(arguments)
            except WecomCliProcessError as error:
                # records list/update 是幂等操作，进程异常可以安全重放；records add
                # 可能已经在服务端成功，不能因客户端退出异常再次创建重复记录。
                if action == "add" or not error.retryable:
                    raise
                if attempt == self._retry_count:
                    raise WecomCliTransportError(
                        f"wecom-cli 进程调用失败：{error.error_code}"
                    ) from error
                self._log_retry(resource, action, attempt, f"ProcessExit:{error.error_code}")
                time.sleep(0.2 * (attempt + 1))
                continue
            except (OSError, subprocess.TimeoutExpired) as error:
                if attempt == self._retry_count:
                    # 最后一次仍失败时隐藏底层请求和响应，避免异常泄露表格数据。
                    raise WecomCliTransportError("wecom-cli 调用失败") from error
                self._log_retry(resource, action, attempt, type(error).__name__)
                time.sleep(0.2 * (attempt + 1))
                continue

            if self._is_transient_network_error(response):
                # CLI 明确标记的 NetworkError 才允许重放同一个请求。
                if attempt == self._retry_count:
                    raise WecomCliTransportError("wecom-cli 网络调用失败")
                self._log_retry(resource, action, attempt, "NetworkError")
                time.sleep(0.2 * (attempt + 1))
                continue
            # 非暂态响应先做统一错误码校验，再交给具体解析器处理结构。
            self._raise_for_error(response)
            return response

        raise AssertionError("已覆盖全部 CLI 重试分支")

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
                "wecom_cli_process_failed error_code=%s",
                error_code,
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
    ) -> dict[str, str]:
        """把业务字段转换为标题键及 CLI 1.3.x 要求的 JSON 值字符串。

        参数：fields 为业务层字段补丁；schema 为当前真实字段快照。
        返回：键为真实字段标题、值为单元格 JSON 序列化文本的映射。
        异常：字段未配置、成员身份或 AI待确认选项不合法时抛出异常。
        副作用：无。
        """
        converted: dict[str, str] = {}
        for canonical_name, value in fields.items():
            field = schema.get_field(canonical_name)
            if field is None:
                raise WecomCliProtocolError(f"智能表格未配置字段：{canonical_name}")
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
    ) -> str | None:
        """先按字段类型构造 CellValue，再序列化为 CLI map 中的字符串。

        参数：canonical_name 为业务规范字段名；field 为真实字段定义；value 为业务层值。
        返回：CLI 1.3.x `values` map 所需的 JSON 序列化值；不合法日期返回 None 并跳过该字段。
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
            # CLI 的 CellUserValue 写入格式为数组；业务层只保留企业微信销售身份字符串。
            cli_value = [{"userId": value}]
        elif field.field_type is SmartTableFieldType.PHONE_NUMBER and isinstance(value, str):
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
            if not field.options:
                # 兼容旧测试替身缺少 options 的响应；真实表结构 readiness 会拒绝缺少选项。
                cli_value = value
            else:
                options = {option.name.removeprefix("*"): option for option in field.options}
                try:
                    cli_value = [
                        {"id": options[item].option_id, "text": options[item].name}
                        for item in values
                    ]
                except KeyError as error:
                    raise ValueError(
                        f"选择字段缺少选项：{canonical_name}={error.args[0]}"
                    ) from error
        return json.dumps(cli_value, ensure_ascii=False, separators=(",", ":"))

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
        name = value[0].get("userName")
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
            if (
                not isinstance(value, list)
                or len(value) != 1
                or not isinstance(value[0], Mapping)
                or not isinstance(value[0].get("userId"), str)
            ):
                raise WecomCliProtocolError("MEMBER 字段返回值不符合单成员 CLI 契约")
            return value[0]["userId"]
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

    def _parse_written_record(
        self, response: Mapping[str, object], schema: SmartTableSchema
    ) -> SmartTableRecord:
        """从 records add 或 update 响应中取得唯一写入后的记录。

        参数：response 为已验证成功的 CLI 写入响应。
        返回：唯一的创建或更新记录快照。
        异常：响应未返回恰好一条带字段值的记录时抛出异常。
        副作用：无。
        """
        records = response.get("records")
        if not isinstance(records, list) or len(records) != 1:
            # 单条写入必须返回唯一结果，批量或空结果会导致调用方无法确定真实记录。
            raise WecomCliProtocolError("wecom-cli 写入响应未返回唯一记录")
        return self._parse_record(self._as_mapping(records[0], "写入记录"), schema)

    @staticmethod
    def _log_retry(resource: str, action: str, attempt: int, error_type: str) -> None:
        """记录不包含请求数据、内部标识或 CLI 响应的重试日志。

        参数：resource、action 标识调用；attempt 为从零开始的重试次数；error_type 为异常类型。
        副作用：写入结构化警告日志。
        """
        logger.warning(
            "wecom_cli_smart_table_retry",
            extra={
                "smart_table_resource": resource,
                "smart_table_action": action,
                "retry_attempt": attempt + 1,
                "error_type": error_type,
            },
        )
