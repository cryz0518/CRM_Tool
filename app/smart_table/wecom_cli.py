"""通过 wecom-cli 访问企业微信智能表格的真实适配器。"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

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
    "phone_number": SmartTableFieldType.TEXT,
    "email": SmartTableFieldType.EMAIL,
    "single_select": SmartTableFieldType.SINGLE_SELECT,
    "select": SmartTableFieldType.MULTI_SELECT,
    "date_time": SmartTableFieldType.DATE,
    "attachment": SmartTableFieldType.ATTACHMENT,
    "location": SmartTableFieldType.LOCATION,
    "user": SmartTableFieldType.MEMBER,
}


class WecomCliSmartTableAdapterError(RuntimeError):
    """表示 wecom-cli 返回了不能安全继续处理的响应。"""


class WecomCliSmartTableAdapter:
    """以 wecom-cli 实现冻结的智能表格读写契约。"""

    def __init__(
        self,
        *,
        doc_id: str,
        sheet_id: str,
        sales_can_create_records: bool | None = None,
        sales_can_delete_records: bool | None = None,
        command: str = "wecom-cli",
        timeout_seconds: float = 20.0,
        retry_count: int = 1,
        runner: CliRunner | None = None,
    ) -> None:
        """初始化固定文档、子表和可替换的 CLI 执行入口。

        参数：doc_id、sheet_id 为管理员配置的目标表标识；两个 sales 参数仅接受管理员
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
        self._sales_can_create_records = sales_can_create_records
        self._sales_can_delete_records = sales_can_delete_records
        self._command = command
        self._timeout_seconds = timeout_seconds
        self._retry_count = retry_count
        self._runner = runner or self._run_subprocess
        self._schema: SmartTableSchema | None = None

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
        for record in self.get_records():
            # records list 没有按记录标识读取的独立接口，只能在分页结果中精确定位。
            if record.record_id == record_id:
                return record
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
        return self._parse_written_record(response, schema)

    def update_record(self, record_id: str, fields: Mapping[str, object]) -> SmartTableRecord:
        """对指定记录执行单次字段补丁更新。

        参数：record_id 为目标记录标识；fields 只包含本次变更的字段和值。
        返回：CLI 返回的更新后记录快照。
        异常：记录不存在或 CLI 写入失败时抛出异常。
        副作用：只修改目标记录的传入字段，不传递任何旧字段。
        """
        if self.get_record(record_id) is None:
            raise SmartTableRecordNotFoundError(f"智能表格记录不存在：{record_id}")

        schema = self.get_schema()
        response = self._call(
            "records",
            "update",
            {
                "records": [
                    {"record_id": record_id, "values": self._to_cli_fields(fields, schema)}
                ]
            },
        )
        return self._parse_written_record(response, schema)

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
                raise WecomCliSmartTableAdapterError(
                    f"wecom-cli {resource} list 返回的 {result_key} 不是列表"
                )
            # 外部 JSON 数组逐项收窄为对象，拒绝异常条目而不是静默丢失字段或记录。
            items.extend(self._as_mapping(item, result_key) for item in raw_items)

            next_cursor = response.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                # 没有后续游标即表明当前快照读取完整。
                return items
            if next_cursor in seen_cursors:
                raise WecomCliSmartTableAdapterError(
                    f"wecom-cli {resource} list 返回了循环分页游标"
                )
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
            except (OSError, subprocess.TimeoutExpired) as error:
                if attempt == self._retry_count:
                    # 最后一次仍失败时隐藏底层请求和响应，避免异常泄露表格数据。
                    raise WecomCliSmartTableAdapterError("wecom-cli 调用失败") from error
                self._log_retry(resource, action, attempt, type(error).__name__)
                time.sleep(0.2 * (attempt + 1))
                continue

            if self._is_transient_network_error(response) and attempt < self._retry_count:
                # CLI 明确标记的 NetworkError 才允许重放同一个请求。
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
            # 非零退出不解析可能包含敏感上下文的标准错误输出。
            raise WecomCliSmartTableAdapterError(
                f"wecom-cli 退出失败，退出码：{completed.returncode}"
            )
        try:
            parsed: Any = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            # CLI 成功退出但协议异常时，拒绝将非 JSON 文本当作业务数据继续处理。
            raise WecomCliSmartTableAdapterError("wecom-cli 未返回 JSON 对象") from error
        return self._as_mapping(parsed, "CLI 响应")

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
            raise WecomCliSmartTableAdapterError("wecom-cli 返回外部服务错误")
        errcode = response.get("errcode")
        if errcode not in (None, 0):
            # errcode 非零属于业务失败，不应走网络重试或继续解析写入结果。
            raise WecomCliSmartTableAdapterError("wecom-cli 返回业务错误")

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
            raise WecomCliSmartTableAdapterError(f"wecom-cli {name} 返回的对象结构无效")
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
            raise WecomCliSmartTableAdapterError("wecom-cli 字段定义缺少标识、名称或类型")
        # 只将冻结契约中声明的 CLI 类型映射为领域类型，未知类型必须阻断 readiness。
        field_type = _FIELD_TYPES.get(raw_type)
        if field_type is None:
            raise WecomCliSmartTableAdapterError(f"冻结契约不支持智能表格字段类型：{raw_type}")

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
            raise WecomCliSmartTableAdapterError("wecom-cli 字段属性不是对象")
        raw_options = property_value.get("options", [])
        if not isinstance(raw_options, list):
            raise WecomCliSmartTableAdapterError("wecom-cli 字段选项不是列表")
        options: list[SmartTableOption] = []
        for raw_option in raw_options:
            # 服务端 option ID 是后续真实写入必需的值，不能用展示文本替代或自行生成。
            option = self._as_mapping(raw_option, "字段选项")
            option_id = option.get("id")
            name = option.get("text")
            if not isinstance(option_id, str) or not isinstance(name, str):
                raise WecomCliSmartTableAdapterError("wecom-cli 字段选项缺少标识或名称")
            options.append(SmartTableOption(option_id=option_id, name=name))
        return tuple(options)

    def _to_cli_fields(
        self, fields: Mapping[str, object], schema: SmartTableSchema
    ) -> dict[str, object]:
        """把业务规范字段和值转换为真实字段标题及 CLI 原生值。

        参数：fields 为业务层字段补丁；schema 为当前真实字段快照。
        返回：可直接传给 wecom-cli 的字段标题和值。
        异常：字段未配置、成员身份或 AI待确认选项不合法时抛出异常。
        副作用：无。
        """
        converted: dict[str, object] = {}
        for canonical_name, value in fields.items():
            field = schema.get_field(canonical_name)
            if field is None:
                raise WecomCliSmartTableAdapterError(f"智能表格未配置字段：{canonical_name}")
            # 字段名称唯一由 schema 解析，业务层绝不拼接管理员维护的必填前缀。
            converted[field.name] = self._to_cli_value(canonical_name, field, value)
        return converted

    def _to_cli_value(
        self, canonical_name: str, field: SmartTableField, value: object
    ) -> object:
        """按字段类型转换成员和 AI待确认的 CLI 值，其余字段保持既有契约。

        参数：canonical_name 为业务规范字段名；field 为真实字段定义；value 为业务层值。
        返回：匹配 CLI 字段类型的原生值。
        异常：成员身份或 AI待确认选项格式不合法时抛出异常。
        副作用：无。
        """
        if field.field_type is SmartTableFieldType.MEMBER:
            if not isinstance(value, str) or not value:
                raise ValueError(f"MEMBER 字段必须传入非空 sales_user_id：{canonical_name}")
            # CLI 的 CellUserValue 写入格式为数组；业务层只保留企业微信销售身份字符串。
            return [{"userId": value}]
        if canonical_name == "AI待确认":
            if not isinstance(value, list) or not all(isinstance(name, str) for name in value):
                raise ValueError("AI待确认必须传入规范字段名列表")
            # 管理员可为业务字段选项增加必填前缀；写入必须使用 schema 中真实 option ID。
            options = {option.name.removeprefix("*"): option for option in field.options}
            try:
                return [
                    {"id": options[name].option_id, "text": options[name].name} for name in value
                ]
            except KeyError as error:
                raise ValueError(f"AI待确认缺少字段选项：{error.args[0]}") from error
        return value

    def _parse_record(
        self, item: Mapping[str, object], schema: SmartTableSchema
    ) -> SmartTableRecord:
        """把 CLI 行记录转换为保留原始字段值的领域快照。

        参数：item 为 records list 或写入接口返回的单条记录。
        返回：记录标识和规范字段名到业务层值的映射。
        异常：记录标识或 values 对象缺失时抛出异常。
        副作用：无。
        """
        record_id = item.get("record_id")
        fields = item.get("values")
        if not isinstance(record_id, str) or not isinstance(fields, Mapping):
            raise WecomCliSmartTableAdapterError("wecom-cli 记录缺少 record_id 或 values")
        normalized: dict[str, object] = {}
        for display_name, value in self._as_mapping(fields, "记录 values").items():
            # 真实记录可能包含管理员后续新增字段；未知字段保留原名以避免读数据丢失。
            field = schema.get_field(display_name)
            canonical_name = field.name.removeprefix("*") if field is not None else display_name
            normalized[canonical_name] = self._from_cli_value(canonical_name, field, value)
        return SmartTableRecord(record_id=record_id, fields=normalized)

    @staticmethod
    def _from_cli_value(
        canonical_name: str, field: SmartTableField | None, value: object
    ) -> object:
        """将成员与 AI待确认的真实返回值恢复为业务层规范值。

        参数：canonical_name 为规范字段名；field 为可选真实字段定义；value 为 CLI 返回值。
        返回：业务层可直接比较和持久化的值。
        异常：成员或 AI待确认返回结构不符合 CLI 契约时抛出异常。
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
                raise WecomCliSmartTableAdapterError("MEMBER 字段返回值不符合单成员 CLI 契约")
            return value[0]["userId"]
        if canonical_name == "AI待确认":
            if not isinstance(value, list):
                raise WecomCliSmartTableAdapterError("AI待确认返回值不是选项列表")
            names: list[str] = []
            for option_value in value:
                # 读回选项文本同样去除管理员必填前缀，使 T09 永远与规范字段名比较。
                if not isinstance(option_value, Mapping) or not isinstance(
                    option_value.get("text"), str
                ):
                    raise WecomCliSmartTableAdapterError("AI待确认选项返回值无效")
                names.append(option_value["text"].removeprefix("*"))
            return names
        return value

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
            raise WecomCliSmartTableAdapterError("wecom-cli 写入响应未返回唯一记录")
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
