"""业务审计事件到 Smart Table 管理员镜像子表的安全转换。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from app.messaging.models import BusinessAuditEvent
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapter,
    SmartTableAdapterConfigurationError,
)
from app.smart_table.models import (
    SmartTableField,
    SmartTableFieldType,
    SmartTableRecord,
    SmartTableSchema,
)

AUDIT_SHEET_TITLE = "系统审计日志"
AUDIT_MIRROR_KEY_FIELD = "日志唯一键"
MAX_AUDIT_ORIGINAL_MESSAGE_LENGTH = 2000
AUDIT_MIRROR_FIELD_NAMES = (
    AUDIT_MIRROR_KEY_FIELD,
    "事件ID",
    "时间",
    "操作人",
    "消息ID",
    "销售原始消息",
    "线索归属ID",
    "表格记录ID",
    "交互操作ID",
    "操作类型",
    "处理状态",
    "原因码",
    "缺失字段",
    "失败分类",
    "失败代码",
    "CRM操作",
)

_DETAIL_FIELD_NAMES = (
    "smart_table_record_id",
    "action_id",
    "operation",
    "status",
    "reason_code",
    "missing_fields",
    "failure_category",
    "failure_code",
    "crm_operation",
)
_DETAIL_FIELD_OUTPUT_NAMES = {
    "smart_table_record_id": "表格记录ID",
    "action_id": "交互操作ID",
    "operation": "操作类型",
    "status": "处理状态",
    "reason_code": "原因码",
    "missing_fields": "缺失字段",
    "failure_category": "失败分类",
    "failure_code": "失败代码",
    "crm_operation": "CRM操作",
}


def build_mock_audit_schema() -> SmartTableSchema:
    """构造测试用审计子表结构，不代表生产 ACL 或管理员配置。

    返回值：包含镜像字段的 Mock Smart Table schema。
    异常：无。
    副作用：无；仅在内存中创建字段定义。
    """
    return SmartTableSchema(
        fields=tuple(
            SmartTableField(
                field_id=f"mock-audit-field-{index}",
                name=name,
                # 人员单元格保留企微 userId，由适配器编码为 CLI 成员结构。
                field_type=(
                    SmartTableFieldType.MEMBER
                    if name == "操作人"
                    else SmartTableFieldType.TEXT
                ),
            )
            for index, name in enumerate(AUDIT_MIRROR_FIELD_NAMES, start=1)
        )
    )


class SmartTableAuditSink:
    """将结构化审计事实和受控原始消息幂等写入独立 Smart Table 子表。"""

    _DETAIL_FIELDS = _DETAIL_FIELD_NAMES

    def __init__(self, adapter: SmartTableAdapter) -> None:
        """保存审计子表专用适配器。

        参数：adapter 为已绑定同一 Smart Table 文档下审计子表的适配器。
        返回值：无。
        异常：不访问外部系统；缺失字段在 mirror 时转换为配置异常。
        副作用：仅保存依赖。
        """
        self._adapter = adapter

    @staticmethod
    def mirror_key(event: BusinessAuditEvent) -> str:
        """返回来源消息对应的稳定镜像键。

        参数：event 为已持久化或即将镜像的业务审计事件。
        返回值：以来源消息 ID 组成的稳定键，使同一消息的多个事件归并到一行。
        异常：事件尚未持久化或消息 ID 为空时抛出 ValueError。
        副作用：无。
        """
        if event.id is None:
            raise ValueError("业务审计事件尚未获得持久化 ID")
        if not event.message_id:
            raise ValueError("业务审计事件缺少来源消息 ID")
        return f"message:{event.message_id}"

    def build_fields(
        self,
        event: BusinessAuditEvent,
        *,
        sales_original_message: str | None = None,
        resolved_lead_id: str | None = None,
    ) -> dict[str, object]:
        """把业务审计事实和受控原始消息上下文转换为表格字段。

        参数：event 为业务审计事实；sales_original_message 仅接受 Worker 从
        IncomingMessage.normalized_text 读取且确认未 scrub 的文本；resolved_lead_id 为 Worker
        按当前数据库归属事实解析的 Lead.id；details 仅允许受控字段进入镜像。
        返回值：可直接写入审计子表的字段补丁。
        异常：无；未知字段会被丢弃而不是原样透传。
        副作用：无，不修改审计事实。
        """
        fields: dict[str, object] = {
            AUDIT_MIRROR_KEY_FIELD: self.mirror_key(event),
            "事件ID": str(event.id),
            "时间": event.created_at.isoformat(),
            "操作人": event.sales_user_id.strip(),
            "消息ID": event.message_id,
        }
        if isinstance(resolved_lead_id, str) and resolved_lead_id.strip():
            fields["线索归属ID"] = resolved_lead_id.strip()
        # 系统审计日志中的销售原始消息是管理员审计副本；Smart Table ACL 由管理员控制；
        # 该副本不会因 IncomingMessage 后续 scrub 自动删除。正文只来自受控参数，不读取 raw_payload。
        if isinstance(sales_original_message, str):
            original_message = sales_original_message.strip()
            if original_message:
                fields["销售原始消息"] = original_message[:MAX_AUDIT_ORIGINAL_MESSAGE_LENGTH]
        details = event.details if isinstance(event.details, Mapping) else {}
        for name in self._DETAIL_FIELDS:
            value = details.get(name)
            output_name = _DETAIL_FIELD_OUTPUT_NAMES[name]
            if name == "missing_fields":
                if isinstance(value, (list, tuple)) and all(
                    isinstance(item, str) for item in value
                ):
                    fields[output_name] = "、".join(value)
                else:
                    fields[output_name] = ""
                continue
            fields[output_name] = value[:256] if isinstance(value, str) and value else ""
        return fields

    def mirror(
        self,
        event: BusinessAuditEvent,
        *,
        sales_original_message: str | None = None,
        resolved_lead_id: str | None = None,
    ) -> SmartTableRecord:
        """按稳定镜像键查重并写入一条审计子表记录。

        参数：event 为待镜像业务审计事实；sales_original_message 为未 scrub 的标准化销售文本；
        resolved_lead_id 为 Worker 从当前数据库归属事实解析出的 Lead.id。
        返回值：已存在或本次创建的审计子表记录。
        异常：审计子表缺少镜像键字段或外部写入失败时抛出适配器异常。
        副作用：重复重试通过远端稳定键查重避免重复新增。
        """
        return self.mirror_batch([(event, sales_original_message, resolved_lead_id)])[0]

    def mirror_batch(
        self,
        events: Sequence[tuple[BusinessAuditEvent, str | None, str | None]],
    ) -> list[SmartTableRecord]:
        """按来源消息归并审计事件，并批量新增缺少镜像的消息行。

        参数：events 为审计事实、未 scrub 原文和线索归属 ID 元组。
        返回值：按输入顺序返回对应消息的镜像行，同一消息的事件共用一行。
        异常：结构缺失、查询、更新、批量新增或写后核实失败时抛出适配器异常。
        副作用：更新每条消息最新事件的投影；业务审计事件本身仍逐事件保存在数据库。
        """
        if not events:
            return []
        latest_by_message: dict[str, tuple[BusinessAuditEvent, str | None, str | None]] = {}
        for event, original_message, lead_id in events:
            previous = latest_by_message.get(event.message_id)
            if previous is None or (event.created_at, event.id or 0) >= (
                previous[0].created_at,
                previous[0].id or 0,
            ):
                latest_by_message[event.message_id] = (event, original_message, lead_id)
        fields_by_message = {
            message_id: self.build_fields(
                event,
                sales_original_message=original_message,
                resolved_lead_id=lead_id,
            )
            for message_id, (event, original_message, lead_id) in latest_by_message.items()
        }
        schema = self._adapter.get_schema()
        missing_fields = [
            name for name in AUDIT_MIRROR_FIELD_NAMES if schema.get_field(name) is None
        ]
        if missing_fields:
            raise SmartTableAdapterConfigurationError(
                f"审计子表缺少必需字段：{','.join(missing_fields)}"
            )
        operator_field = schema.get_field("操作人")
        if (
            operator_field is None
            or operator_field.field_type is not SmartTableFieldType.MEMBER
        ):
            raise SmartTableAdapterConfigurationError("审计子表字段“操作人”必须为人员类型")
        message_ids = list(fields_by_message)
        rows_by_message: dict[str, list[SmartTableRecord]] = {}
        for record in self._adapter.find_records_by_field_values("消息ID", message_ids):
            message_id = record.fields.get("消息ID")
            if isinstance(message_id, str) and message_id in fields_by_message:
                rows_by_message.setdefault(message_id, []).append(record)
        existing_by_message: dict[str, SmartTableRecord] = {}
        for message_id, rows in rows_by_message.items():
            stable_key = str(fields_by_message[message_id][AUDIT_MIRROR_KEY_FIELD])
            # 优先复用新版消息键；迁移期复用一条旧事件行，避免再次新建。
            existing_by_message[message_id] = next(
                (row for row in rows if row.fields.get(AUDIT_MIRROR_KEY_FIELD) == stable_key),
                min(rows, key=lambda row: row.record_id),
            )
        results_by_message: dict[str, SmartTableRecord] = {}
        for message_id, record in existing_by_message.items():
            fields = dict(fields_by_message[message_id])
            # 兼容尚未删除该列的旧表结构，更新旧行时清空已废弃的事件类型值。
            if schema.get_field("事件类型") is not None:
                fields["事件类型"] = ""
            if "线索归属ID" not in fields:
                fields["线索归属ID"] = ""
            results_by_message[message_id] = self._adapter.update_record(
                record.record_id,
                fields,
                skip_preflight=True,
            )
        missing_message_ids = [
            message_id for message_id in message_ids if message_id not in results_by_message
        ]
        # 审计子表 ACL 由管理员在企微端配置；代码只使用机器人身份写入，不管理 ACL。
        created = self._adapter.create_records(
            [fields_by_message[message_id] for message_id in missing_message_ids],
            actor=SmartTableActor.ROBOT,
        )
        if len(created) != len(missing_message_ids):
            raise RuntimeError("审计批量新增返回的记录数与请求不一致")
        results_by_message.update(zip(missing_message_ids, created, strict=True))
        return [results_by_message[event.message_id] for event, _, _ in events]
