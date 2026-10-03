"""业务审计事件到 Smart Table 管理员镜像子表的安全转换。"""

from __future__ import annotations

from collections.abc import Mapping

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
    "事件类型",
    "操作人ID",
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
                field_type=SmartTableFieldType.TEXT,
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
        """返回业务审计事件的稳定镜像键。

        参数：event 为已持久化或即将镜像的业务审计事件。
        返回值：以数据库审计事件 ID 组成的稳定键。
        异常：事件尚未取得 ID 时抛出 ValueError。
        副作用：无。
        """
        if event.id is None:
            raise ValueError("业务审计事件尚未获得持久化 ID")
        return f"audit:{event.id}"

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
            "事件类型": event.event_type,
            "操作人ID": event.sales_user_id,
            "消息ID": event.message_id,
        }
        # 系统审计日志中的销售原始消息是管理员审计副本；Smart Table ACL 由管理员控制；
        # 该副本不会因 IncomingMessage 后续 scrub 自动删除。正文只来自受控参数，不读取 raw_payload。
        if isinstance(sales_original_message, str):
            original_message = sales_original_message.strip()
            if original_message:
                fields["销售原始消息"] = original_message[:MAX_AUDIT_ORIGINAL_MESSAGE_LENGTH]
        if isinstance(resolved_lead_id, str) and resolved_lead_id.strip():
            fields["线索归属ID"] = resolved_lead_id.strip()
        details = event.details if isinstance(event.details, Mapping) else {}
        for name in self._DETAIL_FIELDS:
            value = details.get(name)
            output_name = _DETAIL_FIELD_OUTPUT_NAMES[name]
            if name == "missing_fields":
                if isinstance(value, (list, tuple)) and all(
                    isinstance(item, str) for item in value
                ):
                    fields[output_name] = "、".join(value)
                continue
            if isinstance(value, str) and value:
                fields[output_name] = value[:256]
        return fields

    def mirror(
        self,
        event: BusinessAuditEvent,
        *,
        sales_original_message: str | None = None,
        resolved_lead_id: str | None = None,
    ) -> SmartTableRecord:
        """按稳定镜像键写前查重后创建一条审计子表记录。

        参数：event 为待镜像业务审计事实；sales_original_message 为未 scrub 的标准化销售文本；
        resolved_lead_id 为 Worker 从当前数据库归属事实解析出的 Lead.id。
        返回值：已存在或本次创建的审计子表记录。
        异常：审计子表缺少镜像键字段或外部写入失败时抛出适配器异常。
        副作用：配合服务端 claim fencing，重复重试通过远端查重避免重复新增。
        """
        fields = self.build_fields(
            event,
            sales_original_message=sales_original_message,
            resolved_lead_id=resolved_lead_id,
        )
        schema = self._adapter.get_schema()
        missing_fields = [
            name for name in AUDIT_MIRROR_FIELD_NAMES if schema.get_field(name) is None
        ]
        if missing_fields:
            raise SmartTableAdapterConfigurationError(
                f"审计子表缺少必需字段：{','.join(missing_fields)}"
            )
        existing = self._adapter.find_records(
            {AUDIT_MIRROR_KEY_FIELD: fields[AUDIT_MIRROR_KEY_FIELD]}
        )
        if existing:
            return existing[0]
        # 审计子表 ACL 由管理员在企微端配置；代码只使用机器人身份写入，不管理 ACL。
        return self._adapter.create_record(fields, actor=SmartTableActor.ROBOT)
