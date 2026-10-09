"""统一新线索首次录入时间和可替换业务默认值的持久化边界。"""

from datetime import UTC
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.leads.models import Lead, LeadFieldProvenance, serialize_field_value
from app.messaging.models import IncomingMessage
from app.smart_table.registry import DEFAULT_LEAD_BUSINESS_VALUES


def initialize_lead_capture(session: Session, lead: Lead) -> None:
    """为刚创建的草稿初始化默认值和首次接收时间，不修改历史记录。

    参数：session 为创建事务；lead 为刚 flush 的新 Lead。
    返回值：无。异常：数据库错误向调用方传播。
    副作用：写入草稿和系统默认来源；管理员无消息补建使用其受审计创建时间。
    """
    values = dict(lead.field_values)
    message = (
        session.get(IncomingMessage, lead.source_message_id) if lead.source_message_id else None
    )
    received_at = message.received_at if message is not None else lead.created_at
    # SQLite 无时区读数按 UTC 解释，后台独立保留秒级接收时间，不写入表格系统创建时间列。
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=UTC)
    values["录入时间"] = received_at.astimezone(ZoneInfo("Asia/Shanghai")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    for name, value in DEFAULT_LEAD_BUSINESS_VALUES.items():
        if values.get(name):
            continue
        values[name] = value
        # 默认值虽非空仍允许可靠 AI 替换；最后同步基线用于保护后续真实人工编辑。
        session.add(
            LeadFieldProvenance(
                lead_id=lead.id,
                source_message_id=lead.source_message_id,
                field_name=name,
                value=serialize_field_value(value),
                last_ai_synced_value=serialize_field_value(value),
                is_system_default=True,
            )
        )
    lead.field_values = values
