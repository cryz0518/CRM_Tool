"""首条文本线索、字段来源与智能表格同步结果的持久化模型。"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.messaging.models import Base, utc_now


def new_lead_id() -> str:
    """生成不依赖智能表格或 CRM 的线索标识。

    返回值：可作为 Lead 主键保存的随机 UUID 字符串。
    异常：无。
    副作用：无。
    """
    return str(uuid4())


class Lead(Base):
    """保存首次采集销售和当前智能表格维护销售均明确的线索草稿。"""

    __tablename__ = "leads"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    source_message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, unique=True
    )
    original_capturing_sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    smart_table_owner_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    smart_table_record_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    lifecycle_state: Mapped[str] = mapped_column(String(64), default="temporary", nullable=False)
    field_values: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class LeadFieldProvenance(Base):
    """保存某个正式字段由哪条来源消息以什么值贡献，供后续安全合并使用。"""

    __tablename__ = "lead_field_provenances"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    source_message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False
    )
    field_name: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class SmartTableSync(Base):
    """记录线索创建时的智能表格写入结果，外部失败不伪装为已同步。"""

    __tablename__ = "smart_table_syncs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, unique=True)
    source_message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, unique=True
    )
    smart_table_record_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
