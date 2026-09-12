"""首条文本线索、字段来源与智能表格同步结果的持久化模型。"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
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
        ForeignKey("incoming_messages.message_id"), nullable=False
    )
    source_segment_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    original_capturing_sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    smart_table_owner_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    smart_table_record_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    lifecycle_state: Mapped[str] = mapped_column(String(64), default="temporary", nullable=False)
    field_values: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    enrichment_values: Mapped[dict[str, str]] = mapped_column(JSON, default=dict, nullable=False)
    standard_company_name: Mapped[str | None] = mapped_column(String(512), index=True)
    company_region: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    company_verification_status: Mapped[str] = mapped_column(
        String(64), default="incomplete_company", nullable=False
    )
    qcc_company_id: Mapped[str | None] = mapped_column(String(128))
    qcc_candidates: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list, nullable=False)
    company_confirmed_by_user: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("source_message_id", "source_segment_index"),
        UniqueConstraint("smart_table_owner_user_id", "standard_company_name"),
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
    value: Mapped[str] = mapped_column(Text, nullable=False)
    last_ai_synced_value: Mapped[str | None] = mapped_column(Text)
    is_user_modified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_user_confirmed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class UserConfirmationEvent(Base):
    """保存销售经机器人显式确认 AI 待确认字段的不可变审计事实。"""

    __tablename__ = "user_confirmation_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    field_name: Mapped[str] = mapped_column(String(64), nullable=False)
    confirmed_value: Mapped[str] = mapped_column(String(512), nullable=False)
    operator_sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False
    )
    confirmation_source: Mapped[str] = mapped_column(String(32), default="robot", nullable=False)
    confirmed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class SalesLeadContext(Base):
    """保存一名销售最近一次可安全补充的线索上下文。"""

    __tablename__ = "sales_lead_contexts"

    sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), primary_key=True
    )
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    last_message_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class LeadMessageResolution(Base):
    """保存消息是否已归属线索，供待归属审核与后续人工处理使用。"""

    __tablename__ = "lead_message_resolutions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, index=True
    )
    segment_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lead_id: Mapped[str | None] = mapped_column(ForeignKey("leads.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (UniqueConstraint("message_id", "segment_index"),)


class MessageReassignmentAudit(Base):
    """保存人工重新归属的原目标、新目标、操作人角色和原因。"""

    __tablename__ = "message_reassignment_audits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False
    )
    segment_index: Mapped[int] = mapped_column(Integer, nullable=False)
    previous_lead_id: Mapped[str | None] = mapped_column(ForeignKey("leads.id"))
    new_lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False)
    operator_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False
    )
    operator_role: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="processing", nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class SmartTableSync(Base):
    """记录线索创建时的智能表格写入结果，外部失败不伪装为已同步。"""

    __tablename__ = "smart_table_syncs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, unique=True)
    source_message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False
    )
    source_segment_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    smart_table_record_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("source_message_id", "source_segment_index"),)
