"""Operations Console 新增的审计与 AI 执行持久化模型。"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.messaging.models import Base, utc_now


def new_console_event_id() -> str:
    """生成 Console 事件使用的不透明 UUID 标识。"""
    return str(uuid4())


class BreakGlassAccessAudit(Base):
    """保存单次 Break-glass 授权、完成或失败事实，记录采用追加方式。"""

    __tablename__ = "break_glass_access_audits"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_console_event_id)
    access_id: Mapped[str] = mapped_column(String(36), index=True)
    request_id: Mapped[str] = mapped_column(String(128), index=True)
    operator_subject: Mapped[str] = mapped_column(String(128), index=True)
    operator_role: Mapped[str] = mapped_column(String(64), nullable=False)
    auth_source: Mapped[str] = mapped_column(String(64), nullable=False)
    object_type: Mapped[str] = mapped_column(String(32), nullable=False)
    object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    access_type: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    phase: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    data_returned: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    request_context: Mapped[dict[str, str]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        Index("ix_break_glass_audits_object_created", "object_type", "object_id", "created_at"),
        Index("ix_break_glass_audits_access_created", "access_type", "created_at"),
    )


class AIExecutionRecord(Base):
    """保存 AI 调用运行元数据，不保存 Prompt、响应或原始业务文本。"""

    __tablename__ = "ai_execution_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_console_event_id)
    trace_id: Mapped[str] = mapped_column(String(128), unique=True)
    message_id: Mapped[str | None] = mapped_column(String(128), index=True)
    lead_id: Mapped[str | None] = mapped_column(String(36), index=True)
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(128), nullable=False)
    model: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    call_count: Mapped[int | None] = mapped_column()
    input_tokens: Mapped[int | None] = mapped_column()
    output_tokens: Mapped[int | None] = mapped_column()
    duration_ms: Mapped[int | None] = mapped_column()
    error_type: Mapped[str | None] = mapped_column(String(128))
    error_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("ix_ai_execution_records_status_created", "status", "created_at"),)
