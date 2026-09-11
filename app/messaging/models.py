"""消息接收、销售授权与事务发件箱持久化模型。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    """返回带 UTC 时区的当前时间，供持久化审计字段统一使用。

    参数：无。
    返回值：当前 UTC 时间。
    异常：无。
    副作用：无。
    """
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """定义消息接收模块的 SQLAlchemy 元数据根类。

    参数：无。
    返回值：无。
    异常：无。
    副作用：收集本模块映射表元数据。
    """


class SalesAuthorization(Base):
    """保存销售授权目录及该销售的持久化消息顺序号。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后成为销售身份判定权威数据。
    """

    __tablename__ = "sales_authorizations"

    wecom_user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    display_name: Mapped[str | None] = mapped_column(String(128))
    department_id: Mapped[str | None] = mapped_column(String(128))
    crm_user_id: Mapped[str | None] = mapped_column(String(128))
    is_authorized: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_administrator: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    next_message_sequence: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class IncomingMessage(Base):
    """保存授权销售已接收的原始消息及标准化文本。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后形成后续处理事实。
    """

    __tablename__ = "incoming_messages"

    message_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    normalized_text: Mapped[str | None] = mapped_column(String)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (UniqueConstraint("sales_user_id", "sequence"),)


class OutboxEvent(Base):
    """保存等待 Worker 消费的消息处理事件，禁止在事务内执行外部调用。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后允许 Worker 异步处理消息。
    """

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, unique=True
    )
    sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), default="message_received", nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (UniqueConstraint("message_id", "event_type"),)


class NotificationRecord(Base):
    """保存可幂等投递的机器人通知，不将通知重复计入业务处理任务。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后可由接入层按通知键去重发送。
    """

    __tablename__ = "notification_records"

    notification_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    notification_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    provider_message_id: Mapped[str | None] = mapped_column(String(128))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class BusinessAuditEvent(Base):
    """保存 T02 消息接收链路的可查询业务审计事件。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后可追溯消息接收、去重和未授权拒绝。
    """

    __tablename__ = "business_audit_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (UniqueConstraint("message_id", "event_type"),)


class MessageAttachment(Base):
    """保存仅归属来源消息的二进制工件元数据，不直接关联线索。"""

    __tablename__ = "message_attachments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, index=True
    )
    media_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    declared_mime_type: Mapped[str | None] = mapped_column(String(128))
    detected_mime_type: Mapped[str | None] = mapped_column(String(128))
    size_bytes: Mapped[int | None] = mapped_column(Integer)
    sha256: Mapped[str | None] = mapped_column(String(64), unique=True)
    storage_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    scan_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    processing_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    recognized_text: Mapped[str | None] = mapped_column(String)
    error_summary: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MediaProcessingTask(Base):
    """保存一次 OCR 或 ASR 的独立异步处理状态，失败不改变消息会话状态。"""

    __tablename__ = "media_processing_tasks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    attachment_id: Mapped[str] = mapped_column(ForeignKey("message_attachments.id"), nullable=False)
    task_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("attachment_id", "task_type"),)
