"""消息接收、销售授权与事务发件箱持久化模型。"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
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
    created_by: Mapped[str | None] = mapped_column(String(128))
    updated_by: Mapped[str | None] = mapped_column(String(128))
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
    requires_media_enrichment: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
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
    failure_category: Mapped[str | None] = mapped_column(String(32))
    failure_summary: Mapped[str | None] = mapped_column(String(128))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
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
    content: Mapped[str | None] = mapped_column(String(512))
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_claim_token: Mapped[str | None] = mapped_column(String(64))
    provider_message_id: Mapped[str | None] = mapped_column(String(128))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class WecomActionStatus(StrEnum):
    """集中定义 T18 业务动作的生命周期状态。"""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    DENIED = "denied"
    EXPIRED = "expired"
    FAILED = "failed"
    PENDING_RECOVERY = "pending_recovery"


class WecomActionOutboxStatus(StrEnum):
    """集中定义 T18 动作执行 Outbox 的任务状态。"""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WecomCallbackProcessingStatus(StrEnum):
    """集中定义 callback delivery evidence 的处理结果。"""

    RECEIVED = "received"
    CLAIMED = "claimed"
    DUPLICATED = "duplicated"
    REJECTED = "rejected"
    COMPLETED = "completed"


def new_wecom_action_id() -> str:
    """生成服务端内部业务 action UUID。"""

    return str(uuid4())


class WecomAction(Base):
    """保存一张服务端生成、不可由 callback 客户端重构的企业微信业务动作。"""

    __tablename__ = "wecom_actions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_wecom_action_id)
    task_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    issuance_key: Mapped[str | None] = mapped_column(String(128), unique=True)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    bound_actor_wecom_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(128), nullable=False)
    expected_action_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default=WecomActionStatus.PENDING.value, nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_code: Mapped[str | None] = mapped_column(String(64))
    result_summary: Mapped[str | None] = mapped_column(String(256))
    context: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'succeeded', 'denied', 'expired', 'failed', "
            "'pending_recovery')",
            name="ck_wecom_actions_status",
        ),
    )


class WecomActionOutbox(Base):
    """保存 callback claim 后等待 Worker 执行的一次性动作任务。"""

    __tablename__ = "wecom_action_outbox"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    action_id: Mapped[str] = mapped_column(
        ForeignKey("wecom_actions.id"), nullable=False, unique=True
    )
    status: Mapped[str] = mapped_column(
        String(32), default=WecomActionOutboxStatus.PENDING.value, nullable=False
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    dispatch_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dispatch_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claim_token: Mapped[str | None] = mapped_column(String(64))
    domain_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    domain_operation_key: Mapped[str | None] = mapped_column(String(128))
    domain_operation_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    remote_effect_status: Mapped[str | None] = mapped_column(String(32))
    remote_effect_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'succeeded', 'failed')",
            name="ck_wecom_action_outbox_status",
        ),
    )


class WecomCallbackDelivery(Base):
    """保存企业微信 callback 的白名单传输证据，不保存原始 payload。"""

    __tablename__ = "wecom_callback_deliveries"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    action_id: Mapped[str | None] = mapped_column(ForeignKey("wecom_actions.id"), index=True)
    provider_msgid: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    req_id: Mapped[str] = mapped_column(String(128), nullable=False)
    actor_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_key: Mapped[str] = mapped_column(String(128), nullable=False)
    task_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    processing_status: Mapped[str] = mapped_column(
        String(32), default=WecomCallbackProcessingStatus.RECEIVED.value, nullable=False
    )
    result_code: Mapped[str | None] = mapped_column(String(64))
    transport_stage: Mapped[str | None] = mapped_column(String(64))
    transport_status: Mapped[str | None] = mapped_column(String(32))
    transport_failure_code: Mapped[str | None] = mapped_column(String(64))
    transport_failure_summary: Mapped[str | None] = mapped_column(String(128))
    transport_failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "processing_status IN ('received', 'claimed', 'duplicated', 'rejected', 'completed')",
            name="ck_wecom_callback_delivery_status",
        ),
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
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
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
    sha256: Mapped[str | None] = mapped_column(String(64), index=True)
    storage_key: Mapped[str | None] = mapped_column(String(256), unique=True)
    storage_provider: Mapped[str | None] = mapped_column(String(32))
    storage_encryption_mode: Mapped[str | None] = mapped_column(String(64))
    storage_etag: Mapped[str | None] = mapped_column(String(256))
    scan_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    scan_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scan_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    recognized_text: Mapped[str | None] = mapped_column(String)
    error_summary: Mapped[str | None] = mapped_column(String(128))
    retention_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    retention_policy_version: Mapped[str | None] = mapped_column(String(64))
    deletion_status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deletion_reason: Mapped[str | None] = mapped_column(String(128))
    cleanup_operation_id: Mapped[str | None] = mapped_column(String(36), index=True)
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


class StorageIngestOperation(Base):
    """保存媒体写入后数据库 finalize 失败时的可恢复外部事实。"""

    __tablename__ = "storage_ingest_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    operation_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    attachment_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    message_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    storage_provider: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String(256))
    storage_key_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="reconcile_required", nullable=False)
    failure_summary: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class StorageCleanupOperation(Base):
    """保存一次冻结 retention policy 的媒体清理和远端删除恢复事实。"""

    __tablename__ = "storage_cleanup_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    operation_key: Mapped[str] = mapped_column(String(192), unique=True, nullable=False)
    data_class: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    storage_provider: Mapped[str | None] = mapped_column(String(32))
    storage_key: Mapped[str | None] = mapped_column(String(256))
    storage_key_digest: Mapped[str | None] = mapped_column(String(64))
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    retention_cutoff: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    claim_token: Mapped[str | None] = mapped_column(String(64))
    generation: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    remote_outcome: Mapped[str | None] = mapped_column(String(32))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failure_kind: Mapped[str | None] = mapped_column(String(64))
    failure_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'retrying', 'reconcile_required', "
            "'succeeded', 'failed_pending_review')",
            name="ck_storage_cleanup_operation_status",
        ),
        CheckConstraint(
            "remote_outcome IS NULL OR remote_outcome IN "
            "('confirmed_deleted', 'not_found', 'unknown')",
            name="ck_storage_cleanup_remote_outcome",
        ),
    )
