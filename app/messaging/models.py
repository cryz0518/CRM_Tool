"""消息接收、Actor Registry 与事务发件箱持久化模型。"""

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
    Index,
    Integer,
    String,
    UniqueConstraint,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


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
    """兼容保存 Actor Registry 及该成员的持久化消息顺序号。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后成为企业微信 actor、消息顺序和管理员身份的事实来源；
    `is_authorized` 仅保留兼容字段，不再作为普通业务访问门槛。
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
    """保存有效 WeCom actor 已接收的原始消息及标准化文本。

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
    chat_id: Mapped[str | None] = mapped_column(String(128), index=True)
    chat_type: Mapped[str | None] = mapped_column(String(32), index=True)
    requires_media_enrichment: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    scrubbed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retention_policy_version: Mapped[str | None] = mapped_column(String(64))
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
    scrubbed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retention_policy_version: Mapped[str | None] = mapped_column(String(64))
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


class MessageQuoteResolutionStatus(StrEnum):
    """集中定义企业微信引用关系的确定性解析终态。"""

    RESOLVED = "resolved"
    NOT_FOUND = "not_found"
    AMBIGUOUS = "ambiguous"
    CONFLICT = "conflict"


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
    """保存可查询的结构化业务审计事实。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后可追溯消息接收、去重、业务操作和受控失败结果；
    原始消息、客户联系方式和外部响应正文不得放入 details。
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


class MessageQuoteResolution(Base):
    """保存当前消息到历史 IncomingMessage 的确定性引用解析结果。

    参数：字段由 SQLAlchemy 映射初始化；current_message_id 是当前消息，
    quoted_source_message_id 是系统 matcher 解析出的内部历史消息外键。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：持久化后形成引用解析审计事实；不保存引用原文副本。
    """

    __tablename__ = "message_quote_resolutions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    current_message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False
    )
    quoted_source_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=True
    )
    resolution_status: Mapped[str] = mapped_column(String(32), nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False)
    matched_by: Mapped[str | None] = mapped_column(String(32))
    conflict_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("current_message_id"),
        Index("ix_message_quote_resolutions_quoted_source", "quoted_source_message_id"),
    )


class AuditMirrorOutbox(Base):
    """保存业务审计事件的 Smart Table 镜像任务。

    参数：字段由 SQLAlchemy 映射初始化。
    返回值：无。
    异常：数据库约束异常由会话层抛出。
    副作用：允许 Worker 独立重试管理员审计镜像，不改变原业务状态。
    """

    __tablename__ = "audit_mirror_outbox"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    audit_event_id: Mapped[int] = mapped_column(
        ForeignKey("business_audit_events.id"), nullable=False, unique=True
    )
    mirror_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claim_token: Mapped[str | None] = mapped_column(String(64))
    failure_category: Mapped[str | None] = mapped_column(String(32))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


@event.listens_for(Session, "after_flush")
def _enqueue_audit_mirror_outbox(session: Session, _flush_context: object) -> None:
    """为本次事务新建的业务审计事件生成唯一镜像任务。

    参数：session 为当前 SQLAlchemy 会话；flush_context 为 SQLAlchemy flush 上下文。
    返回值：无。
    异常：对象构造或会话写入异常由当前事务传播并回滚。
    副作用：在审计事实成功 flush 后追加 AuditMirrorOutbox；不调用外部系统。
    """
    # after_flush 时审计事件已经获得数据库 ID，镜像键因此稳定且不携带客户数据。
    # SQLAlchemy 在 after_flush 期间仍会把新对象留在 session.new；用会话标记防止递归 flush。
    queued_event_ids = session.info.setdefault("audit_mirror_queued_event_ids", set())
    for audit_event in tuple(session.new):
        if not isinstance(audit_event, BusinessAuditEvent) or audit_event.id is None:
            continue
        if audit_event.id in queued_event_ids:
            continue
        # 每个事件只入队一次；数据库唯一约束继续作为并发兜底。
        session.add(
            AuditMirrorOutbox(
                audit_event_id=audit_event.id,
                mirror_key=f"audit:{audit_event.id}",
            )
        )
        queued_event_ids.add(audit_event.id)


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
    ingest_operation_id: Mapped[str | None] = mapped_column(String(36), index=True)
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
    """保存外部上传前已提交的 intent 及其可恢复远端事实。"""

    __tablename__ = "storage_ingest_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    operation_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    attachment_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    message_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    storage_provider: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String(256))
    storage_key_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    content_sha256: Mapped[str | None] = mapped_column(String(64))
    content_type: Mapped[str | None] = mapped_column(String(128))
    expected_size_bytes: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    remote_outcome: Mapped[str | None] = mapped_column(String(32))
    remote_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claim_token: Mapped[str | None] = mapped_column(String(64))
    generation: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_summary: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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
    remote_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
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
