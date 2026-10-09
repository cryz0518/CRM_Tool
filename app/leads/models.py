"""首条文本线索、字段来源与智能表格同步结果的持久化模型。"""

from __future__ import annotations

import json
from datetime import datetime
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
    Text,
    UniqueConstraint,
    select,
    text,
)
from sqlalchemy.orm import Mapped, Session, mapped_column

from app.messaging.models import Base, utc_now


def new_lead_id() -> str:
    """生成不依赖智能表格或 CRM 的线索标识。

    返回值：可作为 Lead 主键保存的随机 UUID 字符串。
    异常：无。
    副作用：无。
    """
    return str(uuid4())


def serialize_field_value(value: object) -> str:
    """将智能表格字段值保存为字段来源表可接受的文本。

    参数：value 为字符串或工艺多选列表等业务字段值。
    返回值：字符串原样返回；列表以无空格 JSON 保存，便于精确恢复多选顺序和值。
    异常：无法 JSON 序列化的值向调用方抛出 TypeError。
    副作用：无。
    """
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def deserialize_field_value(value: str | None) -> object:
    """恢复字段来源表中可能保存的多选 JSON 值。

    参数：value 为历史或当前字段来源文本。
    返回值：JSON 数组恢复为列表，其余值按历史文本返回。
    异常：非本项目多选格式的 JSON 文本按普通字符串处理。
    副作用：无。
    """
    if value is None or not value.startswith("["):
        return value
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return value
    return decoded if isinstance(decoded, list) else value


def field_values_equal(left: object, right: object) -> bool:
    """比较当前表格值与来源基线，兼容多选列表和旧文本基线。

    参数：left、right 为当前值与字段来源值。
    返回值：语义值相等时为 True。
    异常：无。
    副作用：无。
    """
    return left == deserialize_field_value(right) if isinstance(right, str) else left == right


class Lead(Base):
    """保存首次采集销售和当前智能表格维护销售均明确的线索草稿。"""

    __tablename__ = "leads"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    source_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=True
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
    field_values: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    enrichment_values: Mapped[dict[str, str]] = mapped_column(JSON, default=dict, nullable=False)
    standard_company_name: Mapped[str | None] = mapped_column(String(512), index=True)
    company_region: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    company_verification_status: Mapped[str] = mapped_column(
        String(64), default="incomplete_company", nullable=False
    )
    tyc_customer_id: Mapped[str | None] = mapped_column(String(128))
    tyc_candidates: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list, nullable=False)
    company_confirmed_by_user: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("source_message_id", "source_segment_index"),
    )

    @property
    def qcc_company_id(self) -> str | None:
        """兼容历史属性读取；新代码统一使用天眼查客户标识。"""
        return self.tyc_customer_id

    @qcc_company_id.setter
    def qcc_company_id(self, value: str | None) -> None:
        """兼容历史属性写入并转存到天眼查客户标识列。"""
        self.tyc_customer_id = value

    @property
    def qcc_candidates(self) -> list[dict[str, str]]:
        """兼容历史属性读取；新代码统一使用天眼查候选。"""
        return self.tyc_candidates

    @qcc_candidates.setter
    def qcc_candidates(self, value: list[dict[str, str]]) -> None:
        """兼容历史属性写入并转存到天眼查候选列。"""
        self.tyc_candidates = value


class LeadFieldProvenance(Base):
    """保存某个正式字段由哪条来源消息以什么值贡献，供后续安全合并使用。"""

    __tablename__ = "lead_field_provenances"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    source_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=True
    )
    field_name: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    last_ai_synced_value: Mapped[str | None] = mapped_column(Text)
    is_system_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
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
    operation_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    confirmation_source: Mapped[str] = mapped_column(String(32), default="robot", nullable=False)
    confirmed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class SalesLeadContext(Base):
    """保存一名销售最近一次可安全补充的线索上下文及其消息顺序。"""

    __tablename__ = "sales_lead_contexts"

    sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), primary_key=True
    )
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    last_message_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_message_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
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


class LeadProgressSession(Base):
    """保存单个销售一次连续需求录入进度会话及其独立汇报时钟。"""

    __tablename__ = "lead_progress_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    sales_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False, index=True
    )
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_activity_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_report_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_reason: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'closed', 'disabled')", name="ck_lead_progress_session_status"
        ),
        Index(
            "uq_lead_progress_sessions_active_sales_user",
            "sales_user_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
            sqlite_where=text("status = 'active'"),
        ),
        Index("ix_lead_progress_sessions_due", "status", "next_report_at"),
    )


class LeadProgressMessage(Base):
    """把待处理或已确认的需求消息唯一关联到其进度统计窗口。"""

    __tablename__ = "lead_progress_messages"

    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), primary_key=True
    )
    progress_session_id: Mapped[str | None] = mapped_column(
        ForeignKey("lead_progress_sessions.id"), index=True
    )
    status: Mapped[str] = mapped_column(
        String(16), default="processing", server_default="processing", nullable=False
    )
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('awaiting_intent', 'processing', 'included', 'ignored')",
            name="ck_lead_progress_message_status",
        ),
    )


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
    operation_id: Mapped[str | None] = mapped_column(String(128), unique=True)
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


class CrmSyncRecord(Base):
    """保存一个 CRM 提交 generation 及其全部重试的冻结事实。"""

    __tablename__ = "crm_sync_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    operation: Mapped[str] = mapped_column(String(32), nullable=False)
    # generation 只用于 create；update、validation 等历史操作保持 NULL。
    generation: Mapped[int | None] = mapped_column(Integer)
    supersedes_sync_record_id: Mapped[int | None] = mapped_column(
        ForeignKey("crm_sync_records.id")
    )
    smart_table_record_id: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    canonical_payload: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    submitting_sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    submitting_crm_user_id: Mapped[str | None] = mapped_column(String(128))
    crm_lead_id: Mapped[str | None] = mapped_column(String(128))
    crm_lead_owner_user_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_category: Mapped[str | None] = mapped_column(String(32))
    failure_kind: Mapped[str | None] = mapped_column(String(64))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_summary: Mapped[str | None] = mapped_column(String(128))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    response_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "operation != 'create' OR (generation IS NOT NULL AND generation >= 1)",
            name="ck_crm_sync_records_create_generation",
        ),
        # 每个 create generation 独立冻结；generation 1 保留历史兼容语义。
        Index(
            "uq_crm_sync_records_create_generation",
            "lead_id",
            "generation",
            unique=True,
            postgresql_where=(operation == "create"),
            sqlite_where=(operation == "create"),
        ),
        # 一个 generation 最多只能有一个直接 successor；NULL 可由多条历史记录共享。
        Index(
            "uq_crm_sync_records_successor",
            "supersedes_sync_record_id",
            unique=True,
        ),
        Index(
            "uq_crm_sync_records_update_snapshot",
            "lead_id",
            "snapshot_hash",
            unique=True,
            postgresql_where=(operation == "update"),
            sqlite_where=(operation == "update"),
        ),
    )


def latest_crm_create_sync(
    session: Session, lead_id: str, *, for_update: bool = False
) -> CrmSyncRecord | None:
    """读取某条 Lead 最新的 CRM create generation。

    参数：session 为当前数据库会话；lead_id 为目标线索；for_update 表示是否锁定最新行。
    返回值：按 generation、主键倒序的最新 create 记录；不存在时返回 None。
    异常：数据库读取失败时向调用方传播。
    副作用：for_update=True 时在当前事务中锁定返回行。
    """
    statement = (
        select(CrmSyncRecord)
        .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create")
        .order_by(CrmSyncRecord.generation.desc().nullslast(), CrmSyncRecord.id.desc())
        .limit(1)
    )
    if for_update:
        statement = statement.with_for_update()
    return session.scalar(statement)


class MessageRetryAttempt(Base):
    """保存一次失败消息的受保护补充重试，不改变原始消息顺序检查点。"""

    __tablename__ = "message_retry_attempts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        ForeignKey("incoming_messages.message_id"), nullable=False, index=True
    )
    segment_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lead_id: Mapped[str | None] = mapped_column(ForeignKey("leads.id"), index=True)
    operator_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False
    )
    request_id: Mapped[str | None] = mapped_column(String(128), index=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="processing", nullable=False)
    failure_category: Mapped[str | None] = mapped_column(String(32))
    error_summary: Mapped[str | None] = mapped_column(String(128))
    updated_fields: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    protected_fields: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )

    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint(
            "message_id",
            "segment_index",
            "attempt_number",
            name="uq_message_retry_attempts_message_segment_attempt",
        ),
    )


class LeadDiscardRequest(Base):
    """保存线索逻辑废弃请求，作为 CRM 在途事实与线索生命周期之间的协调记录。"""

    __tablename__ = "lead_discard_requests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, unique=True)
    operator_user_id: Mapped[str] = mapped_column(
        ForeignKey("sales_authorizations.wecom_user_id"), nullable=False
    )
    operator_role: Mapped[str] = mapped_column(String(32), nullable=False)
    operation_id: Mapped[str | None] = mapped_column(String(128), unique=True)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CrmCompanyIdentity(Base):
    """保存标准公司名称到 CRM 身份的全局、持久化预留事实。"""

    __tablename__ = "crm_company_identities"

    standard_company_name: Mapped[str] = mapped_column(String(512), primary_key=True)
    crm_lead_id: Mapped[str | None] = mapped_column(String(128))
    crm_lead_owner_user_id: Mapped[str | None] = mapped_column(String(128))
    state: Mapped[str] = mapped_column(String(32), default="reserving", nullable=False)
    creating_lead_id: Mapped[str | None] = mapped_column(ForeignKey("leads.id"))
    creating_sync_record_id: Mapped[int | None] = mapped_column(ForeignKey("crm_sync_records.id"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class AdminLeadCreationOperation(Base):
    """保存管理员人工补建线索的本地与远端协调状态。"""

    __tablename__ = "admin_lead_creation_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, unique=True)
    smart_table_record_id: Mapped[str | None] = mapped_column(String(128))
    request_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    operator_subject: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_role: Mapped[str] = mapped_column(String(64), nullable=False)
    auth_source: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    original_capturing_sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    smart_table_owner_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    remote_update_state: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    final_status: Mapped[str] = mapped_column(String(64), default="processing", nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class SmartTableOwnerTransferOperation(Base):
    """保存智能表格负责人转交的不可逆远端事实与本地收敛状态。"""

    __tablename__ = "smart_table_owner_transfer_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    lead_id: Mapped[str] = mapped_column(ForeignKey("leads.id"), nullable=False, index=True)
    smart_table_record_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    operator_subject: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_role: Mapped[str] = mapped_column(String(64), nullable=False)
    auth_source: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(String(512), nullable=False)
    old_owner_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    new_owner_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    original_capturing_sales_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    crm_lead_owner_user_id: Mapped[str | None] = mapped_column(String(128))
    remote_update_state: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    permission_verification_state: Mapped[str] = mapped_column(
        String(32), default="pending", nullable=False
    )
    final_status: Mapped[str] = mapped_column(String(64), default="processing", nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class ConsoleMaintenanceAudit(Base):
    """记录 Console 管理写操作的统一操作者、原因、前后状态和结果。"""

    __tablename__ = "console_maintenance_audits"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_lead_id)
    operation_id: Mapped[str | None] = mapped_column(String(36), index=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    operation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    object_type: Mapped[str] = mapped_column(String(64), nullable=False)
    object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_subject: Mapped[str] = mapped_column(String(128), nullable=False)
    operator_role: Mapped[str] = mapped_column(String(64), nullable=False)
    auth_source: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(512))
    before_state: Mapped[dict[str, object]] = mapped_column(JSON, default=dict, nullable=False)
    after_state: Mapped[dict[str, object]] = mapped_column(JSON, default=dict, nullable=False)
    result: Mapped[str] = mapped_column(String(64), nullable=False)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_summary: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
