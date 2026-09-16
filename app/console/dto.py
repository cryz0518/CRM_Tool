"""Operations Console 对外使用的脱敏数据传输对象。"""

from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class ConsoleDTO(BaseModel):
    """定义禁止从 ORM 自动暴露字段的 Console DTO 基类。"""

    model_config = ConfigDict(extra="forbid", from_attributes=False)


class ConsoleMessageDTO(ConsoleDTO):
    """展示消息元数据与已脱敏摘要，不承载原始消息正文。"""

    message_id: str
    sales_user_id: str
    sequence: int
    received_at: datetime
    text_summary: str | None = None
    has_raw_payload: bool = False
    attachment_count: int = 0
    resolution_status: str | None = None


class ConsoleLeadDTO(ConsoleDTO):
    """展示线索状态、归属语义和脱敏后的字段摘要。"""

    lead_id: str
    original_capturing_sales_user_id: str
    smart_table_owner_user_id: str
    lifecycle_state: str
    company_region: str
    company_verification_status: str
    company_confirmed_by_user: bool
    masked_field_values: dict[str, str] = Field(default_factory=dict)
    enrichment_field_names: tuple[str, ...] = ()
    updated_at: datetime


class ConsoleAttachmentDTO(ConsoleDTO):
    """展示附件安全和处理元数据，不返回附件内容或存储键。"""

    attachment_id: str
    message_id: str
    media_kind: str
    declared_mime_type: str | None = None
    detected_mime_type: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    scan_status: str
    processing_status: str
    error_summary: str | None = None


class ConsoleTaskDTO(ConsoleDTO):
    """统一展示既有 Outbox、媒体、重试、表格和 CRM 任务状态。"""

    task_id: str
    task_kind: str
    subject_id: str
    status: str
    attempts: int | None = None
    failure_category: str | None = None
    error_summary: str | None = None
    created_at: datetime
    completed_at: datetime | None = None


class ConsoleAIExecutionDTO(ConsoleDTO):
    """展示 AI 调用的运行元数据，不返回 Prompt、响应或原始文本。"""

    execution_id: str
    trace_id: str
    message_id: str | None = None
    lead_id: str | None = None
    operation: str
    provider: str
    model: str | None = None
    status: str
    call_count: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    duration_ms: int | None = None
    error_type: str | None = None
    error_summary: str | None = None
    created_at: datetime
    completed_at: datetime | None = None


class ConsoleSyncDTO(ConsoleDTO):
    """展示外部同步事实和脱敏错误摘要。"""

    sync_id: str
    sync_kind: str
    lead_id: str
    operation: str | None = None
    status: str
    record_id: str | None = None
    attempts: int | None = None
    failure_category: str | None = None
    error_summary: str | None = None
    snapshot_hash: str | None = None
    created_at: datetime
    completed_at: datetime | None = None


class ConsoleConflictDTO(ConsoleDTO):
    """将多个领域状态投影为可检索的冲突事实。"""

    conflict_id: str
    source: str
    conflict_kind: str
    severity: str
    status: str
    lead_id: str | None = None
    message_id: str | None = None
    source_status: str | None = None
    failure_category: str | None = None
    affected_fields: tuple[str, ...] = ()
    detail: str | None = None
    created_at: datetime
    updated_at: datetime


class ConsoleAuditEventDTO(ConsoleDTO):
    """展示业务或 Break-glass 审计的安全白名单字段。"""

    audit_id: str
    audit_kind: str
    event_type: str
    operator_subject: str | None = None
    operator_role: str | None = None
    object_type: str | None = None
    object_id: str | None = None
    access_type: str | None = None
    reason: str | None = None
    request_context: dict[str, str] = Field(default_factory=dict)
    phase: str | None = None
    outcome: str | None = None
    data_returned: bool = False
    detail: str | None = None
    created_at: datetime


class ConsoleHealthStatusDTO(ConsoleDTO):
    """展示单个应用依赖的健康状态。"""

    name: str
    status: str
    detail: str | None = None
    checked_at: datetime


class ConsoleOverviewDTO(ConsoleDTO):
    """展示 Console 首页的健康状态、数量和就绪问题。"""

    services: list[ConsoleHealthStatusDTO]
    counts: dict[str, int]
    readiness_issues: list[str]


class ConsoleConfigIssueDTO(ConsoleDTO):
    """展示管理员预配置或运行配置的脱敏问题。"""

    source: str
    status: str
    issues: list[str] = Field(default_factory=list)


T = TypeVar("T", bound=ConsoleDTO)


class ConsolePage(ConsoleDTO, Generic[T]):
    """承载受限游标分页结果，不允许调用方绕过上限读取全表。"""

    items: list[T]
    next_cursor: str | None = None
