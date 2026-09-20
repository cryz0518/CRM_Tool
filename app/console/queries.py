"""Operations Console 只读查询服务及领域状态投影。"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TypeVar

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.console.conflicts import ConflictProjection
from app.console.dto import (
    ConsoleAIExecutionDTO,
    ConsoleAttachmentDTO,
    ConsoleAuditEventDTO,
    ConsoleConfigIssueDTO,
    ConsoleConflictDTO,
    ConsoleDTO,
    ConsoleLeadDTO,
    ConsoleMessageDTO,
    ConsoleOverviewDTO,
    ConsolePage,
    ConsoleSalesAuthorizationDTO,
    ConsoleSyncDTO,
    ConsoleTaskDTO,
)
from app.console.health import ConsoleHealthProvider
from app.console.masking import MaskingPolicy
from app.console.models import AIExecutionRecord, BreakGlassAccessAudit
from app.leads.models import (
    ConsoleMaintenanceAudit,
    CrmSyncRecord,
    Lead,
    LeadDiscardRequest,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageRetryAttempt,
    SmartTableSync,
)
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    MediaProcessingTask,
    MessageAttachment,
    OutboxEvent,
    SalesAuthorization,
)

T = TypeVar("T", bound=ConsoleDTO)


class ConsoleQueryService:
    """以小而稳定的只读接口隐藏 ORM 查询、脱敏和冲突组合复杂度。"""

    _MAX_PAGE_SIZE = 100

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        health_provider: ConsoleHealthProvider,
        masking_policy: MaskingPolicy | None = None,
    ) -> None:
        """注入数据库、健康探针和可替换脱敏策略。

        参数：session_factory 为只读查询会话工厂；health_provider 为健康状态来源；
        masking_policy 为默认字段脱敏实现。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不读取数据库。
        """
        self._session_factory = session_factory
        self._health_provider = health_provider
        self._masking_policy = masking_policy or MaskingPolicy()

    def get_overview(self) -> ConsoleOverviewDTO:
        """查询 Console 首页数量、健康状态和 readiness 问题。

        返回值：脱敏的 Overview DTO。
        异常：数据库查询异常向调用方传播；健康探针自行转换单项失败。
        副作用：读取数据库并执行健康探针，不修改领域状态。
        """
        with self._session_factory() as session:
            counts = {
                "leads": session.scalar(select(func.count()).select_from(Lead)) or 0,
                "messages": session.scalar(select(func.count()).select_from(IncomingMessage)) or 0,
                "failed_outbox": session.scalar(
                    select(func.count()).select_from(OutboxEvent).where(
                        OutboxEvent.status == "failed_pending_review"
                    )
                )
                or 0,
                "unassigned_messages": session.scalar(
                    select(func.count()).select_from(LeadMessageResolution).where(
                        LeadMessageResolution.status == "unassigned"
                    )
                )
                or 0,
            }
        services = self._health_provider.snapshot()
        readiness_issues = [
            f"{item.name}: {item.detail or item.status}"
            for item in services
            if item.status != "ok"
        ]
        return ConsoleOverviewDTO(
            services=services,
            counts=counts,
            readiness_issues=readiness_issues,
        )

    def list_messages(
        self, *, limit: int = 50, cursor: str | None = None, unassigned: bool = False
    ) -> ConsolePage[ConsoleMessageDTO]:
        """查询脱敏消息元数据，可选只返回待归属消息。

        参数：limit 为页面上限；cursor 为页偏移游标；unassigned 控制待归属筛选。
        返回值：消息 DTO 分页结果。
        异常：非法游标抛出 ValueError；数据库异常向上传播。
        副作用：仅读取消息、附件数量和归属状态。
        """
        offset = self._parse_cursor(cursor)
        attachment_count = (
            select(func.count(MessageAttachment.id))
            .where(MessageAttachment.message_id == IncomingMessage.message_id)
            .correlate(IncomingMessage)
            .scalar_subquery()
        )
        resolution_status = (
            select(LeadMessageResolution.status)
            .where(LeadMessageResolution.message_id == IncomingMessage.message_id)
            .order_by(LeadMessageResolution.id.desc())
            .limit(1)
            .correlate(IncomingMessage)
            .scalar_subquery()
        )
        statement = select(IncomingMessage, attachment_count, resolution_status)
        if unassigned:
            statement = statement.join(
                LeadMessageResolution,
                LeadMessageResolution.message_id == IncomingMessage.message_id,
            ).where(LeadMessageResolution.status == "unassigned")
        statement = statement.order_by(
            IncomingMessage.received_at.desc(), IncomingMessage.message_id.desc()
        )
        with self._session_factory() as session:
            rows = session.execute(
                statement.offset(offset).limit(self._bounded_limit(limit) + 1)
            ).all()
        items = [
            ConsoleMessageDTO(
                message_id=message.message_id,
                sales_user_id=message.sales_user_id,
                sequence=message.sequence,
                received_at=message.received_at,
                text_summary=self._message_summary(message),
                has_raw_payload=bool(message.raw_payload),
                attachment_count=int(attachment_total),
                resolution_status=resolution,
            )
            for message, attachment_total, resolution in rows
        ]
        return self._page(items, limit, offset)

    def list_leads(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
        status: str | None = None,
        smart_table_owner_user_id: str | None = None,
    ) -> ConsolePage[ConsoleLeadDTO]:
        """查询线索生命周期和脱敏字段摘要。

        参数：limit、cursor 控制分页；status 可按线索生命周期筛选；
        smart_table_owner_user_id 可按当前智能表格负责人筛选。
        返回值：线索 DTO 分页结果。
        异常：非法游标抛出 ValueError；数据库异常向上传播。
        副作用：仅读取 Lead 事实。
        """
        statement = select(Lead)
        if status:
            statement = statement.where(Lead.lifecycle_state == status)
        if smart_table_owner_user_id:
            statement = statement.where(
                Lead.smart_table_owner_user_id == smart_table_owner_user_id
            )
        statement = statement.order_by(Lead.updated_at.desc(), Lead.id.desc())
        with self._session_factory() as session:
            leads = session.scalars(
                statement.offset(self._parse_cursor(cursor)).limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page(
            [self._lead_dto(lead) for lead in leads], limit, self._parse_cursor(cursor)
        )

    def list_attachments(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> ConsolePage[ConsoleAttachmentDTO]:
        """查询附件安全和处理元数据，不返回存储键或文件内容。"""
        statement = select(MessageAttachment).order_by(
            MessageAttachment.created_at.desc(), MessageAttachment.id.desc()
        )
        with self._session_factory() as session:
            attachments = session.scalars(
                statement.offset(self._parse_cursor(cursor)).limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page(
            [
                ConsoleAttachmentDTO(
                    attachment_id=item.id,
                    message_id=item.message_id,
                    media_kind=item.media_kind,
                    declared_mime_type=item.declared_mime_type,
                    detected_mime_type=item.detected_mime_type,
                    size_bytes=item.size_bytes,
                    sha256=item.sha256,
                    scan_status=item.scan_status,
                    processing_status=item.processing_status,
                    error_summary=self._safe_text(item.error_summary),
                )
                for item in attachments
            ],
            limit,
            self._parse_cursor(cursor),
        )

    def list_tasks(
        self, *, limit: int = 50, status: str | None = None
    ) -> ConsolePage[ConsoleTaskDTO]:
        """聚合既有任务模型，保持 Lead 生命周期与任务状态分离。"""
        bounded_limit = self._bounded_limit(limit)
        with self._session_factory() as session:
            tasks = self._outbox_tasks(session, bounded_limit, status)
            tasks.extend(self._retry_tasks(session, bounded_limit, status))
            tasks.extend(self._media_tasks(session, bounded_limit, status))
            tasks.extend(self._smart_table_tasks(session, bounded_limit, status))
            tasks.extend(self._crm_tasks(session, bounded_limit, status))
            tasks.extend(self._discard_tasks(session, bounded_limit, status))
        tasks.sort(key=lambda item: (item.created_at, item.task_id), reverse=True)
        return self._page(tasks, limit)

    def list_ai_executions(
        self, *, limit: int = 50, cursor: str | None = None, status: str | None = None
    ) -> ConsolePage[ConsoleAIExecutionDTO]:
        """查询 AI 执行元数据，不返回 Prompt、响应或原始文本。"""
        statement = select(AIExecutionRecord)
        if status:
            statement = statement.where(AIExecutionRecord.status == status)
        statement = statement.order_by(
            AIExecutionRecord.created_at.desc(), AIExecutionRecord.id.desc()
        )
        with self._session_factory() as session:
            records = session.scalars(
                statement.offset(self._parse_cursor(cursor)).limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page(
            [
                ConsoleAIExecutionDTO(
                    execution_id=item.id,
                    trace_id=item.trace_id,
                    message_id=item.message_id,
                    lead_id=item.lead_id,
                    operation=item.operation,
                    provider=item.provider,
                    model=item.model,
                    status=item.status,
                    call_count=item.call_count,
                    input_tokens=item.input_tokens,
                    output_tokens=item.output_tokens,
                    duration_ms=item.duration_ms,
                    error_type=item.error_type,
                    error_summary=self._safe_text(item.error_summary),
                    created_at=item.created_at,
                    completed_at=item.completed_at,
                )
                for item in records
            ],
            limit,
            self._parse_cursor(cursor),
        )

    def list_smart_table_syncs(self, *, limit: int = 50) -> ConsolePage[ConsoleSyncDTO]:
        """查询当前智能表格同步事实。"""
        with self._session_factory() as session:
            records = session.scalars(
                select(SmartTableSync)
                .order_by(SmartTableSync.created_at.desc(), SmartTableSync.id.desc())
                .limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page(
            [
                ConsoleSyncDTO(
                    sync_id=str(item.id),
                    sync_kind="smart_table",
                    lead_id=item.lead_id,
                    status=item.status,
                    record_id=item.smart_table_record_id,
                    error_summary=self._safe_text(item.error_summary),
                    created_at=item.created_at,
                    completed_at=item.completed_at,
                )
                for item in records
            ],
            limit,
        )

    def list_crm_syncs(self, *, limit: int = 50) -> ConsolePage[ConsoleSyncDTO]:
        """查询 CRM 同步事实和快照 hash，不返回 CRM Payload。"""
        with self._session_factory() as session:
            records = session.scalars(
                select(CrmSyncRecord)
                .order_by(CrmSyncRecord.created_at.desc(), CrmSyncRecord.id.desc())
                .limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page(
            [
                ConsoleSyncDTO(
                    sync_id=str(item.id),
                    sync_kind="crm",
                    lead_id=item.lead_id,
                    operation=item.operation,
                    status=item.status,
                    record_id=item.crm_lead_id,
                    attempts=item.attempts,
                    failure_category=item.failure_category,
                    failure_kind=item.failure_kind,
                    failure_code=item.failure_code,
                    error_summary=self._safe_text(item.failure_summary or item.response_summary),
                    snapshot_hash=item.snapshot_hash,
                    created_at=item.created_at,
                    completed_at=item.completed_at,
                )
                for item in records
            ],
            limit,
        )

    def list_config(self) -> ConsolePage[ConsoleConfigIssueDTO]:
        """返回当前依赖 readiness 和配置风险的安全摘要。"""
        items = [
            ConsoleConfigIssueDTO(
                source=status.name,
                status=status.status,
                issues=[status.detail] if status.detail else [],
            )
            for status in self._health_provider.snapshot()
        ]
        # 只展示授权与 CRM 映射的计数，不在配置页暴露成员姓名或凭据。
        with self._session_factory() as session:
            authorized_count = session.scalar(
                select(func.count(SalesAuthorization.wecom_user_id)).where(
                    SalesAuthorization.is_authorized.is_(True),
                    SalesAuthorization.is_active.is_(True),
                )
            ) or 0
            missing_mapping_count = session.scalar(
                select(func.count(SalesAuthorization.wecom_user_id)).where(
                    SalesAuthorization.is_authorized.is_(True),
                    SalesAuthorization.is_active.is_(True),
                    or_(
                        SalesAuthorization.crm_user_id.is_(None),
                        func.trim(SalesAuthorization.crm_user_id) == "",
                    ),
                )
            ) or 0
        items.append(
            ConsoleConfigIssueDTO(
                source="sales_authorization",
                status="ok" if missing_mapping_count == 0 else "not_ready",
                issues=[
                    f"已启用销售授权 {authorized_count} 条；CRM 映射缺失 {missing_mapping_count} 条"
                ],
            )
        )
        return ConsolePage(items=items)

    def list_sales_authorizations(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> ConsolePage[ConsoleSalesAuthorizationDTO]:
        """分页返回销售授权目录及其 CRM 映射异常影响范围。

        参数：limit 与 cursor 控制只读分页。
        返回值：不含 CRM 用户标识的销售授权目录 DTO 分页结果。
        异常：非法游标抛出 ValueError；数据库读取失败时向上传播。
        副作用：仅读取授权目录和待同步线索，不修改任何业务状态。
        """
        offset = self._parse_cursor(cursor)
        bounded_limit = self._bounded_limit(limit)
        with self._session_factory() as session:
            # 目录和待同步线索均为既有事实；聚合不回填 CRM 映射，也不改写负责人。
            authorizations = session.scalars(
                select(SalesAuthorization)
                .order_by(SalesAuthorization.updated_at.desc(), SalesAuthorization.wecom_user_id)
                .offset(offset)
                .limit(bounded_limit + 1)
            ).all()
            pending_counts: dict[str, int] = {
                owner_user_id: count
                for owner_user_id, count in session.execute(
                    select(Lead.smart_table_owner_user_id, func.count())
                    .where(Lead.lifecycle_state.in_(("pending_create", "pending_update")))
                    .group_by(Lead.smart_table_owner_user_id)
                ).tuples()
            }
        return self._page(
            [
                # 仅把待处理线索归因给实际阻塞 CRM 提交的有效销售映射异常。
                ConsoleSalesAuthorizationDTO(
                    wecom_user_id=item.wecom_user_id,
                    display_name=item.display_name,
                    department_id=item.department_id,
                    is_authorized=item.is_authorized,
                    is_active=item.is_active,
                    crm_mapping_status=self._crm_mapping_status(
                        item.crm_user_id, item.is_authorized, item.is_active
                    ),
                    affected_pending_lead_count=(
                        pending_counts.get(item.wecom_user_id, 0)
                        if self._crm_mapping_status(
                            item.crm_user_id, item.is_authorized, item.is_active
                        )
                        == "mapping_missing"
                        else 0
                    ),
                    created_by=item.created_by,
                    updated_by=item.updated_by,
                )
                for item in authorizations
            ],
            limit,
            offset,
        )

    def list_mapping_missing_leads(
        self, sales_user_id: str, *, limit: int = 50, cursor: str | None = None
    ) -> ConsolePage[ConsoleLeadDTO]:
        """返回指定映射异常销售名下、当前等待 CRM 提交的脱敏线索。

        参数：sales_user_id 为授权目录中的企业微信用户标识；limit 与 cursor 控制分页。
        返回值：仅含 pending_create 与 pending_update 的线索 DTO。
        异常：非法游标抛出 ValueError；数据库读取失败时向上传播。
        副作用：仅读取目录和线索，不修改授权、映射或线索状态。
        """
        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, sales_user_id)
            if authorization is None or self._crm_mapping_status(
                authorization.crm_user_id, authorization.is_authorized, authorization.is_active
            ) != "mapping_missing":
                return ConsolePage(items=[])
            offset = self._parse_cursor(cursor)
            leads = session.scalars(
                select(Lead)
                .where(
                    Lead.smart_table_owner_user_id == sales_user_id,
                    Lead.lifecycle_state.in_(("pending_create", "pending_update")),
                )
                .order_by(Lead.updated_at.desc(), Lead.id.desc())
                .offset(offset)
                .limit(self._bounded_limit(limit) + 1)
            ).all()
        return self._page([self._lead_dto(lead) for lead in leads], limit, offset)

    def list_conflicts(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
        source: str | None = None,
    ) -> ConsolePage[ConsoleConflictDTO]:
        """查询可按来源筛选并使用稳定游标分页的完整冲突投影。

        参数：limit 为单页上限；cursor 为上一页返回的稳定游标；source 为冲突来源筛选。
        返回值：不含敏感原值的冲突 DTO 分页结果。
        异常：非法 limit 或 cursor 抛出 ValueError；数据库异常向上传播。
        副作用：仅读取既有线索、任务和审计事实，不修改领域状态。
        """
        with self._session_factory() as session:
            conflicts: list[ConsoleConflictDTO] = []
            lead_statement = select(Lead).where(
                or_(
                    Lead.lifecycle_state == "company_identity_change_pending_review",
                    Lead.company_verification_status.in_(
                        tuple(ConflictProjection._VERIFICATION_CONFLICTS)
                    ),
                    LeadFieldProvenance.is_user_modified.is_(True),
                )
            ).outerjoin(LeadFieldProvenance).distinct()
            for lead in session.scalars(lead_statement).all():
                provenances = session.scalars(
                    select(LeadFieldProvenance).where(LeadFieldProvenance.lead_id == lead.id)
                ).all()
                conflicts.extend(ConflictProjection.for_lead(lead, provenances))

            for event in session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.status.in_(tuple(ConflictProjection._TASK_CONFLICT_STATUSES)))
            ).all():
                conflict = ConflictProjection.for_task(
                    task_kind="outbox",
                    subject_id=str(event.id),
                    status=event.status,
                    failure_category=event.failure_category,
                    detail=self._safe_text(event.failure_summary),
                    created_at=event.created_at,
                    message_id=event.message_id,
                )
                if conflict is not None:
                    conflicts.append(conflict)

            for attempt in session.scalars(
                select(MessageRetryAttempt)
                .where(MessageRetryAttempt.status.in_(tuple(ConflictProjection._TASK_CONFLICT_STATUSES)))
            ).all():
                conflict = ConflictProjection.for_task(
                    task_kind="message_retry",
                    subject_id=str(attempt.id),
                    status=attempt.status,
                    failure_category=attempt.failure_category,
                    detail=self._safe_text(attempt.error_summary),
                    created_at=attempt.created_at,
                    lead_id=attempt.lead_id,
                    message_id=attempt.message_id,
                )
                if conflict is not None:
                    conflicts.append(conflict)

            for media_task in session.scalars(
                select(MediaProcessingTask)
                .where(
                    MediaProcessingTask.status.in_(
                        tuple(ConflictProjection._TASK_CONFLICT_STATUSES)
                    )
                )
            ).all():
                conflict = ConflictProjection.for_task(
                    task_kind="media",
                    subject_id=str(media_task.id),
                    status=media_task.status,
                    failure_category=None,
                    detail=self._safe_text(media_task.error_summary),
                    created_at=media_task.created_at,
                )
                if conflict is not None:
                    conflicts.append(conflict)

            for smart_task in session.scalars(
                select(SmartTableSync)
                .where(
                    SmartTableSync.status.in_(tuple(ConflictProjection._TASK_CONFLICT_STATUSES))
                )
            ).all():
                conflict = ConflictProjection.for_task(
                    task_kind="smart_table",
                    subject_id=str(smart_task.id),
                    status=smart_task.status,
                    failure_category=None,
                    detail=self._safe_text(smart_task.error_summary),
                    created_at=smart_task.created_at,
                    lead_id=smart_task.lead_id,
                )
                if conflict is not None:
                    conflicts.append(conflict)

            for crm_task in session.scalars(
                select(CrmSyncRecord)
                .where(CrmSyncRecord.status.in_(tuple(ConflictProjection._TASK_CONFLICT_STATUSES)))
            ).all():
                conflict = ConflictProjection.for_task(
                    task_kind="crm",
                    subject_id=str(crm_task.id),
                    status=crm_task.status,
                    failure_category=crm_task.failure_category,
                    detail=self._safe_text(
                        crm_task.failure_summary or crm_task.response_summary
                    ),
                    created_at=crm_task.created_at,
                    lead_id=crm_task.lead_id,
                )
                if conflict is not None:
                    conflicts.append(conflict)

            for resolution in session.scalars(
                select(LeadMessageResolution)
                .where(LeadMessageResolution.status == "unassigned")
            ).all():
                conflicts.append(
                    ConsoleConflictDTO(
                        conflict_id=f"resolution:{resolution.id}",
                        source="assignment",
                        conflict_kind="assignment",
                        severity="review_required",
                        status="unassigned",
                        lead_id=resolution.lead_id,
                        message_id=resolution.message_id,
                        source_status=resolution.status,
                        detail="消息尚未可靠归属线索",
                        created_at=resolution.created_at,
                        updated_at=resolution.created_at,
                    )
                )

            for audit in session.scalars(
                select(BusinessAuditEvent)
                .where(BusinessAuditEvent.event_type == "discard_request_not_effective")
            ).all():
                conflict = ConflictProjection.for_audit(
                    audit_id=str(audit.id),
                    event_type=audit.event_type,
                    message_id=audit.message_id,
                    created_at=audit.created_at,
                )
                if conflict is not None:
                    conflicts.append(conflict)
        if source is not None:
            # 来源筛选让运维可单独翻阅某一类任务，避免跨来源合并时丢失定位上下文。
            conflicts = [item for item in conflicts if item.source == source]

        conflicts.sort(
            key=lambda item: (self._normalise_datetime(item.updated_at), item.conflict_id),
            reverse=True,
        )
        conflict_cursor = self._parse_conflict_cursor(cursor)
        if conflict_cursor is not None:
            # 游标按“最后一条记录之后”继续，避免新增记录导致 offset 页面漂移。
            conflicts = [
                item
                for item in conflicts
                if (
                    self._normalise_datetime(item.updated_at),
                    item.conflict_id,
                )
                < conflict_cursor
            ]

        bounded_limit = self._bounded_limit(limit)
        page_items = conflicts[:bounded_limit]
        next_cursor = (
            self._encode_conflict_cursor(page_items[-1])
            if len(conflicts) > bounded_limit and page_items
            else None
        )
        return ConsolePage(items=page_items, next_cursor=next_cursor)

    def list_audits(self, *, limit: int = 50) -> ConsolePage[ConsoleAuditEventDTO]:
        """查询业务审计和 Break-glass 审计的安全白名单字段。"""
        with self._session_factory() as session:
            business = session.scalars(
                select(BusinessAuditEvent)
                .order_by(BusinessAuditEvent.created_at.desc(), BusinessAuditEvent.id.desc())
                .limit(self._bounded_limit(limit))
            ).all()
            break_glass = session.scalars(
                select(BreakGlassAccessAudit)
                .order_by(BreakGlassAccessAudit.created_at.desc(), BreakGlassAccessAudit.id.desc())
                .limit(self._bounded_limit(limit))
            ).all()
            maintenance = session.scalars(
                select(ConsoleMaintenanceAudit)
                .order_by(
                    ConsoleMaintenanceAudit.created_at.desc(),
                    ConsoleMaintenanceAudit.id.desc(),
                )
                .limit(self._bounded_limit(limit))
            ).all()
        items = [
            ConsoleAuditEventDTO(
                audit_id=str(item.id),
                audit_kind="business",
                event_type=item.event_type,
                operator_subject=item.sales_user_id,
                object_type="message",
                object_id=item.message_id,
                detail=self._business_audit_detail(item),
                created_at=item.created_at,
            )
            for item in business
        ]
        items.extend(
            ConsoleAuditEventDTO(
                audit_id=item.id,
                audit_kind="break_glass",
                event_type=f"{item.phase}:{item.outcome}",
                operator_subject=item.operator_subject,
                operator_role=item.operator_role,
                object_type=item.object_type,
                object_id=item.object_id,
                access_type=item.access_type,
                reason=self._masking_policy.mask_text(item.reason),
                request_context=self._safe_request_context(item.request_context),
                phase=item.phase,
                outcome=item.outcome,
                data_returned=item.data_returned,
                detail="Break-glass 访问审计（原因已记录）",
                created_at=item.created_at,
            )
            for item in break_glass
        )
        items.extend(
            ConsoleAuditEventDTO(
                audit_id=item.id,
                audit_kind="maintenance",
                event_type=item.operation_type,
                operator_subject=item.operator_subject,
                operator_role=item.operator_role,
                object_type=item.object_type,
                object_id=item.object_id,
                reason=self._masking_policy.mask_text(item.reason or ""),
                request_context={"auth_source": item.auth_source, "request_id": item.request_id},
                outcome=item.result,
                detail=item.failure_summary or f"管理维护结果：{item.result}",
                created_at=item.created_at,
            )
            for item in maintenance
        )
        items.sort(key=lambda item: (item.created_at, item.audit_id), reverse=True)
        return self._page(items, limit)

    def get_message(self, message_id: str) -> ConsoleMessageDTO | None:
        """查询单条消息的默认脱敏详情。"""
        with self._session_factory() as session:
            message = session.get(IncomingMessage, message_id)
            if message is None:
                return None
            attachment_count = session.scalar(
                select(func.count(MessageAttachment.id)).where(
                    MessageAttachment.message_id == message_id
                )
            ) or 0
            resolution = session.scalar(
                select(LeadMessageResolution.status)
                .where(LeadMessageResolution.message_id == message_id)
                .order_by(LeadMessageResolution.id.desc())
                .limit(1)
            )
        return ConsoleMessageDTO(
            message_id=message.message_id,
            sales_user_id=message.sales_user_id,
            sequence=message.sequence,
            received_at=message.received_at,
            text_summary=self._message_summary(message),
            has_raw_payload=bool(message.raw_payload),
            attachment_count=int(attachment_count),
            resolution_status=resolution,
        )

    def get_lead(self, lead_id: str) -> ConsoleLeadDTO | None:
        """查询单条线索的默认脱敏详情。"""
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
        return self._lead_dto(lead) if lead is not None else None

    def _lead_dto(self, lead: Lead) -> ConsoleLeadDTO:
        """将 Lead ORM 对象转换为不含原始字段值的 DTO。"""
        masked_values = {
            field_name: self._masking_policy.mask_field(field_name, value) or "[已隐藏]"
            for field_name, value in lead.field_values.items()
        }
        return ConsoleLeadDTO(
            lead_id=lead.id,
            original_capturing_sales_user_id=lead.original_capturing_sales_user_id,
            smart_table_owner_user_id=lead.smart_table_owner_user_id,
            lifecycle_state=lead.lifecycle_state,
            company_region=lead.company_region,
            company_verification_status=lead.company_verification_status,
            company_confirmed_by_user=lead.company_confirmed_by_user,
            masked_field_values=masked_values,
            enrichment_field_names=tuple(sorted(lead.enrichment_values)),
            updated_at=lead.updated_at,
        )

    @staticmethod
    def _message_summary(message: IncomingMessage) -> str:
        """根据消息类型生成不含客户描述的安全摘要。

        参数：message 为已持久化的消息事实，仅读取媒体处理标记。
        返回值：不包含 normalized_text、raw payload 或客户原始描述的固定摘要。
        异常：无。
        副作用：无，不读取或修改消息正文。
        """
        # 默认 Console 只展示结构化消息类型，绝不将原始正文伪装成摘要返回。
        if message.requires_media_enrichment:
            return "已接收媒体消息，原文默认隐藏"
        return "已接收文本消息，原文默认隐藏"

    def _safe_text(self, value: str | None) -> str | None:
        """统一脱敏并限制错误摘要长度，避免传递外部响应正文。"""
        if value is None:
            return None
        masked = self._masking_policy.mask_text(value)
        return masked[:256] if masked is not None else None

    @staticmethod
    def _crm_mapping_status(
        crm_user_id: str | None, is_authorized: bool, is_active: bool
    ) -> str:
        """将 CRM 用户标识和销售有效状态转换为控制台可展示的非敏感映射状态。

        参数：crm_user_id 为目录中的 CRM 用户标识；is_authorized 和 is_active 为销售有效状态。
        返回值：mapped、mapping_missing 或不要求 CRM 映射的 not_required。
        异常：无。
        副作用：无，不修改授权目录或 CRM 映射。
        """
        if not is_authorized or not is_active:
            return "not_required"
        return "mapped" if crm_user_id is not None and crm_user_id.strip() else "mapping_missing"

    @staticmethod
    def _normalise_datetime(value: datetime) -> datetime:
        """将数据库返回的有无时区时间统一为 UTC，供游标稳定比较。

        参数：value 为数据库记录中的时间值。
        返回值：带 UTC 时区的时间值。
        异常：无。
        副作用：无，不修改原时间对象。
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _encode_conflict_cursor(cls, item: ConsoleConflictDTO) -> str:
        """编码冲突列表最后一条记录的排序键为不透明游标。

        参数：item 为当前页最后一条冲突 DTO。
        返回值：包含更新时间和冲突标识的不透明 URL-safe 游标。
        异常：序列化失败时向上传播异常。
        副作用：无，不修改冲突 DTO。
        """
        payload = {
            "updated_at": cls._normalise_datetime(item.updated_at).isoformat(),
            "conflict_id": item.conflict_id,
        }
        return base64.urlsafe_b64encode(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        ).decode()

    @classmethod
    def _parse_conflict_cursor(cls, cursor: str | None) -> tuple[datetime, str] | None:
        """解析冲突游标并校验其排序键结构。

        参数：cursor 为上一页返回的不透明游标，可为空表示第一页。
        返回值：规范化后的更新时间和冲突标识；无游标时返回 None。
        异常：游标编码、JSON 或排序键结构非法时抛出 ValueError。
        副作用：无，不访问数据库。
        """
        if cursor is None:
            return None
        try:
            payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            updated_at = datetime.fromisoformat(payload["updated_at"])
            conflict_id = payload["conflict_id"]
        except (
            binascii.Error,
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as error:
            raise ValueError("Console 冲突 cursor 非法") from error
        if not isinstance(conflict_id, str):
            raise ValueError("Console 冲突 cursor 非法")
        return cls._normalise_datetime(updated_at), conflict_id

    def _business_audit_detail(self, item: BusinessAuditEvent) -> str:
        """将业务审计详情压缩为允许展示的键值摘要。"""
        allowed_keys = {
            "lead_id",
            "record_id",
            "crm_sync_record_id",
            "old_standard_company_name",
            "new_candidate_company_name",
            "status",
            "failure_category",
        }
        values = [
            f"{key}={self._masking_policy.mask_text(str(item.details[key]))}"
            for key in sorted(allowed_keys)
            if key in item.details and item.details[key] is not None
        ]
        return "; ".join(values) if values else "业务审计事件"

    @staticmethod
    def _safe_request_context(context: Mapping[str, object]) -> dict[str, str]:
        """只保留 Break-glass 请求上下文中的固定安全字段。"""
        return {
            key: str(context[key])[:128]
            for key in ("route", "source")
            if key in context and context[key] is not None
        }

    @classmethod
    def _bounded_limit(cls, limit: int) -> int:
        """将页面大小限制在 1 到 100 之间，拒绝无界查询。"""
        if limit < 1:
            raise ValueError("Console 查询 limit 必须大于 0")
        return min(limit, cls._MAX_PAGE_SIZE)

    @staticmethod
    def _parse_cursor(cursor: str | None) -> int:
        """解析只含非负偏移的内部分页游标。"""
        if cursor is None:
            return 0
        try:
            offset = int(cursor)
        except ValueError as error:
            raise ValueError("Console 查询 cursor 非法") from error
        if offset < 0:
            raise ValueError("Console 查询 cursor 非法")
        return offset

    @classmethod
    def _page(cls, items: Sequence[T], limit: int, offset: int = 0) -> ConsolePage[T]:
        """截断查询结果并生成下一页游标。"""
        bounded_limit = cls._bounded_limit(limit)
        values = list(items)
        has_more = len(values) > bounded_limit
        return ConsolePage(
            items=values[:bounded_limit],
            next_cursor=str(offset + bounded_limit) if has_more else None,
        )

    def _outbox_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取 Outbox 任务并映射为统一任务 DTO。"""
        statement = select(OutboxEvent).order_by(OutboxEvent.created_at.desc()).limit(limit)
        if status:
            statement = statement.where(OutboxEvent.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="outbox",
                subject_id=item.message_id,
                status=item.status,
                attempts=item.attempts,
                failure_category=item.failure_category,
                error_summary=self._safe_text(item.failure_summary),
                created_at=item.created_at,
                completed_at=item.failed_at,
            )
            for item in session.scalars(statement).all()
        ]

    def _retry_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取 T14 受保护补充重试任务。"""
        statement = (
            select(MessageRetryAttempt)
            .order_by(MessageRetryAttempt.created_at.desc())
            .limit(limit)
        )
        if status:
            statement = statement.where(MessageRetryAttempt.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="message_retry",
                subject_id=item.message_id,
                status=item.status,
                attempts=item.attempt_number,
                failure_category=item.failure_category,
                error_summary=self._safe_text(item.error_summary),
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in session.scalars(statement).all()
        ]

    def _media_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取媒体 OCR/ASR 任务。"""
        statement = (
            select(MediaProcessingTask)
            .order_by(MediaProcessingTask.created_at.desc())
            .limit(limit)
        )
        if status:
            statement = statement.where(MediaProcessingTask.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="media",
                subject_id=item.attachment_id,
                status=item.status,
                attempts=item.attempts,
                error_summary=self._safe_text(item.error_summary),
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in session.scalars(statement).all()
        ]

    def _smart_table_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取智能表格同步任务。"""
        statement = select(SmartTableSync).order_by(SmartTableSync.created_at.desc()).limit(limit)
        if status:
            statement = statement.where(SmartTableSync.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="smart_table_sync",
                subject_id=item.lead_id,
                status=item.status,
                error_summary=self._safe_text(item.error_summary),
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in session.scalars(statement).all()
        ]

    def _crm_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取 CRM 同步任务及 T14 外部未知状态。"""
        statement = select(CrmSyncRecord).order_by(CrmSyncRecord.created_at.desc()).limit(limit)
        if status:
            statement = statement.where(CrmSyncRecord.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="crm_sync",
                subject_id=item.lead_id,
                status=item.status,
                attempts=item.attempts,
                failure_category=item.failure_category,
                failure_kind=item.failure_kind,
                failure_code=item.failure_code,
                error_summary=self._safe_text(item.failure_summary or item.response_summary),
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in session.scalars(statement).all()
        ]

    def _discard_tasks(
        self, session: Session, limit: int, status: str | None
    ) -> list[ConsoleTaskDTO]:
        """读取线索废弃请求任务，不执行废弃操作。"""
        statement = (
            select(LeadDiscardRequest)
            .order_by(LeadDiscardRequest.created_at.desc())
            .limit(limit)
        )
        if status:
            statement = statement.where(LeadDiscardRequest.status == status)
        return [
            ConsoleTaskDTO(
                task_id=str(item.id),
                task_kind="lead_discard",
                subject_id=item.lead_id,
                status=item.status,
                created_at=item.created_at,
                completed_at=item.completed_at,
            )
            for item in session.scalars(statement).all()
        ]
