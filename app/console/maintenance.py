"""Operations Console 管理写入的编排与统一审计服务。"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.console.auth import AdminPrincipal
from app.core.failures import safe_audit_text, safe_failure_summary
from app.leads.admin_create import AdminLeadCreationResult, AdminLeadCreationService
from app.leads.discard import LeadDiscardResult, LeadDiscardService
from app.leads.models import ConsoleMaintenanceAudit, Lead
from app.leads.service import FirstTextLeadWorkspaceService, LeadReassignmentService
from app.leads.transfer import SmartTableOwnerTransferResult, SmartTableOwnerTransferService
from app.messaging.models import SalesAuthorization

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsoleMaintenanceResult:
    """承载 Console 维护操作的安全结果字段。"""

    operation_id: str | None
    status: str
    lead_id: str | None = None
    message_id: str | None = None
    attempt_id: int | None = None
    record_id: str | None = None
    detail: str | None = None


class ConsoleMaintenanceService:
    """为 Console 路由提供 auth 后的单一管理写入入口。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        retry_service: FirstTextLeadWorkspaceService,
        reassignment_service: LeadReassignmentService,
        discard_service: LeadDiscardService,
        admin_create_service: AdminLeadCreationService,
        transfer_service: SmartTableOwnerTransferService,
    ) -> None:
        """注入既有 domain service 和 T16 新增服务，禁止路由持有 ORM。"""

        self._session_factory = session_factory
        self._retry = retry_service
        self._reassignment = reassignment_service
        self._discard = discard_service
        self._admin_create = admin_create_service
        self._transfer = transfer_service

    def require_admin(self, principal: AdminPrincipal) -> None:
        """校验 Console principal 与持久化 SalesAuthorization 的双重管理员条件。"""

        with self._session_factory() as session:
            operator = session.get(SalesAuthorization, principal.subject)
            if operator is None or not operator.is_active or not operator.is_administrator:
                raise PermissionError("Console 主体不是 active administrator")

    def retry_failed_message(
        self, principal: AdminPrincipal, *, message_id: str, segment_index: int, request_id: str
    ) -> ConsoleMaintenanceResult:
        """复用 T14 retry domain service，并写入最小重试审计。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        replay = self._begin_audit(
            request_id, "retry", "message", message_id, principal,
            before={"segment_index": segment_index}, reason=None,
        )
        if replay is not None:
            return replay
        try:
            result = self._retry.retry_failed_message(
                message_id,
                operator_user_id=principal.subject,
                segment_index=segment_index,
                request_id=request_id,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "retry",
                "message",
                message_id,
                principal,
                None,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_retry_failed",
                extra={
                    "message_id": message_id,
                    "segment_index": segment_index,
                    "request_id": request_id,
                },
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "retry",
            "message",
            message_id,
            principal,
            before={"segment_index": segment_index},
            after={"status": result.status.value, "attempt_id": result.attempt_id},
        )
        return ConsoleMaintenanceResult(
            operation_id=str(result.attempt_id) if result.attempt_id is not None else None,
            status=result.status.value,
            message_id=message_id,
            attempt_id=result.attempt_id,
        )

    def reassign(
        self,
        principal: AdminPrincipal,
        *,
        message_id: str,
        segment_index: int,
        new_lead_id: str,
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """复用 T14 LeadReassignmentService，不在 Console 重实现归属规则。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        replay = self._begin_audit(
            request_id, "reassign", "message", message_id, principal,
            before={"segment_index": segment_index}, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            self._reassignment.reassign(
                message_id,
                segment_index,
                new_lead_id,
                principal.subject,
                reason,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "reassign",
                "message",
                message_id,
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_reassign_failed",
                extra={"message_id": message_id, "request_id": request_id},
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "reassign",
            "message",
            message_id,
            principal,
            before={"segment_index": segment_index},
            after={"new_lead_id": new_lead_id, "status": "succeeded"},
            reason=reason,
        )
        return ConsoleMaintenanceResult(None, "succeeded", message_id=message_id)

    def discard(
        self,
        principal: AdminPrincipal,
        *,
        lead_id: str,
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """复用 T14 LeadDiscardService，不物理删除线索或表格记录。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        before_state = self._lead_state(lead_id)
        replay = self._begin_audit(
            request_id, "discard", "lead", lead_id, principal,
            before=before_state, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            result: LeadDiscardResult = self._discard.discard(lead_id, principal.subject, reason)
        except Exception as exc:
            self._audit_failure(
                request_id,
                "discard",
                "lead",
                lead_id,
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_discard_failed",
                extra={"lead_id": lead_id, "request_id": request_id},
                exc_info=True,
            )
            raise
        status = str(result.status)
        self._audit_success(
            request_id,
            "discard",
            "lead",
            lead_id,
            principal,
            before=before_state,
            after={**self._lead_state(lead_id), "status": status},
            reason=reason,
        )
        return ConsoleMaintenanceResult(None, status, lead_id=lead_id)

    def create_lead(
        self,
        principal: AdminPrincipal,
        *,
        original_capturing_sales_user_id: str,
        smart_table_owner_user_id: str,
        field_values: dict[str, str],
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """调用独立管理员创建服务，明确四种身份的初始边界。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        replay = self._begin_audit(
            request_id, "create", "lead", "pending", principal,
            before={}, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            result: AdminLeadCreationResult = self._admin_create.create(
                original_capturing_sales_user_id=original_capturing_sales_user_id,
                smart_table_owner_user_id=smart_table_owner_user_id,
                field_values=field_values,
                operator_subject=principal.subject,
                operator_role="administrator",
                auth_source=principal.auth_source,
                request_id=request_id,
                reason=reason,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "create",
                "lead",
                "pending",
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_create_failed",
                extra={"operator": principal.subject, "request_id": request_id},
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "create",
            "lead",
            result.lead_id,
            principal,
            before={},
            after={
                **self._lead_state(result.lead_id),
                "status": result.status,
                "operation_id": result.operation_id,
            },
            reason=reason,
            result=result.status,
            operation_id=result.operation_id,
        )
        return ConsoleMaintenanceResult(
            result.operation_id,
            result.status,
            lead_id=result.lead_id,
            record_id=None,
            detail=result.failure_summary,
        )

    def transfer_owner(
        self,
        principal: AdminPrincipal,
        *,
        lead_id: str,
        new_owner_user_id: str,
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """调用独立负责人转交服务，不允许 Console 直接更新 ORM。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        before_state = self._lead_state(lead_id)
        replay = self._begin_audit(
            request_id, "transfer", "lead", lead_id, principal,
            before=before_state, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            result: SmartTableOwnerTransferResult = self._transfer.transfer(
                lead_id=lead_id,
                new_owner_user_id=new_owner_user_id,
                operator_subject=principal.subject,
                operator_role="administrator",
                auth_source=principal.auth_source,
                request_id=request_id,
                reason=reason,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "transfer",
                "lead",
                lead_id,
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_transfer_failed",
                extra={"lead_id": lead_id, "request_id": request_id},
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "transfer",
            "lead",
            lead_id,
            principal,
            before=before_state,
            after={
                **self._lead_state(lead_id),
                "smart_table_owner_user_id": new_owner_user_id,
                "status": result.status,
                "operation_id": result.operation_id,
            },
            reason=reason,
            result=result.status,
            operation_id=result.operation_id,
        )
        return ConsoleMaintenanceResult(
            result.operation_id,
            result.status,
            lead_id=lead_id,
            detail=result.failure_summary,
        )

    def reconcile_transfer(
        self,
        principal: AdminPrincipal,
        *,
        operation_id: str,
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """通过受控 recovery 入口核验远端负责人并恢复既有 transfer operation。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        replay = self._begin_audit(
            request_id, "transfer_reconcile", "transfer_operation", operation_id, principal,
            before={"operation_id": operation_id}, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            result: SmartTableOwnerTransferResult = self._transfer.reconcile(
                operation_id=operation_id,
                operator_subject=principal.subject,
                operator_role="administrator",
                auth_source=principal.auth_source,
                request_id=request_id,
                reason=reason,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "transfer_reconcile",
                "transfer_operation",
                operation_id,
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_transfer_reconcile_failed",
                extra={"operation_id": operation_id, "request_id": request_id},
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "transfer_reconcile",
            "transfer_operation",
            operation_id,
            principal,
            before={"operation_id": operation_id},
            after={
                **self._lead_state(result.lead_id),
                "status": result.status,
                "operation_id": result.operation_id,
            },
            reason=reason,
            result=result.status,
            operation_id=result.operation_id,
        )
        return ConsoleMaintenanceResult(
            result.operation_id,
            result.status,
            lead_id=result.lead_id,
            detail=result.failure_summary,
        )

    def reconcile_create(
        self,
        principal: AdminPrincipal,
        *,
        operation_id: str,
        reason: str,
        request_id: str,
    ) -> ConsoleMaintenanceResult:
        """通过受控 recovery 入口核验管理员补建的远端记录。"""

        self._require_request_id(request_id)
        self.require_admin(principal)
        reason = self._require_reason(reason)
        replay = self._begin_audit(
            request_id, "create_reconcile", "creation_operation", operation_id, principal,
            before={"operation_id": operation_id}, reason=reason,
        )
        if replay is not None:
            return replay
        try:
            result: AdminLeadCreationResult = self._admin_create.reconcile(
                operation_id=operation_id,
                operator_subject=principal.subject,
                operator_role="administrator",
                auth_source=principal.auth_source,
                request_id=request_id,
                reason=reason,
            )
        except Exception as exc:
            self._audit_failure(
                request_id,
                "create_reconcile",
                "creation_operation",
                operation_id,
                principal,
                reason,
                safe_failure_summary(exc),
            )
            logger.error(
                "console_maintenance_create_reconcile_failed",
                extra={"operation_id": operation_id, "request_id": request_id},
                exc_info=True,
            )
            raise
        self._audit_success(
            request_id,
            "create_reconcile",
            "creation_operation",
            operation_id,
            principal,
            before={"operation_id": operation_id},
            after={"status": result.status, "operation_id": result.operation_id},
            reason=reason,
            result=result.status,
            operation_id=result.operation_id,
        )
        return ConsoleMaintenanceResult(
            result.operation_id,
            result.status,
            lead_id=result.lead_id,
            detail=result.failure_summary,
        )

    def _audit_success(
        self,
        request_id: str,
        operation_type: str,
        object_type: str,
        object_id: str,
        principal: AdminPrincipal,
        *,
        before: dict[str, object],
        after: dict[str, object],
        reason: str | None = None,
        result: str = "succeeded",
        operation_id: str | None = None,
    ) -> None:
        """记录不含联系方式、CRM payload 或原始聊天内容的成功审计。"""

        self._write_audit(
            request_id,
            operation_type,
            object_type,
            object_id,
            principal,
            before,
            after,
            result,
            reason,
            None,
            operation_id,
        )

    def _audit_failure(
        self,
        request_id: str,
        operation_type: str,
        object_type: str,
        object_id: str,
        principal: AdminPrincipal,
        reason: str | None,
        summary: str,
    ) -> None:
        """记录管理写入失败，不把异常中的敏感正文写入审计。"""

        self._write_audit(
            request_id,
            operation_type,
            object_type,
            object_id,
            principal,
            {"object_type": object_type, "object_id": object_id},
            {"status": "failed"},
            "failed",
            reason,
            summary[:256],
        )

    def _write_audit(
        self,
        request_id: str,
        operation_type: str,
        object_type: str,
        object_id: str,
        principal: AdminPrincipal,
        before: dict[str, object],
        after: dict[str, object],
        result: str,
        reason: str | None,
        failure_summary: str | None,
        operation_id: str | None = None,
    ) -> None:
        """幂等写入 ConsoleMaintenanceAudit，统一保留 auth source 和 request id。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(ConsoleMaintenanceAudit).where(
                    ConsoleMaintenanceAudit.request_id == request_id
                ).with_for_update()
            )
            if existing is not None:
                existing.operation_id = operation_id or existing.operation_id
                existing.object_type = object_type
                existing.object_id = safe_audit_text(object_id, max_length=128)
                existing.before_state = before
                existing.after_state = after
                existing.result = result
                existing.reason = safe_audit_text(reason) if reason else None
                existing.failure_code = "maintenance_failed" if result == "failed" else None
                existing.failure_summary = failure_summary
                return
            safe_object_id = safe_audit_text(object_id, max_length=128)
            session.add(
                ConsoleMaintenanceAudit(
                    id=str(uuid4()),
                    operation_id=operation_id,
                    request_id=request_id,
                    operation_type=operation_type,
                    object_type=object_type,
                    object_id=safe_object_id,
                    operator_subject=principal.subject,
                    operator_role="administrator",
                    auth_source=safe_audit_text(principal.auth_source, max_length=128),
                    reason=safe_audit_text(reason) if reason else None,
                    before_state=before,
                    after_state=after,
                    result=result,
                    failure_code="maintenance_failed" if result == "failed" else None,
                    failure_summary=failure_summary,
                )
            )

    def _begin_audit(
        self,
        request_id: str,
        operation_type: str,
        object_type: str,
        object_id: str,
        principal: AdminPrincipal,
        *,
        before: dict[str, object],
        reason: str | None,
    ) -> ConsoleMaintenanceResult | None:
        """在领域副作用前写入 processing 审计，并返回重复请求的冻结结果。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(ConsoleMaintenanceAudit)
                .where(ConsoleMaintenanceAudit.request_id == request_id)
                .with_for_update()
            )
            if existing is not None:
                return ConsoleMaintenanceResult(
                    existing.operation_id,
                    existing.result,
                    lead_id=existing.object_id if existing.object_type == "lead" else None,
                    message_id=existing.object_id if existing.object_type == "message" else None,
                    detail=existing.failure_summary,
                )
            session.add(
                ConsoleMaintenanceAudit(
                    id=str(uuid4()),
                    request_id=request_id,
                    operation_type=operation_type,
                    object_type=object_type,
                    object_id=safe_audit_text(object_id, max_length=128),
                    operator_subject=principal.subject,
                    operator_role="administrator",
                    auth_source=safe_audit_text(principal.auth_source, max_length=128),
                    reason=safe_audit_text(reason) if reason else None,
                    before_state=before,
                    after_state={},
                    result="processing",
                )
            )
        return None

    @staticmethod
    def _require_reason(reason: str) -> str:
        """统一拒绝空管理原因。"""

        normalized = safe_audit_text(reason)
        if not normalized:
            raise ValueError("管理写操作必须填写原因")
        return normalized

    @staticmethod
    def _require_request_id(request_id: str) -> None:
        """统一拒绝缺失的管理请求幂等标识。"""

        if not request_id.strip() or re.fullmatch(
            r"[A-Za-z0-9._:-]{1,128}", request_id.strip()
        ) is None:
            raise ValueError("管理写操作必须填写 request_id")

    def _lead_state(self, lead_id: str) -> dict[str, object]:
        """读取不含业务字段的线索前置状态，供 Console 审计 before 快照使用。"""

        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                return {"lead_id": lead_id, "status": "missing"}
            return {
                "lead_id": lead_id,
                "lifecycle_state": lead.lifecycle_state,
                "smart_table_owner_user_id": lead.smart_table_owner_user_id,
            }
