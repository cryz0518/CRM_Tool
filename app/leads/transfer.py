"""智能表格负责人转交领域服务。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.failures import safe_audit_text, safe_failure_summary, validate_request_id
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    SalesLeadContext,
    SmartTableOwnerTransferOperation,
)
from app.messaging.models import SalesAuthorization, utc_now
from app.smart_table.adapter import SmartTableAdapter, SmartTableDefiniteRemoteFailure
from app.smart_table.permissions import (
    SmartTablePermissionVerificationProvider,
    SmartTablePermissionVerificationUnavailable,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmartTableOwnerTransferResult:
    """返回转交 operation 及其可恢复状态。"""

    operation_id: str
    lead_id: str
    status: str
    failure_code: str | None = None
    failure_summary: str | None = None


@dataclass(frozen=True)
class _TransferOperationSnapshot:
    """承载一次转交 recovery 所需的不可变 operation 快照。"""

    operation_id: str
    lead_id: str
    record_id: str
    old_owner: str
    new_owner: str
    final_status: str
    remote_state: str


class SmartTableOwnerTransferService:
    """只改变 Smart Table Owner，并在权限验证后收敛本地身份事实。"""

    _RECOVERY_LEASE = timedelta(minutes=5)

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table: SmartTableAdapter,
        permission_verifier: SmartTablePermissionVerificationProvider,
    ) -> None:
        """注入数据库、智能表格写端口和记录级权限验证 seam。"""

        self._session_factory = session_factory
        self._smart_table = smart_table
        self._permission_verifier = permission_verifier

    def transfer(
        self,
        *,
        lead_id: str,
        new_owner_user_id: str,
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> SmartTableOwnerTransferResult:
        """执行可恢复的负责人转交，不修改 CRM owner 或业务字段。

        参数：lead_id 为目标线索；new_owner_user_id 为目标销售；其余为管理认证与审计事实。
        返回值：最终状态及 operation 标识；远端已成功但本地未收敛时返回待恢复状态。
        异常：管理员、目标销售、线索、记录或同公司冲突不满足时抛出 ValueError/PermissionError；
        远端写失败则返回失败 operation，不伪装成本地成功。
        副作用：只写智能表格负责人字段；验证成功后更新 Lead owner 并清理旧上下文。
        """

        normalized_reason = safe_audit_text(reason)
        if not normalized_reason:
            raise ValueError("智能表格负责人转交必须填写原因")
        request_id = validate_request_id(request_id)
        auth_source = safe_audit_text(auth_source, max_length=128)
        if not auth_source:
            raise ValueError("智能表格负责人转交必须填写 auth_source")

        operation_id, old_owner, record_id, existing_status, is_new = self._prepare_operation(
            lead_id=lead_id,
            new_owner_user_id=new_owner_user_id,
            operator_subject=operator_subject,
            operator_role=operator_role,
            auth_source=auth_source,
            request_id=request_id,
            reason=normalized_reason,
        )
        # 已完成或已明确失败的 operation 只返回冻结事实，绝不重复产生外部副作用。
        if not is_new and existing_status == "succeeded":
            return SmartTableOwnerTransferResult(operation_id, lead_id, existing_status)
        if not is_new and existing_status == "remote_update_failed":
            self._claim_definite_retry(operation_id, operator_subject, operator_role)
            return self._execute_remote_transfer(
                operation_id=operation_id,
                lead_id=lead_id,
                record_id=record_id,
                old_owner=old_owner,
                new_owner=new_owner_user_id,
                request_id=request_id,
            )

        # 旧请求重新进入 processing/pending_recovery 时统一走事实核验，不能盲目重放。
        if not is_new:
            return self.reconcile(
                operation_id=operation_id,
                operator_subject=operator_subject,
                operator_role=operator_role,
                auth_source=auth_source,
                request_id=request_id,
                reason=normalized_reason,
            )

        return self._execute_remote_transfer(
            operation_id=operation_id,
            lead_id=lead_id,
            record_id=record_id,
            old_owner=old_owner,
            new_owner=new_owner_user_id,
            request_id=request_id,
        )

    def reconcile(
        self,
        *,
        operation_id: str,
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> SmartTableOwnerTransferResult:
        """先读取远端负责人事实，再安全恢复一个未完成转交 operation。

        参数：operation_id 为待恢复 operation；其余参数为当前管理员认证和审计事实。
        返回值：恢复后的成功、处理中或待人工核验状态；不会在远端事实未知时重复写入。
        异常：管理员、原因或 operation 不合法时抛出 ValueError/PermissionError。
        副作用：只在远端明确仍为旧负责人且本地事实允许时重试一次负责人更新。
        """

        self._validate_recovery_input(auth_source, request_id, reason, operator_role)
        snapshot, claimed = self._claim_operation(
            operation_id=operation_id,
            operator_subject=operator_subject,
            operator_role=operator_role,
        )
        if snapshot.final_status in {"succeeded", "remote_update_failed"}:
            return SmartTableOwnerTransferResult(
                operation_id, snapshot.lead_id, snapshot.final_status
            )
        if not claimed:
            # 新鲜 processing 仍可能由另一执行者持有；只报告状态，不抢占其副作用。
            return SmartTableOwnerTransferResult(operation_id, snapshot.lead_id, "processing")

        try:
            record = self._smart_table.get_record(snapshot.record_id)
        except Exception as exc:
            summary = self._safe_summary(exc)
            self._mark_pending_recovery(
                operation_id,
                remote_update_state="unknown",
                permission_state="pending",
                failure_code="remote_read_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id, snapshot.lead_id, "pending_recovery", "remote_read_failed", summary
            )

        if record is None:
            summary = "无法读取远端负责人事实"
            self._mark_pending_recovery(
                operation_id,
                remote_update_state="unknown",
                permission_state="pending",
                failure_code="remote_read_missing",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id, snapshot.lead_id, "pending_recovery", "remote_read_missing", summary
            )

        remote_owner = record.fields.get("负责人")
        if remote_owner == snapshot.new_owner:
            # 读取到目标负责人即冻结远端成功事实，再进行权限验证和本地收敛。
            self._mark_remote_succeeded(operation_id)
            return self._verify_and_finalize(snapshot, request_id)
        if remote_owner != snapshot.old_owner:
            summary = "远端负责人既不是原负责人也不是目标负责人"
            self._mark_pending_recovery(
                operation_id,
                remote_update_state="unknown",
                permission_state="pending",
                failure_code="unexpected_remote_owner",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                snapshot.lead_id,
                "pending_recovery",
                "unexpected_remote_owner",
                summary,
            )

        # 只有远端仍是旧负责人时，才允许一次新的合法更新尝试。
        try:
            self._smart_table.update_record(
                snapshot.record_id, {"负责人": snapshot.new_owner}
            )
        except SmartTableDefiniteRemoteFailure as exc:
            summary = self._safe_summary(exc)
            self._finish_operation(
                operation_id,
                remote_update_state="failed",
                permission_verification_state="not_started",
                final_status="remote_update_failed",
                failure_code="definite_remote_failure",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                snapshot.lead_id,
                "remote_update_failed",
                "definite_remote_failure",
                summary,
            )
        except Exception as exc:
            summary = self._safe_summary(exc)
            self._mark_pending_recovery(
                operation_id,
                remote_update_state="unknown",
                permission_state="pending",
                failure_code="remote_outcome_unknown",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                snapshot.lead_id,
                "pending_recovery",
                "remote_outcome_unknown",
                summary,
            )

        self._mark_remote_succeeded(operation_id)
        return self._verify_and_finalize(snapshot, request_id)

    def _execute_remote_transfer(
        self,
        *,
        operation_id: str,
        lead_id: str,
        record_id: str,
        old_owner: str,
        new_owner: str,
        request_id: str,
    ) -> SmartTableOwnerTransferResult:
        """执行首次远端负责人补丁，并将未知结果保留为待恢复事实。"""

        try:
            # 远端只写负责人字段，绝不把当前整行快照提交回去。
            logger.info(
                "smart_table_owner_transfer_remote_started",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "old_owner_user_id": old_owner,
                    "new_owner_user_id": new_owner,
                },
            )
            self._smart_table.update_record(record_id, {"负责人": new_owner})
        except SmartTableDefiniteRemoteFailure as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_remote_failed",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._finish_operation(
                operation_id,
                remote_update_state="failed",
                permission_verification_state="not_started",
                final_status="remote_update_failed",
                failure_code="definite_remote_failure",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id, lead_id, "remote_update_failed", "definite_remote_failure", summary
            )
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_remote_unknown",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._mark_pending_recovery(
                operation_id,
                remote_update_state="unknown",
                permission_state="pending",
                failure_code="remote_outcome_unknown",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                lead_id,
                "pending_recovery",
                "remote_outcome_unknown",
                summary,
            )

        self._mark_remote_succeeded(operation_id)
        snapshot = _TransferOperationSnapshot(
            operation_id, lead_id, record_id, old_owner, new_owner, "processing", "succeeded"
        )
        return self._verify_and_finalize(snapshot, request_id)

    def _verify_and_finalize(
        self, snapshot: _TransferOperationSnapshot, request_id: str
    ) -> SmartTableOwnerTransferResult:
        """验证 A/B 权限后再收敛本地 owner/context。"""

        try:
            verification = self._permission_verifier.verify_owner_transfer(
                snapshot.record_id, snapshot.old_owner, snapshot.new_owner
            )
        except SmartTablePermissionVerificationUnavailable as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_permission_unavailable",
                extra={
                    "operation_id": snapshot.operation_id,
                    "lead_id": snapshot.lead_id,
                    "request_id": request_id,
                },
                exc_info=True,
            )
            self._finish_operation(
                snapshot.operation_id,
                remote_update_state="succeeded",
                permission_verification_state="unavailable",
                # 远端负责人已不可逆写入，但本地尚未证明新旧负责人权限；
                # 该事实必须进入可恢复状态，不能被当作终态并允许重复转交。
                final_status="pending_recovery",
                failure_code="permission_verification_unavailable",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                snapshot.operation_id,
                snapshot.lead_id,
                "permission_verification_unavailable",
                "permission_verification_unavailable",
                summary,
            )
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_permission_failed",
                extra={
                    "operation_id": snapshot.operation_id,
                    "lead_id": snapshot.lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._finish_operation(
                snapshot.operation_id,
                remote_update_state="succeeded",
                permission_verification_state="failed",
                final_status="pending_recovery",
                failure_code="permission_verification_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                snapshot.operation_id,
                snapshot.lead_id,
                "permission_verification_failed",
                "permission_verification_failed",
                summary,
            )

        if not verification.verified:
            summary = "目标负责人权限不满足可见/可编辑且原负责人不可见"
            logger.error(
                "smart_table_owner_transfer_permission_rejected",
                extra={
                    "operation_id": snapshot.operation_id,
                    "lead_id": snapshot.lead_id,
                    "request_id": request_id,
                },
                exc_info=True,
            )
            self._finish_operation(
                snapshot.operation_id,
                remote_update_state="succeeded",
                permission_verification_state="failed",
                final_status="pending_recovery",
                failure_code="permission_verification_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                snapshot.operation_id,
                snapshot.lead_id,
                "permission_verification_failed",
                "permission_verification_failed",
                summary,
            )

        try:
            self._finalize_local_transfer(
                snapshot.operation_id,
                snapshot.lead_id,
                snapshot.old_owner,
                snapshot.new_owner,
            )
        except Exception as exc:
            # 远端事实不可逆，不能反向写回；仅保留待恢复事实。
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_local_finalize_failed",
                extra={
                    "operation_id": snapshot.operation_id,
                    "lead_id": snapshot.lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._finish_operation(
                snapshot.operation_id,
                remote_update_state="succeeded",
                permission_verification_state="verified",
                final_status="pending_recovery",
                failure_code="local_finalize_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                snapshot.operation_id,
                snapshot.lead_id,
                "pending_recovery",
                "local_finalize_failed",
                summary,
            )

        return SmartTableOwnerTransferResult(snapshot.operation_id, snapshot.lead_id, "succeeded")

    def _prepare_operation(
        self,
        *,
        lead_id: str,
        new_owner_user_id: str,
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> tuple[str, str, str, str, bool]:
        """锁定线索并持久化远端调用前的 operation 快照。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(SmartTableOwnerTransferOperation).where(
                    SmartTableOwnerTransferOperation.request_id == request_id
                )
            )
            if existing is not None:
                if existing.lead_id != lead_id:
                    raise ValueError("request_id 已绑定另一笔负责人转交")
                operator = session.get(SalesAuthorization, operator_subject)
                if operator is None or not operator.is_active or not operator.is_administrator:
                    raise PermissionError("只有活跃管理员可以恢复智能表格负责人转交")
                if operator_role != "administrator" or not auth_source.strip():
                    raise PermissionError("维护恢复必须使用 administrator 和有效 auth_source")
                if (
                    existing.new_owner_user_id != new_owner_user_id
                    and existing.final_status != "succeeded"
                ):
                    raise ValueError("该线索已有进行中的另一目标负责人的转交")
                return (
                    existing.id,
                    existing.old_owner_user_id,
                    existing.smart_table_record_id,
                    existing.final_status,
                    False,
                )
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            operator = session.get(SalesAuthorization, operator_subject)
            target = session.get(SalesAuthorization, new_owner_user_id)
            if lead is None or operator is None:
                raise ValueError("线索或管理员不存在")
            if not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以转交智能表格负责人")
            if operator_role != "administrator":
                raise PermissionError("维护写入角色必须是 administrator")
            if not auth_source.strip():
                raise ValueError("管理操作必须记录 auth_source")
            if target is None or not target.is_active or not target.is_authorized:
                raise ValueError("目标销售必须处于 active 且 authorized 状态")
            if not lead.smart_table_record_id:
                raise ValueError("线索尚未绑定智能表格记录")
            old_owner = lead.smart_table_owner_user_id
            if old_owner == new_owner_user_id:
                raise ValueError("目标销售已经是当前智能表格负责人")
            duplicate = session.scalar(
                select(Lead).where(
                    Lead.smart_table_owner_user_id == new_owner_user_id,
                    Lead.standard_company_name == lead.standard_company_name,
                    Lead.id != lead.id,
                )
            )
            if duplicate is not None:
                raise ValueError("目标销售已有同公司线索，不允许自动合并")
            active = session.scalar(
                select(SmartTableOwnerTransferOperation)
                .where(
                    SmartTableOwnerTransferOperation.lead_id == lead.id,
                    SmartTableOwnerTransferOperation.final_status.in_(
                        ("processing", "pending_recovery")
                    ),
                )
                .with_for_update()
            )
            if active is not None:
                if active.new_owner_user_id != new_owner_user_id:
                    raise ValueError("该线索已有进行中的另一目标负责人的转交")
                return (
                    active.id,
                    active.old_owner_user_id,
                    active.smart_table_record_id,
                    active.final_status,
                    False,
                )
            identity = None
            if lead.standard_company_name:
                identity = session.get(CrmCompanyIdentity, lead.standard_company_name)
            crm_owner = identity.crm_lead_owner_user_id if identity else None
            if crm_owner is None:
                # Identity 记录缺失或尚未回填时，从最近成功 CRM sync 保留历史 owner 快照。
                latest_sync = session.scalar(
                    select(CrmSyncRecord)
                    .where(
                        CrmSyncRecord.lead_id == lead.id,
                        CrmSyncRecord.status == "succeeded",
                        CrmSyncRecord.crm_lead_owner_user_id.is_not(None),
                    )
                    .order_by(CrmSyncRecord.id.desc())
                    .limit(1)
                )
                crm_owner = (
                    latest_sync.crm_lead_owner_user_id if latest_sync is not None else None
                )
            operation = SmartTableOwnerTransferOperation(
                lead_id=lead.id,
                smart_table_record_id=lead.smart_table_record_id,
                request_id=request_id,
                operator_subject=operator_subject,
                operator_role=operator_role,
                auth_source=auth_source,
                reason=reason,
                old_owner_user_id=old_owner,
                new_owner_user_id=new_owner_user_id,
                original_capturing_sales_user_id=lead.original_capturing_sales_user_id,
                crm_lead_owner_user_id=crm_owner,
            )
            session.add(operation)
            session.flush()
            return operation.id, old_owner, lead.smart_table_record_id, operation.final_status, True

    def _claim_operation(
        self, *, operation_id: str, operator_subject: str, operator_role: str
    ) -> tuple[_TransferOperationSnapshot, bool]:
        """以行锁和 updated_at lease claim 未完成 operation，防止并发重复外部写入。"""

        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(SmartTableOwnerTransferOperation)
                .where(SmartTableOwnerTransferOperation.id == operation_id)
                .with_for_update()
            )
            operator = session.get(SalesAuthorization, operator_subject)
            if operation is None:
                raise ValueError("转交 operation 不存在")
            if operator is None or not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以恢复智能表格负责人转交")
            if operator_role != "administrator":
                raise PermissionError("维护恢复角色必须是 administrator")
            snapshot = _TransferOperationSnapshot(
                operation.id,
                operation.lead_id,
                operation.smart_table_record_id,
                operation.old_owner_user_id,
                operation.new_owner_user_id,
                operation.final_status,
                operation.remote_update_state,
            )
            if operation.final_status in {"succeeded", "remote_update_failed"}:
                return snapshot, False
            if (
                operation.final_status == "processing"
                and operation.remote_update_state == "pending"
                and not self._operation_lease_expired(operation.updated_at)
            ):
                return snapshot, False
            # pending_recovery 或过期 processing 获得新的短 lease。
            operation.final_status = "processing"
            operation.updated_at = utc_now()
            return snapshot, True

    def _claim_definite_retry(
        self, operation_id: str, operator_subject: str, operator_role: str
    ) -> None:
        """只允许明确未发生远端写入的失败 operation 安全重试。"""

        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(SmartTableOwnerTransferOperation)
                .where(SmartTableOwnerTransferOperation.id == operation_id)
                .with_for_update()
            )
            operator = session.get(SalesAuthorization, operator_subject)
            if operation is None or operator is None:
                raise ValueError("转交 operation 或管理员不存在")
            if not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以重试智能表格负责人转交")
            if operator_role != "administrator":
                raise PermissionError("维护重试角色必须是 administrator")
            if operation.final_status != "remote_update_failed":
                raise ValueError("只有明确未产生远端副作用的失败才允许重试")
            operation.final_status = "processing"
            operation.remote_update_state = "pending"
            operation.permission_verification_state = "pending"
            operation.failure_code = None
            operation.failure_summary = None
            operation.updated_at = utc_now()

    def _operation_lease_expired(self, updated_at: datetime | None) -> bool:
        """判断 operation 的现有更新时间是否已经超过 recovery lease。"""

        if updated_at is None:
            return True
        normalized = updated_at.replace(tzinfo=UTC) if updated_at.tzinfo is None else updated_at
        return utc_now() - normalized >= self._RECOVERY_LEASE

    @staticmethod
    def _validate_recovery_input(
        auth_source: str, request_id: str, reason: str, operator_role: str
    ) -> None:
        """校验 recovery 必须携带的审计与认证字段。"""

        if operator_role != "administrator":
            raise PermissionError("维护恢复角色必须是 administrator")
        validate_request_id(request_id)
        if not safe_audit_text(auth_source, max_length=128) or not safe_audit_text(reason):
            raise ValueError("负责人转交恢复必须填写 auth_source、request_id 和 reason")

    def _mark_pending_recovery(
        self,
        operation_id: str,
        *,
        remote_update_state: str,
        permission_state: str,
        failure_code: str,
        failure_summary: str,
    ) -> None:
        """保存远端事实不足或本地未收敛的待恢复状态。"""

        self._finish_operation(
            operation_id,
            remote_update_state=remote_update_state,
            permission_verification_state=permission_state,
            final_status="pending_recovery",
            failure_code=failure_code,
            failure_summary=failure_summary,
        )

    def _mark_remote_succeeded(self, operation_id: str) -> None:
        """将不可逆远端负责人写入记录为已成功，等待权限结论。"""

        self._finish_operation(
            operation_id,
            remote_update_state="succeeded",
            permission_verification_state="pending",
            final_status="processing",
            failure_code=None,
            failure_summary=None,
        )

    def _finalize_local_transfer(
        self, operation_id: str, lead_id: str, old_owner: str, new_owner: str
    ) -> None:
        """在同一事务锁定线索后只收敛 owner 与旧上下文。"""

        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(SmartTableOwnerTransferOperation)
                .where(SmartTableOwnerTransferOperation.id == operation_id)
                .with_for_update()
            )
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            if operation is None or lead is None:
                raise ValueError("转交 operation 或线索不存在")
            if lead.smart_table_owner_user_id != old_owner:
                raise ValueError("线索负责人已变化，远端事实需要人工恢复")
            if lead.lifecycle_state == "discarded":
                raise ValueError("线索已废弃，远端负责人事实需要人工恢复")
            lead.smart_table_owner_user_id = new_owner
            session.execute(
                delete(SalesLeadContext).where(
                    SalesLeadContext.sales_user_id == old_owner,
                    SalesLeadContext.lead_id == lead_id,
                )
            )
            operation.permission_verification_state = "verified"
            operation.final_status = "succeeded"
            operation.failure_code = None
            operation.failure_summary = None
            operation.updated_at = utc_now()

    def _finish_operation(
        self,
        operation_id: str,
        *,
        remote_update_state: str,
        permission_verification_state: str,
        final_status: str,
        failure_code: str | None,
        failure_summary: str | None,
    ) -> None:
        """以短事务记录 operation 状态，不记录敏感业务载荷。"""

        with self._session_factory.begin() as session:
            operation = session.get(SmartTableOwnerTransferOperation, operation_id)
            if operation is None:
                raise ValueError("转交 operation 不存在")
            # 旧 worker 在 recovery lease 之后返回时，不能把已收敛或待人工核验事实降级覆盖。
            if operation.final_status == "succeeded":
                return
            if operation.final_status == "pending_recovery" and final_status == "processing":
                return
            operation.remote_update_state = remote_update_state
            operation.permission_verification_state = permission_verification_state
            operation.final_status = final_status
            operation.failure_code = failure_code
            operation.failure_summary = failure_summary
            operation.updated_at = utc_now()

    @staticmethod
    def _safe_summary(error: Exception) -> str:
        """将异常压缩为不含 CRM、联系方式或密钥的有限摘要。"""

        return safe_failure_summary(error)
