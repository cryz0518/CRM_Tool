"""智能表格负责人转交领域服务。"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.failures import safe_failure_summary
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    SalesLeadContext,
    SmartTableOwnerTransferOperation,
)
from app.messaging.models import SalesAuthorization
from app.smart_table.adapter import SmartTableAdapter
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


class SmartTableOwnerTransferService:
    """只改变 Smart Table Owner，并在权限验证后收敛本地身份事实。"""

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

        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("智能表格负责人转交必须填写原因")
        if not request_id.strip():
            raise ValueError("智能表格负责人转交必须填写 request_id")

        operation_id, old_owner, record_id, existing_status = self._prepare_operation(
            lead_id=lead_id,
            new_owner_user_id=new_owner_user_id,
            operator_subject=operator_subject,
            operator_role=operator_role,
            auth_source=auth_source,
            request_id=request_id,
            reason=normalized_reason,
        )
        # 相同 request_id 重试只返回已持久化状态，绝不重复写远端或制造本地冲突。
        if existing_status != "processing":
            return SmartTableOwnerTransferResult(operation_id, lead_id, existing_status)

        try:
            # 远端只写负责人字段，绝不把当前整行快照提交回去。
            logger.info(
                "smart_table_owner_transfer_remote_started",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "old_owner_user_id": old_owner,
                    "new_owner_user_id": new_owner_user_id,
                },
            )
            self._smart_table.update_record(record_id, {"负责人": new_owner_user_id})
        except Exception as exc:
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
                failure_code="remote_update_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id, lead_id, "remote_update_failed", "remote_update_failed", summary
            )

        self._mark_remote_succeeded(operation_id)
        try:
            verification = self._permission_verifier.verify_owner_transfer(
                record_id, old_owner, new_owner_user_id
            )
        except SmartTablePermissionVerificationUnavailable as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_permission_unavailable",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                },
                exc_info=True,
            )
            self._finish_operation(
                operation_id,
                remote_update_state="succeeded",
                permission_verification_state="unavailable",
                # 远端负责人已不可逆写入，但本地尚未证明新旧负责人权限；
                # 该事实必须进入可恢复状态，不能被当作终态并允许重复转交。
                final_status="pending_recovery",
                failure_code="permission_verification_unavailable",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                lead_id,
                "permission_verification_unavailable",
                "permission_verification_unavailable",
                summary,
            )
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_permission_failed",
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
                remote_update_state="succeeded",
                permission_verification_state="failed",
                final_status="pending_recovery",
                failure_code="permission_verification_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                lead_id,
                "permission_verification_failed",
                "permission_verification_failed",
                summary,
            )

        if not verification.verified:
            summary = "目标负责人权限不满足可见/可编辑且原负责人不可见"
            logger.error(
                "smart_table_owner_transfer_permission_rejected",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                },
                exc_info=True,
            )
            self._finish_operation(
                operation_id,
                remote_update_state="succeeded",
                permission_verification_state="failed",
                final_status="pending_recovery",
                failure_code="permission_verification_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id,
                lead_id,
                "permission_verification_failed",
                "permission_verification_failed",
                summary,
            )

        try:
            self._finalize_local_transfer(operation_id, lead_id, old_owner, new_owner_user_id)
        except Exception as exc:
            # 远端事实不可逆，不能反向写回；仅保留待恢复事实。
            summary = self._safe_summary(exc)
            logger.error(
                "smart_table_owner_transfer_local_finalize_failed",
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
                remote_update_state="succeeded",
                permission_verification_state="verified",
                final_status="pending_recovery",
                failure_code="local_finalize_failed",
                failure_summary=summary,
            )
            return SmartTableOwnerTransferResult(
                operation_id, lead_id, "pending_recovery", "local_finalize_failed", summary
            )

        return SmartTableOwnerTransferResult(operation_id, lead_id, "succeeded")

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
    ) -> tuple[str, str, str, str]:
        """锁定线索并持久化远端调用前的 operation 快照。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(SmartTableOwnerTransferOperation).where(
                    SmartTableOwnerTransferOperation.request_id == request_id
                )
            )
            if existing is not None:
                return (
                    existing.id,
                    existing.old_owner_user_id,
                    existing.smart_table_record_id,
                    existing.final_status,
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
                    Lead.lifecycle_state != "discarded",
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
                raise ValueError("该线索已有进行中的负责人转交")
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
            return operation.id, old_owner, lead.smart_table_record_id, operation.final_status

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
            operation.remote_update_state = remote_update_state
            operation.permission_verification_state = permission_verification_state
            operation.final_status = final_status
            operation.failure_code = failure_code
            operation.failure_summary = failure_summary

    @staticmethod
    def _safe_summary(error: Exception) -> str:
        """将异常压缩为不含 CRM、联系方式或密钥的有限摘要。"""

        return safe_failure_summary(error)
