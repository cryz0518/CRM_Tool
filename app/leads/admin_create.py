"""管理员维护流程的人工补建线索领域服务。"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.failures import safe_failure_summary
from app.leads.models import AdminLeadCreationOperation, Lead, new_lead_id
from app.messaging.models import SalesAuthorization
from app.smart_table.adapter import SmartTableActor, SmartTableAdapter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdminLeadCreationResult:
    """返回管理员补建线索的 operation、Lead 和最终状态。"""

    operation_id: str
    lead_id: str
    status: str
    failure_code: str | None = None
    failure_summary: str | None = None


class AdminLeadCreationService:
    """创建没有来源消息的管理员线索，并异步语义地协调 Smart Table。"""

    _REQUIRED_FIELDS = frozenset({"线索名称", "业务线"})
    _CONTACT_FIELDS = frozenset({"手机", "电话", "邮箱"})

    def __init__(
        self, session_factory: sessionmaker[Session], smart_table: SmartTableAdapter
    ) -> None:
        """注入数据库事务工厂和智能表格适配器。"""

        self._session_factory = session_factory
        self._smart_table = smart_table

    def create(
        self,
        *,
        original_capturing_sales_user_id: str,
        smart_table_owner_user_id: str,
        field_values: dict[str, str],
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> AdminLeadCreationResult:
        """补建完整的 pending_create 线索，不伪造 IncomingMessage。

        参数：两个明确销售身份、最小业务字段以及管理认证和审计信息。
        返回值：成功或可恢复失败的创建 operation 结果。
        异常：输入、权限或最小字段不合法时抛出 ValueError/PermissionError。
        副作用：写入无 source_message_id 的 Lead、创建 operation 并新增一条 Smart Table 记录。
        """

        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("管理员创建线索必须填写原因")
        if not request_id.strip() or not auth_source.strip():
            raise ValueError("管理员创建线索必须填写 request_id 和 auth_source")
        if operator_role != "administrator":
            raise PermissionError("维护写入角色必须是 administrator")
        normalized_fields = {key.strip(): value.strip() for key, value in field_values.items()}
        if any(not key or not value for key, value in normalized_fields.items()):
            raise ValueError("管理员创建线索的字段名和值不能为空")
        if not self._REQUIRED_FIELDS.issubset(normalized_fields):
            raise ValueError("管理员创建线索缺少线索名称或业务线")
        if not self._CONTACT_FIELDS.intersection(normalized_fields):
            raise ValueError("管理员创建线索至少需要手机、电话或邮箱")

        operation_id, lead_id, existing_status = self._prepare(
            original_capturing_sales_user_id=original_capturing_sales_user_id,
            smart_table_owner_user_id=smart_table_owner_user_id,
            field_values=normalized_fields,
            operator_subject=operator_subject,
            operator_role=operator_role,
            auth_source=auth_source,
            request_id=request_id,
            reason=normalized_reason,
        )
        # 相同 request_id 的重试只返回本地 operation 状态，不重复创建远端记录。
        if existing_status != "processing":
            return AdminLeadCreationResult(operation_id, lead_id, existing_status)
        table_fields = {
            **normalized_fields,
            "创建人": smart_table_owner_user_id,
            "负责人": smart_table_owner_user_id,
        }
        try:
            logger.info(
                "admin_lead_creation_remote_started",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                },
            )
            record = self._smart_table.create_record(table_fields, actor=SmartTableActor.ADMIN)
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "admin_lead_creation_remote_failed",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._finish(operation_id, "failed", "remote_create_failed", summary)
            return AdminLeadCreationResult(
                operation_id, lead_id, "remote_create_failed", "remote_create_failed", summary
            )

        try:
            with self._session_factory.begin() as session:
                lead = session.get(Lead, lead_id)
                operation = session.get(AdminLeadCreationOperation, operation_id)
                if lead is None or operation is None:
                    raise ValueError("管理员创建 operation 或线索不存在")
                lead.smart_table_record_id = record.record_id
                operation.smart_table_record_id = record.record_id
                operation.remote_update_state = "succeeded"
                operation.final_status = "succeeded"
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "admin_lead_creation_local_finalize_failed",
                extra={
                    "operation_id": operation_id,
                    "lead_id": lead_id,
                    "request_id": request_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
            self._finish(
                operation_id,
                "succeeded",
                "pending_recovery",
                summary,
                smart_table_record_id=record.record_id,
            )
            return AdminLeadCreationResult(
                operation_id, lead_id, "pending_recovery", "local_finalize_failed", summary
            )
        return AdminLeadCreationResult(operation_id, lead_id, "succeeded")

    def _prepare(
        self,
        *,
        original_capturing_sales_user_id: str,
        smart_table_owner_user_id: str,
        field_values: dict[str, str],
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> tuple[str, str, str]:
        """在远端调用前锁定身份并保存可恢复本地 operation。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(AdminLeadCreationOperation).where(
                    AdminLeadCreationOperation.request_id == request_id
                )
            )
            if existing is not None:
                return existing.id, existing.lead_id, existing.final_status
            operator = session.get(SalesAuthorization, operator_subject)
            capture = session.get(SalesAuthorization, original_capturing_sales_user_id)
            owner = session.get(SalesAuthorization, smart_table_owner_user_id)
            if operator is None or not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以补建线索")
            if capture is None or not capture.is_active or not capture.is_authorized:
                raise ValueError("Original Capturing Salesperson 必须是 active 且 authorized")
            if owner is None or not owner.is_active or not owner.is_authorized:
                raise ValueError("Smart Table Owner 必须是 active 且 authorized")
            lead_id = new_lead_id()
            company_name = field_values["线索名称"]
            duplicate = session.scalar(
                select(Lead).where(
                    Lead.smart_table_owner_user_id == smart_table_owner_user_id,
                    Lead.standard_company_name == company_name,
                    Lead.lifecycle_state != "discarded",
                )
            )
            if duplicate is not None:
                raise ValueError("Smart Table Owner 已有同公司线索，不允许重复补建")
            lead = Lead(
                id=lead_id,
                source_message_id=None,
                original_capturing_sales_user_id=original_capturing_sales_user_id,
                smart_table_owner_user_id=smart_table_owner_user_id,
                lifecycle_state="pending_create",
                field_values=field_values,
                standard_company_name=company_name,
                company_region="unknown",
                company_verification_status="user_confirmed_unverified",
                company_confirmed_by_user=True,
            )
            operation = AdminLeadCreationOperation(
                id=new_lead_id(),
                lead_id=lead_id,
                request_id=request_id,
                operator_subject=operator_subject,
                operator_role=operator_role,
                auth_source=auth_source,
                reason=reason,
                original_capturing_sales_user_id=original_capturing_sales_user_id,
                smart_table_owner_user_id=smart_table_owner_user_id,
            )
            session.add_all([lead, operation])
            session.flush()
            return operation.id, lead_id, operation.final_status

    def _finish(
        self,
        operation_id: str,
        remote_state: str,
        final_status: str,
        summary: str,
        *,
        smart_table_record_id: str | None = None,
    ) -> None:
        """持久化管理员创建的远端失败或本地待恢复状态。"""

        with self._session_factory.begin() as session:
            operation = session.get(AdminLeadCreationOperation, operation_id)
            if operation is None:
                raise ValueError("管理员创建 operation 不存在")
            operation.remote_update_state = remote_state
            if smart_table_record_id is not None:
                operation.smart_table_record_id = smart_table_record_id
            operation.final_status = final_status
            operation.failure_code = final_status
            operation.failure_summary = summary

    @staticmethod
    def _safe_summary(error: Exception) -> str:
        """裁剪异常摘要，避免把敏感载荷写入审计。"""

        return safe_failure_summary(error)
