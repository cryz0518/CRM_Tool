"""管理员维护流程的人工补建线索领域服务。"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.failures import safe_audit_text, safe_failure_summary, validate_request_id
from app.leads.models import AdminLeadCreationOperation, Lead, new_lead_id
from app.messaging.models import SalesAuthorization, utc_now
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapter,
    SmartTableDefiniteRemoteFailure,
)
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import DEFAULT_SMART_TABLE_FIELD_VALUES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdminLeadCreationResult:
    """返回管理员补建线索的 operation、Lead 和最终状态。"""

    operation_id: str
    lead_id: str
    status: str
    failure_code: str | None = None
    failure_summary: str | None = None


@dataclass(frozen=True)
class _AdminCreationSnapshot:
    """承载管理员补建 recovery 所需的 Lead 与 operation 冻结事实。"""

    operation_id: str
    lead_id: str
    owner_user_id: str
    record_id: str | None
    final_status: str
    remote_state: str


class AdminLeadCreationService:
    """创建没有来源消息的管理员线索，并异步语义地协调 Smart Table。"""

    _REQUIRED_FIELDS = frozenset({"线索名称", "业务线"})
    _CONTACT_FIELDS = frozenset({"手机", "电话", "邮箱"})
    _RECOVERY_LEASE = timedelta(minutes=5)

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
        field_values: dict[str, object],
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

        normalized_reason = safe_audit_text(reason)
        if not normalized_reason:
            raise ValueError("管理员创建线索必须填写原因")
        request_id = validate_request_id(request_id)
        auth_source = safe_audit_text(auth_source, max_length=128)
        if not auth_source:
            raise ValueError("管理员创建线索必须填写 request_id 和 auth_source")
        if operator_role != "administrator":
            raise PermissionError("维护写入角色必须是 administrator")
        normalized_fields = {
            key.strip(): value.strip() if isinstance(value, str) else value
            for key, value in field_values.items()
        }
        # 管理员补建也遵守智能表格的国内默认值；显式国外值会在后续展开中覆盖默认值。
        normalized_fields = {**DEFAULT_SMART_TABLE_FIELD_VALUES, **normalized_fields}
        if any(not key or value in (None, "", []) for key, value in normalized_fields.items()):
            raise ValueError("管理员创建线索的字段名和值不能为空")
        if not self._REQUIRED_FIELDS.issubset(normalized_fields):
            raise ValueError("管理员创建线索缺少线索名称或业务线")
        if not self._CONTACT_FIELDS.intersection(normalized_fields):
            raise ValueError("管理员创建线索至少需要手机、电话或邮箱")

        operation_id, lead_id, existing_status, is_new = self._prepare(
            original_capturing_sales_user_id=original_capturing_sales_user_id,
            smart_table_owner_user_id=smart_table_owner_user_id,
            field_values=normalized_fields,
            operator_subject=operator_subject,
            operator_role=operator_role,
            auth_source=auth_source,
            request_id=request_id,
            reason=normalized_reason,
        )
        # 已完成或未知远端结果只进入明确的 frozen recovery，不直接再次 create。
        if not is_new and existing_status in {"succeeded", "pending_recovery", "processing"}:
            return self.reconcile(
                operation_id=operation_id,
                operator_subject=operator_subject,
                operator_role=operator_role,
                auth_source=auth_source,
                request_id=request_id,
                reason=normalized_reason,
            )
        if not is_new and existing_status == "remote_create_failed":
            self._claim_definite_retry(operation_id, operator_subject, operator_role)

        # 新 operation 或 adapter 已明确证明未产生远端副作用的 retry 才能执行 create。
        if not is_new and existing_status not in {"remote_create_failed"}:
            return AdminLeadCreationResult(operation_id, lead_id, existing_status)
        # retry 必须使用持久化 Lead 快照，不能信任重放请求中被篡改的字段或 owner。
        persisted_owner, persisted_fields = self._creation_facts(lead_id)
        table_fields = {
            **persisted_fields,
            "创建人": persisted_owner,
            "负责人": persisted_owner,
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
        except SmartTableDefiniteRemoteFailure as exc:
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
        except Exception as exc:
            summary = self._safe_summary(exc)
            logger.error(
                "admin_lead_creation_remote_unknown",
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
                "unknown",
                "pending_recovery",
                summary,
                failure_code="remote_create_unknown",
            )
            return AdminLeadCreationResult(
                operation_id, lead_id, "pending_recovery", "remote_create_unknown", summary
            )

        try:
            # 先单独冻结远端 record id；进程在后续本地 finalize 前崩溃时仍可核验事实。
            self._finish(
                operation_id,
                "succeeded",
                "processing",
                "",
                failure_code=None,
                smart_table_record_id=record.record_id,
            )
            self._finalize_local_creation(operation_id, lead_id, record.record_id)
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

    def reconcile(
        self,
        *,
        operation_id: str,
        operator_subject: str,
        operator_role: str,
        auth_source: str,
        request_id: str,
        reason: str,
    ) -> AdminLeadCreationResult:
        """核验远端已有记录并完成管理员补建，不盲目重复 create。"""

        auth_source = safe_audit_text(auth_source, max_length=128)
        if operator_role != "administrator" or not auth_source:
            raise PermissionError("管理员补建恢复必须使用 administrator 和有效 auth_source")
        request_id = validate_request_id(request_id)
        if not safe_audit_text(reason):
            raise ValueError("管理员补建恢复必须填写 request_id 和 reason")
        snapshot = self._claim_operation(operation_id, operator_subject, operator_role)
        if snapshot.final_status == "succeeded":
            return AdminLeadCreationResult(operation_id, snapshot.lead_id, "succeeded")
        if snapshot.final_status == "processing" and snapshot.remote_state == "pending":
            return AdminLeadCreationResult(operation_id, snapshot.lead_id, "processing")

        with self._session_factory() as session:
            lead = session.get(Lead, snapshot.lead_id)
            operator = session.get(SalesAuthorization, operator_subject)
            if (
                lead is None
                or operator is None
                or not operator.is_active
                or not operator.is_administrator
            ):
                raise PermissionError("只有活跃管理员可以恢复管理员补建")
            expected = {
                **lead.field_values,
                "创建人": snapshot.owner_user_id,
                "负责人": snapshot.owner_user_id,
            }
            record_id = snapshot.record_id
            company_name = lead.standard_company_name

        try:
            if record_id is not None:
                record = self._smart_table.get_record(record_id)
                candidates = [] if record is None else [record]
            else:
                candidates = self._smart_table.find_records(
                    {"线索名称": company_name, "负责人": snapshot.owner_user_id}
                )
        except Exception as exc:
            summary = self._safe_summary(exc)
            self._finish(
                operation_id,
                "unknown",
                "pending_recovery",
                summary,
                failure_code="remote_reconcile_failed",
            )
            return AdminLeadCreationResult(
                operation_id,
                snapshot.lead_id,
                "pending_recovery",
                "remote_reconcile_failed",
                summary,
            )

        matches = [
            candidate for candidate in candidates if self._matches_expected(candidate, expected)
        ]
        if len(matches) != 1:
            summary = "无法唯一证明远端记录属于该管理员补建 operation"
            self._finish(
                operation_id,
                "unknown",
                "pending_recovery",
                summary,
                failure_code="remote_record_unproven",
            )
            return AdminLeadCreationResult(
                operation_id,
                snapshot.lead_id,
                "pending_recovery",
                "remote_record_unproven",
                summary,
            )

        record_id = matches[0].record_id
        self._finish(
            operation_id,
            "succeeded",
            "processing",
            "",
            failure_code=None,
            smart_table_record_id=record_id,
        )
        try:
            self._finalize_local_creation(operation_id, snapshot.lead_id, record_id)
        except Exception as exc:
            summary = self._safe_summary(exc)
            self._finish(
                operation_id,
                "succeeded",
                "pending_recovery",
                summary,
                failure_code="local_finalize_failed",
                smart_table_record_id=record_id,
            )
            return AdminLeadCreationResult(
                operation_id, snapshot.lead_id, "pending_recovery", "local_finalize_failed", summary
            )
        return AdminLeadCreationResult(operation_id, snapshot.lead_id, "succeeded")

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
    ) -> tuple[str, str, str, bool]:
        """在远端调用前锁定身份并保存可恢复本地 operation。"""

        with self._session_factory.begin() as session:
            existing = session.scalar(
                select(AdminLeadCreationOperation).where(
                    AdminLeadCreationOperation.request_id == request_id
                )
            )
            if existing is not None:
                operator = session.get(SalesAuthorization, operator_subject)
                if operator is None or not operator.is_active or not operator.is_administrator:
                    raise PermissionError("只有活跃管理员可以恢复管理员补建")
                if existing.operator_subject != operator_subject:
                    raise PermissionError("request_id 不属于当前管理员")
                lead = session.get(Lead, existing.lead_id)
                if lead is None:
                    raise ValueError("request_id 对应的管理员补建线索不存在")
                if (
                    lead.original_capturing_sales_user_id != original_capturing_sales_user_id
                    or lead.smart_table_owner_user_id != smart_table_owner_user_id
                ):
                    raise ValueError("request_id 对应的四种身份事实不可修改")
                return existing.id, existing.lead_id, existing.final_status, False
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
            return operation.id, lead_id, operation.final_status, True

    def _claim_operation(
        self, operation_id: str, operator_subject: str, operator_role: str
    ) -> _AdminCreationSnapshot:
        """以 operation 行锁和更新时间 lease claim 未完成补建。"""

        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(AdminLeadCreationOperation)
                .where(AdminLeadCreationOperation.id == operation_id)
                .with_for_update()
            )
            operator = session.get(SalesAuthorization, operator_subject)
            if operation is None:
                raise ValueError("管理员创建 operation 不存在")
            if operator is None or not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以恢复管理员补建")
            if operator_role != "administrator":
                raise PermissionError("维护恢复角色必须是 administrator")
            snapshot = _AdminCreationSnapshot(
                operation.id,
                operation.lead_id,
                operation.smart_table_owner_user_id,
                operation.smart_table_record_id,
                operation.final_status,
                operation.remote_update_state,
            )
            if operation.final_status == "succeeded":
                return snapshot
            if operation.final_status == "processing" and not self._lease_expired(
                operation.updated_at
            ):
                return snapshot
            operation.final_status = "processing"
            operation.updated_at = utc_now()
            return snapshot

    def _claim_definite_retry(
        self, operation_id: str, operator_subject: str, operator_role: str
    ) -> None:
        """只对适配器明确未产生远端副作用的失败 operation claim retry。"""

        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(AdminLeadCreationOperation)
                .where(AdminLeadCreationOperation.id == operation_id)
                .with_for_update()
            )
            operator = session.get(SalesAuthorization, operator_subject)
            if operation is None or operator is None:
                raise ValueError("管理员创建 operation 或操作人不存在")
            if not operator.is_active or not operator.is_administrator:
                raise PermissionError("只有活跃管理员可以重试管理员补建")
            if operator_role != "administrator":
                raise PermissionError("维护重试角色必须是 administrator")
            if operation.final_status != "remote_create_failed":
                raise ValueError("只有明确未产生远端副作用的失败才允许重试")
            operation.final_status = "processing"
            operation.remote_update_state = "pending"
            operation.failure_code = None
            operation.failure_summary = None
            operation.updated_at = utc_now()

    def _finalize_local_creation(
        self, operation_id: str, lead_id: str, record_id: str
    ) -> None:
        """在远端记录已被核验后，原子收敛本地 Lead 与 operation。"""

        with self._session_factory.begin() as session:
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            operation = session.scalar(
                select(AdminLeadCreationOperation)
                .where(AdminLeadCreationOperation.id == operation_id)
                .with_for_update()
            )
            if lead is None or operation is None:
                raise ValueError("管理员创建 operation 或线索不存在")
            if lead.smart_table_record_id not in (None, record_id):
                raise ValueError("线索已有不同的智能表格记录事实")
            lead.smart_table_record_id = record_id
            operation.smart_table_record_id = record_id
            operation.remote_update_state = "succeeded"
            operation.final_status = "succeeded"
            operation.failure_code = None
            operation.failure_summary = None

    def _creation_facts(self, lead_id: str) -> tuple[str, dict[str, str]]:
        """读取管理员补建持久化快照，供首次写入和安全重试复用。"""

        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError("管理员补建线索不存在")
            return lead.smart_table_owner_user_id, dict(lead.field_values)

    @staticmethod
    def _matches_expected(record: SmartTableRecord, expected: Mapping[str, object]) -> bool:
        """判断远端记录是否完整匹配 frozen 字段，避免误认领其他记录。"""

        return all(record.fields.get(name) == value for name, value in expected.items())

    def _lease_expired(self, updated_at: datetime | None) -> bool:
        """判断 operation 更新时间是否超过安全恢复 lease。"""

        if updated_at is None:
            return True
        normalized = updated_at.replace(tzinfo=UTC) if updated_at.tzinfo is None else updated_at
        return utc_now() - normalized >= self._RECOVERY_LEASE

    def _finish(
        self,
        operation_id: str,
        remote_state: str,
        final_status: str,
        summary: str,
        *,
        failure_code: str | None = None,
        smart_table_record_id: str | None = None,
    ) -> None:
        """持久化管理员创建的远端失败或本地待恢复状态。"""

        with self._session_factory.begin() as session:
            operation = session.get(AdminLeadCreationOperation, operation_id)
            if operation is None:
                raise ValueError("管理员创建 operation 不存在")
            # 旧 worker 的晚到结果不得覆盖 recovery 已确认的 terminal 事实。
            if operation.final_status == "succeeded":
                return
            operation.remote_update_state = remote_state
            if smart_table_record_id is not None:
                operation.smart_table_record_id = smart_table_record_id
            operation.final_status = final_status
            operation.failure_code = failure_code
            operation.failure_summary = summary or None
            operation.updated_at = utc_now()

    @staticmethod
    def _safe_summary(error: Exception) -> str:
        """裁剪异常摘要，避免把敏感载荷写入审计。"""

        return safe_failure_summary(error)
