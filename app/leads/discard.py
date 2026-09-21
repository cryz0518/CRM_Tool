"""提供受控的 Lead 逻辑废弃与 CRM create 在途协调。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.leads.models import CrmCompanyIdentity, CrmSyncRecord, Lead, LeadDiscardRequest
from app.messaging.models import BusinessAuditEvent, SalesAuthorization, utc_now


class LeadDiscardStatus(StrEnum):
    """描述一次受控逻辑废弃请求的结果。"""

    DISCARDED = "discarded"
    WAITING_FOR_CRM = "waiting_for_crm"
    ALREADY_DISCARDED = "already_discarded"
    NOT_EFFECTIVE = "not_effective"


@dataclass(frozen=True)
class LeadDiscardResult:
    """返回废弃请求状态及其目标线索标识。"""

    status: LeadDiscardStatus
    lead_id: str
    discard_request_id: int | None = None


class LeadDiscardService:
    """以确定性权限和事务顺序执行线索逻辑废弃，不物理删除表格或来源事实。"""

    _DISCARDABLE_LIFECYCLE_STATES = frozenset({"temporary", "pending_create"})
    _IN_FLIGHT_CRM_STATUSES = frozenset({"pending", "processing", "retrying"})

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """注入逻辑废弃所需的数据库事务工厂。

        参数：session_factory 创建短事务会话。
        返回值：无。
        异常：无。
        副作用：仅保存数据库依赖，不读写数据。
        """
        self._session_factory = session_factory

    def discard(
        self,
        lead_id: str,
        operator_user_id: str,
        reason: str,
        *,
        operation_id: str | None = None,
    ) -> LeadDiscardResult:
        """受控废弃未同步线索，并在 CRM create 在途时等待外部最终事实。

        参数：lead_id 为目标线索；operator_user_id 为当前授权销售或管理员；reason 为审计原因。
        返回值：废弃成功、等待 CRM 或已处理的确定性结果。
        异常：线索/操作人不存在、权限不足、状态不允许或原因为空时抛出 ValueError/PermissionError。
        副作用：新增废弃请求和审计；仅在未启动 create 时将生命周期改为 discarded，
        绝不删除 Smart Table 记录。
        """
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("废弃线索必须填写原因")
        with self._session_factory.begin() as session:
            # 先锁 Lead，确保本事务之后读取的 CRM create 是锁释放后的最新提交事实。
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            # CRM create 也采用 Lead -> Sync 锁序，避免 discard 与 create 互相等待。
            sync = session.scalar(
                select(CrmSyncRecord)
                .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create")
                .with_for_update()
            )
            operator = session.get(SalesAuthorization, operator_user_id)
            if lead is None or operator is None:
                raise ValueError("线索或废弃操作人不存在")
            if not operator.is_active or not (operator.is_authorized or operator.is_administrator):
                raise PermissionError("操作人没有废弃线索权限")
            if not operator.is_administrator and lead.smart_table_owner_user_id != operator_user_id:
                raise PermissionError("普通销售只能废弃自己负责的线索")
            operator_role = "administrator" if operator.is_administrator else "sales"
            previous_lifecycle_state = lead.lifecycle_state
            if operation_id is not None:
                operation_request = session.scalar(
                    select(LeadDiscardRequest)
                    .where(LeadDiscardRequest.operation_id == operation_id)
                    .with_for_update()
                )
                if operation_request is not None:
                    operation_status = {
                        "effective": LeadDiscardStatus.DISCARDED,
                        "not_effective": LeadDiscardStatus.NOT_EFFECTIVE,
                        "pending": LeadDiscardStatus.WAITING_FOR_CRM,
                    }.get(operation_request.status, LeadDiscardStatus.WAITING_FOR_CRM)
                    return LeadDiscardResult(
                        operation_status,
                        lead_id,
                        operation_request.id,
                    )
            request = session.scalar(
                select(LeadDiscardRequest)
                .where(LeadDiscardRequest.lead_id == lead_id)
                .with_for_update()
            )
            if request is not None:
                if request.status == "not_effective":
                    return LeadDiscardResult(LeadDiscardStatus.NOT_EFFECTIVE, lead_id, request.id)
                if request.status == "effective":
                    return LeadDiscardResult(
                        LeadDiscardStatus.ALREADY_DISCARDED, lead_id, request.id
                    )
                return LeadDiscardResult(LeadDiscardStatus.WAITING_FOR_CRM, lead_id, request.id)
            if lead.lifecycle_state == "discarded":
                return LeadDiscardResult(
                    LeadDiscardStatus.ALREADY_DISCARDED,
                    lead_id,
                    None,
                )
            if lead.lifecycle_state not in self._DISCARDABLE_LIFECYCLE_STATES:
                raise ValueError("已同步或已进入更新流程的线索不能直接废弃")

            request = LeadDiscardRequest(
                lead_id=lead_id,
                operator_user_id=operator_user_id,
                operator_role="administrator" if operator.is_administrator else "sales",
                reason=normalized_reason,
                operation_id=operation_id,
                status="pending",
            )
            session.add(request)
            session.flush()
            if sync is not None and (
                sync.status in self._IN_FLIGHT_CRM_STATUSES
                or (sync.status == "failed_pending_review" and sync.failure_category == "unknown")
            ):
                # 已登记的 create 即使尚未认领外部调用，也必须等待同一冻结操作的最终事实。
                self._record_audit(
                    session,
                    lead,
                    operator_user_id,
                    "lead_discard_requested_while_crm_create_processing",
                    {
                        "lead_id": lead.id,
                        "operator_user_id": operator_user_id,
                        "operator_role": operator_role,
                        "old_lifecycle_state": previous_lifecycle_state,
                        "new_lifecycle_state": previous_lifecycle_state,
                        "discard_request_id": request.id,
                        "crm_sync_record_id": sync.id,
                    },
                )
                return LeadDiscardResult(LeadDiscardStatus.WAITING_FOR_CRM, lead_id, request.id)

            if sync is not None and sync.status == "succeeded":
                # CRM 成功事实优先；异常本地生命周期不能通过 discard 被继续扩大。
                raise ValueError("CRM 已成功创建的线索不能废弃")

            if sync is None or sync.status == "failed_pending_review":
                self._release_pending_identity(session, lead, operator_user_id, operator_role)
            lead.lifecycle_state = "discarded"
            request.status = "effective"
            request.completed_at = utc_now()
            self._record_audit(
                session,
                lead,
                operator_user_id,
                "lead_discarded",
                {
                    "lead_id": lead.id,
                    "operator_user_id": operator_user_id,
                    "operator_role": operator_role,
                    "old_lifecycle_state": previous_lifecycle_state,
                    "new_lifecycle_state": lead.lifecycle_state,
                    "discard_request_id": request.id,
                    "reason": normalized_reason,
                },
            )
            if sync is not None:
                self._record_audit(
                    session,
                    lead,
                    operator_user_id,
                    "crm_create_skipped_after_discard",
                    {
                        "lead_id": lead.id,
                        "operator_user_id": operator_user_id,
                        "operator_role": operator_role,
                        "old_lifecycle_state": previous_lifecycle_state,
                        "new_lifecycle_state": lead.lifecycle_state,
                        "discard_request_id": request.id,
                        "crm_sync_record_id": sync.id,
                    },
                )
            return LeadDiscardResult(LeadDiscardStatus.DISCARDED, lead_id, request.id)

    @staticmethod
    def _release_pending_identity(
        session: Session, lead: Lead, sales_user_id: str, operator_role: str
    ) -> None:
        """释放尚未产生 CRM 外部事实的公司预留，避免废弃后阻塞其他销售。

        参数：session 为当前短事务；lead 为已锁定的废弃线索；sales_user_id 为操作人。
        返回值：无。
        异常：数据库读取或删除失败时由 SQLAlchemy 抛出。
        副作用：仅删除 state=reserving 且属于本 Lead 的临时注册表预留，并写入审计；不删除 CRM 事实。
        """
        identity = session.scalar(
            select(CrmCompanyIdentity)
            .where(
                CrmCompanyIdentity.creating_lead_id == lead.id,
                CrmCompanyIdentity.state == "reserving",
            )
            .with_for_update()
        )
        if identity is None:
            return
        session.delete(identity)
        LeadDiscardService._record_audit(
            session,
            lead,
            sales_user_id,
            "crm_global_identity_reservation_released_after_discard",
            {
                "lead_id": lead.id,
                "operator_user_id": sales_user_id,
                "operator_role": operator_role,
                "standard_company_name_hash": hashlib.sha256(
                    identity.standard_company_name.encode()
                ).hexdigest()
            },
        )

    @staticmethod
    def _record_audit(
        session: Session,
        lead: Lead,
        sales_user_id: str,
        event_type: str,
        details: dict[str, object],
    ) -> None:
        """按来源消息和事件类型幂等保存废弃审计。

        参数：session 为当前事务；lead 提供来源消息；sales_user_id 为审计操作人；
        其余参数为事件事实。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：首次出现的审计事件写入业务审计表。
        """
        # 管理员补建线索没有来源消息；统一 Console 审计已覆盖该操作，不伪造消息审计键。
        if lead.source_message_id is None:
            return
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == lead.source_message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=lead.source_message_id,
                    sales_user_id=sales_user_id,
                    event_type=event_type,
                    details=details,
                )
            )
