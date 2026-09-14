"""确定性 CRM 首次创建命令、最终快照与逻辑重试服务。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.crm.adapter import CRMAdapter
from app.leads.models import CrmCompanyIdentity, CrmSyncRecord, Lead
from app.leads.review import LeadReviewService
from app.messaging.models import BusinessAuditEvent, SalesAuthorization, utc_now
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.registry import CRM_BUSINESS_FIELD_NAMES

_TODAY_COMMAND = "提交今天的线索"
_UPDATES_COMMAND = "提交我的更新"
_BUSINESS_COMPLETENESS_FIELDS = (
    "业务线",
    "线索名称",
    "线索来源",
    "联系人",
    "职务",
    "沟通方式",
    "手机",
    "备注",
)
_CONTACT_FIELDS = ("手机", "电话", "邮箱")
_CRM_PROCESSING_LEASE = timedelta(minutes=5)


@dataclass(frozen=True)
class SubmissionCommand:
    """承载机器人已接收的确定性提交命令事实。"""

    text: str
    sales_user_id: str
    request_message_id: str


@dataclass(frozen=True)
class SubmissionBatchResult:
    """汇总一条确定性提交命令的部分成功结果。"""

    succeeded: int = 0
    incomplete: int = 0
    retrying: int = 0
    processing: int = 0
    failed_pending_review: int = 0
    updates_not_implemented: bool = False
    incomplete_lead_ids: tuple[str, ...] = ()
    updated: int = 0
    unchanged: int = 0
    company_identity_review: int = 0


class CrmSubmissionService:
    """仅执行 T12 首次 CRM create，绝不承担 T13 更新职责。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        crm_adapter: CRMAdapter,
        crm_create_retry_count: int | None = None,
    ) -> None:
        """保存数据库、表格、CRM 与重试上限依赖。

        参数：前三项分别提供持久化、规范表格回读和唯一 create 调用；最后一项可覆盖配置。
        返回值：无。
        异常：无。
        副作用：只保存依赖，不读取数据库或调用外部系统。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._crm_adapter = crm_adapter
        self._crm_create_retry_count = (
            get_settings().crm_create_retry_count
            if crm_create_retry_count is None
            else crm_create_retry_count
        )

    def submit(self, command: SubmissionCommand) -> SubmissionBatchResult:
        """解析固定命令并逐条提交当日本人待创建 Lead，保持批次部分成功。

        参数：command 为机器人已可靠接收的文本命令和提交销售身份。
        返回值：成功、待完善和外部失败的汇总；更新命令明确标记未实现。
        异常：未知命令或未授权销售抛出 ValueError，避免 LLM 推断权限。
        副作用：可能创建或重试一个冻结的 CRM Sync Record，并调用 CRM create。
        """
        if command.text == _UPDATES_COMMAND:
            return self._submit_updates(command)
        if command.text != _TODAY_COMMAND:
            raise ValueError("不支持的 CRM 提交命令")

        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
            candidate_ids = [
                lead_id
                for lead_id in session.scalars(
                    select(Lead.id).where(
                        Lead.smart_table_owner_user_id == command.sales_user_id,
                        Lead.lifecycle_state == "pending_create",
                    )
                )
            ]

        result = SubmissionBatchResult()
        for lead_id in candidate_ids:
            # 每条独立执行；任何一条失败都不得影响后续候选。
            outcome = self._submit_create(lead_id, command)
            result = SubmissionBatchResult(
                succeeded=result.succeeded + (outcome == "succeeded"),
                incomplete=result.incomplete + (outcome == "incomplete"),
                retrying=result.retrying + (outcome == "retrying"),
                processing=result.processing + (outcome == "processing"),
                failed_pending_review=(
                    result.failed_pending_review + (outcome == "failed_pending_review")
                ),
                incomplete_lead_ids=(
                    result.incomplete_lead_ids + (lead_id,)
                    if outcome == "incomplete"
                    else result.incomplete_lead_ids
                ),
            )
        return result

    def _submit_updates(self, command: SubmissionCommand) -> SubmissionBatchResult:
        """扫描当前表格负责人的已同步记录，并仅提交规范业务快照差异。"""
        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
            candidate_ids = list(
                session.scalars(
                    select(Lead.id).where(
                        Lead.smart_table_owner_user_id == command.sales_user_id,
                        Lead.lifecycle_state == "synced",
                        Lead.smart_table_record_id.is_not(None),
                    )
                )
            )
        result = SubmissionBatchResult()
        for lead_id in candidate_ids:
            outcome = self._submit_update(lead_id, command)
            result = SubmissionBatchResult(
                succeeded=result.succeeded,
                incomplete=result.incomplete + (outcome == "incomplete"),
                retrying=result.retrying + (outcome == "retrying"),
                processing=result.processing + (outcome == "processing"),
                failed_pending_review=result.failed_pending_review
                + (outcome == "failed_pending_review"),
                updated=result.updated + (outcome == "succeeded"),
                unchanged=result.unchanged + (outcome == "unchanged"),
                company_identity_review=result.company_identity_review
                + (outcome == "company_identity_review"),
            )
        return result

    def _submit_update(self, lead_id: str, command: SubmissionCommand) -> str:
        """重读一条已同步线索，冻结一个新业务快照或复用其既有 update 重试。"""
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if (
                lead is None
                or lead.smart_table_owner_user_id != command.sales_user_id
                or lead.lifecycle_state != "synced"
            ):
                return "incomplete"
            latest = self._last_successful_sync(session, lead_id)
            if latest is None or latest.crm_lead_id is None:
                return "incomplete"
            # 未完成 update 是冻结事实，必须优先恢复，不能先读表并换掉 payload。
            unfinished = session.scalar(
                select(CrmSyncRecord)
                .where(
                    CrmSyncRecord.lead_id == lead_id,
                    CrmSyncRecord.operation == "update",
                    CrmSyncRecord.status.in_(("pending", "retrying", "processing")),
                )
                .order_by(CrmSyncRecord.id.asc())
            )
            if unfinished is not None:
                return self._claim_and_call(unfinished.id, command.sales_user_id)
        reconciled = LeadReviewService(
            self._session_factory, self._smart_table_adapter
        ).reconcile_submission(lead_id)
        if reconciled.blocking_fields:
            return "incomplete"
        payload = self._canonical_payload(reconciled.fields)
        previous = dict(latest.canonical_payload)
        # 公司身份是全局去重键，变动绝不能伪装成普通字段更新。
        if payload.get("线索名称") != previous.get("线索名称"):
            with self._session_factory.begin() as session:
                lead = session.get(Lead, lead_id)
                if lead is not None:
                    lead.lifecycle_state = "company_identity_change_pending_review"
                    self._record_audit(session, command, "company_identity_change_pending_review")
            return "company_identity_review"
        snapshot_hash = self._snapshot_hash(payload)
        if payload == previous:
            return "unchanged"
        with self._session_factory.begin() as session:
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if lead is None or authorization is None or authorization.crm_user_id is None:
                return "incomplete"
            existing = session.scalar(
                select(CrmSyncRecord).where(
                    CrmSyncRecord.lead_id == lead_id,
                    CrmSyncRecord.operation == "update",
                    CrmSyncRecord.snapshot_hash == snapshot_hash,
                )
            )
            if existing is not None:
                return (
                    "unchanged"
                    if existing.status == "succeeded"
                    else self._claim_and_call(existing.id, command.sales_user_id)
                )
            # 已持久化成功同步事实是全局身份索引；绝不读取其他销售的智能表格。
            target = self._global_crm_identity(session, lead.standard_company_name) or latest
            sync = CrmSyncRecord(
                lead_id=lead.id,
                operation="update",
                smart_table_record_id=lead.smart_table_record_id or "",
                idempotency_key=f"crm:update:{lead.id}:{snapshot_hash}",
                canonical_payload=payload,
                snapshot_hash=snapshot_hash,
                request_message_id=command.request_message_id,
                submitting_sales_user_id=command.sales_user_id,
                submitting_crm_user_id=authorization.crm_user_id,
                crm_lead_id=target.crm_lead_id,
                crm_lead_owner_user_id=target.crm_lead_owner_user_id,
            )
            session.add(sync)
            session.flush()
            sync_id = sync.id
        return self._claim_and_call(sync_id, command.sales_user_id)

    def _submit_create(self, lead_id: str, command: SubmissionCommand) -> str:
        """为单个 Lead 建立或认领一次稳定的 create 逻辑操作。

        参数：lead_id 为候选线索；command 为冻结提交销售和请求消息身份。
        返回值：succeeded、incomplete 或 failed，供批次独立汇总。
        异常：表格读取错误向调用方传播，避免将未知最终快照提交给 CRM。
        副作用：首次操作冻结 payload/映射；可调用一次 CRM create。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None or not self._is_today_owned_candidate(lead, command.sales_user_id):
                return "incomplete"
            existing = session.scalar(
                select(CrmSyncRecord).where(
                    CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create"
                )
            )
            if existing is not None:
                if existing.status == "succeeded":
                    return "incomplete"
                # retry 必须使用现有冻结快照，绝不重新读表替换 payload。
                return self._claim_and_call(existing.id, command.sales_user_id)

        reconciled = LeadReviewService(
            self._session_factory, self._smart_table_adapter
        ).reconcile_submission(lead_id)
        missing_business = self._missing_business_fields(reconciled.fields)
        missing_minimum = self._missing_crm_minimum(reconciled.fields)
        if missing_business:
            # 八项业务完整度仍用于明确审核反馈，不能被 CRM minimum 偷换定义。
            self._audit(command, "crm_business_fields_incomplete")
        if reconciled.blocking_fields or missing_minimum:
            self._audit(command, "crm_create_incomplete")
            return "incomplete"

        canonical_payload = self._canonical_payload(reconciled.fields)
        snapshot_hash = self._snapshot_hash(canonical_payload)
        # 事务内锁定 Lead 后检查 create 单例；数据库 partial unique index 是并发最终兜底。
        try:
            with self._session_factory.begin() as session:
                lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
                authorization = session.scalar(
                    select(SalesAuthorization)
                    .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
                    .with_for_update()
                )
                if (
                    lead is None
                    or authorization is None
                    or not self._is_today_owned_candidate(lead, command.sales_user_id)
                ):
                    return "incomplete"
                if authorization.crm_user_id is None:
                    self._record_audit(session, command, "crm_mapping_missing")
                    return "incomplete"
                existing = session.scalar(
                    select(CrmSyncRecord).where(
                        CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.operation == "create"
                    )
                )
                if existing is not None:
                    return "incomplete" if existing.status == "succeeded" else existing.status
                # 以唯一公司预留锁住首次创建；其他销售绝不能在 reserving 时另建 CRM Lead。
                company_name = lead.standard_company_name
                if not company_name:
                    return "incomplete"
                identity = session.get(CrmCompanyIdentity, company_name)
                if identity is not None and identity.state == "reserving":
                    return "processing"
                if identity is not None and identity.state == "failed_pending_review":
                    return "failed_pending_review"
                target = self._global_crm_identity(session, company_name)
                if identity is None and target is None:
                    identity = CrmCompanyIdentity(
                        standard_company_name=company_name, creating_lead_id=lead.id
                    )
                    session.add(identity)
                    session.flush()
                    self._record_audit(session, command, "crm_global_identity_reserved")
                elif identity is not None and identity.state == "active":
                    self._record_audit(session, command, "crm_global_identity_reused")
                operation = "update" if target is not None else "create"
                idempotency_key = (
                    f"crm:update:{lead.id}:{snapshot_hash}"
                    if target is not None
                    else f"crm:create:{lead.id}"
                )
                sync = CrmSyncRecord(
                    lead_id=lead.id,
                    operation=operation,
                    smart_table_record_id=lead.smart_table_record_id or "",
                    idempotency_key=idempotency_key,
                    canonical_payload=canonical_payload,
                    snapshot_hash=snapshot_hash,
                    request_message_id=command.request_message_id,
                    submitting_sales_user_id=command.sales_user_id,
                    # 销售 CRM 映射在逻辑 create 创建时冻结，retry 不重新读取它。
                    submitting_crm_user_id=authorization.crm_user_id,
                    crm_lead_id=target.crm_lead_id if target is not None else None,
                    crm_lead_owner_user_id=(
                        target.crm_lead_owner_user_id if target is not None else None
                    ),
                )
                session.add(sync)
                session.flush()
                if operation == "create" and identity is not None:
                    identity.creating_sync_record_id = sync.id
                sync_id = sync.id
        except IntegrityError:
            # 并发创建只能有一个胜者；另一个调用不允许再触发 CRM。
            return "incomplete"
        return self._claim_and_call(sync_id, command.sales_user_id)

    def _claim_and_call(self, sync_id: int, sales_user_id: str) -> str:
        """原子认领 pending/retrying 同步记录后调用 CRM，并持久化独立结果。"""
        with self._session_factory.begin() as session:
            sync = session.scalar(
                select(CrmSyncRecord).where(CrmSyncRecord.id == sync_id).with_for_update()
            )
            if sync is None:
                return "failed_pending_review"
            lease_expired = (
                sync.status == "processing"
                and sync.processing_lease_expires_at is not None
                and self._as_utc(sync.processing_lease_expires_at) <= utc_now()
            )
            if sync.status not in {"pending", "retrying"} and not lease_expired:
                return sync.status
            if sync.attempts >= self._crm_create_retry_count:
                # 尝试次数达到上限后保留冻结操作供人工处理，禁止无限重试。
                sync.status = "failed_pending_review"
                sync.response_summary = "CRM retry limit reached"
                return "failed_pending_review"
            sync.status = "processing"
            sync.attempts += 1
            sync.processing_started_at = utc_now()
            sync.processing_lease_expires_at = sync.processing_started_at + _CRM_PROCESSING_LEASE
            payload = dict(sync.canonical_payload)
            idempotency_key = sync.idempotency_key
            crm_user_id = sync.submitting_crm_user_id
            operation = sync.operation
            crm_lead_id = sync.crm_lead_id

        try:
            if operation == "update":
                crm_result = self._crm_adapter.update_lead(
                    crm_lead_id or "",
                    payload,
                    idempotency_key=idempotency_key,
                    crm_user_id=crm_user_id,
                )
            else:
                crm_result = self._crm_adapter.create_lead(
                    payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id
                )
        except (TimeoutError, ConnectionError, OSError):
            with self._session_factory.begin() as session:
                sync = session.get(CrmSyncRecord, sync_id)
                if sync is not None:
                    sync.status = "retrying"
                    sync.processing_started_at = None
                    sync.processing_lease_expires_at = None
                    sync.response_summary = "CRM transport error"
            return "retrying"
        except Exception:
            with self._session_factory.begin() as session:
                sync = session.get(CrmSyncRecord, sync_id)
                if sync is not None:
                    sync.status = "failed_pending_review"
                    sync.processing_started_at = None
                    sync.processing_lease_expires_at = None
                    sync.response_summary = "CRM create failed"
            return "failed_pending_review"

        with self._session_factory.begin() as session:
            sync = session.get(CrmSyncRecord, sync_id)
            lead = session.get(Lead, sync.lead_id) if sync is not None else None
            if sync is None or lead is None:
                return "failed_pending_review"
            sync.status = "succeeded"
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            sync.crm_lead_id = crm_result.crm_lead_id
            # 更新响应无权把既有 CRM 负责人改写为本次提交人。
            if crm_result.crm_lead_owner_user_id is not None:
                sync.crm_lead_owner_user_id = crm_result.crm_lead_owner_user_id
            sync.response_summary = crm_result.response_summary[:256]
            sync.completed_at = utc_now()
            if sync.operation == "create":
                identity = session.scalar(
                    select(CrmCompanyIdentity)
                    .where(CrmCompanyIdentity.creating_sync_record_id == sync.id)
                    .with_for_update()
                )
                if identity is not None:
                    identity.state = "active"
                    identity.crm_lead_id = sync.crm_lead_id
                    identity.crm_lead_owner_user_id = sync.crm_lead_owner_user_id
            lead.lifecycle_state = "synced"
            self._record_audit_for_sync(
                session, sync, sales_user_id, f"crm_{sync.operation}_succeeded"
            )
        return "succeeded"

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """将数据库返回的处理租约时间统一解释为 UTC。"""
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    @staticmethod
    def _canonical_payload(fields: dict[str, object]) -> dict[str, object]:
        """只保留 CRM 注册业务字段并稳定清理空文本，排除 AI待确认 等元数据。"""
        return {
            name: value.strip() if isinstance(value, str) else value
            for name, value in fields.items()
            if name in CRM_BUSINESS_FIELD_NAMES and value not in (None, "")
        }

    @staticmethod
    def _snapshot_hash(payload: dict[str, object]) -> str:
        """计算已冻结规范 payload 的审计哈希；它不参与 create 幂等键。"""
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _missing_business_fields(fields: dict[str, object]) -> tuple[str, ...]:
        """按既有八项业务完整度定义返回缺失字段，不改写为 CRM minimum。"""
        return tuple(name for name in _BUSINESS_COMPLETENESS_FIELDS if not fields.get(name))

    @staticmethod
    def _missing_crm_minimum(fields: dict[str, object]) -> tuple[str, ...]:
        """独立计算首次创建最低字段，联系方式按 OR 条件处理。"""
        missing = [name for name in ("线索名称", "业务线") if not fields.get(name)]
        if not any(fields.get(name) for name in _CONTACT_FIELDS):
            missing.append("联系方式")
        return tuple(missing)

    @staticmethod
    def _is_today_owned_candidate(lead: Lead, sales_user_id: str) -> bool:
        """判定 Lead 是否为提交销售本人负责、当日且仍待首次创建的候选。"""
        shanghai = ZoneInfo("Asia/Shanghai")
        created = lead.created_at if lead.created_at.tzinfo else lead.created_at.replace(tzinfo=UTC)
        return (
            lead.smart_table_owner_user_id == sales_user_id
            and lead.lifecycle_state == "pending_create"
            and created.astimezone(shanghai).date() == datetime.now(shanghai).date()
        )

    @staticmethod
    def _last_successful_sync(session: Session, lead_id: str) -> CrmSyncRecord | None:
        """读取一条线索最后成功的 CRM 快照，作为 update 差异基线。"""
        return session.scalar(
            select(CrmSyncRecord)
            .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.status == "succeeded")
            .order_by(CrmSyncRecord.completed_at.desc(), CrmSyncRecord.id.desc())
        )

    @staticmethod
    def _global_crm_identity(
        session: Session, standard_company_name: str | None
    ) -> CrmSyncRecord | None:
        """仅按标准公司名称查询本地成功 CRM 事实，返回首次成功的既有身份。"""
        if not standard_company_name:
            return None
        return session.scalar(
            select(CrmSyncRecord)
            .join(Lead, CrmSyncRecord.lead_id == Lead.id)
            .where(
                Lead.standard_company_name == standard_company_name,
                CrmSyncRecord.status == "succeeded",
                CrmSyncRecord.crm_lead_id.is_not(None),
            )
            .order_by(CrmSyncRecord.completed_at.asc(), CrmSyncRecord.id.asc())
        )

    def _audit(self, command: SubmissionCommand, event_type: str) -> None:
        """为未创建 CRM 同步记录的普通校验结果保存审计反馈。"""
        with self._session_factory.begin() as session:
            self._record_audit(session, command, event_type)

    @staticmethod
    def _record_audit(session: Session, command: SubmissionCommand, event_type: str) -> None:
        """在来源消息存在时幂等保存一条业务审计事件。"""
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == command.request_message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=command.request_message_id,
                    sales_user_id=command.sales_user_id,
                    event_type=event_type,
                )
            )

    @staticmethod
    def _record_audit_for_sync(
        session: Session, sync: CrmSyncRecord, sales_user_id: str, event_type: str
    ) -> None:
        """为 CRM 成功事实写入以首次请求消息归属的可查询审计。"""
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == sync.request_message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=sync.request_message_id,
                    sales_user_id=sales_user_id,
                    event_type=event_type,
                )
            )
