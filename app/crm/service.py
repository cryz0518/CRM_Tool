"""确定性 CRM 首次创建命令、最终快照与逻辑重试服务。"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.failures import classify_task_failure, safe_failure_summary
from app.crm.adapter import CRMAdapter
from app.crm.user_mapping import CRMUserMapper, DatabaseCRMUserMapper
from app.leads.models import CrmCompanyIdentity, CrmSyncRecord, Lead, LeadDiscardRequest
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
_LOGGER = logging.getLogger(__name__)


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
    mapping_missing: int = 0


@dataclass(frozen=True)
class GlobalIdentityResolution:
    """描述全局 CRM 公司身份注册表的一次确定性解析结果。"""

    state: str
    identity: CrmCompanyIdentity | None = None


class CrmSubmissionService:
    """执行冻结 CRM create/update，并以全局公司身份注册表阻止跨销售重复创建。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        crm_adapter: CRMAdapter,
        crm_create_retry_count: int | None = None,
        crm_user_mapper: CRMUserMapper | None = None,
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
        self._crm_user_mapper = crm_user_mapper or DatabaseCRMUserMapper()

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
                mapping_missing=result.mapping_missing + (outcome == "mapping_missing"),
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
                        Lead.lifecycle_state.in_(("synced", "pending_update")),
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
                mapping_missing=result.mapping_missing + (outcome == "mapping_missing"),
            )
        return result

    def _submit_update(self, lead_id: str, command: SubmissionCommand) -> str:
        """重读一条已同步线索，冻结一个新业务快照或复用其既有 update 重试。"""
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if (
                lead is None
                or lead.smart_table_owner_user_id != command.sales_user_id
                or lead.lifecycle_state not in {"synced", "pending_update"}
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
                    session.add(
                        BusinessAuditEvent(
                            message_id=command.request_message_id,
                            sales_user_id=command.sales_user_id,
                            event_type="company_identity_change_pending_review",
                            details={
                                "old_standard_company_name": previous.get("线索名称"),
                                "new_candidate_company_name": payload.get("线索名称"),
                                "lead_id": lead.id,
                                "smart_table_record_id": lead.smart_table_record_id,
                                "smart_table_owner_user_id": lead.smart_table_owner_user_id,
                            },
                        )
                    )
                    _LOGGER.info(
                        "crm_submission_event=company_identity_change_pending_review "
                        "request_id=%s message_id=%s lead_id=%s record_id=%s wecom_user_id=%s",
                        command.request_message_id,
                        command.request_message_id,
                        lead.id,
                        lead.smart_table_record_id,
                        command.sales_user_id,
                    )
            return "company_identity_review"
        snapshot_hash = self._snapshot_hash(payload)
        if payload == previous:
            if lead.lifecycle_state == "pending_update":
                with self._session_factory.begin() as session:
                    current = session.scalar(
                        select(Lead).where(Lead.id == lead_id).with_for_update()
                    )
                    if current is not None and current.lifecycle_state == "pending_update":
                        current.lifecycle_state = "synced"
            return "unchanged"
        existing_sync_id: int | None = None
        with self._session_factory.begin() as session:
            lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if lead is None or authorization is None:
                return "incomplete"
            crm_user_id = self._crm_user_mapper.get_crm_user_id(session, command.sales_user_id)
            if crm_user_id is None:
                return self._record_mapping_missing(
                    session, lead, command, payload, snapshot_hash, "update"
                )
            existing = session.scalar(
                select(CrmSyncRecord).where(
                    CrmSyncRecord.lead_id == lead_id,
                    CrmSyncRecord.operation == "update",
                    CrmSyncRecord.snapshot_hash == snapshot_hash,
                )
            )
            if existing is not None:
                if existing.status == "succeeded":
                    return "unchanged"
                # 先提交并释放 Lead 行锁，再认领既有冻结操作。
                existing_sync_id = existing.id
            if existing_sync_id is not None:
                # 该分支只记录认领目标，不能在持锁事务中调用 CRM。
                pass
            else:
                # 注册表是身份权威；历史同步仅在注册表缺失时由解析器一次性回填。
                resolution = self._resolve_global_identity(session, lead, command)
                if resolution.state in {"ambiguous", "failed_pending_review"}:
                    return "failed_pending_review"
                if resolution.state == "reserving":
                    return "processing"
                target = resolution.identity
                if target is None or target.crm_lead_id is None:
                    return "failed_pending_review"
                sync = CrmSyncRecord(
                    lead_id=lead.id,
                    operation="update",
                    smart_table_record_id=lead.smart_table_record_id or "",
                    idempotency_key=f"crm:update:{lead.id}:{snapshot_hash}",
                    canonical_payload=payload,
                    snapshot_hash=snapshot_hash,
                    request_message_id=command.request_message_id,
                    submitting_sales_user_id=command.sales_user_id,
                    submitting_crm_user_id=crm_user_id,
                    crm_lead_id=target.crm_lead_id,
                    crm_lead_owner_user_id=target.crm_lead_owner_user_id,
                )
                session.add(sync)
                session.flush()
                self._record_audit_for_sync(
                    session, sync, command.sales_user_id, "crm_update_created"
                )
                sync_id = sync.id
        return self._claim_and_call(existing_sync_id or sync_id, command.sales_user_id)

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
                crm_user_id = self._crm_user_mapper.get_crm_user_id(session, command.sales_user_id)
                if crm_user_id is None:
                    return self._record_mapping_missing(
                        session, lead, command, canonical_payload, snapshot_hash, "create"
                    )
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
                resolution = self._resolve_global_identity(session, lead, command)
                if resolution.state == "reserving":
                    return "processing"
                if resolution.state in {"ambiguous", "failed_pending_review"}:
                    return "failed_pending_review"
                identity = resolution.identity
                if resolution.state == "no_identity":
                    identity = self._reserve_global_identity(session, lead, command)
                    if identity is None:
                        # 唯一键竞争后重新读取的 reservation 已成为其他销售的冻结事实。
                        return "processing"
                if identity is None:
                    return "failed_pending_review"
                if identity.state == "active":
                    self._record_audit(session, command, "crm_global_identity_reused")
                operation = "update" if identity.state == "active" else "create"
                idempotency_key = (
                    f"crm:update:{lead.id}:{snapshot_hash}"
                    if operation == "update"
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
                    submitting_crm_user_id=crm_user_id,
                    crm_lead_id=identity.crm_lead_id if operation == "update" else None,
                    crm_lead_owner_user_id=(
                        identity.crm_lead_owner_user_id if operation == "update" else None
                    ),
                )
                session.add(sync)
                session.flush()
                if operation == "create" and identity is not None:
                    identity.creating_sync_record_id = sync.id
                elif operation == "update":
                    self._record_audit_for_sync(
                        session, sync, command.sales_user_id, "crm_update_created"
                    )
                sync_id = sync.id
        except IntegrityError:
            # 只作为最后一层兜底；正常唯一键竞争会在 savepoint 中恢复并返回 processing。
            return "processing"
        return self._claim_and_call(sync_id, command.sales_user_id)

    def _record_mapping_missing(
        self,
        session: Session,
        lead: Lead,
        command: SubmissionCommand,
        payload: dict[str, object],
        snapshot_hash: str,
        requested_operation: str,
    ) -> str:
        """持久化不调用 CRM 的映射缺失终态验证记录。

        参数：session 为已锁定 Lead 的当前事务；lead 为待提交线索；command 为提交事实；
        payload 与 snapshot_hash 为已审核快照；requested_operation 为原本请求的 create 或 update。
        返回值：固定返回 mapping_missing，供批次汇总明确反馈。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：写入不可自动重试的验证记录；update 同时保留 pending_update 生命周期。
        """
        # 操作、线索和请求消息合计可超过同步表幂等键列的长度限制，统一派生固定长度键。
        validation_identity = f"{requested_operation}:{lead.id}:{command.request_message_id}"
        validation_digest = hashlib.sha256(validation_identity.encode("utf-8")).hexdigest()
        idempotency_key = f"crm:validation:mapping_missing:{validation_digest}"
        existing = session.scalar(
            select(CrmSyncRecord).where(CrmSyncRecord.idempotency_key == idempotency_key)
        )
        if existing is None:
            sync = CrmSyncRecord(
                lead_id=lead.id,
                # 验证失败没有开始 CRM create/update，不能占用两类操作的唯一约束。
                operation="validation",
                smart_table_record_id=lead.smart_table_record_id or "",
                idempotency_key=idempotency_key,
                canonical_payload=payload,
                snapshot_hash=snapshot_hash,
                request_message_id=command.request_message_id,
                submitting_sales_user_id=command.sales_user_id,
                submitting_crm_user_id=None,
                status="failed_pending_review",
                attempts=0,
                failure_category="permanent",
                failure_kind="validation_failed",
                failure_code="mapping_missing",
                failure_summary="CRM 用户映射缺失",
                failed_at=utc_now(),
                completed_at=utc_now(),
            )
            session.add(sync)
            session.flush()
            self._record_audit_for_sync(session, sync, command.sales_user_id, "crm_mapping_missing")
        if requested_operation == "update" and lead.lifecycle_state == "synced":
            # 未提交的有效 CRM 变更必须保留为待更新，映射补齐后可由新命令继续处理。
            lead.lifecycle_state = "pending_update"
        return "mapping_missing"

    def _claim_and_call(self, sync_id: int, sales_user_id: str) -> str:
        """原子认领 pending/retrying 同步记录后调用 CRM，并持久化独立结果。"""
        claim_started_at: datetime
        claim_attempts: int
        with self._session_factory.begin() as session:
            # 先读取不可变的 operation/lead_id，再按 Lead -> Sync 锁序建立一致的短事务边界。
            sync_reference = session.get(CrmSyncRecord, sync_id)
            if sync_reference is None:
                return "failed_pending_review"
            if sync_reference.operation == "create":
                session.scalar(
                    select(Lead).where(Lead.id == sync_reference.lead_id).with_for_update()
                )
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
                # 允许人工从“外部结果未知”的失败检查点恢复同一冻结操作。
                if not (
                    sync.status == "failed_pending_review"
                    and sync.failure_category == "unknown"
                ):
                    return sync.status
            is_unknown_outcome_recovery = lease_expired or (
                sync.status == "failed_pending_review" and sync.failure_category == "unknown"
            )
            if sync.attempts >= self._crm_create_retry_count and not is_unknown_outcome_recovery:
                # 传输失败达到自动重试上限只能确认“外部结果未知”，不得结算废弃请求。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
                return "failed_pending_review"
            # 已登记 Sync 的 discard 已进入等待外部事实路径；create 继续使用原冻结操作。
            sync.status = "processing"
            sync.attempts += 1
            claim_attempts = sync.attempts
            claim_started_at = utc_now()
            sync.processing_started_at = claim_started_at
            sync.processing_lease_expires_at = sync.processing_started_at + _CRM_PROCESSING_LEASE
            payload = dict(sync.canonical_payload)
            idempotency_key = sync.idempotency_key
            crm_user_id = sync.submitting_crm_user_id
            operation = sync.operation
            crm_lead_id = sync.crm_lead_id
            if sync.operation == "update" and sync.attempts > 1:
                self._record_audit_for_sync(session, sync, sales_user_id, "crm_update_retried")

        if crm_user_id is None:
            # validation 记录不会进入认领路径；此处仅防御历史异常数据误触发 CRM 调用。
            return self._record_crm_failure(
                sync_id,
                sales_user_id,
                ValueError("CRM 同步记录缺少冻结提交身份"),
                claim_started_at,
                claim_attempts,
            )

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
        except (TimeoutError, ConnectionError, OSError) as error:
            # PermissionError 属于 OSError，但它是永久失败，必须先于传输重试分支终止。
            if classify_task_failure(error).value != "transient":
                return self._record_crm_failure(
                    sync_id, sales_user_id, error, claim_started_at, claim_attempts
                )
            return self._record_crm_transport_failure(
                sync_id, sales_user_id, error, claim_started_at, claim_attempts
            )
        except Exception as error:
            return self._record_crm_failure(
                sync_id, sales_user_id, error, claim_started_at, claim_attempts
            )

        with self._session_factory.begin() as session:
            # CRM 成功事实与废弃请求竞争时，先锁 Lead 再锁 Sync，保证结算看到真实提交顺序。
            sync_reference = session.get(CrmSyncRecord, sync_id)
            if sync_reference is None:
                return "failed_pending_review"
            lead: Lead | None = None
            if sync_reference.operation == "create":
                lead = session.scalar(
                    select(Lead).where(Lead.id == sync_reference.lead_id).with_for_update()
                )
            sync = session.scalar(
                select(CrmSyncRecord).where(CrmSyncRecord.id == sync_id).with_for_update()
            )
            if sync is not None and sync.operation != "create":
                lead = session.get(Lead, sync.lead_id)
            if sync is None or lead is None:
                return "failed_pending_review"
            if not self._claim_is_current(sync, claim_started_at, claim_attempts):
                # 租约接管者已经提交了新事实；旧 worker 的迟到结果绝不能回写覆盖它。
                return sync.status
            sync.status = "succeeded"
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            sync.crm_lead_id = crm_result.crm_lead_id
            # 更新响应无权把既有 CRM 负责人改写为本次提交人。
            if sync.operation == "create" and crm_result.crm_lead_owner_user_id is not None:
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
                    self._record_audit_for_sync(
                        session, sync, sales_user_id, "crm_global_identity_activated"
                    )
            previous_lifecycle_state = lead.lifecycle_state
            lead.lifecycle_state = "synced"
            discard_request = session.scalar(
                select(LeadDiscardRequest)
                .where(LeadDiscardRequest.lead_id == lead.id)
                .with_for_update()
            )
            if discard_request is not None and discard_request.status in {"pending", "effective"}:
                # 外部 CRM 成功是最终事实；废弃请求只留下未生效审计，不调用不存在的 CRM delete。
                discard_request.status = "not_effective"
                discard_request.completed_at = utc_now()
                self._record_discard_audit(
                    session,
                    lead,
                    discard_request.operator_user_id,
                    "discard_request_not_effective",
                    {
                        "lead_id": lead.id,
                        "operator_user_id": discard_request.operator_user_id,
                        "operator_role": discard_request.operator_role,
                        "old_lifecycle_state": previous_lifecycle_state,
                        "new_lifecycle_state": lead.lifecycle_state,
                        "crm_sync_record_id": sync.id,
                        "discard_request_id": discard_request.id,
                    },
                )
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

    def _resolve_global_identity(
        self, session: Session, lead: Lead, command: SubmissionCommand
    ) -> GlobalIdentityResolution:
        """以 registry 为权威解析公司身份，必要时从成功历史同步原子回填。"""
        company_name = lead.standard_company_name
        if not company_name:
            return GlobalIdentityResolution("no_identity")
        # PostgreSQL 对尚不存在的唯一键没有可锁行；事务级 advisory lock
        # 让同一公司名称的“查 registry → 建 reservation”成为一个数据库临界区。
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": self._company_advisory_lock_key(company_name)},
            )
        registry = session.get(CrmCompanyIdentity, company_name)
        if registry is not None:
            return GlobalIdentityResolution(registry.state, registry)

        # 仅查询当前数据库中已成功的最小身份事实，绝不读取其他销售的表格或 payload。
        rows = session.execute(
            select(CrmSyncRecord.crm_lead_id, CrmSyncRecord.crm_lead_owner_user_id)
            .join(Lead, CrmSyncRecord.lead_id == Lead.id)
            .where(
                Lead.standard_company_name == company_name,
                CrmSyncRecord.status == "succeeded",
                CrmSyncRecord.crm_lead_id.is_not(None),
            )
        ).all()
        owners_by_identity: dict[str, set[str | None]] = {}
        for crm_lead_id, owner_user_id in rows:
            if crm_lead_id is not None:
                owners_by_identity.setdefault(crm_lead_id, set()).add(owner_user_id)
        if not owners_by_identity:
            return GlobalIdentityResolution("no_identity")
        if len(owners_by_identity) != 1 or any(
            len(owners) != 1 for owners in owners_by_identity.values()
        ):
            ambiguous = self._insert_identity(
                session, company_name, state="ambiguous", creating_lead_id=lead.id
            )
            if ambiguous is not None:
                self._record_audit(
                    session,
                    command,
                    "crm_global_identity_ambiguous",
                    details=self._identity_audit_details(company_name, lead),
                )
            return GlobalIdentityResolution("ambiguous", ambiguous)
        crm_lead_id, owners = next(iter(owners_by_identity.items()))
        restored = self._insert_identity(
            session,
            company_name,
            state="active",
            creating_lead_id=lead.id,
            crm_lead_id=crm_lead_id,
            crm_lead_owner_user_id=next(iter(owners)),
        )
        if restored is None:
            # 并发写入后重读到的 registry 会在下一轮按照其真实状态处理。
            registry = session.get(CrmCompanyIdentity, company_name)
            return GlobalIdentityResolution(
                registry.state if registry is not None else "reserving", registry
            )
        self._record_audit(
            session,
            command,
            "crm_global_identity_reused",
            details=self._identity_audit_details(company_name, lead),
        )
        return GlobalIdentityResolution("active", restored)

    def _reserve_global_identity(
        self, session: Session, lead: Lead, command: SubmissionCommand
    ) -> CrmCompanyIdentity | None:
        """插入首创 reservation，并在唯一键竞争后安全地返回已存在的事实。"""
        company_name = lead.standard_company_name
        if not company_name:
            return None
        identity = self._insert_identity(
            session, company_name, state="reserving", creating_lead_id=lead.id
        )
        if identity is not None:
            self._record_audit(
                session,
                command,
                "crm_global_identity_reserved",
                details=self._identity_audit_details(company_name, lead),
            )
            return identity
        # PostgreSQL 唯一索引竞争会被 savepoint 回滚；此时以已提交 reservation 的实际状态为准。
        return session.get(CrmCompanyIdentity, company_name)

    @staticmethod
    def _insert_identity(
        session: Session,
        company_name: str,
        *,
        state: str,
        creating_lead_id: str,
        crm_lead_id: str | None = None,
        crm_lead_owner_user_id: str | None = None,
    ) -> CrmCompanyIdentity | None:
        """在 savepoint 内写注册表，避免唯一键竞争污染外层 CRM 同步事务。"""
        identity: CrmCompanyIdentity | None = None
        try:
            with session.begin_nested():
                identity = CrmCompanyIdentity(
                    standard_company_name=company_name,
                    state=state,
                    creating_lead_id=creating_lead_id,
                    crm_lead_id=crm_lead_id,
                    crm_lead_owner_user_id=crm_lead_owner_user_id,
                )
                session.add(identity)
                session.flush()
            return identity
        except IntegrityError:
            # 回滚 savepoint 后清空失败 INSERT 留在 identity map 的暂态对象，
            # 使调用方只能重新读取 PostgreSQL 已提交的 reservation。
            session.expire_all()
            if identity is not None and identity in session:
                session.expunge(identity)
            return None

    @staticmethod
    def _identity_audit_details(company_name: str, lead: Lead) -> dict[str, str]:
        """生成不含 payload、联系方式或其他销售身份的全局身份审计元数据。"""
        return {
            "standard_company_name_hash": hashlib.sha256(company_name.encode()).hexdigest(),
            "lead_id": lead.id,
            "record_id": lead.smart_table_record_id or "",
        }

    @staticmethod
    def _company_advisory_lock_key(company_name: str) -> int:
        """以 SHA-256 前 64 位生成跨进程稳定的 PostgreSQL advisory lock 键。"""
        digest = hashlib.sha256(company_name.encode()).digest()
        return int.from_bytes(digest[:8], "big", signed=True)

    def _audit(self, command: SubmissionCommand, event_type: str) -> None:
        """为未创建 CRM 同步记录的普通校验结果保存审计反馈。"""
        with self._session_factory.begin() as session:
            self._record_audit(session, command, event_type)

    def _fail_company_identity(
        self, session: Session, sync: CrmSyncRecord, sales_user_id: str
    ) -> None:
        """将首创失败与其公司预留在同一事务转为人工处理状态。"""
        if sync.operation != "create":
            return
        identity = session.scalar(
            select(CrmCompanyIdentity).where(CrmCompanyIdentity.creating_sync_record_id == sync.id)
        )
        if identity is not None:
            identity.state = "failed_pending_review"
            self._record_audit_for_sync(
                session, sync, sales_user_id, "crm_global_identity_failed_pending_review"
            )

    def _record_crm_failure(
        self,
        sync_id: int,
        sales_user_id: str,
        error: BaseException,
        claim_started_at: datetime,
        claim_attempts: int,
    ) -> str:
        """将 CRM 永久失败及其废弃协调事实写入一个短事务。

        参数：sync_id 为冻结 CRM 操作；sales_user_id 为提交销售；error 为外部失败异常。
        返回值：固定返回 failed_pending_review，供批次统计。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：按 Lead 后 Sync 锁序保存失败分类；只有已确认的永久失败才释放公司失败事实，
        并结算废弃请求。
        """
        with self._session_factory.begin() as session:
            # create 与 discard 统一先锁 Lead 再锁 Sync；update 只需锁自身 Sync。
            sync_reference = session.get(CrmSyncRecord, sync_id)
            if sync_reference is None:
                return "failed_pending_review"
            if sync_reference.operation == "create":
                session.scalar(
                    select(Lead).where(Lead.id == sync_reference.lead_id).with_for_update()
                )
            sync = session.scalar(
                select(CrmSyncRecord).where(CrmSyncRecord.id == sync_id).with_for_update()
            )
            if sync is not None and sync.operation != "create":
                session.get(Lead, sync.lead_id)
            if sync is None:
                return "failed_pending_review"
            if not self._claim_is_current(sync, claim_started_at, claim_attempts):
                # 旧 worker 的异常不能把接管者已经持久化的 CRM 结果改写成失败。
                return sync.status
            failure_category = classify_task_failure(error)
            sync.status = "failed_pending_review"
            sync.failure_category = failure_category.value
            sync.failure_summary = safe_failure_summary(error)
            sync.failed_at = utc_now()
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            if failure_category.value == "unknown":
                # 未知异常可能发生在远端已提交而响应丢失之后，不能把它当作确定失败结算 discard。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
            else:
                sync.response_summary = f"CRM {sync.operation} failed"
                self._fail_company_identity(session, sync, sales_user_id)
                self._finalize_pending_discard(session, sync)
        return "failed_pending_review"

    def _record_crm_transport_failure(
        self,
        sync_id: int,
        sales_user_id: str,
        error: BaseException,
        claim_started_at: datetime,
        claim_attempts: int,
    ) -> str:
        """在 claim fencing 通过时记录 CRM 传输失败，否则保留接管者结果。

        参数：sync_id 为冻结 CRM 操作；sales_user_id 为当前处理销售；error 为传输异常；
        claim_started_at 为本次 worker 的 processing claim 时间戳。
        返回值：retrying 或接管者已经写入的当前状态。
        异常：数据库写入错误向调用方传播。
        副作用：仅更新仍属于本 worker 的租约，不覆盖后续 worker 的最终事实。
        """
        with self._session_factory.begin() as session:
            sync_reference = session.get(CrmSyncRecord, sync_id)
            if sync_reference is None:
                return "failed_pending_review"
            if sync_reference.operation == "create":
                session.scalar(
                    select(Lead).where(Lead.id == sync_reference.lead_id).with_for_update()
                )
            sync = session.scalar(
                select(CrmSyncRecord).where(CrmSyncRecord.id == sync_id).with_for_update()
            )
            if sync is None:
                return "failed_pending_review"
            if not self._claim_is_current(sync, claim_started_at, claim_attempts):
                return sync.status
            sync.status = "retrying"
            sync.failure_category = classify_task_failure(error).value
            sync.failure_summary = safe_failure_summary(error)
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            sync.response_summary = "CRM transport error"
        return "retrying"

    @staticmethod
    def _claim_is_current(
        sync: CrmSyncRecord, claim_started_at: datetime, claim_attempts: int
    ) -> bool:
        """判断 Sync 当前租约是否仍属于指定 worker，阻止迟到结果覆盖接管事实。

        参数：sync 为当前锁定的 CRM 同步；claim_started_at 为 worker 认领时的时间戳；
        claim_attempts 为 worker 认领时的单调尝试序号。
        返回值：状态、时间戳和尝试序号均未被新的 claim 替换时返回 True。
        异常：无。
        副作用：无。
        """
        return (
            sync.status == "processing"
            and sync.attempts == claim_attempts
            and sync.processing_started_at is not None
            and CrmSubmissionService._as_utc(sync.processing_started_at)
            == CrmSubmissionService._as_utc(claim_started_at)
        )

    def _mark_unknown_crm_outcome(
        self, session: Session, sync: CrmSyncRecord, sales_user_id: str
    ) -> None:
        """保存 CRM 外部结果未知的失败检查点、审计和结构化日志。

        参数：session 为当前短事务；sync 为冻结 CRM 操作；sales_user_id 为处理销售。
        返回值：无。
        异常：数据库写入错误向调用方传播。
        副作用：结束当前租约并保留公司预留与废弃等待，不伪造永久失败或 CRM 删除事实。
        """
        sync.status = "failed_pending_review"
        sync.failure_category = "unknown"
        sync.failure_summary = "crm_external_outcome_unknown"
        sync.failed_at = utc_now()
        sync.processing_started_at = None
        sync.processing_lease_expires_at = None
        sync.response_summary = "CRM external outcome unknown"
        self._record_audit_for_sync(
            session,
            sync,
            sales_user_id,
            "crm_external_outcome_unknown",
            details={
                "failure_category": "unknown",
                "attempts": sync.attempts,
                "retry_limit": self._crm_create_retry_count,
            },
        )
        _LOGGER.error(
            "crm_external_outcome_unknown",
            extra={
                "crm_sync_record_id": sync.id,
                "lead_id": sync.lead_id,
                "operation": sync.operation,
                "attempts": sync.attempts,
                "retry_limit": self._crm_create_retry_count,
            },
        )

    def _finalize_pending_discard(self, session: Session, sync: CrmSyncRecord) -> None:
        """在 CRM create 已得到最终失败事实后落实此前等待中的废弃请求。

        参数：session 为已按 Lead 后 Sync 加锁的短事务；sync 为失败的 CRM 同步事实。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：将 pending 废弃请求和未同步 Lead 置为有效废弃，并写入审计；
        不改变已确认的 CRM 失败事实。
        """
        if sync.operation != "create":
            return
        request = session.scalar(
            select(LeadDiscardRequest)
            .where(LeadDiscardRequest.lead_id == sync.lead_id)
            .with_for_update()
        )
        if request is None or request.status != "pending":
            return
        lead = session.scalar(select(Lead).where(Lead.id == sync.lead_id).with_for_update())
        if lead is None:
            return
        previous_lifecycle_state = lead.lifecycle_state
        request.status = "effective"
        request.completed_at = utc_now()
        lead.lifecycle_state = "discarded"
        self._record_discard_audit(
            session,
            lead,
            request.operator_user_id,
            "lead_discarded_after_crm_create_failed",
            {
                "lead_id": lead.id,
                "operator_user_id": request.operator_user_id,
                "operator_role": request.operator_role,
                "old_lifecycle_state": previous_lifecycle_state,
                "new_lifecycle_state": lead.lifecycle_state,
                "crm_sync_record_id": sync.id,
                "discard_request_id": request.id,
            },
        )

    @staticmethod
    def _record_discard_audit(
        session: Session,
        lead: Lead,
        sales_user_id: str,
        event_type: str,
        details: dict[str, object],
    ) -> None:
        """按线索来源消息幂等写入 CRM create 与废弃请求的协调审计。

        参数：session 为当前事务；lead 提供来源消息；sales_user_id 为原废弃操作人；
        其余参数为审计详情。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：首次出现的事件新增 BusinessAuditEvent。
        """
        # 管理员补建线索没有来源消息；不为 CRM 在途协调伪造消息审计关联。
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

    @staticmethod
    def _record_audit(
        session: Session,
        command: SubmissionCommand,
        event_type: str,
        *,
        details: dict[str, str] | None = None,
    ) -> None:
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
                    details=details or {},
                )
            )
            # 关键转换仅记录可关联标识和公司哈希，绝不输出 CRM payload 或联系方式。
            _LOGGER.info(
                "crm_submission_event=%s request_id=%s message_id=%s lead_id=%s record_id=%s",
                event_type,
                command.request_message_id,
                command.request_message_id,
                (details or {}).get("lead_id", ""),
                (details or {}).get("record_id", ""),
            )

    @staticmethod
    def _record_audit_for_sync(
        session: Session,
        sync: CrmSyncRecord,
        sales_user_id: str,
        event_type: str,
        details: dict[str, object] | None = None,
    ) -> None:
        """为 CRM 操作事实写入以首次请求消息归属的可查询审计。

        参数：session 为当前事务；sync 为 CRM 操作；sales_user_id 为处理销售；
        event_type 为审计类型；details 为可选的失败或重试上下文。
        返回值：无。
        异常：数据库写入错误向调用方传播。
        副作用：首次出现的操作审计写入业务审计表并输出脱敏结构化日志。
        """
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
                    details={
                        "lead_id": sync.lead_id,
                        "record_id": sync.smart_table_record_id,
                        "crm_sync_record_id": str(sync.id),
                        **(details or {}),
                    },
                )
            )
            _LOGGER.info(
                "crm_submission_event=%s message_id=%s lead_id=%s record_id=%s "
                "crm_sync_record_id=%s",
                event_type,
                sync.request_message_id,
                sync.lead_id,
                sync.smart_table_record_id,
                sync.id,
            )
