"""确定性 CRM 提交命令、最终快照与逻辑重试服务。"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Collection, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import NotRequired, TypedDict
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.companies.models import CompanyVerificationStatus
from app.core.config import get_settings
from app.core.failures import classify_task_failure, safe_audit_text, safe_failure_summary
from app.crm.adapter import CRMAdapter, CRMSearchResult
from app.crm.employee_directory import EmployeeDirectory, EmployeeDirectoryError
from app.crm.payload import CrmEnumMappingError, CrmPayloadBuilder, CrmPayloadError
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    LeadDiscardRequest,
    latest_crm_create_sync,
)
from app.leads.review import LeadReviewService
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.models import SmartTableRecord

_TODAY_COMMAND = "提交今天的线索"
_ALL_COMMAND = "提交我所有线索"
_ABANDONED_COMMAND = "帮我提交放弃提交的线索"
_UPDATES_COMMAND = "提交我的更新"
_RETRY_INCOMPLETE_COMMAND = "重新提交待完善的线索"
CROSS_OWNER_SUBMISSION_COMMAND = "提交授权范围线索"
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
# CRM CreateLeadsCommand 还强制要求客户行业；与业务侧八项完整度分开，
# 避免把远端硬校验遗漏到创建阶段。
_CRM_REQUIRED_FIELDS = (*_BUSINESS_COMPLETENESS_FIELDS, "客户行业")
_CRM_PROCESSING_LEASE = timedelta(minutes=5)
_LOGGER = logging.getLogger(__name__)


def _merge_missing_fields(
    current: tuple[str, ...], additional: tuple[str, ...]
) -> tuple[str, ...]:
    """合并批次内缺失必填字段名称并保持稳定顺序。

    参数：current 为已累计字段；additional 为当前线索缺失或待确认字段。
    返回值：去重后的字段名称元组，供销售补全提示使用。
    异常：无。
    副作用：无，不包含线索标识或敏感值。
    """
    return tuple(dict.fromkeys((*current, *additional)))


@dataclass(frozen=True)
class SubmissionCommand:
    """承载机器人已接收的确定性提交命令事实。"""

    text: str
    sales_user_id: str
    request_message_id: str
    target_lead_id: str | None = None
    target_owner_name: str | None = None
    owner_scope_user_ids: tuple[str, ...] | None = None
    target_capture_date: date | None = None


class SubmissionItemStatus(StrEnum):
    """定义单条 CRM 提交结果允许对外展示的状态集合。"""

    CREATED = "created"
    UPDATED = "updated"
    FOLLOWED_UP = "followed_up"
    UNCHANGED = "unchanged"
    INCOMPLETE = "incomplete"
    DUPLICATE_CONFIRMATION = "duplicate_confirmation"
    MAPPING_MISSING = "mapping_missing"
    PROCESSING = "processing"
    RETRYING = "retrying"
    FAILED_PENDING_REVIEW = "failed_pending_review"
    COMPANY_IDENTITY_REVIEW = "company_identity_review"
    NOT_SUBMITTED = "not_submitted"


@dataclass(frozen=True)
class SubmissionItemResult:
    """保存一条线索的脱敏提交结果及受控原因。"""

    lead_id: str
    status: SubmissionItemStatus
    reason_code: str | None = None
    missing_fields: tuple[str, ...] = ()
    failure_category: str | None = None
    adapter_category: str | None = None
    http_status: int | None = None
    failure_code: str | None = None
    duplicate_entity_type: str | None = None


@dataclass(frozen=True)
class SubmissionBatchResult:
    """汇总一条确定性提交命令的部分成功结果。"""

    succeeded: int = 0
    incomplete: int = 0
    incomplete_missing_fields: tuple[str, ...] = ()
    retrying: int = 0
    processing: int = 0
    failed_pending_review: int = 0
    updates_not_implemented: bool = False
    incomplete_lead_ids: tuple[str, ...] = ()
    updated: int = 0
    unchanged: int = 0
    company_identity_review: int = 0
    mapping_missing: int = 0
    duplicate_confirmations: tuple["DuplicateSubmission", ...] = ()
    items: tuple[SubmissionItemResult, ...] = ()
    not_submitted: int = 0
    followed_up: int = 0


@dataclass(frozen=True)
class DuplicateSubmission:
    """描述一条等待销售决定是否覆盖的 CRM 重复线索。"""

    lead_id: str
    company_name: str
    crm_lead_id: str
    crm_lead_owner_user_id: str | None


@dataclass(frozen=True)
class SubmissionCandidate:
    """描述卡片中可由当前销售选择的待提交线索。"""

    lead_id: str
    company_name: str
    owner_user_id: str | None = None


@dataclass(frozen=True)
class DuplicateConfirmationResult:
    """描述重复线索卡片一次继续或停止操作的结果。"""

    submitted: int = 0
    abandoned: int = 0
    remaining: int = 0
    failed: int = 0
    failure_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class CreateSubmissionOutcome:
    """保存单条首次提交的状态及可选重复线索事实。"""

    status: str
    duplicate: DuplicateSubmission | None = None
    missing_fields: tuple[str, ...] = ()
    reason_code: str | None = None
    failure_category: str | None = None
    adapter_category: str | None = None
    http_status: int | None = None
    failure_code: str | None = None
    duplicate_entity_type: str | None = None


class CRMFailureEvidence(TypedDict):
    """描述用于审计和逐条结果的受控 CRM 失败字段。"""

    failure_category: str
    adapter_category: str | None
    http_status: int | None
    failure_code: str | None
    duplicate_entity_type: NotRequired[str]


_CREATE_REASON_CODES = {
    "incomplete": "missing_required_fields",
    "duplicate_confirmation": "duplicate_confirmation_required",
    "mapping_missing": "crm_user_mapping_missing",
    "processing": "sync_processing",
    "retrying": "crm_create_retrying",
    "failed_pending_review": "crm_create_failed_pending_review",
    "not_submitted": "candidate_state_changed",
    "followed_up": "crm_followed_up",
}
_ITEM_STATUS_BY_OUTCOME = {
    "succeeded": SubmissionItemStatus.CREATED,
    "updated": SubmissionItemStatus.UPDATED,
    "followed_up": SubmissionItemStatus.FOLLOWED_UP,
    "unchanged": SubmissionItemStatus.UNCHANGED,
    "incomplete": SubmissionItemStatus.INCOMPLETE,
    "duplicate_confirmation": SubmissionItemStatus.DUPLICATE_CONFIRMATION,
    "mapping_missing": SubmissionItemStatus.MAPPING_MISSING,
    "processing": SubmissionItemStatus.PROCESSING,
    "retrying": SubmissionItemStatus.RETRYING,
    "failed_pending_review": SubmissionItemStatus.FAILED_PENDING_REVIEW,
    "company_identity_review": SubmissionItemStatus.COMPANY_IDENTITY_REVIEW,
    "not_submitted": SubmissionItemStatus.NOT_SUBMITTED,
}


def _create_outcome(
    status: str,
    *,
    duplicate: DuplicateSubmission | None = None,
    missing_fields: tuple[str, ...] = (),
    reason_code: str | None = None,
    failure_category: str | None = None,
    adapter_category: str | None = None,
    http_status: int | None = None,
    failure_code: str | None = None,
    duplicate_entity_type: str | None = None,
) -> CreateSubmissionOutcome:
    """构造单条 create 结果并补齐受控原因码。

    参数：status 为领域状态；duplicate 为重复线索事实；missing_fields 为本条缺失字段；
    reason_code 与 duplicate_entity_type 为受控原因和对象类别。
    返回值：包含状态、原因和缺失字段的不可变结果。
    异常：无。
    副作用：无，不调用数据库或外部服务。
    """

    return CreateSubmissionOutcome(
        status=status,
        duplicate=duplicate,
        missing_fields=missing_fields,
        reason_code=reason_code or _CREATE_REASON_CODES.get(status),
        failure_category=failure_category,
        adapter_category=adapter_category,
        http_status=http_status,
        failure_code=failure_code,
        duplicate_entity_type=duplicate_entity_type,
    )


def _append_create_result(
    result: SubmissionBatchResult, lead_id: str, outcome: CreateSubmissionOutcome
) -> SubmissionBatchResult:
    """把单条 create 结果追加到批次汇总，保留逐线索身份和缺失字段。

    参数：result 为当前批次累计结果；lead_id 为服务端接受的线索标识；outcome 为单条结果。
    返回值：追加一个且仅一个 SubmissionItemResult 的新批次结果。
    异常：无。
    副作用：无，不修改输入对象或执行外部调用。
    """

    item_status = _ITEM_STATUS_BY_OUTCOME.get(
        outcome.status, SubmissionItemStatus.FAILED_PENDING_REVIEW
    )
    item = SubmissionItemResult(
        lead_id=lead_id,
        status=item_status,
        reason_code=outcome.reason_code,
        missing_fields=outcome.missing_fields,
        failure_category=outcome.failure_category,
        adapter_category=outcome.adapter_category,
        http_status=outcome.http_status,
        failure_code=outcome.failure_code,
        duplicate_entity_type=outcome.duplicate_entity_type,
    )
    return replace(
        result,
        succeeded=result.succeeded + (outcome.status == "succeeded"),
        followed_up=result.followed_up + (outcome.status == "followed_up"),
        incomplete=result.incomplete + (outcome.status == "incomplete"),
        incomplete_missing_fields=_merge_missing_fields(
            result.incomplete_missing_fields, outcome.missing_fields
        ),
        retrying=result.retrying + (outcome.status == "retrying"),
        processing=result.processing + (outcome.status == "processing"),
        failed_pending_review=result.failed_pending_review
        + (outcome.status == "failed_pending_review"),
        incomplete_lead_ids=(
            result.incomplete_lead_ids + (lead_id,)
            if outcome.status == "incomplete"
            else result.incomplete_lead_ids
        ),
        mapping_missing=result.mapping_missing + (outcome.status == "mapping_missing"),
        company_identity_review=result.company_identity_review
        + (outcome.status == "company_identity_review"),
        not_submitted=result.not_submitted + (outcome.status == "not_submitted"),
        duplicate_confirmations=(
            result.duplicate_confirmations + (outcome.duplicate,)
            if outcome.duplicate is not None
            else result.duplicate_confirmations
        ),
        items=(*result.items, item),
    )


def _append_update_result(
    result: SubmissionBatchResult, lead_id: str, status: str
) -> SubmissionBatchResult:
    """把单条 update 结果追加到批次汇总并转换为统一单条状态。

    参数：result 为当前批次累计结果；lead_id 为服务端线索标识；status 为既有领域状态。
    返回值：追加一个逐线索结果的新批次对象。
    异常：无。
    副作用：无，不修改输入对象或执行外部调用。
    """

    item_status = {
        "succeeded": SubmissionItemStatus.UPDATED,
        "followed_up": SubmissionItemStatus.FOLLOWED_UP,
        "unchanged": SubmissionItemStatus.UNCHANGED,
        "incomplete": SubmissionItemStatus.INCOMPLETE,
        "enum_mapping_missing": SubmissionItemStatus.INCOMPLETE,
    }.get(status, _ITEM_STATUS_BY_OUTCOME.get(status, SubmissionItemStatus.FAILED_PENDING_REVIEW))
    reason_code = {
        "followed_up": "crm_followed_up",
        "enum_mapping_missing": "crm_enum_mapping_missing",
        "incomplete": "crm_update_incomplete",
        "mapping_missing": "crm_user_mapping_missing",
        "processing": "sync_processing",
        "retrying": "crm_update_retrying",
        "company_identity_review": "company_identity_change_pending_review",
        "failed_pending_review": "crm_update_failed_pending_review",
    }.get(status)
    item = SubmissionItemResult(lead_id, item_status, reason_code)
    return replace(
        result,
        retrying=result.retrying + (status == "retrying"),
        incomplete=result.incomplete + (status in {"incomplete", "enum_mapping_missing"}),
        processing=result.processing + (status == "processing"),
        failed_pending_review=result.failed_pending_review + (status == "failed_pending_review"),
        updated=result.updated + (status == "succeeded"),
        followed_up=result.followed_up + (status == "followed_up"),
        unchanged=result.unchanged + (status == "unchanged"),
        company_identity_review=result.company_identity_review
        + (status == "company_identity_review"),
        mapping_missing=result.mapping_missing + (status == "mapping_missing"),
        items=(*result.items, item),
    )


class CrmSubmissionService:
    """执行冻结 CRM 提交；公司重复判断与分流由 CRM 统一接口处理。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        crm_adapter: CRMAdapter,
        crm_create_retry_count: int | None = None,
        employee_directory: EmployeeDirectory | None = None,
        robot_submission_confirmation_available: bool | None = None,
    ) -> None:
        """保存数据库、表格、CRM 与重试上限依赖。

        参数：前三项分别提供持久化、规范表格回读和 CRM 统一提交；可选参数覆盖重试、
        owner 目录与卡片确认能力配置。
        返回值：无。
        异常：无。
        副作用：只保存依赖，不读取数据库或调用外部系统。
        """
        settings = get_settings()
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._crm_adapter = crm_adapter
        self._crm_create_retry_count = (
            settings.crm_create_retry_count
            if crm_create_retry_count is None
            else crm_create_retry_count
        )
        self._crm_create_retry_delay_seconds = settings.crm_create_retry_delay_seconds
        self._crm_submission_max_concurrency = settings.crm_submission_max_concurrency
        self._employee_directory = employee_directory
        # 当前销售只能提交本人负责且已从智能表格读取到的线索，CRM headerId 由负责人目录解析。
        self._crm_payload_builder = CrmPayloadBuilder(settings.crm_customer_level_scheme)
        self._robot_submission_confirmation_available = (
            settings.wecom_card_callback_ready()
            if robot_submission_confirmation_available is None
            else robot_submission_confirmation_available
        )

    def retry_due_creates(self, *, limit: int = 20) -> int:
        """处理已到期的 CRM create 重试及失效租约恢复。

        参数：limit 为本轮最多扫描的同步记录数，必须为正整数。
        返回值：本轮认领候选数；并发下候选可能已由其他 Worker 完成。
        异常：数据库读写错误向 Celery 传播；单条 CRM 错误由同步状态机保存。
        副作用：按配置上限并发重用已冻结 payload 与幂等键调用 CRM，并登记最终结果通知；
        批次收尾后按持久化成功状态批量回写表格，SQLite 保持串行。
        """
        if limit <= 0:
            raise ValueError("CRM 重试扫描数量必须大于 0")
        current_time = utc_now()
        with self._session_factory() as session:
            due_syncs = session.execute(
                select(
                    CrmSyncRecord.id,
                    CrmSyncRecord.submitting_sales_user_id,
                    CrmSyncRecord.lead_id,
                )
                .where(
                    CrmSyncRecord.operation == "create",
                    or_(
                        and_(
                            CrmSyncRecord.status == "retrying",
                            CrmSyncRecord.failure_category == "transient",
                            CrmSyncRecord.next_retry_at <= current_time,
                        ),
                        and_(
                            CrmSyncRecord.status == "processing",
                            CrmSyncRecord.processing_lease_expires_at <= current_time,
                        ),
                    ),
                )
                .order_by(CrmSyncRecord.next_retry_at, CrmSyncRecord.id)
                .limit(limit)
            ).all()
        if not due_syncs:
            return 0
        bind = self._session_factory.kw.get("bind")
        max_workers = min(self._crm_submission_max_concurrency, len(due_syncs))
        try:
            if len(due_syncs) == 1 or (
                bind is not None and getattr(getattr(bind, "dialect", None), "name", None) == "sqlite"
            ):
                # 单条不启动线程；SQLite 开发库串行写入，避免共享连接争用。
                for sync_id, sales_user_id, _ in due_syncs:
                    self._claim_and_call(
                        sync_id,
                        sales_user_id,
                        notify_retry_result=True,
                        defer_smart_table_submission_status=True,
                    )
            else:
                # 每条仍独立认领和提交，成功状态统一留给批次收尾回写。
                with ThreadPoolExecutor(
                    max_workers=max_workers,
                    thread_name_prefix="crm-retry",
                ) as executor:
                    futures = [
                        executor.submit(
                            self._claim_and_call,
                            sync_id,
                            sales_user_id,
                            notify_retry_result=True,
                            defer_smart_table_submission_status=True,
                        )
                        for sync_id, sales_user_id, _ in due_syncs
                    ]
                    # CRM 单条错误已由状态机收敛；数据库异常仍传播给 Celery 重试任务。
                    for future in as_completed(futures):
                        future.result()
        finally:
            self._write_back_batch_submission_status(
                tuple(dict.fromkeys(lead_id for _, _, lead_id in due_syncs))
            )
        return len(due_syncs)

    def _resolve_crm_owner(
        self,
        lead: Lead,
        *,
        initial_record: SmartTableRecord | None = None,
    ) -> str | None:
        """在 CRM 调用前把企微负责人 ID 转为姓名并解析唯一 employee.id。

        智能表格成员字段继续以 userId 作为权限和归属事实；只有企业微信响应附带的
        userName 才用于 EmployeeDirectory 姓名/花名解析。缺少可读姓名、姓名不唯一或
        记录归属不一致时返回 None，调用方必须 fail closed，不调用 CRM。
        参数：lead 为当前线索；initial_record 为本次提交已回读的同一行快照（如有）。
        返回值：唯一 CRM 员工标识；无法唯一解析时为 None。
        副作用：无；必要时读取智能表格记录。
        """
        directory = self._employee_directory
        if directory is None:
            directory = EmployeeDirectory(get_settings().employee_directory_path)
            self._employee_directory = directory
        owner_name: str | None = None
        if lead.smart_table_record_id:
            # 批量提交复用同批回读的成员名称；单条提交仍从智能表格获取当前值。
            record = (
                initial_record
                if initial_record is not None
                and initial_record.record_id == lead.smart_table_record_id
                else self._read_smart_table_record(lead.smart_table_record_id)
            )
            if record is not None:
                # 后台负责人和当前表格负责人不一致时，不读取或映射他人的成员身份。
                if record.fields.get("负责人") != lead.smart_table_owner_user_id:
                    return None
                member_names = getattr(record, "member_names", {})
                if isinstance(member_names, Mapping):
                    candidate = member_names.get("负责人")
                    if isinstance(candidate, str) and candidate.strip():
                        owner_name = candidate.strip()
                # owner 字段只保留企微 userId；没有同一响应提供的 display name 时不能反向猜姓名。
        if owner_name is None:
            # 缺少可信成员显示名时，禁止把 opaque userId 交给员工目录解析。
            return None
        try:
            return directory.resolve(owner_name)
        except EmployeeDirectoryError:
            return None

    def _read_smart_table_record(self, record_id: str) -> SmartTableRecord | None:
        """按记录标识精确回读单条线索，禁止提交路径退化为整表扫描。

        参数：record_id 为本次提交目标记录的智能表格标识。
        返回值：标识完全匹配的当前记录；记录不存在时返回 None。
        异常：适配器无法执行服务端精确过滤时向调用方传播。
        副作用：仅读取指定记录。
        """
        return next(
            (
                record
                for record in self._smart_table_adapter.get_records((record_id,))
                if record.record_id == record_id
            ),
            None,
        )

    def _submit_create_batch(
        self,
        submissions: Sequence[tuple[str, SubmissionCommand, SmartTableRecord | None]],
        *,
        on_item_result: Callable[[SubmissionItemResult], None] | None = None,
    ) -> dict[str, CreateSubmissionOutcome]:
        """并发处理已完成服务端校验的创建目标，并保留逐条结果与回调。

        参数：submissions 为线索 ID、固定命令和本请求表格快照组成的序列；
        on_item_result 为可选的逐条结果持久化回调。
        返回值：按线索 ID 索引的独立提交结果。
        异常：回调异常向调用方传播；单条提交流程的未捕获异常被收敛为待人工检查。
        副作用：并发调用已冻结的单条 CRM 流程，收尾时查询持久化成功状态并批量回写表格。
        """
        outcomes: dict[str, CreateSubmissionOutcome] = {}
        if not submissions:
            return outcomes
        bind = self._session_factory.kw.get("bind")
        max_workers = min(self._crm_submission_max_concurrency, len(submissions))
        try:
            if len(submissions) == 1 or (
                bind is not None and getattr(getattr(bind, "dialect", None), "name", None) == "sqlite"
            ):
                # 单条不启动线程；SQLite 开发库也串行写入，避免共享连接争用。
                for lead_id, command, record in submissions:
                    try:
                        outcome = self._submit_create(
                            lead_id,
                            command,
                            initial_record=record,
                            defer_smart_table_submission_status=True,
                        )
                    except Exception as error:
                        _LOGGER.warning(
                            "crm_batch_submission_item_failed",
                            extra={"lead_id": lead_id, "failure": safe_failure_summary(error)},
                        )
                        outcome = _create_outcome("failed_pending_review")
                    outcomes[lead_id] = outcome
                    if on_item_result is not None:
                        item = _append_create_result(SubmissionBatchResult(), lead_id, outcome).items[0]
                        on_item_result(item)
            else:
                # 每条 CRM 操作保留独立冻结状态，所有成功项的表格状态留到批次结束统一更新。
                with ThreadPoolExecutor(
                    max_workers=max_workers,
                    thread_name_prefix="crm-submit",
                ) as executor:
                    futures = {
                        executor.submit(
                            self._submit_create,
                            lead_id,
                            command,
                            initial_record=record,
                            defer_smart_table_submission_status=True,
                        ): lead_id
                        for lead_id, command, record in submissions
                    }
                    for future in as_completed(futures):
                        lead_id = futures[future]
                        try:
                            outcome = future.result()
                        except Exception as error:
                            _LOGGER.warning(
                                "crm_batch_submission_item_failed",
                                extra={"lead_id": lead_id, "failure": safe_failure_summary(error)},
                            )
                            outcome = _create_outcome("failed_pending_review")
                        outcomes[lead_id] = outcome
                        if on_item_result is not None:
                            item = _append_create_result(SubmissionBatchResult(), lead_id, outcome).items[0]
                            on_item_result(item)
        finally:
            # 回调异常也不能漏掉已经持久化成功的 CRM 记录。
            self._write_back_batch_submission_status(
                tuple(lead_id for lead_id, _, _ in submissions)
            )
        return outcomes

    def submit(self, command: SubmissionCommand) -> SubmissionBatchResult:
        """解析固定命令并逐条提交本人待创建 Lead，保持批次部分成功。

        参数：command 为机器人已可靠接收的文本命令和提交销售身份。
        返回值：成功、待完善和外部失败的汇总；更新命令明确标记未实现。
        异常：命令不支持或 actor 不存在/已停用时抛出 ValueError，避免 LLM 推断权限。
        副作用：可能创建或重试一个冻结的 CRM Sync Record，并调用 CRM create。
        """
        if command.text == _UPDATES_COMMAND:
            return self._submit_updates(command)
        if command.text in {
            _TODAY_COMMAND,
            _ALL_COMMAND,
            _ABANDONED_COMMAND,
            CROSS_OWNER_SUBMISSION_COMMAND,
        }:
            # 批量命令只能发行候选确认动作，不能由服务层直接触发 CRM。
            raise ValueError("批量提交必须通过服务端候选确认动作")
        if command.text != "提交指定线索":
            raise ValueError("不支持的 CRM 提交命令")
        if command.target_lead_id is not None and command.text != "提交指定线索":
            raise ValueError("目标线索只能用于指定线索提交命令")

        target_missing = False
        with self._session_factory() as session:
            if command.text == "提交指定线索":
                if command.target_lead_id is None:
                    raise ValueError("指定线索提交缺少目标线索")
                target = session.scalar(
                    select(Lead).where(
                        Lead.id == command.target_lead_id,
                        Lead.smart_table_owner_user_id == command.sales_user_id,
                        Lead.lifecycle_state.in_(("pending_create", "temporary")),
                    )
                )
                target_missing = target is None
                candidate_ids = [target.id] if target is not None else []
            else:
                candidates = session.scalars(
                    select(Lead).where(
                        Lead.smart_table_owner_user_id == command.sales_user_id,
                        Lead.lifecycle_state == "pending_create",
                    )
                )
                candidate_ids = [
                    lead.id
                    for lead in candidates
                    if self._is_today_owned_candidate(lead, command.sales_user_id)
                ]

        if target_missing:
            return _append_create_result(
                SubmissionBatchResult(),
                command.target_lead_id or "",
                _create_outcome("not_submitted", reason_code="candidate_state_changed"),
            )
        result = SubmissionBatchResult()
        for lead_id in candidate_ids:
            # 每条独立执行；任何一条失败都不得影响后续候选。
            outcome = self._submit_create(lead_id, command)
            result = _append_create_result(result, lead_id, outcome)
        return result

    def submit_incomplete_retry(
        self, command: SubmissionCommand, selected_lead_ids: tuple[str, ...]
    ) -> SubmissionBatchResult:
        """重读并重提当前销售最近一次已选择但当时不完整的服务端目标。

        参数：command 为本次重提消息事实；selected_lead_ids 来自服务端历史 action 结果。
        返回值：每个历史目标各有一条当前状态结果。
        异常：命令不匹配或 actor 不存在/已停用时抛出 ValueError；外部依赖错误按既有服务边界处理。
        副作用：只对重新验证仍有效且当前完整的目标复用首次创建流程。
        """
        if command.text != _RETRY_INCOMPLETE_COMMAND:
            raise ValueError("不支持待完善线索重提命令")
        selected = tuple(dict.fromkeys(selected_lead_ids))
        with self._session_factory() as session:
            reasons: dict[str, str] = {}
            eligible_leads: dict[str, Lead] = {}
            for lead_id in selected:
                lead = session.get(Lead, lead_id)
                if lead is None or lead.smart_table_owner_user_id != command.sales_user_id:
                    reasons[lead_id] = "candidate_state_changed"
                    continue
                sync = latest_crm_create_sync(session, lead_id)
                if sync is not None and sync.status == "succeeded":
                    reasons[lead_id] = (
                        "request_succeeded"
                        if sync.request_message_id == command.request_message_id
                        else "already_submitted"
                    )
                    continue
                if sync is not None and sync.status == "abandoned":
                    reasons[lead_id] = "candidate_state_changed"
                    continue
                if lead.lifecycle_state not in {"temporary", "pending_create"}:
                    reasons[lead_id] = "candidate_state_changed"
                    continue
                eligible_leads[lead_id] = lead
        # 只批量回读已通过负责人与状态校验的勾选记录，不拉取整张表。
        requested_record_ids = {
            lead.smart_table_record_id
            for lead in eligible_leads.values()
            if lead.smart_table_record_id is not None
        }
        smart_table_snapshot = (
            {
                record.record_id: record
                for record in self._smart_table_adapter.get_records(requested_record_ids)
            }
            if requested_record_ids
            else {}
        )
        eligible: set[str] = set()
        for lead_id, lead in eligible_leads.items():
            if self._is_unsubmitted_smart_table_record(lead, snapshot=smart_table_snapshot):
                eligible.add(lead_id)
            else:
                reasons[lead_id] = "candidate_state_changed"

        submissions = [
            (
                lead_id,
                SubmissionCommand(
                    "提交指定线索",
                    command.sales_user_id,
                    command.request_message_id,
                    target_lead_id=lead_id,
                ),
                smart_table_snapshot.get(
                    eligible_leads[lead_id].smart_table_record_id or ""
                ),
            )
            for lead_id in selected
            if lead_id in eligible
        ]
        outcomes = self._submit_create_batch(submissions)
        result = SubmissionBatchResult()
        for lead_id in selected:
            if lead_id in eligible:
                outcome = outcomes[lead_id]
            elif reasons.get(lead_id) == "request_succeeded":
                # 同一命令重放时保留真实成功；其它请求只报告已提交，绝不重复创建。
                outcome = _create_outcome("succeeded")
            else:
                outcome = _create_outcome(
                    "not_submitted",
                    reason_code=reasons.get(lead_id, "candidate_state_changed"),
                )
            result = _append_create_result(result, lead_id, outcome)
        return result

    def list_submission_candidates(
        self,
        command_text: str,
        sales_user_id: str,
        *,
        owner_scope_user_ids: tuple[str, ...] | None = None,
        target_capture_date: date | None = None,
        owner_display_names: Mapping[str, str] | None = None,
    ) -> tuple[SubmissionCandidate, ...]:
        """列出操作人获准在机器人卡片中选择的线索，不调用 CRM。

        参数：command_text 为精确命令；sales_user_id 为操作人；owner_scope_user_ids 仅供
        已获跨员工提交权限的调用指定负责人范围；owner_display_names 仅用于服务端负责人预筛。
        返回值：只包含指定负责人范围与实时表格状态符合卡片条件的候选。
        异常：actor 不存在/已停用、权限不足或命令不支持时抛出 ValueError。
        副作用：只读取数据库及智能表格，不改变 Lead 生命周期。
        """
        if command_text not in {
            _TODAY_COMMAND,
            _ALL_COMMAND,
            _ABANDONED_COMMAND,
            CROSS_OWNER_SUBMISSION_COMMAND,
        }:
            raise ValueError("不支持候选卡片的 CRM 命令")
        cross_owner = command_text == CROSS_OWNER_SUBMISSION_COMMAND
        if cross_owner != (owner_scope_user_ids is not None):
            raise ValueError("跨员工提交负责人范围缺失或不适用于当前命令")
        if target_capture_date is not None and not cross_owner:
            raise ValueError("按日期跨员工提交只能用于跨员工候选")
        owner_user_ids = (
            owner_scope_user_ids if owner_scope_user_ids is not None else (sales_user_id,)
        )
        # “今天”按来源消息/Lead 首次采集时间筛选；显式跨员工日期命令按表格日期筛选。
        query_capture_date = target_capture_date
        query_submission_status = (
            "未提交" if command_text == _TODAY_COMMAND or query_capture_date is not None else None
        )
        query_owner_names = dict(owner_display_names or {})
        with self._session_factory() as session:
            authorization = (
                session.get(SalesAuthorization, sales_user_id) if cross_owner else None
            )
            # 本人提交以线索负责人范围校验；只有跨员工提交才读取专用权限目录。
            if cross_owner and (
                authorization is None
                or not authorization.is_active
                or not self._can_submit_other_sales_leads(authorization)
            ):
                raise ValueError("当前账号没有跨员工 CRM 提交权限")
            if query_submission_status is not None and not query_owner_names:
                # 负责人姓名只用于服务端预筛，最终仍由表格成员 ID 与 owner_user_ids 精确校验。
                query_owner_names = dict(
                    session.execute(
                        select(
                            SalesAuthorization.wecom_user_id, SalesAuthorization.display_name
                        ).where(SalesAuthorization.wecom_user_id.in_(owner_user_ids))
                    ).all()
                )
                query_owner_names = {
                    user_id: display_name
                    for user_id, display_name in query_owner_names.items()
                    if isinstance(display_name, str) and display_name.strip()
                }
            leads = list(
                session.scalars(
                    select(Lead).where(Lead.smart_table_owner_user_id.in_(owner_user_ids))
                )
            )
            # 一次批量读取来源消息时间；缺少来源消息的历史补链记录再按表格录入时间判日。
            source_message_ids = {
                lead.source_message_id for lead in leads if lead.source_message_id is not None
            }
            source_received_at = (
                dict(
                    session.execute(
                        select(IncomingMessage.message_id, IncomingMessage.received_at).where(
                            IncomingMessage.message_id.in_(source_message_ids)
                        )
                    ).all()
                )
                if source_message_ids
                else {}
            )
            sync_statuses = {}
            for lead in leads:
                sync = latest_crm_create_sync(session, lead.id)
                sync_statuses[lead.id] = sync.status if sync is not None else None
        if query_submission_status is not None:
            # 日期命令直接按负责人、状态与目标日期回读目标行，避免拼接整批 record_id。
            queried_records = self._smart_table_adapter.get_records(
                owner_user_ids=owner_user_ids,
                owner_display_names=query_owner_names or None,
                submission_status=query_submission_status,
                capture_date=query_capture_date,
            )
        else:
            # 无日期的旧命令保持按本地候选 ID 回读，避免扩大读取范围。
            record_ids = tuple(
                dict.fromkeys(
                    lead.smart_table_record_id
                    for lead in leads
                    if lead.smart_table_record_id is not None
                )
            )
            queried_records = self._smart_table_adapter.get_records(record_ids=record_ids)
        smart_table_snapshot = {record.record_id: record for record in queried_records}
        candidates: list[SubmissionCandidate] = []
        for lead in leads:
            sync_status = sync_statuses[lead.id]
            record = smart_table_snapshot.get(lead.smart_table_record_id or "")
            snapshot_fields = record.fields if record is not None else lead.field_values
            if command_text == _ABANDONED_COMMAND:
                if lead.lifecycle_state != "pending_create" or sync_status != "abandoned":
                    continue
            elif command_text == _TODAY_COMMAND:
                # 候选资格只看所有权、日期、生命周期与实时未提交状态；字段完整性留给最终提交。
                if (
                    lead.lifecycle_state not in {"pending_create", "temporary"}
                    or not self._is_today_owned_candidate(lead, sales_user_id)
                ):
                    continue
                if sync_status in {"succeeded", "abandoned"}:
                    continue
                if not self._is_unsubmitted_smart_table_record(
                    lead, snapshot=smart_table_snapshot
                ):
                    continue
            else:
                # 只有待创建和仍在补全的草稿能进入卡片；已同步、废弃或失败终态保持冻结。
                if lead.lifecycle_state not in {"pending_create", "temporary"}:
                    continue
                if target_capture_date is not None and not self._is_capture_date_owned_candidate(
                    lead,
                    owner_user_ids,
                    target_capture_date,
                    source_received_at.get(lead.source_message_id),
                    table_fields=snapshot_fields,
                    prefer_table_date=True,
                ):
                    continue
                if sync_status in {"succeeded", "abandoned"}:
                    continue
                if not self._is_unsubmitted_smart_table_record(
                    lead, snapshot=smart_table_snapshot
                ):
                    continue
            # 筛选以当前 Smart Table 快照为准；候选只保留名称，字段快照不进入确认通知。
            company_name = snapshot_fields.get("线索名称") or lead.standard_company_name
            if isinstance(company_name, str) and company_name.strip():
                candidates.append(
                    SubmissionCandidate(
                        lead.id,
                        company_name.strip(),
                        lead.smart_table_owner_user_id,
                    )
                )
        return tuple(candidates)

    def submit_selected(
        self,
        command: SubmissionCommand,
        selected_lead_ids: tuple[str, ...],
        *,
        on_item_result: Callable[[SubmissionItemResult], None] | None = None,
    ) -> SubmissionBatchResult:
        """提交卡片勾选的线索，并在服务端重新校验候选归属和状态。

        参数：command 为原始精确命令；selected_lead_ids 为 callback 中的服务端候选标识。
        返回值：与普通批量提交相同的部分成功汇总。
        异常：actor 不存在/已停用或命令不支持时抛出 ValueError；结果回调异常向调用方传播。
        副作用：每个 callback 已接受的目标均得到结果；仅仍有效的目标调用首次提交流程，
        先按负责人权限重新校验候选状态，再对选中记录批量读取一次并在本请求内复用快照；
        单条处理结束后可选地持久化该条结果。
        """
        if command.text not in {
            _TODAY_COMMAND,
            _ALL_COMMAND,
            _ABANDONED_COMMAND,
            CROSS_OWNER_SUBMISSION_COMMAND,
        }:
            raise ValueError("不支持卡片选择提交")
        cross_owner = command.text == CROSS_OWNER_SUBMISSION_COMMAND
        if cross_owner != (command.owner_scope_user_ids is not None):
            raise ValueError("跨员工提交负责人范围缺失或不适用于当前命令")
        if command.target_capture_date is not None and not cross_owner:
            raise ValueError("按日期跨员工提交只能用于跨员工候选")
        selected = tuple(dict.fromkeys(selected_lead_ids))
        with self._session_factory() as session:
            authorization = (
                session.get(SalesAuthorization, command.sales_user_id) if cross_owner else None
            )
            cross_owner_authorized = not cross_owner or (
                authorization is not None
                and authorization.is_active
                and self._can_submit_other_sales_leads(authorization)
            )
            owner_scope = set(command.owner_scope_user_ids or ())
            valid_ids: set[str] = set()
            selected_record_ids: dict[str, str] = {}
            # 将卡片勾选项的 Lead 与最新 create 状态各用一次查询读齐，避免逐条 N+1 查询。
            selected_leads = {
                lead.id: lead for lead in session.scalars(select(Lead).where(Lead.id.in_(selected)))
            }
            latest_sync_by_lead: dict[str, CrmSyncRecord] = {}
            for sync in session.scalars(
                select(CrmSyncRecord)
                .where(
                    CrmSyncRecord.lead_id.in_(selected),
                    CrmSyncRecord.operation == "create",
                )
                .order_by(
                    CrmSyncRecord.lead_id,
                    CrmSyncRecord.generation.desc().nullslast(),
                    CrmSyncRecord.id.desc(),
                )
            ):
                latest_sync_by_lead.setdefault(sync.lead_id, sync)
            for lead_id in selected:
                lead = selected_leads.get(lead_id)
                if lead is None:
                    continue
                if lead.smart_table_record_id is not None:
                    selected_record_ids[lead_id] = lead.smart_table_record_id
                sync = latest_sync_by_lead.get(lead_id)
                if command.text == _ABANDONED_COMMAND:
                    # 确认卡可能跨越线索转交；只允许当前负责人或授权范围匹配的操作者继续。
                    if (
                        (
                            lead.smart_table_owner_user_id in owner_scope
                            if cross_owner and cross_owner_authorized
                            else lead.smart_table_owner_user_id == command.sales_user_id
                        )
                        and lead.lifecycle_state == "pending_create"
                        and sync is not None
                        and sync.status == "abandoned"
                    ):
                        valid_ids.add(lead_id)
                elif command.text == _TODAY_COMMAND:
                    # frozen candidate 只重验候选状态；必填字段仍由 _submit_create 的最终快照校验。
                    if (
                        lead.smart_table_owner_user_id == command.sales_user_id
                        and lead.lifecycle_state in {"pending_create", "temporary"}
                        and self._is_today_owned_candidate(lead, command.sales_user_id)
                        and (sync is None or sync.status not in {"succeeded", "abandoned"})
                    ):
                        valid_ids.add(lead_id)
                elif (
                    command.text == _ALL_COMMAND
                    and lead.smart_table_owner_user_id == command.sales_user_id
                    and lead.lifecycle_state in {"pending_create", "temporary"}
                    and (sync is None or sync.status not in {"succeeded", "abandoned"})
                ):
                    valid_ids.add(lead_id)
                elif (
                    cross_owner
                    and cross_owner_authorized
                    and lead.smart_table_owner_user_id in owner_scope
                    and lead.lifecycle_state in {"pending_create", "temporary"}
                    and (sync is None or sync.status not in {"succeeded", "abandoned"})
                ):
                    # 日期范围在批量读取到表格当前快照后校验，不能用可能过期的数据库字段提前排除。
                    valid_ids.add(lead_id)
        # 回调只批量读取仍有效的勾选记录；快照仅在当前请求内复用，不跨请求缓存。
        requested_record_ids = {
            selected_record_ids[lead_id] for lead_id in valid_ids if lead_id in selected_record_ids
        }
        snapshot_by_record_id: dict[str, SmartTableRecord] = {}
        if requested_record_ids:
            try:
                for record in self._smart_table_adapter.get_records(requested_record_ids):
                    if record.record_id in requested_record_ids:
                        snapshot_by_record_id[record.record_id] = record
            except Exception as error:
                # 批量快照失败时逐条读取回退；单条依赖故障仍只影响对应线索。
                _LOGGER.warning(
                    "crm_submission_batch_snapshot_failed failure=%s",
                    safe_failure_summary(error),
                )
        invalid_outcomes = {
            lead_id: _create_outcome(
                "not_submitted",
                reason_code=(
                    "crm_cross_owner_submission_not_authorized"
                    if cross_owner and not cross_owner_authorized
                    else None
                ),
            )
            for lead_id in selected
            if lead_id not in valid_ids
        }
        for lead_id, outcome in invalid_outcomes.items():
            if on_item_result is not None:
                item = _append_create_result(SubmissionBatchResult(), lead_id, outcome).items[0]
                on_item_result(item)
        submission_command = SubmissionCommand(
            command.text,
            command.sales_user_id,
            command.request_message_id,
            owner_scope_user_ids=command.owner_scope_user_ids,
            target_capture_date=command.target_capture_date,
        )
        submissions = [
            (
                lead_id,
                submission_command,
                snapshot_by_record_id.get(selected_record_ids.get(lead_id, "")),
            )
            for lead_id in selected
            if lead_id in valid_ids
        ]
        outcomes = self._submit_create_batch(
            submissions,
            on_item_result=on_item_result,
        )
        result = SubmissionBatchResult()
        for lead_id in selected:
            outcome = outcomes[lead_id] if lead_id in outcomes else invalid_outcomes[lead_id]
            result = _append_create_result(result, lead_id, outcome)
        return result

    def _submit_updates(self, command: SubmissionCommand) -> SubmissionBatchResult:
        """扫描当前负责人已同步记录，仅将有业务字段变化的快照提交到 CRM。

        参数：command 为已鉴权的本人更新提交命令。
        返回值：各线索更新结果及部分成功汇总。
        异常：单条表格或 CRM 异常记录为待人工核对，并继续处理其它负责人记录。
        副作用：只批量回读当前负责人的目标记录，并可能创建冻结 CRM 同步记录。
        """
        with self._session_factory() as session:
            candidates = list(
                session.scalars(
                    select(Lead).where(
                        Lead.smart_table_owner_user_id == command.sales_user_id,
                        Lead.lifecycle_state.in_(("synced", "pending_update")),
                        Lead.smart_table_record_id.is_not(None),
                    )
                )
            )
            # 冻结更新尚有未决结果时不得读取新表格快照替换原 CRM 请求。
            record_ids = {
                lead.smart_table_record_id
                for lead in candidates
                if lead.smart_table_record_id is not None
                and self._last_successful_sync(session, lead.id) is not None
                and self._unfinished_update(session, lead.id) is None
            }
        smart_table_snapshot: dict[str, SmartTableRecord] = {}
        if record_ids:
            try:
                smart_table_snapshot = {
                    record.record_id: record
                    for record in self._smart_table_adapter.get_records(record_ids)
                }
            except Exception as error:
                # 整批回读失败时逐条按记录 ID 重读，避免一个表格故障中止整个更新批次。
                _LOGGER.warning(
                    "crm_update_batch_snapshot_failed failure=%s",
                    safe_failure_summary(error),
                )
        outcomes: dict[str, str] = {}

        def submit_one(lead: Lead) -> tuple[str, str]:
            """独立提交一条已同步线索，并将未预期异常收敛为待人工核对。"""
            try:
                outcome = self._submit_update(
                    lead.id,
                    command,
                    initial_record=smart_table_snapshot.get(lead.smart_table_record_id or ""),
                )
            except Exception as error:
                # 某条最终快照或 CRM 异常只影响本条；未知结果进入人工核对，不自动重复覆盖。
                _LOGGER.warning(
                    "crm_update_item_failed error_type=%s",
                    safe_failure_summary(error),
                    extra={"lead_id": lead.id, "failure": safe_failure_summary(error)},
                )
                outcome = "failed_pending_review"
            return lead.id, outcome

        bind = self._session_factory.kw.get("bind")
        max_workers = min(self._crm_submission_max_concurrency, len(candidates))
        if len(candidates) <= 1 or (
            bind is not None and getattr(getattr(bind, "dialect", None), "name", None) == "sqlite"
        ):
            # 单条不启动线程；SQLite 开发库串行写入，避免共享连接争用。
            outcomes.update(submit_one(lead) for lead in candidates)
        else:
            # CRM 调用按配置限流；每条同步状态仍独立提交，批次部分成功。
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="crm-update",
            ) as executor:
                futures = {executor.submit(submit_one, lead): lead.id for lead in candidates}
                for future in as_completed(futures):
                    lead_id = futures[future]
                    try:
                        outcome_lead_id, outcome = future.result()
                    except Exception as error:
                        # 线程边界异常也只影响本条线索。
                        _LOGGER.warning(
                            "crm_update_item_failed",
                            extra={"lead_id": lead_id, "failure": safe_failure_summary(error)},
                        )
                        outcome_lead_id, outcome = lead_id, "failed_pending_review"
                    outcomes[outcome_lead_id] = outcome

        result = SubmissionBatchResult()
        for lead in candidates:
            result = _append_update_result(result, lead.id, outcomes[lead.id])
        return result

    def _submit_update(
        self,
        lead_id: str,
        command: SubmissionCommand,
        *,
        initial_record: SmartTableRecord | None = None,
    ) -> str:
        """重读已同步线索，冻结更新快照并通过 CRM 统一接口提交。

        参数：lead_id 为目标线索；command 为提交命令；initial_record 为本批负责人范围内
        已读取的目标快照；动作前仍按记录标识读取最新行，缺失时同样逐条读取。
        返回值：标准 CRM 更新状态或业务校验结果。
        异常：智能表格和数据库故障传播；CRM 传输结果由同步记录认领器处理。
        副作用：读取目标表格记录、更新人工编辑审计，并可能调用 CRM。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if (
                lead is None
                or lead.smart_table_owner_user_id != command.sales_user_id
                or lead.lifecycle_state not in {"synced", "pending_update"}
            ):
                return "incomplete"
            latest = self._last_successful_sync(session, lead_id)
            if latest is None:
                return "incomplete"
            # 未完成 update 是冻结事实，必须优先恢复，不能先读表并换掉 payload。
            unfinished = self._unfinished_update(session, lead_id)
            if unfinished is not None:
                return self._claim_and_call(unfinished.id, command.sales_user_id)
        review_service = LeadReviewService(
            self._session_factory,
            self._smart_table_adapter,
            robot_submission_confirmation_available=self._robot_submission_confirmation_available,
        )
        if (
            initial_record is not None
            and initial_record.record_id == lead.smart_table_record_id
            and initial_record.fields.get("负责人") != command.sales_user_id
        ):
            # 负责人范围快照已显示转交，直接拒绝该项，避免再读取范围外记录。
            return "incomplete"
        # 批量发现快照可能过期；对当前负责人逐条读取一次，并只 reconcile 这份最新快照。
        current_record = self._read_smart_table_record(lead.smart_table_record_id or "")
        if current_record is None:
            raise ValueError("智能表格记录不存在")
        with self._session_factory.begin() as session:
            # 表格读取后可能立即转交；锁内复核负责人和记录标识后再采纳该快照。
            current_lead = session.scalar(
                select(Lead).where(Lead.id == lead_id).with_for_update()
            )
            if (
                current_lead is None
                or current_lead.smart_table_owner_user_id != command.sales_user_id
                or current_lead.lifecycle_state not in {"synced", "pending_update"}
                or current_lead.smart_table_record_id != current_record.record_id
                or current_record.fields.get("负责人")
                != current_lead.smart_table_owner_user_id
            ):
                return "incomplete"
        reconciled = review_service.reconcile_submission(
            lead_id, initial_record=current_record
        )
        previous = dict(latest.canonical_payload)
        try:
            payload = self._canonical_payload(
                reconciled.fields,
                tyc_customer_id=self._reliable_tyc_customer_id(lead, reconciled.fields),
                fallback_entry_time=self._as_utc(lead.created_at),
            )
        except CrmEnumMappingError as error:
            # 新增合法表格选项不能越过 CRM 字典边界；保存待更新状态，不调用远端接口。
            with self._session_factory.begin() as session:
                current = session.get(Lead, lead_id)
                if current is not None and current.lifecycle_state == "synced":
                    current.lifecycle_state = "pending_update"
            self._audit(
                command, "crm_update_enum_mapping_missing", details={"field_name": error.field_name}
            )
            return "enum_mapping_missing"
        except CrmPayloadError:
            # 字典、日期或备注格式不合法时只阻止当前线索，不让批次或其他线索被异常打断。
            self._audit(command, "crm_update_payload_invalid")
            return "incomplete"
        # 公司身份是全局去重键，变动绝不能伪装成普通字段更新。
        if payload.get("name") != previous.get("name"):
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
                                "old_standard_company_name": previous.get("name"),
                                "new_candidate_company_name": payload.get("name"),
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
            # 候选读取后可能发生负责人转交；CRM 写入前必须在锁内再次确认操作人仍是负责人。
            if (
                lead is None
                or lead.smart_table_owner_user_id != command.sales_user_id
                or lead.lifecycle_state not in {"synced", "pending_update"}
            ):
                return "incomplete"
            # 回读期间可能完成另一次覆盖，持有 Lead 锁后再次检查，禁止新快照绕过旧事实。
            unfinished = self._unfinished_update(session, lead_id)
            if unfinished is not None:
                return (
                    "failed_pending_review" if unfinished.status == "failed_pending_review"
                    else "processing"
                )
            crm_user_id = self._resolve_crm_owner(lead, initial_record=reconciled.record)
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
                # CRM 统一接口不需要本地查重得到的 CRM 目标 ID。
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
                    crm_lead_id=latest.crm_lead_id,
                    crm_lead_owner_user_id=latest.crm_lead_owner_user_id,
                )
                session.add(sync)
                session.flush()
                self._record_audit_for_sync(
                    session, sync, command.sales_user_id, "crm_update_created"
                )
                sync_id = sync.id
        return self._claim_and_call(existing_sync_id or sync_id, command.sales_user_id)

    def _submit_create(
        self,
        lead_id: str,
        command: SubmissionCommand,
        *,
        initial_record: SmartTableRecord | None = None,
        defer_smart_table_submission_status: bool = False,
    ) -> CreateSubmissionOutcome:
        """隔离统一 CRM 提交异常，并判断是否能安全重试前置依赖。

        参数：lead_id 为待提交线索；command 为固定提交命令事实；initial_record 为本次请求内的
        批量快照；defer_smart_table_submission_status 表示将表格状态留给外层批次统一回写。
        返回值：单条提交结果；外部暂态故障以可安全重试的审核结果返回。
        异常：业务异常转换为受控结果；状态查询异常按不可安全重试处理。
        副作用：写入提交审计或 CRM 同步状态。
        """
        try:
            return self._submit_create_once(
                lead_id,
                command,
                initial_record=initial_record,
                defer_smart_table_submission_status=defer_smart_table_submission_status,
            )
        except Exception as error:
            failure_category = classify_task_failure(error)
            safe_before_crm = False
            try:
                # CRM 统一接口仅在同步记录持久化后调用，因此无记录可证明尚未提交。
                with self._session_factory() as session:
                    safe_before_crm = latest_crm_create_sync(session, lead_id) is None
            except Exception as check_error:
                _LOGGER.error(
                    "crm_preflight_state_check_failed error_type=%s",
                    safe_failure_summary(check_error),
                )
            _LOGGER.warning(
                "crm_submission_item_failed lead_id=%s error_type=%s failure_category=%s",
                lead_id,
                safe_failure_summary(error),
                failure_category.value,
            )
            if safe_before_crm and failure_category.value == "transient":
                failure_code = getattr(error, "error_code", None)
                if not isinstance(failure_code, str):
                    external_code = getattr(error, "external_error_code", None)
                    failure_code = str(external_code) if type(external_code) is int else None
                reason_code = (
                    "wecom_smart_table_retryable"
                    if failure_code in {"rate_limited", "network_error"}
                    else "pre_crm_transient_failure"
                )
                return _create_outcome(
                    "failed_pending_review",
                    reason_code=reason_code,
                    failure_category=failure_category.value,
                    adapter_category="transport",
                    failure_code=failure_code,
                )
            return _create_outcome("failed_pending_review")

    def _submit_create_once(
        self,
        lead_id: str,
        command: SubmissionCommand,
        *,
        initial_record: SmartTableRecord | None = None,
        defer_smart_table_submission_status: bool = False,
    ) -> CreateSubmissionOutcome:
        """冻结首次提交快照并调用 CRM 统一线索接口。

        参数：lead_id 为待提交线索；command 为固定提交命令事实；initial_record 为当前请求内的
        目标记录快照；defer_smart_table_submission_status 表示将表格状态留给外层批次统一回写。
        返回值：单条提交结果；创建或跟进由 CRM 内部处理。
        异常：智能表格或数据库异常按既有同步状态转换并记录审计。
        副作用：读取最终表格快照，写入冻结 CRM 同步记录并调用统一提交接口。
        """
        allow_non_today_target = (
            command.text
            in {
                _ALL_COMMAND,
                _ABANDONED_COMMAND,
                CROSS_OWNER_SUBMISSION_COMMAND,
            }
            or command.target_lead_id is not None
        )
        # 单条确认卡已冻结唯一 Lead 目标；它可提交 temporary 草稿，但仍须通过后续字段校验。
        allow_temporary = command.text in {
            _ALL_COMMAND,
            _TODAY_COMMAND,
            CROSS_OWNER_SUBMISSION_COMMAND,
        } or (
            command.text == "提交指定线索" and command.target_lead_id == lead_id
        )
        with self._session_factory() as session:
            if command.owner_scope_user_ids is not None:
                authorization = session.get(SalesAuthorization, command.sales_user_id)
                if (
                    authorization is None
                    or not authorization.is_active
                    or not self._can_submit_other_sales_leads(authorization)
                ):
                    return _create_outcome(
                        "not_submitted",
                        reason_code="crm_cross_owner_submission_not_authorized",
                    )
            lead = session.get(Lead, lead_id)
            source_message = (
                session.get(IncomingMessage, lead.source_message_id)
                if lead is not None
                and command.target_capture_date is not None
                and lead.source_message_id
                else None
            )
            if lead is None or not self._is_create_candidate(
                lead,
                command.sales_user_id,
                allow_non_today_target,
                allow_temporary=allow_temporary,
                owner_scope_user_ids=command.owner_scope_user_ids,
            ):
                return _create_outcome("not_submitted")
            # 未完成操作复用冻结快照，不能因重试而替换已经提交或结果未知的字段。
            unfinished_update = self._unfinished_update(session, lead_id)
            if unfinished_update is not None:
                return _create_outcome(
                    "failed_pending_review" if unfinished_update.status == "failed_pending_review"
                    else "processing", reason_code="crm_outcome_unknown",
                )
            existing = latest_crm_create_sync(session, lead_id)
            if existing is not None:
                if existing.status == "succeeded":
                    return _create_outcome("not_submitted", reason_code="already_submitted")
                if existing.status == "awaiting_duplicate_confirmation":
                    # 旧版本只完成过查重；将同一冻结操作交给新接口，无需再让销售判断。
                    return _create_outcome(
                        self._claim_and_call(
                            existing.id,
                            command.sales_user_id,
                            defer_smart_table_submission_status=defer_smart_table_submission_status,
                        )
                    )
                if existing.status == "abandoned" and command.text == _ABANDONED_COMMAND:
                    # 只有专用“重新提交放弃线索”命令允许重开服务端放弃事实。
                    pass
                else:
                    # retry 必须使用现有冻结快照，绝不重新读表替换 payload。
                    return _create_outcome(
                        self._claim_and_call(
                            existing.id,
                            command.sales_user_id,
                            defer_smart_table_submission_status=defer_smart_table_submission_status,
                        )
                    )

        # 卡片回调已批量读取选中记录；只复用本次请求内与目标 ID 一致的快照。
        reconciled = LeadReviewService(
            self._session_factory,
            self._smart_table_adapter,
            robot_submission_confirmation_available=self._robot_submission_confirmation_available,
        ).reconcile_submission(lead_id, initial_record=initial_record)
        if command.target_capture_date is not None and not self._is_capture_date_owned_candidate(
            lead,
            command.owner_scope_user_ids or (),
            command.target_capture_date,
            source_message.received_at if source_message is not None else None,
            table_fields=reconciled.fields,
            prefer_table_date=True,
        ):
            # 复核卡片时必须以此刻重读的表格录入日为准，越界项目不应显示为待完善。
            return _create_outcome("not_submitted")
        allowed_submission_statuses = {"未提交"}
        if command.text == _ABANDONED_COMMAND:
            allowed_submission_statuses.add("放弃提交")
        if reconciled.fields.get("提交状态") not in allowed_submission_statuses:
            # Final Snapshot 已显示该记录并非未提交，保留逐 Lead 结果且不调用 CRM。
            return _create_outcome("not_submitted")
        missing_business = self._missing_business_fields(reconciled.fields)
        missing_minimum = self._missing_crm_minimum(reconciled.fields)
        if missing_business:
            # 八项业务完整度仍用于明确审核反馈，不能被 CRM minimum 偷换定义。
            self._audit(command, "crm_business_fields_incomplete")
        if missing_minimum:
            self._audit(command, "crm_create_incomplete")
            return _create_outcome("incomplete", missing_fields=missing_minimum)

        try:
            canonical_payload = self._canonical_payload(
                reconciled.fields,
                tyc_customer_id=self._reliable_tyc_customer_id(lead, reconciled.fields),
                fallback_entry_time=self._as_utc(lead.created_at),
            )
        except CrmEnumMappingError as error:
            # 只返回当前字段缺映射提示，既不改为其他也不把 option ID 当成 CRM ID。
            self._audit(
                command, "crm_create_enum_mapping_missing", details={"field_name": error.field_name}
            )
            return _create_outcome(
                "incomplete",
                reason_code="crm_enum_mapping_missing",
                missing_fields=(str(error),),
            )
        except CrmPayloadError:
            # CRM DTO 校验失败属于当前线索待完善，禁止进入远端写操作。
            self._audit(command, "crm_create_payload_invalid")
            return _create_outcome("incomplete")
        company_name = canonical_payload.get("name")
        if not isinstance(company_name, str) or not company_name:
            return _create_outcome("incomplete")
        # 全局预留跟随已核验标准名；CRM 查重仍使用本次表格最终快照。
        identity_name = lead.standard_company_name or company_name
        snapshot_hash = self._snapshot_hash(canonical_payload)

        # 映射缺失必须在任何 CRM 请求前阻断；智能表格成员名称沿用本次已回读快照。
        crm_user_id = self._resolve_crm_owner(lead, initial_record=reconciled.record)
        if crm_user_id is None:
            with self._session_factory.begin() as session:
                current_lead = session.get(Lead, lead_id)
                if current_lead is None:
                    return _create_outcome("incomplete")
                return _create_outcome(
                    self._record_mapping_missing(
                        session, current_lead, command, canonical_payload, snapshot_hash, "create"
                    )
                )

        # 先检查持久化公司预留，避免明显并发冲突再发一次 CRM 查重请求。
        with self._session_factory() as session:
            identity_before_search = session.get(CrmCompanyIdentity, identity_name)
            if (
                identity_before_search is not None
                and identity_before_search.state == "reserving"
                and identity_before_search.creating_lead_id != lead_id
            ):
                return _create_outcome("processing", reason_code="crm_company_identity_processing")
            if (
                identity_before_search is not None
                and identity_before_search.state == "failed_pending_review"
            ):
                return _create_outcome(
                    "company_identity_review", reason_code="crm_company_identity_pending_review"
                )
            initial_identity_state = (
                identity_before_search.state if identity_before_search is not None else None
            )
            initial_identity_crm_lead_id = (
                identity_before_search.crm_lead_id if identity_before_search is not None else None
            )

        try:
            duplicate_results = tuple(self._crm_adapter.search_by_company_name(canonical_payload))
        except Exception as error:
            evidence = self._crm_failure_evidence(error)
            failure_category = classify_task_failure(error).value
            with self._session_factory.begin() as session:
                session.add(
                    BusinessAuditEvent(
                        message_id=command.request_message_id,
                        sales_user_id=command.sales_user_id,
                        event_type=(
                            f"crm_duplicate_search_failed:{lead_id}:{failure_category}"
                        ),
                        details={"lead_id": lead_id, **evidence},
                    )
                )
            reason_code = (
                "duplicate_target_unavailable"
                if evidence.get("adapter_category") == "duplicate_target_unavailable"
                else "crm_duplicate_search_retrying"
                if failure_category == "transient"
                else "crm_duplicate_search_failed"
            )
            return _create_outcome(
                "retrying" if failure_category == "transient" else "failed_pending_review",
                reason_code=reason_code,
                **evidence,
            )

        duplicate_result: CRMSearchResult | None = None
        if initial_identity_state == "active":
            duplicate_result = next(
                (
                    item
                    for item in duplicate_results
                    if item.crm_lead_id == initial_identity_crm_lead_id
                ),
                None,
            )
            if duplicate_result is None:
                with self._session_factory.begin() as session:
                    session.add(
                        BusinessAuditEvent(
                            message_id=command.request_message_id,
                            sales_user_id=command.sales_user_id,
                            event_type="crm_global_identity_conflict",
                        details={"lead_id": lead_id, "standard_company_name": identity_name},
                        )
                    )
                return _create_outcome(
                    "failed_pending_review", reason_code="crm_company_identity_conflict"
                )
        elif duplicate_results:
            # SOP 查重结果按稳定返回顺序展示，确认后由 CRM Adapter 执行单条更新。
            duplicate_result = duplicate_results[0]

        try:
            with self._session_factory.begin() as session:
                lead = session.scalar(select(Lead).where(Lead.id == lead_id).with_for_update())
                # 来源日期与负责人范围都要基于持锁后的 Lead 事实再次确认。
                source_message = (
                    session.get(IncomingMessage, lead.source_message_id)
                    if lead is not None
                    and command.target_capture_date is not None
                    and lead.source_message_id
                    else None
                )
                authorization = None
                if command.owner_scope_user_ids is not None:
                    # 候选检查后权限可能被撤销；持锁读取授权行后再决定是否允许跨员工提交。
                    authorization = session.scalar(
                        select(SalesAuthorization)
                        .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
                        .with_for_update()
                    )
                    if (
                        authorization is None
                        or not authorization.is_active
                        or not self._can_submit_other_sales_leads(authorization)
                    ):
                        return _create_outcome(
                            "not_submitted",
                            reason_code="crm_cross_owner_submission_not_authorized",
                        )
                if command.target_capture_date is not None and lead is not None and not (
                    self._is_capture_date_owned_candidate(
                        lead,
                        command.owner_scope_user_ids or (),
                        command.target_capture_date,
                        source_message.received_at if source_message is not None else None,
                        table_fields=reconciled.fields,
                        prefer_table_date=True,
                    )
                ):
                    return _create_outcome("not_submitted")
                if lead is None or not self._is_create_candidate(
                    lead,
                    command.sales_user_id,
                    allow_non_today_target,
                    allow_temporary=allow_temporary,
                    owner_scope_user_ids=command.owner_scope_user_ids,
                ):
                    return _create_outcome("incomplete")
                if lead.lifecycle_state == "temporary":
                    # 临时线索通过最终快照和字段校验后，统一转为待提交状态。
                    lead.lifecycle_state = "pending_create"
                # 持有 Lead 锁复核后再冻结操作，避免并发请求替换未知结果。
                if self._unfinished_update(session, lead_id) is not None:
                    return _create_outcome(
                        "failed_pending_review", reason_code="crm_outcome_unknown"
                    )
                crm_user_id = self._resolve_crm_owner(lead, initial_record=reconciled.record)
                if crm_user_id is None:
                    return _create_outcome(
                        self._record_mapping_missing(
                            session, lead, command, canonical_payload, snapshot_hash, "create"
                        )
                    )
                existing = latest_crm_create_sync(session, lead_id, for_update=True)
                create_sync = True
                if existing is not None:
                    if existing.status == "succeeded":
                        return _create_outcome(
                            "not_submitted", reason_code="already_submitted"
                        )
                    if existing.status == "awaiting_duplicate_confirmation":
                        # 兼容旧版本已查重但未提交的记录，沿用原幂等键交给统一接口。
                        existing.status = "pending"
                        existing.crm_lead_id = None
                        existing.crm_lead_owner_user_id = None
                        existing.duplicate_entity_type = None
                        existing.response_summary = None
                        sync_id = existing.id
                        create_sync = False
                    elif existing.status == "abandoned" and command.text == _ABANDONED_COMMAND:
                        # 已放弃的历史操作保持不可变；专用重提命令追加新的幂等 generation。
                        generation = (existing.generation or 1) + 1
                        supersedes_sync_record_id = existing.id
                    else:
                        return _create_outcome(existing.status)
                else:
                    if command.text == _ABANDONED_COMMAND:
                        # 卡片发行后若历史候选已消失，禁止无旧事实地凭命令创建。
                        return _create_outcome("incomplete")
                    generation = 1
                    supersedes_sync_record_id = None
                identity = None
                if create_sync:
                    identity = session.scalar(
                        select(CrmCompanyIdentity)
                        .where(CrmCompanyIdentity.standard_company_name == identity_name)
                        .with_for_update()
                    )
                    if identity is not None and identity.state == "active":
                        # 公司身份在本次查重后才被其他事务激活，交由下一次回读重试。
                        if initial_identity_state is None:
                            return _create_outcome(
                                "processing", reason_code="crm_company_identity_processing"
                            )
                        if (
                            duplicate_result is None
                            or identity.crm_lead_id != duplicate_result.crm_lead_id
                        ):
                            return _create_outcome(
                                "failed_pending_review",
                                reason_code="crm_company_identity_conflict",
                            )
                    elif (
                        identity is not None
                        and identity.state == "reserving"
                        and identity.creating_lead_id != lead.id
                    ):
                        return _create_outcome(
                            "processing", reason_code="crm_company_identity_processing"
                        )
                    elif identity is not None and identity.state != "reserving":
                        return _create_outcome(
                            "company_identity_review",
                            reason_code="crm_company_identity_pending_review",
                        )
                if create_sync:
                    sync = CrmSyncRecord(
                        lead_id=lead.id,
                        operation="create",
                        generation=generation,
                        supersedes_sync_record_id=supersedes_sync_record_id,
                        smart_table_record_id=lead.smart_table_record_id or "",
                        status=(
                            "awaiting_duplicate_confirmation"
                            if duplicate_result is not None
                            else "pending"
                        ),
                        crm_lead_id=(
                            duplicate_result.crm_lead_id if duplicate_result is not None else None
                        ),
                        crm_lead_owner_user_id=(
                            duplicate_result.crm_lead_owner_user_id
                            if duplicate_result is not None
                            else None
                        ),
                        idempotency_key=(
                            f"crm:create:{lead.id}"
                            if generation == 1
                            else f"crm:create:{lead.id}:g{generation}"
                        ),
                        canonical_payload=canonical_payload,
                        snapshot_hash=snapshot_hash,
                        request_message_id=command.request_message_id,
                        submitting_sales_user_id=command.sales_user_id,
                        submitting_crm_user_id=crm_user_id,
                    )
                    session.add(sync)
                    session.flush()
                    sync_id = sync.id
                    if identity is None:
                        identity = CrmCompanyIdentity(
                            standard_company_name=identity_name,
                            state="reserving",
                            creating_lead_id=lead.id,
                        )
                        session.add(identity)
                    if identity.state == "reserving":
                        identity.creating_lead_id = lead.id
                        identity.creating_sync_record_id = sync.id
                    if duplicate_result is not None:
                        self._record_audit_for_sync(
                            session,
                            sync,
                            command.sales_user_id,
                            "crm_duplicate_confirmation_required",
                        )
        except IntegrityError:
            # Lead 幂等键或全局公司身份预留冲突时，只允许一个提交者调用 CRM。
            return _create_outcome("processing")
        if duplicate_result is not None:
            return _create_outcome(
                "duplicate_confirmation",
                duplicate=DuplicateSubmission(
                    lead_id, company_name, duplicate_result.crm_lead_id,
                    duplicate_result.crm_lead_owner_user_id,
                ),
            )
        return _create_outcome(
            self._claim_and_call(
                sync_id,
                command.sales_user_id,
                defer_smart_table_submission_status=defer_smart_table_submission_status,
            )
        )

    def resolve_duplicate_confirmation(
        self,
        request_message_id: str,
        sales_user_id: str,
        *,
        continue_submission: bool,
        selected_lead_ids: tuple[str, ...] = (),
        duplicate_lead_ids: tuple[str, ...] = (),
        owner_scope_user_ids: tuple[str, ...] | None = None,
        target_capture_date: date | None = None,
    ) -> DuplicateConfirmationResult:
        """执行重复线索卡片的继续或停止决定，并只更新服务端冻结的目标。

        参数：request_message_id 和 sales_user_id 定位原提交请求及操作者；
        duplicate_lead_ids 为确认卡冻结目标；owner_scope_user_ids 为已授权负责人范围；
        continue_submission 表示继续覆盖还是停止；selected_lead_ids 为操作人勾选的本地线索。
        返回值：已覆盖、已放弃、未选择和失败数量汇总。
        异常：操作者不存在/已停用或目标不属于当前销售时抛出 ValueError。
        副作用：更新 CRM 同步状态、审计记录和智能表格提交状态，必要时调用 CRM update。
        """
        selected = set(selected_lead_ids)
        selected_sync_ids: list[int] = []
        submitted_lead_ids: list[str] = []
        abandoned_lead_ids: list[str] = []
        remaining = 0
        owner_scope = owner_scope_user_ids or (sales_user_id,)
        date_snapshot: dict[str, SmartTableRecord] = {}
        source_received_at: dict[str, datetime] = {}
        if target_capture_date is not None:
            with self._session_factory() as session:
                # 日期确认卡只回读本请求冻结的重复线索，不扫描整张表。
                statement = (
                    select(CrmSyncRecord, Lead)
                    .join(Lead, Lead.id == CrmSyncRecord.lead_id)
                    .where(
                        CrmSyncRecord.request_message_id == request_message_id,
                        CrmSyncRecord.operation == "create",
                        CrmSyncRecord.status == "awaiting_duplicate_confirmation",
                        Lead.smart_table_owner_user_id.in_(owner_scope),
                    )
                )
                if duplicate_lead_ids:
                    statement = statement.where(Lead.id.in_(duplicate_lead_ids))
                date_rows = session.execute(statement).all()
                record_ids = tuple(
                    dict.fromkeys(
                        lead.smart_table_record_id
                        for _, lead in date_rows
                        if lead.smart_table_record_id is not None
                    )
                )
                source_ids = {
                    lead.source_message_id
                    for _, lead in date_rows
                    if lead.source_message_id is not None
                }
                if source_ids:
                    source_received_at = dict(
                        session.execute(
                            select(IncomingMessage.message_id, IncomingMessage.received_at).where(
                                IncomingMessage.message_id.in_(source_ids)
                            )
                        ).all()
                    )
            if record_ids:
                date_snapshot = {
                    record.record_id: record
                    for record in self._smart_table_adapter.get_records(record_ids=record_ids)
                }
        with self._session_factory.begin() as session:
            # 跨员工卡片可能延迟点击；执行时必须重新确认操作人仍有跨员工权限。
            if owner_scope_user_ids is not None:
                authorization = session.get(SalesAuthorization, sales_user_id)
                if (
                    not owner_scope_user_ids
                    or authorization is None
                    or not authorization.is_active
                    or not authorization.can_submit_other_sales_leads
                ):
                    raise PermissionError("跨员工 CRM 提交权限已失效")
            statement = (
                select(CrmSyncRecord, Lead)
                .join(Lead, Lead.id == CrmSyncRecord.lead_id)
                .where(
                    CrmSyncRecord.request_message_id == request_message_id,
                    CrmSyncRecord.operation == "create",
                    CrmSyncRecord.status == "awaiting_duplicate_confirmation",
                    Lead.smart_table_owner_user_id.in_(owner_scope),
                )
            )
            # 实际卡片回调只能作用于发行时冻结的重复目标集合。
            if duplicate_lead_ids:
                statement = statement.where(Lead.id.in_(duplicate_lead_ids))
            rows = session.execute(statement.with_for_update()).all()
            for sync, lead in rows:
                record = date_snapshot.get(lead.smart_table_record_id or "")
                if target_capture_date is not None and not self._is_capture_date_owned_candidate(
                    lead,
                    owner_scope,
                    target_capture_date,
                    source_received_at.get(lead.source_message_id or ""),
                    table_fields=record.fields if record is not None else lead.field_values,
                    prefer_table_date=True,
                ):
                    # 冻结确认卡可能已过期或线索日期被修正；绝不对范围外目标执行动作。
                    remaining += 1
                    continue
                if not continue_submission:
                    sync.status = "abandoned"
                    sync.failure_category = "user_abandoned"
                    sync.response_summary = "用户停止提交重复线索"
                    sync.completed_at = utc_now()
                    abandoned_lead_ids.append(lead.id)
                    identity = session.scalar(
                        select(CrmCompanyIdentity)
                        .where(CrmCompanyIdentity.creating_sync_record_id == sync.id)
                        .with_for_update()
                    )
                    if identity is not None and identity.state == "reserving":
                        session.delete(identity)
                    self._record_audit_for_sync(
                        session, sync, sales_user_id, "crm_duplicate_submission_abandoned"
                    )
                    continue
                if lead.id not in selected:
                    remaining += 1
                    continue
                # 复用同一冻结记录，但把后续远端动作明确切换为 CRM update。
                sync.operation = "update"
                sync.idempotency_key = f"crm:update:{lead.id}:{sync.snapshot_hash}"
                sync.status = "pending"
                sync.response_summary = None
                selected_sync_ids.append(sync.id)
                submitted_lead_ids.append(lead.id)
                self._record_audit_for_sync(
                    session, sync, sales_user_id, "crm_duplicate_update_confirmed"
                )

        if not continue_submission:
            for lead_id in abandoned_lead_ids:
                self._set_smart_table_submission_status(lead_id, "放弃提交")
            return DuplicateConfirmationResult(abandoned=len(abandoned_lead_ids))

        submitted = 0
        failed = 0
        failure_reasons: list[str] = []
        try:
            for sync_id in selected_sync_ids:
                state = self._claim_and_call(
                    sync_id,
                    sales_user_id,
                    defer_smart_table_submission_status=True,
                )
                if state == "succeeded":
                    submitted += 1
                else:
                    failed += 1
                    # 销售只看到受控分类说明，不透传 CRM 响应、客户数据或凭据。
                    with self._session_factory() as session:
                        sync = session.get(CrmSyncRecord, sync_id)
                        reason = self._duplicate_failure_reason(sync)
                    if reason not in failure_reasons:
                        failure_reasons.append(reason)
        finally:
            self._write_back_batch_submission_status(tuple(submitted_lead_ids))
        return DuplicateConfirmationResult(
            submitted=submitted,
            remaining=remaining,
            failed=failed,
            failure_reasons=tuple(failure_reasons),
        )

    @staticmethod
    def _duplicate_failure_reason(sync: CrmSyncRecord | None) -> str:
        """返回重复覆盖失败的安全中文说明；参数为同步事实，返回固定文案，无外部调用。"""
        if sync is None or sync.failure_category == "unknown" or (
            sync.operation == "update" and sync.attempts > 0 and sync.failure_kind is None
        ):
            return "CRM 远端覆盖结果待核实，已停止自动重试；请联系管理员核对后处理，勿重复提交。"
        if sync.failure_kind == "authentication":
            return "CRM 身份认证失败，已停止覆盖；请联系管理员检查调用身份。"
        if sync.failure_kind in {"business", "business_rejection"}:
            return "CRM 明确拒绝覆盖，已停止重试；请联系管理员核对编辑权限及字段校验。"
        if sync.failure_kind == "gateway":
            return "CRM 网关拒绝请求，已停止覆盖；请联系管理员核查网关状态和权限。"
        return "CRM 覆盖未完成，已停止自动重试；请联系管理员查看同步记录的安全失败原因。"

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

    @staticmethod
    def _unfinished_update(
        session: Session, lead_id: str, *, exclude_sync_id: int | None = None,
    ) -> CrmSyncRecord | None:
        """查找阻止新覆盖的冻结更新；旧已尝试但无 kind 的失败也按待核实阻断。

        参数：session 为当前事务，lead_id 为线索，exclude_sync_id 排除本次认领自身。
        返回：最早的未结更新或 None；数据库异常传播；只读，不改写历史分类和记录。
        """
        return session.scalar(
            select(CrmSyncRecord).where(
                CrmSyncRecord.lead_id == lead_id,
                CrmSyncRecord.operation == "update",
                exclude_sync_id is None or CrmSyncRecord.id != exclude_sync_id,
                or_(
                    CrmSyncRecord.status.in_(("pending", "retrying", "processing")),
                    and_(
                        CrmSyncRecord.status == "failed_pending_review",
                        or_(
                            CrmSyncRecord.failure_category == "unknown",
                            and_(CrmSyncRecord.attempts > 0, CrmSyncRecord.failure_kind.is_(None)),
                        ),
                    ),
                ),
            ).order_by(CrmSyncRecord.id.asc()).limit(1)
        )

    def _claim_and_call(
        self,
        sync_id: int,
        sales_user_id: str,
        *,
        notify_retry_result: bool = False,
        defer_smart_table_submission_status: bool = False,
    ) -> str:
        """原子认领同步记录并持久化 CRM 结果；批量调用时延后表格状态更新。"""
        claim_started_at: datetime
        claim_attempts: int
        completed_lead_id: str | None = None
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
            if sync.status not in {
                "pending", "retrying", "awaiting_duplicate_confirmation"
            } and not lease_expired:
                # 同一冻结请求可从外部结果未知的检查点恢复，避免重新读取并替换提交快照。
                if not (
                    sync.status == "failed_pending_review"
                    and sync.failure_category == "unknown"
                ):
                    return self._completed_sync_outcome(sync)
            is_unknown_outcome_recovery = lease_expired or (
                sync.status == "failed_pending_review" and sync.failure_category == "unknown"
            )
            unfinished = self._unfinished_update(session, sync.lead_id, exclude_sync_id=sync.id)
            if unfinished is not None:
                return (
                    "failed_pending_review" if unfinished.status == "failed_pending_review"
                    else "processing"
                )
            if sync.operation == "update" and sync.attempts > 0 and lease_expired:
                # 过期更新租约可能仍有旧 Worker 在途；自动接管不能并发重放未知覆盖。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
                return "failed_pending_review"
            if sync.attempts >= self._crm_create_retry_count and not is_unknown_outcome_recovery:
                # 传输失败达到自动重试上限只能确认“外部结果未知”，不得结算废弃请求。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
                if notify_retry_result:
                    self._enqueue_crm_retry_result_notification(session, sync)
                return "failed_pending_review"
            if sync.status == "awaiting_duplicate_confirmation":
                # 历史查重记录尚未调用 CRM 写接口，可安全转换为统一提交操作。
                sync.crm_lead_id = None
                sync.crm_lead_owner_user_id = None
                sync.duplicate_entity_type = None
                sync.response_summary = None
            # 已登记 Sync 的 discard 已进入等待外部事实路径；create 继续使用原冻结操作。
            sync.status = "processing"
            sync.next_retry_at = None
            sync.attempts += 1
            claim_attempts = sync.attempts
            claim_started_at = utc_now()
            sync.processing_started_at = claim_started_at
            sync.processing_lease_expires_at = sync.processing_started_at + _CRM_PROCESSING_LEASE
            payload = dict(sync.canonical_payload)
            idempotency_key = sync.idempotency_key
            crm_user_id = sync.submitting_crm_user_id
            operation = sync.operation
            generation = sync.generation
            crm_lead_id = sync.crm_lead_id

        if crm_user_id is None:
            # validation 记录不会进入认领路径；此处仅防御历史异常数据误触发 CRM 调用。
            return self._record_crm_failure(
                sync_id,
                sales_user_id,
                ValueError("CRM 同步记录缺少冻结提交身份"),
                claim_started_at,
                claim_attempts,
                notify_retry_result=notify_retry_result,
            )

        try:
            # generation 仅存在于查重确认后转为 update 的创建记录；普通更新继续走统一提交接口。
            if operation == "update" and generation is not None:
                if not crm_lead_id:
                    raise ValueError("CRM 更新记录缺少目标线索标识")
                crm_result = self._crm_adapter.update_lead(
                    crm_lead_id,
                    payload,
                    idempotency_key=idempotency_key,
                    crm_user_id=crm_user_id,
                )
            else:
                crm_result = self._crm_adapter.submit_lead(
                    payload, idempotency_key=idempotency_key, crm_user_id=crm_user_id
                )
        except (TimeoutError, ConnectionError, OSError) as error:
            # PermissionError 属于 OSError，但它是永久失败，必须先于传输重试分支终止。
            if classify_task_failure(error).value != "transient":
                return self._record_crm_failure(
                    sync_id,
                    sales_user_id,
                    error,
                    claim_started_at,
                    claim_attempts,
                    notify_retry_result=notify_retry_result,
                )
            return self._record_crm_transport_failure(
                sync_id,
                sales_user_id,
                error,
                claim_started_at,
                claim_attempts,
                notify_retry_result=notify_retry_result,
            )
        except Exception as error:
            if classify_task_failure(error).value == "transient":
                return self._record_crm_transport_failure(
                    sync_id,
                    sales_user_id,
                    error,
                    claim_started_at,
                    claim_attempts,
                    notify_retry_result=notify_retry_result,
                )
            return self._record_crm_failure(
                sync_id,
                sales_user_id,
                error,
                claim_started_at,
                claim_attempts,
                notify_retry_result=notify_retry_result,
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
                return self._completed_sync_outcome(sync)
            sync.status = "succeeded"
            sync.next_retry_at = None
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            if crm_result.crm_lead_id is not None:
                # FOLLOW_UP 不会返回新线索 ID；更新既有线索时保留上次 CRM 身份。
                sync.crm_lead_id = crm_result.crm_lead_id
            if crm_result.crm_lead_owner_user_id is not None:
                sync.crm_lead_owner_user_id = crm_result.crm_lead_owner_user_id
            sync.response_summary = crm_result.response_summary[:256]
            sync.completed_at = utc_now()
            completed_lead_id = lead.id
            identity = session.scalar(
                select(CrmCompanyIdentity)
                .where(CrmCompanyIdentity.creating_sync_record_id == sync.id)
                .with_for_update()
            )
            if identity is not None:
                if sync.crm_lead_id is None and sync.operation == "create":
                    # FOLLOW_UP 不产生新 CRM 身份，释放仅由本次 create 创建的临时预留。
                    session.delete(identity)
                    self._record_audit_for_sync(
                        session,
                        sync,
                        sales_user_id,
                        "crm_global_identity_reservation_released_after_unified_submit",
                    )
                elif sync.crm_lead_id is not None:
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
            event_type = (
                "crm_follow_up_succeeded"
                if crm_result.action == "FOLLOW_UP"
                else f"crm_{sync.operation}_succeeded"
            )
            self._record_audit_for_sync(session, sync, sales_user_id, event_type)
            if notify_retry_result:
                self._enqueue_crm_retry_result_notification(session, sync)
        if completed_lead_id is not None and not defer_smart_table_submission_status:
            try:
                # CRM 成功事实已提交后再更新审核表状态，避免远端成功被表格慢调用锁住。
                self._set_smart_table_submission_status(completed_lead_id, "已提交")
            except Exception as error:
                # CRM 已成功，状态写入失败只能保留可观测告警，不能重试 CRM 写操作。
                _LOGGER.warning(
                    "smart_table_submission_status_update_failed",
                    extra={
                        "lead_id": completed_lead_id,
                        "status": "已提交",
                        "error_type": type(error).__name__,
                    },
                )
        return "followed_up" if crm_result.action == "FOLLOW_UP" else "succeeded"

    @staticmethod
    def _completed_sync_outcome(sync: CrmSyncRecord) -> str:
        """将已完成同步记录中的 CRM 动作恢复为可展示的结果状态。"""
        if sync.status == "succeeded" and (
            "FOLLOW_UP" in (sync.response_summary or "")
            or (sync.operation == "create" and sync.crm_lead_id is None)
        ):
            return "followed_up"
        return sync.status

    def _set_smart_table_submission_status(self, lead_id: str, status: str) -> None:
        """把 CRM 提交结果增量写入智能表格的提交状态列。

        参数：lead_id 为本地线索；status 必须是业务规定的提交状态枚举值。
        返回值：无。
        异常：智能表格更新失败时向调用方抛出，由提交成功路径记录告警。
        副作用：增量更新智能表格，并同步保存后台状态快照。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None or lead.smart_table_record_id is None:
                return
            record_id = lead.smart_table_record_id
        self._smart_table_adapter.update_record(record_id, {"提交状态": status})
        with self._session_factory.begin() as session:
            lead = session.get(Lead, lead_id)
            if lead is not None:
                lead.field_values = {**lead.field_values, "提交状态": status}

    def _write_back_batch_submission_status(self, lead_ids: tuple[str, ...]) -> None:
        """查询本批最新 CRM 状态，仅批量回写已成功线索的智能表格提交状态。

        参数：lead_ids 为本批提交目标。
        返回值：无。
        异常：不向提交调用方传播；查询、智能表格或后台快照回写错误会记录告警。
        副作用：最多调用一次智能表格批量更新，并同步本地 Lead 字段快照。
        """
        if not lead_ids:
            return
        try:
            with self._session_factory() as session:
                rows = session.execute(
                    select(
                        CrmSyncRecord.lead_id,
                        CrmSyncRecord.status,
                        Lead.smart_table_record_id,
                    )
                    .join(Lead, Lead.id == CrmSyncRecord.lead_id)
                    .where(
                        CrmSyncRecord.lead_id.in_(lead_ids),
                        CrmSyncRecord.operation.in_(("create", "update")),
                    )
                    .order_by(CrmSyncRecord.lead_id, CrmSyncRecord.id.desc())
                ).all()
            latest: dict[str, tuple[str, str | None]] = {}
            for lead_id, sync_status, record_id in rows:
                latest.setdefault(lead_id, (sync_status, record_id))
            successful = [
                (lead_id, record_id)
                for lead_id, (sync_status, record_id) in latest.items()
                if sync_status == "succeeded" and record_id
            ]
            if not successful:
                return
            self._smart_table_adapter.update_records(
                [(record_id, {"提交状态": "已提交"}) for _, record_id in successful]
            )
            with self._session_factory.begin() as session:
                for lead_id, _ in successful:
                    lead = session.get(Lead, lead_id)
                    if lead is not None:
                        lead.field_values = {**lead.field_values, "提交状态": "已提交"}
        except Exception as error:
            # 表格投影失败不能回滚 CRM 成功事实，也不能逐条重放批量写入。
            _LOGGER.warning(
                "smart_table_batch_submission_status_update_failed",
                extra={
                    "lead_count": len(lead_ids),
                    "error_type": type(error).__name__,
                },
            )

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """将数据库返回的处理租约时间统一解释为 UTC。"""
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    def _canonical_payload(
        self,
        fields: dict[str, object],
        *,
        tyc_customer_id: str | None = None,
        fallback_entry_time: datetime,
    ) -> dict[str, object]:
        """构造稳定的 CRM 英文键快照并排除智能表格审核元数据。

        参数：fields 为提交前回读的中文智能表格字段；tyc_customer_id 为天眼查客户标识；
        fallback_entry_time 为表格系统录入时间缺失时使用的稳定 Lead 创建时间。
        返回值：可直接冻结、哈希和发送给 CRM Adapter 的英文键 payload。
        异常：字段枚举、日期或备注不符合 CRM 契约时抛出 CrmPayloadError。
        副作用：无，不调用外部系统。
        """
        # 表格系统时间缺失时回退到固定的 Lead 创建时间，避免重提时 payload 时间漂移。
        payload_fields = dict(fields)
        if not payload_fields.get("录入时间"):
            payload_fields["录入时间"] = payload_fields.get("创建时间") or fallback_entry_time
        return self._crm_payload_builder.build(payload_fields, tyc_customer_id=tyc_customer_id)

    @staticmethod
    def _reliable_tyc_customer_id(
        lead: Lead, fields: Mapping[str, object]
    ) -> str | None:
        """只为唯一核验且名称未被销售改写的线索返回天眼查 ID。"""
        if lead.company_verification_status != CompanyVerificationStatus.TYC_VERIFIED.value:
            return None
        customer_id = lead.tyc_customer_id
        company_name = fields.get("线索名称")
        if (
            not isinstance(customer_id, str)
            or not customer_id
            or not isinstance(company_name, str)
            or company_name != lead.standard_company_name
        ):
            return None
        return customer_id

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
        """按 CRM CreateLeadsCommand 计算首次创建的严格必填字段。"""
        # 远端 DTO 的客户行业也是非空字段；缺失时在本地阻断，避免创建后才进入人工失败。
        return tuple(name for name in _CRM_REQUIRED_FIELDS if not fields.get(name))

    @staticmethod
    def _is_today_owned_candidate(lead: Lead, sales_user_id: str) -> bool:
        """判定 Lead 是否由提交销售负责且首次采集日期为上海时区当天。"""
        shanghai = ZoneInfo("Asia/Shanghai")
        return CrmSubmissionService._is_capture_date_owned_candidate(
            lead, (sales_user_id,), datetime.now(shanghai).date()
        )

    @staticmethod
    def _is_capture_date_owned_candidate(
        lead: Lead,
        owner_user_ids: Collection[str],
        capture_date: date,
        source_received_at: datetime | None = None,
        *,
        table_fields: Mapping[str, object] | None = None,
        prefer_table_date: bool = False,
    ) -> bool:
        """判断负责人线索是否属于指定上海自然日。

        参数：lead 为待筛选线索；owner_user_ids 为允许的表格负责人；capture_date 为目标日期；
        source_received_at 为原始消息接收时间；table_fields 为当前表格快照；prefer_table_date
        为 True 时优先使用表格录入时间，否则优先使用消息接收时间。
        返回值：负责人和所选日期口径均匹配时返回 True。
        异常：无；不支持的表格日期格式按缺失处理。
        副作用：无，仅解析内存中的时间值。
        """
        fields = table_fields if table_fields is not None else lead.field_values
        table_date = CrmSubmissionService._parse_capture_date(
            fields.get("录入时间")
            or fields.get("创建时间")
            or lead.field_values.get("录入时间")
            or lead.field_values.get("创建时间")
        )
        captured_date = table_date if prefer_table_date else None
        if captured_date is None and source_received_at is not None:
            captured_date = CrmSubmissionService._shanghai_date(source_received_at)
        if captured_date is None:
            captured_date = table_date
        if captured_date is None:
            captured_date = CrmSubmissionService._shanghai_date(lead.created_at)
        return (
            lead.smart_table_owner_user_id in owner_user_ids
            and captured_date == capture_date
        )

    @staticmethod
    def _shanghai_date(value: datetime) -> date:
        """将时间戳转换为上海自然日。

        参数：value 为企微来源消息或 Lead 时间戳；无时区值按数据库 UTC 语义解释。
        返回值：Asia/Shanghai 下的日历日期。
        异常：时区转换错误由调用方处理。
        副作用：无，只转换内存中的时间值。
        """
        normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.astimezone(ZoneInfo("Asia/Shanghai")).date()

    @staticmethod
    def _parse_capture_date(value: object) -> date | None:
        """解析智能表格的录入时间值，供历史补链线索恢复原始日期。

        参数：value 为 Lead.field_values 中的日期、时间或文本值。
        返回值：可解析时返回上海自然日；缺失或格式无效时返回 None 以回退创建时间。
        异常：无；不支持的格式被视为缺失。
        副作用：无，只读取并转换内存中的字段值。
        """
        if isinstance(value, datetime):
            return CrmSubmissionService._shanghai_date(value)
        if isinstance(value, date):
            return value
        if not isinstance(value, str) or not value.strip():
            return None
        normalized = value.strip()
        # 先按完整 ISO 时间解析；日期-only 字符串同样由 fromisoformat 接受。
        try:
            parsed_datetime = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            try:
                return date.fromisoformat(normalized[:10])
            except ValueError:
                return None
        if parsed_datetime.tzinfo is None:
            # 智能表格系统录入时间是不带时区的上海本地时间，不能当作 UTC 再偏移八小时。
            return parsed_datetime.date()
        return CrmSubmissionService._shanghai_date(parsed_datetime)

    def _is_unsubmitted_smart_table_record(
        self, lead: Lead, *, snapshot: Mapping[str, SmartTableRecord] | None = None
    ) -> bool:
        """读取智能表格当前提交状态，确认批量提交只处理“未提交”记录。

        参数：lead 为后台线索；snapshot 为本次批量操作已读取的表格快照，可避免重复 CLI 调用。
        返回值：记录存在且当前状态为未提交时返回 True。
        异常：表格读取失败时由适配器抛出。
        副作用：snapshot 缺失时读取一次远端记录，不修改数据。
        """
        if lead.smart_table_record_id is None:
            return False
        record = (
            snapshot.get(lead.smart_table_record_id)
            if snapshot is not None
            else self._read_smart_table_record(lead.smart_table_record_id)
        )
        return (
            record is not None
            and record.fields.get("负责人") == lead.smart_table_owner_user_id
            and record.fields.get("提交状态") == "未提交"
        )

    @staticmethod
    def _is_create_candidate(
        lead: Lead,
        sales_user_id: str,
        allow_non_today_target: bool,
        *,
        allow_temporary: bool = False,
        owner_scope_user_ids: tuple[str, ...] | None = None,
    ) -> bool:
        """判定目标线索是否满足当前提交命令的负责人范围和生命周期。

        参数：lead 为当前数据库线索；sales_user_id 为操作人企微标识；
        allow_non_today_target 表示是否允许提交非今日线索；allow_temporary 表示是否允许临时草稿；
        owner_scope_user_ids 为授权提交时由服务端冻结的负责人范围，None 表示仅本人。
        返回值：负责人、线索状态和采集日期均符合本次命令时返回 True。
        异常：无。
        副作用：无，仅读取线索字段并在需要时检查今日采集资格。
        """

        # 只有带服务端冻结范围的授权命令才能跳过“本人负责”约束。
        owner_matches = (
            lead.smart_table_owner_user_id in owner_scope_user_ids
            if owner_scope_user_ids is not None
            else lead.smart_table_owner_user_id == sales_user_id
        )
        if (
            not owner_matches
            or (
                lead.lifecycle_state != "pending_create"
                and not (allow_temporary and lead.lifecycle_state == "temporary")
            )
        ):
            return False
        return allow_non_today_target or CrmSubmissionService._is_today_owned_candidate(
            lead, sales_user_id
        )

    @staticmethod
    def _can_submit_other_sales_leads(authorization: SalesAuthorization) -> bool:
        """判断当前 actor 是否持有跨员工 CRM 提交授权。

        参数：authorization 为数据库中的当前企微员工授权记录。
        返回值：持有专用跨员工提交权限时返回 True。
        异常：无。
        副作用：无，仅读取授权状态。
        """
        return authorization.can_submit_other_sales_leads

    @staticmethod
    def _last_successful_sync(session: Session, lead_id: str) -> CrmSyncRecord | None:
        """读取一条线索最后成功的 CRM 快照，作为 update 差异基线。"""
        return session.scalar(
            select(CrmSyncRecord)
            .where(CrmSyncRecord.lead_id == lead_id, CrmSyncRecord.status == "succeeded")
            .order_by(CrmSyncRecord.completed_at.desc(), CrmSyncRecord.id.desc())
        )

    def _audit(
        self,
        command: SubmissionCommand,
        event_type: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """为未创建 CRM 同步记录的普通校验结果保存审计反馈。"""
        with self._session_factory.begin() as session:
            self._record_audit(session, command, event_type, details=details)

    @staticmethod
    def _crm_failure_evidence(error: BaseException) -> CRMFailureEvidence:
        """提取 CRM 失败的受控可观测证据，不保存异常正文。

        参数：error 为 CRM 适配器异常。
        返回值：失败分类、HTTP 状态、安全协议码及可选对象类别组成的审计字段。
        异常：无；未知字段会被忽略。
        副作用：无，不读取或记录原始响应内容。
        """
        details: CRMFailureEvidence = {
            "failure_category": classify_task_failure(error).value,
            "adapter_category": None,
            "http_status": None,
            "failure_code": None,
        }
        adapter_category = getattr(error, "category", None)
        if isinstance(adapter_category, str) and adapter_category in {
            "transport",
            "authentication",
            "business",
            "business_rejection",
            "duplicate_target_unavailable",
            "gateway",
            "malformed_response",
        }:
            details["adapter_category"] = adapter_category
        duplicate_entity_type = getattr(error, "duplicate_entity_type", None)
        if duplicate_entity_type in {"lead", "customer", "dealer", "unknown"}:
            details["duplicate_entity_type"] = duplicate_entity_type
        http_status = getattr(error, "http_status", None)
        if isinstance(http_status, int):
            details["http_status"] = http_status
        protocol_code = getattr(error, "sub_code", None) or getattr(error, "error_code", None)
        if isinstance(protocol_code, (str, int)):
            normalized = str(protocol_code).strip()
            if normalized and len(normalized) <= 64 and all(
                character.isalnum() or character in "._:-" for character in normalized
            ):
                details["failure_code"] = normalized
        return details

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
        *,
        notify_retry_result: bool = False,
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
            # 仅持久化 SOP 非敏感协议码，便于人工定位业务拒绝，不保存响应正文。
            failure_evidence = self._crm_failure_evidence(error)
            sync.failure_kind = failure_evidence.get("adapter_category") or type(error).__name__
            protocol_code = failure_evidence.get("failure_code")
            if isinstance(protocol_code, str):
                sync.failure_code = protocol_code
            sync.failure_summary = safe_failure_summary(error)
            sync.failed_at = utc_now()
            sync.next_retry_at = None
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            if failure_category.value == "unknown" or (
                sync.operation == "update" and (
                    sync.failure_kind == "malformed_response"
                    or (getattr(error, "http_status", None) or 0) >= 500
                )
            ):
                # 未知异常可能发生在远端已提交而响应丢失之后，不能把它当作确定失败结算 discard。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
            else:
                sync.response_summary = f"CRM {sync.operation} failed"
                self._fail_company_identity(session, sync, sales_user_id)
                self._finalize_pending_discard(session, sync)
                self._record_audit_for_sync(
                    session,
                    sync,
                    sales_user_id,
                    f"crm_{sync.operation}_failed",
                    details=failure_evidence,
                )
            if notify_retry_result:
                self._enqueue_crm_retry_result_notification(session, sync)
        return "failed_pending_review"

    def _record_crm_transport_failure(
        self,
        sync_id: int,
        sales_user_id: str,
        error: BaseException,
        claim_started_at: datetime,
        claim_attempts: int,
        *,
        notify_retry_result: bool = False,
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
            sync.failure_category = classify_task_failure(error).value
            failure_evidence = self._crm_failure_evidence(error)
            sync.failure_kind = failure_evidence.get("adapter_category") or type(error).__name__
            protocol_code = failure_evidence.get("failure_code")
            if isinstance(protocol_code, str):
                sync.failure_code = protocol_code
            sync.failure_summary = safe_failure_summary(error)
            sync.processing_started_at = None
            sync.processing_lease_expires_at = None
            http_status = failure_evidence.get("http_status")
            if isinstance(http_status, int):
                protocol_summary = (
                    f" code {protocol_code}" if isinstance(protocol_code, str) else ""
                )
                sync.response_summary = f"CRM gateway HTTP {http_status}{protocol_summary}"
            elif isinstance(protocol_code, str):
                sync.response_summary = f"CRM gateway code {protocol_code}"
            else:
                sync.response_summary = "CRM transport error"
            if sync.operation == "update":
                # 即使异常被归为 transient，远端也可能已覆盖；只允许核对，禁止重复写。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
                return "failed_pending_review"
            if notify_retry_result and sync.attempts >= self._crm_create_retry_count:
                # 最后一次传输失败的远端写入结果未知，立即转人工核对，不再显示虚假的“重试中”。
                self._mark_unknown_crm_outcome(session, sync, sales_user_id)
                if notify_retry_result:
                    self._enqueue_crm_retry_result_notification(session, sync)
                return "failed_pending_review"
            retry_delay = self._crm_create_retry_delay_seconds * 2 ** (sync.attempts - 1)
            sync.status = "retrying"
            sync.next_retry_at = utc_now() + timedelta(seconds=retry_delay)
            _LOGGER.warning(
                "crm_submission_retry_scheduled",
                extra={
                    "crm_sync_record_id": sync.id,
                    "attempt": sync.attempts,
                    "retry_at": sync.next_retry_at.isoformat(),
                    "failure_kind": sync.failure_kind,
                    "failure_code": sync.failure_code,
                    "http_status": http_status,
                },
            )
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
        sync.next_retry_at = None
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

    @staticmethod
    def _enqueue_crm_retry_result_notification(session: Session, sync: CrmSyncRecord) -> None:
        """将自动重试的最终状态登记到现有企微通知 Outbox。

        参数：session 为当前持久化事务；sync 为已完成或需人工核对的 CRM 操作。
        返回值：无。
        异常：数据库约束错误由事务层传播。
        副作用：按 sync ID 幂等创建一条发给原提交人的通知，不直接调用企微。
        """
        key = hashlib.sha256(f"crm_retry_result:{sync.id}".encode()).hexdigest()
        if session.get(NotificationRecord, key) is not None:
            return
        raw_name = sync.canonical_payload.get("name")
        lead_name = safe_audit_text(
            raw_name if isinstance(raw_name, str) else "该线索", max_length=120
        )
        if sync.status == "succeeded":
            result = (
                "CRM 已存在并跟进"
                if "FOLLOW_UP" in (sync.response_summary or "")
                else "CRM 提交成功"
            )
            content = f"CRM 自动重试完成：{lead_name}，{result}。"
        elif sync.failure_category == "unknown":
            content = (
                f"CRM 自动重试已结束：{lead_name} 的提交结果仍无法确认，"
                "请人工核对 CRM，避免重复提交。"
            )
        else:
            content = f"CRM 自动重试失败：{lead_name}，需要人工处理。"
        session.add(
            NotificationRecord(
                notification_key=key,
                sales_user_id=sync.submitting_sales_user_id,
                source_message_id=sync.request_message_id,
                notification_type="crm_submission_retry_summary",
                content=content,
            )
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
        details: Mapping[str, object] | None = None,
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
