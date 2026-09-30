"""确定性 CRM 首次创建命令、最终快照与逻辑重试服务。"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import NotRequired, TypedDict
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.companies.models import CompanyVerificationStatus
from app.core.config import get_settings
from app.core.failures import classify_task_failure, safe_failure_summary
from app.crm.adapter import CRMAdapter
from app.crm.employee_directory import EmployeeDirectory, EmployeeDirectoryError
from app.crm.payload import CrmPayloadBuilder, CrmPayloadError
from app.leads.models import (
    CrmCompanyIdentity,
    CrmSyncRecord,
    Lead,
    LeadDiscardRequest,
    latest_crm_create_sync,
)
from app.leads.review import LeadReviewService
from app.messaging.models import BusinessAuditEvent, SalesAuthorization, utc_now
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.models import SmartTableRecord

_TODAY_COMMAND = "提交今天的线索"
_ALL_COMMAND = "提交我所有线索"
_ABANDONED_COMMAND = "帮我提交放弃提交的线索"
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


class SubmissionItemStatus(StrEnum):
    """定义单条 CRM 提交结果允许对外展示的状态集合。"""

    CREATED = "created"
    UPDATED = "updated"
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
    display_text: str | None = None
    snapshot_fields: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True)
class DuplicateConfirmationResult:
    """描述重复线索卡片一次继续或停止操作的结果。"""

    submitted: int = 0
    abandoned: int = 0
    remaining: int = 0
    failed: int = 0


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
    """描述可同时用于查重审计和逐条结果的受控 CRM 失败字段。"""

    failure_category: str
    adapter_category: str | None
    http_status: int | None
    failure_code: str | None
    duplicate_entity_type: NotRequired[str]


@dataclass(frozen=True)
class GlobalIdentityResolution:
    """描述全局 CRM 公司身份注册表的一次确定性解析结果。"""

    state: str
    identity: CrmCompanyIdentity | None = None


_CREATE_REASON_CODES = {
    "incomplete": "missing_required_fields",
    "duplicate_confirmation": "duplicate_confirmation_required",
    "mapping_missing": "crm_user_mapping_missing",
    "processing": "sync_processing",
    "retrying": "crm_create_retrying",
    "failed_pending_review": "crm_create_failed_pending_review",
    "not_submitted": "candidate_state_changed",
}
_ITEM_STATUS_BY_OUTCOME = {
    "succeeded": SubmissionItemStatus.CREATED,
    "updated": SubmissionItemStatus.UPDATED,
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
        "unchanged": SubmissionItemStatus.UNCHANGED,
        "incomplete": SubmissionItemStatus.INCOMPLETE,
    }.get(status, _ITEM_STATUS_BY_OUTCOME.get(status, SubmissionItemStatus.FAILED_PENDING_REVIEW))
    reason_code = {
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
        incomplete=result.incomplete + (status == "incomplete"),
        processing=result.processing + (status == "processing"),
        failed_pending_review=result.failed_pending_review + (status == "failed_pending_review"),
        updated=result.updated + (status == "succeeded"),
        unchanged=result.unchanged + (status == "unchanged"),
        company_identity_review=result.company_identity_review
        + (status == "company_identity_review"),
        mapping_missing=result.mapping_missing + (status == "mapping_missing"),
        items=(*result.items, item),
    )


class CrmSubmissionService:
    """执行冻结 CRM create/update，并在首次提交时调用 CRM 查重接口。"""

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

        参数：前三项分别提供持久化、规范表格回读和唯一 create 调用；可选参数覆盖重试、
        owner 目录与卡片确认能力配置。
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
        self._employee_directory = employee_directory
        # 当前销售只能提交本人负责且已从智能表格读取到的线索，CRM headerId 由负责人目录解析。
        self._crm_payload_builder = CrmPayloadBuilder(get_settings().crm_customer_level_scheme)
        self._robot_submission_confirmation_available = (
            get_settings().wecom_card_callback_ready()
            if robot_submission_confirmation_available is None
            else robot_submission_confirmation_available
        )

    def _resolve_crm_owner(self, lead: Lead) -> str | None:
        """在 CRM 调用前把企微负责人 ID 转为姓名并解析唯一 employee.id。

        智能表格成员字段继续以 userId 作为权限和归属事实；只有企业微信响应附带的
        userName 才用于 EmployeeDirectory 姓名/花名解析。缺少可读姓名、姓名不唯一或
        记录归属不一致时返回 None，调用方必须 fail closed，不调用 CRM。
        """
        directory = self._employee_directory
        if directory is None:
            directory = EmployeeDirectory(get_settings().employee_directory_path)
            self._employee_directory = directory
        owner_name: str | None = None
        if lead.smart_table_record_id:
            # 重新读取当前审核快照，避免使用 Lead 创建时保存的过期成员显示值。
            record = self._smart_table_adapter.get_record(lead.smart_table_record_id)
            if record is not None:
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

    def submit(self, command: SubmissionCommand) -> SubmissionBatchResult:
        """解析固定命令并逐条提交本人待创建 Lead，保持批次部分成功。

        参数：command 为机器人已可靠接收的文本命令和提交销售身份。
        返回值：成功、待完善和外部失败的汇总；更新命令明确标记未实现。
        异常：未知命令或未授权销售抛出 ValueError，避免 LLM 推断权限。
        副作用：可能创建或重试一个冻结的 CRM Sync Record，并调用 CRM create。
        """
        if command.text == _UPDATES_COMMAND:
            return self._submit_updates(command)
        if command.text in {_TODAY_COMMAND, _ALL_COMMAND, _ABANDONED_COMMAND}:
            # 批量命令只能发行候选确认动作，不能由服务层直接触发 CRM。
            raise ValueError("批量提交必须通过服务端候选确认动作")
        if command.text != "提交指定线索":
            raise ValueError("不支持的 CRM 提交命令")
        if command.target_lead_id is not None and command.text != "提交指定线索":
            raise ValueError("目标线索只能用于指定线索提交命令")

        target_missing = False
        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
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
            return SubmissionBatchResult(
                incomplete=1,
                incomplete_lead_ids=(command.target_lead_id or "",),
            )
        result = SubmissionBatchResult()
        for lead_id in candidate_ids:
            # 每条独立执行；任何一条失败都不得影响后续候选。
            outcome = self._submit_create(lead_id, command)
            result = _append_create_result(result, lead_id, outcome)
        return result

    def list_submission_candidates(
        self, command_text: str, sales_user_id: str
    ) -> tuple[SubmissionCandidate, ...]:
        """列出当前销售可在机器人卡片中选择的线索，不调用 CRM。

        参数：command_text 为精确命令；sales_user_id 为当前销售身份。
        返回值：只包含本人、日期与实时表格状态符合所选卡片范围的线索候选。
        异常：销售未授权或命令不支持时抛出 ValueError。
        副作用：只读取数据库及智能表格，不改变 Lead 生命周期。
        """
        if command_text not in {_TODAY_COMMAND, _ALL_COMMAND, _ABANDONED_COMMAND}:
            raise ValueError("不支持候选卡片的 CRM 命令")
        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
            # 授权通过后，一次卡片候选计算只读取一份当前表格快照。
            smart_table_snapshot = {
                record.record_id: record for record in self._smart_table_adapter.get_records()
            }
            leads = list(
                session.scalars(
                    select(Lead).where(
                        Lead.smart_table_owner_user_id == sales_user_id,
                    )
                )
            )
            candidates: list[SubmissionCandidate] = []
            for lead in leads:
                sync = latest_crm_create_sync(session, lead.id)
                if command_text == _ABANDONED_COMMAND:
                    if (
                        lead.lifecycle_state != "pending_create"
                        or sync is None
                        or sync.status != "abandoned"
                    ):
                        continue
                elif command_text == _TODAY_COMMAND:
                    # 今日候选可包括后台 temporary，但只以当前表格完整快照确认其提交资格。
                    if (
                        lead.lifecycle_state not in {"pending_create", "temporary"}
                        or not self._is_today_owned_candidate(lead, sales_user_id)
                    ):
                        continue
                    if sync is not None and sync.status in {"succeeded", "abandoned"}:
                        continue
                    if not self._is_unsubmitted_smart_table_record(
                        lead, snapshot=smart_table_snapshot
                    ):
                        continue
                    if lead.lifecycle_state == "temporary":
                        record = smart_table_snapshot.get(lead.smart_table_record_id or "")
                        if record is None or self._missing_crm_minimum(dict(record.fields)):
                            continue
                else:
                    # 只有待创建和仍在补全的草稿能进入卡片；已同步、废弃或失败终态保持冻结。
                    if lead.lifecycle_state not in {"pending_create", "temporary"}:
                        continue
                    if sync is not None and sync.status in {"succeeded", "abandoned"}:
                        continue
                    if not self._is_unsubmitted_smart_table_record(
                        lead, snapshot=smart_table_snapshot
                    ):
                        continue
                # 候选文案优先来自当前 Smart Table 快照，避免后台旧字段导致同名项无法区分。
                record = smart_table_snapshot.get(lead.smart_table_record_id or "")
                snapshot_fields = record.fields if record is not None else lead.field_values
                company_name = snapshot_fields.get("线索名称") or lead.standard_company_name
                if isinstance(company_name, str) and company_name.strip():
                    contact_name = snapshot_fields.get("联系人")
                    contact = contact_name.strip() if isinstance(contact_name, str) else ""
                    created = lead.created_at
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=UTC)
                    created_text = created.astimezone(ZoneInfo("Asia/Shanghai")).strftime(
                        "%Y-%m-%d"
                    )
                    # 展示文本来自当前后台快照，不参与身份判断；回调仍只接受冻结 lead_id。
                    display_text = "｜".join(
                        part for part in (company_name.strip(), contact, created_text) if part
                    )
                    # 候选 Markdown 必须使用发行时的同一份 Smart Table 快照，避免卡片与明细错位。
                    display_snapshot = dict(snapshot_fields)
                    if record is not None:
                        for member_field, display_name in record.member_names.items():
                            if isinstance(display_name, str) and display_name.strip():
                                display_snapshot[member_field] = display_name.strip()
                    candidates.append(
                        SubmissionCandidate(
                            lead.id,
                            company_name.strip(),
                            display_text,
                            tuple(display_snapshot.items()),
                        )
                    )
            return tuple(candidates)

    def submit_selected(
        self,
        command: SubmissionCommand,
        selected_lead_ids: tuple[str, ...],
    ) -> SubmissionBatchResult:
        """提交卡片勾选的线索，并在服务端重新校验候选归属和状态。

        参数：command 为原始精确命令；selected_lead_ids 为 callback 中的服务端候选标识。
        返回值：与普通批量提交相同的部分成功汇总。
        异常：销售未授权或命令不支持时抛出 ValueError。
        副作用：每个 callback 已接受的目标均得到结果；仅仍有效的目标调用首次提交流程。
        """
        if command.text not in {_TODAY_COMMAND, _ALL_COMMAND, _ABANDONED_COMMAND}:
            raise ValueError("不支持卡片选择提交")
        selected = tuple(dict.fromkeys(selected_lead_ids))
        with self._session_factory() as session:
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
            # 回调重新校验身份后只读取一份快照，避免多次启动 CLI 导致偶发协议/进程错误。
            smart_table_snapshot = {
                record.record_id: record for record in self._smart_table_adapter.get_records()
            }
            valid_ids: set[str] = set()
            for lead_id in selected:
                lead = session.get(Lead, lead_id)
                if lead is None:
                    continue
                sync = latest_crm_create_sync(session, lead_id)
                if command.text == _ABANDONED_COMMAND:
                    if (
                        lead.lifecycle_state == "pending_create"
                        and sync is not None
                        and sync.status == "abandoned"
                    ):
                        valid_ids.add(lead_id)
                elif command.text == _TODAY_COMMAND:
                    # frozen candidate 的 temporary 状态必须靠当前完整表格快照重新确认。
                    if (
                        lead.smart_table_owner_user_id == command.sales_user_id
                        and lead.lifecycle_state in {"pending_create", "temporary"}
                        and self._is_today_owned_candidate(lead, command.sales_user_id)
                        and (sync is None or sync.status not in {"succeeded", "abandoned"})
                        and self._is_unsubmitted_smart_table_record(
                            lead, snapshot=smart_table_snapshot
                        )
                        and (
                            lead.lifecycle_state != "temporary"
                            or not self._missing_crm_minimum(
                                dict(
                                    smart_table_snapshot[
                                        lead.smart_table_record_id or ""
                                    ].fields
                                )
                            )
                        )
                    ):
                        valid_ids.add(lead_id)
                elif (
                    command.text == _ALL_COMMAND
                    and lead.smart_table_owner_user_id == command.sales_user_id
                    and lead.lifecycle_state in {"pending_create", "temporary"}
                    and (sync is None or sync.status not in {"succeeded", "abandoned"})
                    and self._is_unsubmitted_smart_table_record(
                        lead, snapshot=smart_table_snapshot
                    )
                ):
                    valid_ids.add(lead_id)
        result = SubmissionBatchResult()
        for lead_id in selected:
            if lead_id not in valid_ids:
                outcome = _create_outcome("not_submitted")
            else:
                outcome = self._submit_create(
                    lead_id,
                    SubmissionCommand(
                        command.text,
                        command.sales_user_id,
                        command.request_message_id,
                    ),
                )
            result = _append_create_result(result, lead_id, outcome)
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
            result = _append_update_result(result, lead_id, outcome)
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
            self._session_factory,
            self._smart_table_adapter,
            robot_submission_confirmation_available=self._robot_submission_confirmation_available,
        ).reconcile_submission(lead_id)
        try:
            payload = self._canonical_payload(
                reconciled.fields,
                tyc_customer_id=self._reliable_tyc_customer_id(lead, reconciled.fields),
            )
        except CrmPayloadError:
            # 字典、日期或备注格式不合法时只阻止当前线索，不让批次或其他线索被异常打断。
            self._audit(command, "crm_update_payload_invalid")
            return "incomplete"
        previous = dict(latest.canonical_payload)
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
            authorization = session.get(SalesAuthorization, command.sales_user_id)
            if lead is None or authorization is None:
                return "incomplete"
            crm_user_id = self._resolve_crm_owner(lead)
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

    def _submit_create(self, lead_id: str, command: SubmissionCommand) -> CreateSubmissionOutcome:
        """冻结首次提交快照，先查 CRM 再决定创建或等待重复确认。

        参数：lead_id 为当前销售待提交线索；command 为固定提交命令事实。
        返回值：单条提交结果，以及 CRM 查重命中时的重复线索事实。
        异常：智能表格、数据库或 CRM 查重错误按既有同步状态转换并记录审计。
        副作用：读取最终表格快照，写入冻结 CRM 同步记录，必要时调用 CRM 查重或创建接口。
        """
        allow_non_today_target = command.text in {
            _ALL_COMMAND,
            _ABANDONED_COMMAND,
        } or command.target_lead_id is not None
        # 单条确认卡已冻结唯一 Lead 目标；它可提交 temporary 草稿，但仍须通过后续字段校验。
        allow_temporary = command.text in {_ALL_COMMAND, _TODAY_COMMAND} or (
            command.text == "提交指定线索" and command.target_lead_id == lead_id
        )
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None or not self._is_create_candidate(
                lead,
                command.sales_user_id,
                allow_non_today_target,
                allow_temporary=allow_temporary,
            ):
                return _create_outcome("not_submitted")
            # temporary 草稿可以展示在“所有未提交”卡片中；后续统一按八项必填字段判定，
            # 字段完整的记录允许继续查重，字段不完整的记录返回具体缺失项且不调用 CRM。
            existing = latest_crm_create_sync(session, lead_id)
            if existing is not None:
                if existing.status == "succeeded":
                    return _create_outcome("not_submitted")
                if existing.status == "awaiting_duplicate_confirmation":
                    duplicate = self._duplicate_from_sync(lead, existing)
                    return _create_outcome(
                        (
                            "duplicate_confirmation"
                            if duplicate is not None
                            else "failed_pending_review"
                        ),
                        duplicate=duplicate,
                    )
                if existing.status == "abandoned" and command.text == _ABANDONED_COMMAND:
                    # 只有专用“重新提交放弃线索”命令允许重开服务端放弃事实。
                    pass
                else:
                    # retry 必须使用现有冻结快照，绝不重新读表替换 payload。
                    return _create_outcome(
                        self._claim_and_call(existing.id, command.sales_user_id)
                    )

        reconciled = LeadReviewService(
            self._session_factory,
            self._smart_table_adapter,
            robot_submission_confirmation_available=self._robot_submission_confirmation_available,
        ).reconcile_submission(lead_id)
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
            )
        except CrmPayloadError:
            # CRM DTO 校验失败属于当前线索待完善，禁止进入远端写操作。
            self._audit(command, "crm_create_payload_invalid")
            return _create_outcome("incomplete")
        company_name = canonical_payload.get("name")
        if not isinstance(company_name, str) or not company_name:
            return _create_outcome("incomplete")
        snapshot_hash = self._snapshot_hash(canonical_payload)
        # CRM 用户映射缺失时连查重接口也不能调用，先完成本地确定性校验。
        with self._session_factory.begin() as session:
            current_lead = session.scalar(
                select(Lead).where(Lead.id == lead_id).with_for_update()
            )
            authorization = session.scalar(
                select(SalesAuthorization)
                .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
                .with_for_update()
            )
            if (
                current_lead is None
                or authorization is None
                or not self._is_create_candidate(
                    current_lead,
                    command.sales_user_id,
                    allow_non_today_target,
                    allow_temporary=allow_temporary,
                )
            ):
                return _create_outcome("not_submitted")
            if current_lead.lifecycle_state == "temporary":
                # 临时线索通过最终快照和 CRM payload 校验后，先晋升再进入 owner/查重状态机。
                current_lead.lifecycle_state = "pending_create"
            if self._resolve_crm_owner(current_lead) is None:
                return _create_outcome(
                    self._record_mapping_missing(
                        session, current_lead, command, canonical_payload, snapshot_hash, "create"
                    )
                )
            identity = session.scalar(
                select(CrmCompanyIdentity)
                .where(CrmCompanyIdentity.standard_company_name == company_name)
                .with_for_update()
            )
            if identity is not None and identity.state == "active":
                if identity.crm_lead_id is None:
                    return _create_outcome(
                        "failed_pending_review", reason_code="company_identity_conflict"
                    )
            elif identity is not None and identity.state == "reserving":
                if identity.creating_lead_id != current_lead.id:
                    return _create_outcome(
                        "processing", reason_code="company_identity_reserved"
                    )
            elif identity is None:
                identity = self._reserve_global_identity(session, current_lead, command)
                if identity is None:
                    return _create_outcome(
                        "processing", reason_code="company_identity_reserved"
                    )
            else:
                # 历史身份仍需本次 CRM 查重决定，不能直接当作 create 事实。
                pass
        try:
            # CRM 查重是唯一的首次提交去重边界，智能表格阶段不读取同名线索。
            duplicate_results = tuple(self._crm_adapter.search_by_company_name(canonical_payload))
        except Exception as error:
            failure_category = classify_task_failure(error)
            failure_evidence = self._crm_failure_evidence(error)
            failure_details = {"lead_id": lead_id, **failure_evidence}
            # BusinessAuditEvent 的唯一键是 message + event_type；按 Lead 哈希扩展类型，
            # 让同一批次的失败证据可分别查询且仍能幂等重放。
            event_identity = hashlib.sha256(lead_id.encode()).hexdigest()[:32]
            self._audit(
                command,
                f"crm_duplicate_search_failed:{event_identity}",
                details=failure_details,
            )
            _LOGGER.warning(
                "crm_duplicate_search_failed",
                extra={
                    "lead_id": lead_id,
                    "error_type": type(error).__name__,
                    **failure_evidence,
                },
            )
            # 查重尚未创建 Sync 记录；暂态故障返回 retrying，下一次命令会重新执行查重。
            return _create_outcome(
                "retrying" if failure_category.value == "transient" else "failed_pending_review",
                reason_code=(
                    "duplicate_target_unavailable"
                    if getattr(error, "category", None) == "duplicate_target_unavailable"
                    else (
                        "crm_duplicate_search_retrying"
                        if failure_category.value == "transient"
                        else "crm_duplicate_search_failed"
                    )
                ),
                **failure_evidence,
            )

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
                    or not self._is_create_candidate(
                        lead,
                        command.sales_user_id,
                        allow_non_today_target,
                        allow_temporary=allow_temporary,
                    )
                ):
                    return _create_outcome("incomplete")
                crm_user_id = self._resolve_crm_owner(lead)
                if crm_user_id is None:
                    return _create_outcome(
                        self._record_mapping_missing(
                            session, lead, command, canonical_payload, snapshot_hash, "create"
                        )
                    )
                identity = session.scalar(
                    select(CrmCompanyIdentity)
                    .where(CrmCompanyIdentity.standard_company_name == company_name)
                    .with_for_update()
                )
                if identity is None:
                    # 最终表格回读可能标准化名称，按当前 Lead 取回 reservation。
                    identity = session.scalar(
                        select(CrmCompanyIdentity)
                        .where(
                            CrmCompanyIdentity.creating_lead_id == lead.id,
                            CrmCompanyIdentity.state == "reserving",
                        )
                        .with_for_update()
                    )
                if identity is not None and identity.state == "active":
                    expected_crm_lead_id = identity.crm_lead_id
                    matching_duplicate = (
                        len(duplicate_results) == 1
                        and expected_crm_lead_id is not None
                        and duplicate_results[0].crm_lead_id == expected_crm_lead_id
                    )
                    if not matching_duplicate:
                        # active identity 只能校验 CRM 查重事实，不能替代本次查重决策。
                        self._record_audit(
                            session,
                            command,
                            "crm_global_identity_conflict",
                            details={
                                "lead_id": lead.id,
                                "record_id": lead.smart_table_record_id or "",
                                "expected_crm_lead_id_hash": hashlib.sha256(
                                    (expected_crm_lead_id or "").encode()
                                ).hexdigest(),
                                "duplicate_count": str(len(duplicate_results)),
                            },
                        )
                        return _create_outcome(
                            "failed_pending_review", reason_code="company_identity_conflict"
                        )
                if (
                    identity is not None
                    and identity.state == "reserving"
                    and identity.creating_lead_id != lead.id
                ):
                    # 首创 reservation 已由另一条 Lead 持有；并发调用不得各自创建 CRM。
                    return _create_outcome(
                        "processing", reason_code="company_identity_reserved"
                    )
                existing = latest_crm_create_sync(session, lead_id, for_update=True)
                if existing is not None:
                    if existing.status == "succeeded":
                        return _create_outcome("incomplete")
                    if existing.status == "awaiting_duplicate_confirmation":
                        duplicate = self._duplicate_from_sync(lead, existing)
                        return _create_outcome(
                            "duplicate_confirmation"
                            if duplicate is not None
                            else "failed_pending_review",
                            duplicate=duplicate,
                        )
                    if existing.status != "abandoned" or command.text != _ABANDONED_COMMAND:
                        return _create_outcome(existing.status)
                    # abandoned 是不可修改的冻结事实；专用重提只能追加下一 generation。
                    previous_generation = existing.generation or 1
                    generation = previous_generation + 1
                    supersedes_sync_record_id = existing.id
                else:
                    if command.text == _ABANDONED_COMMAND:
                        # 卡片发行后若历史候选已消失，禁止无旧事实地凭命令创建。
                        return _create_outcome("incomplete")
                    generation = 1
                    supersedes_sync_record_id = None

                first_duplicate = duplicate_results[0] if duplicate_results else None
                idempotency_key = (
                    f"crm:create:{lead.id}"
                    if generation == 1
                    else f"crm:create:{lead.id}:g{generation}"
                )
                sync = CrmSyncRecord(
                    lead_id=lead.id,
                    operation="create",
                    generation=generation,
                    supersedes_sync_record_id=supersedes_sync_record_id,
                    smart_table_record_id=lead.smart_table_record_id or "",
                    idempotency_key=idempotency_key,
                    canonical_payload=canonical_payload,
                    snapshot_hash=snapshot_hash,
                    request_message_id=command.request_message_id,
                    submitting_sales_user_id=command.sales_user_id,
                    submitting_crm_user_id=crm_user_id,
                    crm_lead_id=first_duplicate.crm_lead_id if first_duplicate else None,
                    crm_lead_owner_user_id=(
                        first_duplicate.crm_lead_owner_user_id if first_duplicate else None
                    ),
                    status=(
                        "awaiting_duplicate_confirmation"
                        if first_duplicate is not None
                        else "pending"
                    ),
                    response_summary=(
                        first_duplicate.response_summary[:256] if first_duplicate else None
                    ),
                )
                session.add(sync)
                session.flush()
                if identity is not None and identity.state == "reserving":
                    # reservation 始终指向当前 generation；历史 abandoned 行保持不可变。
                    identity.creating_lead_id = lead.id
                    identity.creating_sync_record_id = sync.id
                sync_id = sync.id
                if first_duplicate is not None:
                    self._record_audit_for_sync(
                        session, sync, command.sales_user_id, "crm_duplicate_awaiting_confirmation"
                    )
                    duplicate = DuplicateSubmission(
                        lead_id=lead.id,
                        company_name=company_name,
                        crm_lead_id=first_duplicate.crm_lead_id,
                        crm_lead_owner_user_id=first_duplicate.crm_lead_owner_user_id,
                    )
                    return _create_outcome("duplicate_confirmation", duplicate=duplicate)
                sync_id = sync.id
        except IntegrityError:
            # 数据库唯一索引只保护同一 Lead 的 create 幂等，不再承担公司名称查重。
            return _create_outcome("processing", reason_code="company_identity_reserved")
        return _create_outcome(self._claim_and_call(sync_id, command.sales_user_id))

    @staticmethod
    def _duplicate_from_sync(lead: Lead, sync: CrmSyncRecord) -> DuplicateSubmission | None:
        """从已冻结的重复同步记录重建卡片所需的脱敏重复事实。

        参数：lead 为重复确认对应的本地线索；sync 为已冻结的 CRM 查重同步记录。
        返回值：可发行卡片的重复线索事实；冻结数据不完整时返回 None。
        异常：无。
        副作用：无，仅读取已持久化对象。
        """
        company_name = sync.canonical_payload.get("name")
        if not isinstance(company_name, str) or not sync.crm_lead_id:
            return None
        return DuplicateSubmission(
            lead_id=lead.id,
            company_name=company_name,
            crm_lead_id=sync.crm_lead_id,
            crm_lead_owner_user_id=sync.crm_lead_owner_user_id,
        )

    def resolve_duplicate_confirmation(
        self,
        request_message_id: str,
        sales_user_id: str,
        *,
        continue_submission: bool,
        selected_lead_ids: tuple[str, ...] = (),
    ) -> DuplicateConfirmationResult:
        """执行重复线索卡片的继续或停止决定，并只更新服务端冻结的目标。

        参数：request_message_id 和 sales_user_id 定位原提交请求及操作者；
        continue_submission 表示继续覆盖还是停止；selected_lead_ids 为销售勾选的本地线索。
        返回值：已覆盖、已放弃、未选择和失败数量汇总。
        异常：操作者未授权或目标不属于当前销售时抛出 ValueError。
        副作用：更新 CRM 同步状态、审计记录和智能表格提交状态，必要时调用 CRM update。
        """
        selected = set(selected_lead_ids)
        selected_sync_ids: list[int] = []
        abandoned_lead_ids: list[str] = []
        remaining = 0
        with self._session_factory.begin() as session:
            authorization = session.get(SalesAuthorization, sales_user_id)
            if (
                authorization is None
                or not authorization.is_authorized
                or not authorization.is_active
            ):
                raise ValueError("提交销售未授权")
            rows = session.execute(
                select(CrmSyncRecord, Lead)
                .join(Lead, Lead.id == CrmSyncRecord.lead_id)
                .where(
                    CrmSyncRecord.request_message_id == request_message_id,
                    CrmSyncRecord.operation == "create",
                    CrmSyncRecord.status == "awaiting_duplicate_confirmation",
                    Lead.smart_table_owner_user_id == sales_user_id,
                )
                .with_for_update()
            ).all()
            for sync, lead in rows:
                if not continue_submission:
                    sync.status = "abandoned"
                    sync.failure_category = "user_abandoned"
                    sync.response_summary = "用户停止提交重复线索"
                    sync.completed_at = utc_now()
                    abandoned_lead_ids.append(lead.id)
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
                self._record_audit_for_sync(
                    session, sync, sales_user_id, "crm_duplicate_update_confirmed"
                )

        if not continue_submission:
            for lead_id in abandoned_lead_ids:
                self._set_smart_table_submission_status(lead_id, "放弃提交")
            return DuplicateConfirmationResult(abandoned=len(abandoned_lead_ids))

        submitted = 0
        failed = 0
        for sync_id in selected_sync_ids:
            state = self._claim_and_call(sync_id, sales_user_id)
            if state == "succeeded":
                submitted += 1
            else:
                failed += 1
        return DuplicateConfirmationResult(
            submitted=submitted,
            remaining=remaining,
            failed=failed,
        )

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
            if classify_task_failure(error).value == "transient":
                return self._record_crm_transport_failure(
                    sync_id, sales_user_id, error, claim_started_at, claim_attempts
                )
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
            completed_lead_id = lead.id
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
        if completed_lead_id is not None:
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
        return "succeeded"

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

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """将数据库返回的处理租约时间统一解释为 UTC。"""
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    def _canonical_payload(
        self, fields: dict[str, object], *, tyc_customer_id: str | None = None
    ) -> dict[str, object]:
        """构造稳定的 CRM 英文键快照并排除智能表格审核元数据。

        参数：fields 为提交前回读的中文智能表格字段；tyc_customer_id 为天眼查客户标识。
        返回值：可直接冻结、哈希和发送给 CRM Adapter 的英文键 payload。
        异常：字段枚举、日期或备注不符合 CRM 契约时抛出 CrmPayloadError。
        副作用：无，不调用外部系统。
        """
        return self._crm_payload_builder.build(fields, tyc_customer_id=tyc_customer_id)

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
        created = lead.created_at if lead.created_at.tzinfo else lead.created_at.replace(tzinfo=UTC)
        return (
            lead.smart_table_owner_user_id == sales_user_id
            and created.astimezone(shanghai).date() == datetime.now(shanghai).date()
        )

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
            else self._smart_table_adapter.get_record(lead.smart_table_record_id)
        )
        return record is not None and record.fields.get("提交状态") == "未提交"

    @staticmethod
    def _is_create_candidate(
        lead: Lead,
        sales_user_id: str,
        allow_non_today_target: bool,
        *,
        allow_temporary: bool = False,
    ) -> bool:
        """判定批量或精确目标线索是否允许进入首次 CRM 提交。"""

        if (
            lead.smart_table_owner_user_id != sales_user_id
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
            protocol_code = failure_evidence.get("failure_code")
            if isinstance(protocol_code, str):
                sync.failure_code = protocol_code
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
            failure_evidence = self._crm_failure_evidence(error)
            protocol_code = failure_evidence.get("failure_code")
            if isinstance(protocol_code, str):
                sync.failure_code = protocol_code
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
