"""将 T02 文本 Outbox 事件写入销售个人智能表格审核工作区。"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.ai.gateway import AIGateway, AIGatewayError
from app.ai.models import ExtractedLeadPatch, LeadAnalysis, LeadFieldValue
from app.companies.models import CompanyRegionEvidence, CompanyUpsertCommand, CompanyUpsertResult
from app.companies.service import CompanyLeadService
from app.core.config import get_settings
from app.core.failures import RetryableTaskFailure, classify_task_failure, safe_failure_summary
from app.core.logging import bind_log_context, reset_log_context
from app.leads.identity import DatabaseSalesIdentityProvider, SalesIdentityProvider
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageReassignmentAudit,
    MessageRetryAttempt,
    SalesLeadContext,
    SmartTableSync,
    deserialize_field_value,
    serialize_field_value,
)
from app.leads.progress import LeadProgressService
from app.leads.quote_routing import (
    QuoteRecoveryExtractionRequest,
    QuoteRecoverySegment,
    QuoteRecoverySegmentResult,
    QuoteResolutionResult,
    resolve_message_quote,
)
from app.leads.review import LeadReviewService, ReviewSyncResult
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    MessageAttachment,
    MessageQuoteResolution,
    MessageQuoteResolutionStatus,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapter,
    SmartTableRecordNotFoundError,
)
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import DEFAULT_SMART_TABLE_FIELD_VALUES, PROCESS_OPTIONS

logger = logging.getLogger(__name__)

COMPLETED_CHECKPOINT_STATUSES = frozenset(
    {"succeeded", "ignored", "unauthorized", "invalid", "failed_pending_review"}
)


class LeadProcessingStatus(StrEnum):
    """描述一次 T02 Outbox 文本消费的可观察业务结论。"""

    CREATED = "created"
    IGNORED = "ignored"
    UNAUTHORIZED = "unauthorized"
    INVALID_EVENT = "invalid_event"
    ALREADY_PROCESSED = "already_processed"
    WAITING_FOR_PREVIOUS = "waiting_for_previous"
    UPDATED = "updated"
    UNASSIGNED = "unassigned"
    QUOTE_UNRESOLVED = "quote_unresolved"
    SYNC_FAILED = "sync_failed"


class ProtectedSupplementStatus(StrEnum):
    """描述失败消息受保护补充重试的确定性结果。"""

    SUCCEEDED = "succeeded"
    PROCESSING = "processing"
    FAILED_PENDING_REVIEW = "failed_pending_review"
    NO_TARGET = "no_target"


@dataclass(frozen=True)
class ProtectedSupplementResult:
    """返回一次失败消息补充重试的审计标识和字段保护结果。"""

    status: ProtectedSupplementStatus
    message_id: str
    lead_id: str | None = None
    attempt_id: int | None = None
    updated_fields: tuple[str, ...] = ()
    protected_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class LeadProcessingResult:
    """返回 Outbox 消费结果及已创建的线索和智能表格记录标识。"""

    status: LeadProcessingStatus
    lead_id: str | None = None
    smart_table_record_id: str | None = None
    lead_ids: tuple[str, ...] = ()
    company_resolution_applied: bool = False


@dataclass(frozen=True)
class ContextUpdateRequest:
    """描述提交事务后可安全执行的一次既有智能表格字段补丁。"""

    source_message_id: str
    lead_id: str
    record_id: str
    fields: Mapping[str, LeadFieldValue]
    outbox_event_id: int
    segment_index: int = 0
    pending_confirmation_fields: tuple[str, ...] = ()
    pending_prefill_allowed_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class QuoteRouteDecision:
    """保存 quote 路由在当前事务内的唯一决策结果。"""

    target_lead: Lead | None = None
    waiting_for_source: bool = False
    fail_closed: bool = False
    recovery_completed: bool = False


def _normalize_company_patch(fields: Mapping[str, object]) -> dict[str, LeadFieldValue]:
    """校验公司服务交给 T09 的字段值形状，拒绝未知类型继续同步。

    参数：fields 为公司解析服务生成的智能表格增量补丁。
    返回值：仅包含字符串或字符串列表的字段映射，供 ExtractedLeadPatch 使用。
    异常：字段值不是允许的 LeadFieldValue 时抛出 ValueError。
    副作用：不修改输入映射，仅创建一个新的规范化字典。
    """
    normalized: dict[str, LeadFieldValue] = {}
    for field_name, value in fields.items():
        if isinstance(value, str):
            normalized[field_name] = value
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            normalized[field_name] = value
        else:
            raise ValueError(f"智能表格字段值类型不支持：{field_name}")
    return normalized


@dataclass(frozen=True)
class MultiLeadSyncRequest:
    """描述已持久化的多客户分段表格同步请求。"""

    sales_user_id: str
    lead_id: str
    segment_index: int
    fields: dict[str, str]
    outbox_event_id: int


@dataclass(frozen=True)
class AIReviewRequest:
    """描述一条已经可靠归属、待经 T09 安全写入的 AI 字段补丁。"""

    source_message_id: str
    sales_user_id: str
    lead_id: str
    outbox_event_id: int
    patch: ExtractedLeadPatch
    creates_lead: bool
    segment_index: int = 0


@dataclass(frozen=True)
class ReassignmentRequest:
    """描述完成权限校验后可安全写入新目标表格的人工重归属请求。"""

    audit_id: int
    message_id: str
    segment_index: int
    new_lead_id: str
    safe_fields: dict[str, str]


class DeterministicFirstTextLeadExtractor:
    """以窄范围规则提取首条消息的确定性字段，避免公司身份依赖 LLM。"""

    _label_to_field = {
        "客户": "线索名称",
        "公司": "线索名称",
        "联系人": "联系人",
        "手机": "手机",
        "手机号": "手机",
        "电话": "电话",
        "邮箱": "邮箱",
        "业务": "业务线",
        "业务线": "业务线",
        "备注": "备注",
    }
    _process_values = PROCESS_OPTIONS

    def extract(self, text: str | None) -> dict[str, str] | None:
        """从显式中文标签文本提取首条可审核线索的安全字段补丁。

        参数：text 为 T02 已标准化并持久化的文本。
        返回值：含线索名称的确定性字段补丁；普通文本或缺少客户名称时返回 None。
        异常：无；不调用 Qwen 或任何外部服务。
        副作用：无。
        """
        fields = self.extract_patch(text)
        return fields if fields.get("线索名称") else None

    def extract_patch(self, text: str | None) -> dict[str, str]:
        """从显式标签文本提取可用于已有线索的最小字段补丁。

        参数：text 为 T02 已标准化并持久化的文本。
        返回值：仅含确定性识别字段的补丁；没有合法标签时返回空字典。
        异常：无；不调用 Qwen 或任何外部服务。
        副作用：无。
        """
        if text is None:
            return {}

        fields: dict[str, str] = {}
        # 仅接受人工可读的“标签：值”片段，避免将普通聊天猜测为客户信息；
        # 换行与中文分隔符都视为字段边界，保证多行表单不会把后续字段吞进公司名。
        segments = re.split(r"[\r\n；;，,、]+", text)
        for segment in segments:
            label, separator, value = segment.partition("：")
            if not separator:
                label, separator, value = segment.partition(":")
            field_name = self._label_to_field.get(label.strip())
            normalized_value = value.strip()
            if field_name is not None and separator and normalized_value:
                fields[field_name] = normalized_value

        # 需求只映射已注册的工艺枚举，不以自由文本虚构 CRM 字段值。
        demand = next(
            (
                value.strip()
                for segment in segments
                for label, separator, value in [segment.partition("：")]
                if separator and label.strip() == "需求"
            ),
            "",
        )
        matched_processes = [process for process in self._process_values if process in demand]
        if matched_processes:
            # 智能表格工艺是多选，单个工艺也按列表写入，避免后续类型漂移。
            fields["工艺"] = matched_processes

        return fields

    @staticmethod
    def extract_leading_company_hint(text: str | None) -> str | None:
        """只从显式客户/公司标签提取公司身份提示。

        参数：text 为销售发送的自由文本消息。
        返回值：存在显式线索名称时返回该名称，否则返回 None。
        异常：无。
        副作用：仅读取当前文本，不调用模型或外部服务。
        """
        company = DeterministicFirstTextLeadExtractor().extract_patch(text).get("线索名称")
        return company.strip() if isinstance(company, str) and company.strip() else None

    def extract_many(self, text: str | None) -> list[dict[str, str]]:
        """从明确客户标签分段中提取一条或多条独立字段补丁。

        参数：text 为已标准化文本。
        返回值：按原消息顺序返回含线索名称的字段补丁；普通文本返回空列表。
        异常：无；不调用外部服务。
        副作用：无。
        """
        if text is None:
            return []
        return [fields for _, fields in self.extract_many_with_segments(text)]

    def extract_many_with_segments(self, text: str | None) -> list[tuple[str, dict[str, str]]]:
        """返回显式多客户分段原文及字段，保持 retry 的原始分段边界。

        参数：text 为已标准化并持久化的消息文本。
        返回值：按消息顺序返回可识别客户分段原文和字段补丁。
        异常：无；不调用外部服务。
        副作用：无。
        """
        if text is None:
            return []
        # 只在下一个明确客户标签前切分；普通字段换行必须留在同一条线索中。
        segments = re.split(
            r"(?:\r?\n\s*(?=(?:客户|公司)\s*[:：])|[；;，,、]\s*(?=(?:客户|公司)\s*[:：]))",
            text,
        )
        return [
            (segment, fields)
            for segment in segments
            if (fields := self.extract(segment)) is not None
        ]

    def extract_segment_patch(self, text: str | None, segment_index: int) -> dict[str, str]:
        """只从原消息指定分段生成字段补丁，不重新解析其他客户。

        参数：text 为原始标准化消息；segment_index 为已持久化的失败分段序号。
        返回值：该分段的确定性字段补丁；索引不存在时返回空字典。
        异常：无；不调用外部服务。
        副作用：无。
        """
        if segment_index < 0:
            return {}
        segments = self.extract_many_with_segments(text)
        if len(segments) <= 1 and segment_index == 0:
            return self.extract_patch(text)
        if segment_index >= len(segments):
            return {}
        return segments[segment_index][1]

    def has_ambiguous_multiple_companies(self, text: str | None) -> bool:
        """判断一条文本是否出现多个不同公司候选却未能可靠拆分。

        参数：text 为已标准化文本。
        返回值：存在至少两个不同显式公司候选时返回 True。
        异常：无。
        副作用：无。
        """
        if text is None:
            return False
        # 只以显式标签值判断歧义；相同名称的客户/公司重复表述不视作多客户。
        candidates = {
            match.group(1).strip()
            for match in re.finditer(r"(?:客户|公司)\s*[:：]\s*([^；;，,、\n]+)", text)
            if match.group(1).strip()
        }
        return len(candidates) > 1


class FirstTextLeadWorkspaceService:
    """消费 T02 已持久化文本事件，创建一条销售个人审核线索和表格记录。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        sales_identity_provider: SalesIdentityProvider | None = None,
        lead_context_ttl_minutes: int | None = None,
        ai_gateway: AIGateway | None = None,
        company_lead_service: CompanyLeadService | None = None,
        robot_submission_confirmation_available: bool | None = None,
        lead_progress_service: LeadProgressService | None = None,
    ) -> None:
        """注入数据库、表格和销售身份边界，避免业务层依赖真实 CLI 或 Qwen。

        参数：session_factory 创建事务；smart_table_adapter 写销售审核表；身份提供器可替换测试实现；
        lead_context_ttl_minutes 可覆盖环境中的上下文有效期；
        ai_gateway 为可替换的 T08 网关；company_lead_service 负责 T10 公司核验与销售内去重；
        robot_submission_confirmation_available 表示真实卡片回调 capability/readiness；
        lead_progress_service 为可选的有效需求消息会话登记服务。
        返回值：无。
        异常：无；依赖错误在消费时按其真实类型处理。
        副作用：仅保存依赖引用，不读写数据库或智能表格。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._sales_identity_provider = sales_identity_provider or DatabaseSalesIdentityProvider()
        self._ai_gateway = ai_gateway
        self._company_lead_service = company_lead_service
        self._lead_progress_service = lead_progress_service
        settings = get_settings()
        self._robot_submission_confirmation_available = (
            settings.wecom_card_callback_ready()
            if robot_submission_confirmation_available is None
            else robot_submission_confirmation_available
        )
        configured_ttl_minutes = (
            settings.lead_context_ttl_minutes
            if lead_context_ttl_minutes is None
            else lead_context_ttl_minutes
        )
        # 上下文有效期与失败重试次数均由部署配置决定，避免业务逻辑内硬编码阈值。
        if configured_ttl_minutes <= 0:
            raise ValueError("当前客户上下文有效期必须大于 0")
        self._lead_context_ttl = timedelta(minutes=configured_ttl_minutes)
        self._lead_message_retry_count = settings.lead_message_retry_count
        self._lead_processing_timeout = timedelta(seconds=settings.lead_processing_timeout_seconds)

    def consume(
        self,
        outbox_event_id: int,
        *,
        claimed_for_processing: bool = False,
        recover_expired_lease: bool = False,
    ) -> LeadProcessingResult:
        """消费一条 Outbox 事件，并在其成为检查点后继续同销售的下一条消息。

        参数：outbox_event_id 为 T02 已提交的待处理事件标识；claimed_for_processing 表示
        Worker 已原子接管调度认领；recover_expired_lease 表示仅执行既有失联租约恢复。
        返回值：返回本次指定事件的确定性处理结论。
        异常：事件或来源消息丢失时抛出 ValueError；数据库异常向调用方传播。
        副作用：本事件成功、忽略、拒绝或失败耗尽后，会串行触发同销售的下一条待处理事件。
        """
        result = self._consume_once(
            outbox_event_id,
            claimed_for_processing=claimed_for_processing,
            recover_expired_lease=recover_expired_lease,
        )
        # 表格首次同步成功后才应用公司核验和销售内去重，避免失败重试中重放外部写入。
        if not result.company_resolution_applied:
            result = self._apply_company_resolution(outbox_event_id, result)
        if self._lead_progress_service is not None and result.status in {
            LeadProcessingStatus.CREATED,
            LeadProcessingStatus.UPDATED,
            LeadProcessingStatus.UNASSIGNED,
            LeadProcessingStatus.QUOTE_UNRESOLVED,
            LeadProcessingStatus.SYNC_FAILED,
        }:
            # 只在本条消息已产生归属分段后登记；提交命令和确认动作不经过此入口。
            with self._session_factory() as session:
                event = session.get(OutboxEvent, outbox_event_id)
                message_id = event.message_id if event is not None else None
            if message_id is not None:
                self._lead_progress_service.record_resolved_message(message_id)
        # 只在本事件已越过首次消费检查点后继续，防止 retrying/processing 事件被错误跳过。
        self._consume_next_after_checkpoint(outbox_event_id)
        return result

    def retry_failed_message(
        self,
        message_id: str,
        operator_user_id: str | None = None,
        *,
        segment_index: int = 0,
        request_id: str | None = None,
    ) -> ProtectedSupplementResult:
        """在当前线索状态上重试失败消息，禁止回放失败消息之后的历史消息。

        参数：message_id 为已进入 failed_pending_review 的原始消息；operator_user_id 为重试操作人，
        省略时仅允许该消息所属销售执行。返回值：受保护补充的状态、目标线索和字段保护结果。
        异常：消息不存在、权限不足或非失败终态时抛出 ValueError/PermissionError；
        外部失败会保存失败事实；历史重试成功只有在 Outbox 与对应首条同步记录一致成功时才幂等返回，
        否则会重新核实同一 Smart Table 行。
        副作用：新增一次指定分段的 MessageRetryAttempt，最多调用一次当前消息的解析和
        T09 安全补丁同步；原始顺序检查点不变。
        """
        if segment_index < 0:
            raise ValueError("失败消息分段序号不能为负数")
        message_text: str | None = None
        source_sales_user_id = ""
        persisted_retry_fields: dict[str, LeadFieldValue] | None = None
        with self._session_factory.begin() as session:
            event = session.scalar(
                select(OutboxEvent).where(OutboxEvent.message_id == message_id).with_for_update()
            )
            message = session.get(IncomingMessage, message_id)
            if event is None or message is None:
                raise ValueError(f"失败消息不存在：{message_id}")
            outbox_event_id = event.id
            # 事务提交后 ORM 对象可能过期，先缓存已完成权限校验的原始销售身份供外部调用使用。
            source_sales_user_id = message.sales_user_id
            if event.status not in {"failed_pending_review", "succeeded"}:
                raise ValueError("只有 failed_pending_review 消息允许人工重试")
            operator_id = operator_user_id or message.sales_user_id
            operator = session.get(SalesAuthorization, operator_id)
            if operator is None or not operator.is_active:
                raise PermissionError("重试操作人没有可用的 actor")
            # 原始采集销售只用于审计；合法转交后，普通销售权限由当前 Lead owner 决定。
            # 具体消息分段和目标 Lead 仍在 _retry_target_lead 中校验，避免借转交跨销售读取。
            operator_role = "administrator" if operator.is_administrator else "sales"
            latest_attempt = session.scalar(
                select(MessageRetryAttempt)
                .where(MessageRetryAttempt.message_id == message_id)
                .where(MessageRetryAttempt.segment_index == segment_index)
                .order_by(MessageRetryAttempt.attempt_number.desc())
                .with_for_update()
                .limit(1)
            )
            latest_sync = (
                session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == latest_attempt.lead_id)
                )
                if latest_attempt is not None and latest_attempt.lead_id is not None
                else None
            )
            if event.status == "succeeded" and not (
                latest_attempt is not None
                and latest_attempt.status == "succeeded"
                and latest_sync is not None
                and latest_sync.status == "succeeded"
            ):
                raise ValueError("成功事件缺少一致的补充恢复终态")
            if latest_attempt is not None and latest_attempt.status == "processing":
                lease_expired = (
                    latest_attempt.processing_lease_expires_at is not None
                    and self._as_utc(latest_attempt.processing_lease_expires_at)
                    <= self._as_utc(utc_now())
                )
                if not lease_expired:
                    return ProtectedSupplementResult(
                        ProtectedSupplementStatus.PROCESSING,
                        message_id,
                        lead_id=latest_attempt.lead_id,
                        attempt_id=latest_attempt.id,
                    )
                # 只恢复同一个 attempt 和同一个目标，绝不新建另一笔补充执行。
                target = self._retry_target_lead(
                    session, message, segment_index, operator_id, operator.is_administrator
                )
                if target is None or target.id != latest_attempt.lead_id:
                    latest_attempt.status = ProtectedSupplementStatus.NO_TARGET.value
                    latest_attempt.failure_category = "permanent"
                    latest_attempt.error_summary = "retry_target_missing_after_lease"
                    latest_attempt.completed_at = utc_now()
                    return ProtectedSupplementResult(
                        ProtectedSupplementStatus.NO_TARGET,
                        message_id,
                        attempt_id=latest_attempt.id,
                    )
                attempt = latest_attempt
                attempt.processing_started_at = utc_now()
                attempt.processing_lease_expires_at = (
                    attempt.processing_started_at + self._lead_processing_timeout
                )
                lead_id = target.id
                attempt_id = attempt.id
                message_text = message.normalized_text
                retry_text = self._retry_text(message_text, segment_index)
                self._record_audit(
                    session,
                    event,
                    "lead_message_protected_retry_recovered",
                    details={"attempt_id": attempt_id, "segment_index": segment_index},
                )
            elif (
                latest_attempt is not None
                and latest_attempt.status == "succeeded"
                and event.status == "succeeded"
                and latest_sync is not None
                and latest_sync.status == "succeeded"
            ):
                # 只有事件与远端同步都已终态成功才可直接重放旧结果；
                # 历史“retry succeeded + sync retrying”须重新核实原记录。
                return ProtectedSupplementResult(
                    ProtectedSupplementStatus.SUCCEEDED,
                    message_id,
                    lead_id=latest_attempt.lead_id,
                    attempt_id=latest_attempt.id,
                    updated_fields=tuple(latest_attempt.updated_fields),
                    protected_fields=tuple(latest_attempt.protected_fields),
                )
            elif latest_attempt is not None and latest_attempt.status == "no_target":
                return ProtectedSupplementResult(
                    ProtectedSupplementStatus.NO_TARGET,
                    message_id,
                    lead_id=latest_attempt.lead_id,
                    attempt_id=latest_attempt.id,
                )
            else:
                target = self._retry_target_lead(
                    session, message, segment_index, operator_id, operator.is_administrator
                )
                if (
                    target is not None
                    and latest_attempt is not None
                    and latest_attempt.status == "succeeded"
                    and event.status == "failed_pending_review"
                    and latest_sync is not None
                    and latest_sync.source_message_id == message_id
                    and latest_sync.status != "succeeded"
                ):
                    # 旧版本可能只把 ACK 记为成功；恢复重放原消息已持久化的安全计划字段。
                    planned_names = tuple(dict.fromkeys(latest_attempt.updated_fields))
                    provenance_rows = session.scalars(
                        select(LeadFieldProvenance)
                        .where(
                            LeadFieldProvenance.lead_id == target.id,
                            LeadFieldProvenance.source_message_id == message_id,
                            LeadFieldProvenance.field_name.in_(planned_names),
                        )
                        .order_by(LeadFieldProvenance.id.desc())
                    ).all()
                    recovered_values: dict[str, LeadFieldValue] = {}
                    for provenance in provenance_rows:
                        if provenance.field_name in recovered_values:
                            continue
                        value = deserialize_field_value(provenance.value)
                        if isinstance(value, str) or (
                            isinstance(value, list)
                            and all(isinstance(item, str) for item in value)
                        ):
                            recovered_values[provenance.field_name] = value
                    if tuple(name for name in planned_names if name not in recovered_values):
                        # 缺少任一历史计划值时不允许用不完整补丁把同步状态伪装成成功。
                        raise ValueError("原同步计划缺少可恢复字段来源")
                    persisted_retry_fields = {
                        name: recovered_values[name] for name in planned_names
                    }
                attempt_number = (
                    latest_attempt.attempt_number if latest_attempt is not None else 0
                ) + 1
                now = utc_now()
                attempt = MessageRetryAttempt(
                    message_id=message_id,
                    segment_index=segment_index,
                    lead_id=target.id if target is not None else None,
                    operator_user_id=operator_id,
                    request_id=request_id,
                    attempt_number=attempt_number,
                    status="processing",
                    processing_started_at=now,
                    processing_lease_expires_at=now + self._lead_processing_timeout,
                    updated_fields=[],
                    protected_fields=[],
                )
                session.add(attempt)
                session.flush()
                self._record_audit(
                    session,
                    event,
                    "lead_message_protected_retry_started",
                    details={
                        "attempt_id": attempt.id,
                        "lead_id": target.id if target else None,
                        "segment_index": segment_index,
                        "operator_user_id": operator_id,
                        "operator_role": operator_role,
                        "request_id": request_id,
                    },
                )
                if target is None:
                    attempt.status = ProtectedSupplementStatus.NO_TARGET.value
                    attempt.failure_category = "permanent"
                    attempt.error_summary = "retry_target_missing"
                    attempt.completed_at = utc_now()
                    return ProtectedSupplementResult(
                        ProtectedSupplementStatus.NO_TARGET,
                        message_id,
                        attempt_id=attempt.id,
                    )
                lead_id = target.id
                attempt_id = attempt.id
                message_text = message.normalized_text
                retry_text = self._retry_text(message_text, segment_index)

        try:
            if persisted_retry_fields is not None:
                # 不重跑 AI；只基于旧尝试记录的字段名和同来源 provenance 恢复原计划。
                patch = ExtractedLeadPatch(
                    trace_id=f"protected-retry-{attempt_id}",
                    analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                    fields=persisted_retry_fields,
                    pending_confirmation_fields=(),
                    low_confidence_candidates={},
                )
            else:
                # 只重新解析这一条原始消息，不调用 consume 或重放其后的历史消息。
                fields = DeterministicFirstTextLeadExtractor().extract_segment_patch(
                    message_text, segment_index
                )
                # 自由文本只重新调用已注入的 T08 网关；不读取其他消息上下文。
                patch = (
                    self._ai_gateway.extract_fields(
                        retry_text or "",
                        source_message_id=message_id,
                        lead_id=lead_id,
                    )
                    if not fields and self._ai_gateway is not None
                    else ExtractedLeadPatch(
                        trace_id=f"protected-retry-{attempt_id}",
                        analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                        fields=fields,
                        pending_confirmation_fields=(),
                        low_confidence_candidates={},
                    )
                )
            sync_result = self._sync_ai_review_patch(
                AIReviewRequest(
                    source_message_id=message_id,
                    sales_user_id=source_sales_user_id,
                    lead_id=lead_id,
                    outbox_event_id=outbox_event_id,
                    patch=patch,
                    creates_lead=False,
                ),
                protected_supplement=True,
                acknowledged_but_unverified_fields=tuple(persisted_retry_fields or ()),
            )
        except Exception as error:
            with self._session_factory.begin() as session:
                retry_attempt = session.get(MessageRetryAttempt, attempt_id)
                if retry_attempt is None:
                    raise ValueError(f"补充重试事实不存在：{attempt_id}")
                retry_attempt.status = ProtectedSupplementStatus.FAILED_PENDING_REVIEW.value
                retry_attempt.failure_category = classify_task_failure(error).value
                retry_attempt.error_summary = safe_failure_summary(error)
                retry_attempt.completed_at = utc_now()
                event = session.scalar(
                    select(OutboxEvent).where(OutboxEvent.message_id == message_id)
                )
                if event is not None:
                    # 仅当本事件是首条同步来源时一起收敛；后续补充失败不降级旧同步事实。
                    event.status = "failed_pending_review"
                    event.failure_category = classify_task_failure(error).value
                    event.failure_summary = safe_failure_summary(error)
                    event.failed_at = utc_now()
                    event.processing_started_at = None
                    sync = session.scalar(
                        select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                    )
                    if sync is not None and sync.source_message_id == message_id:
                        sync.status = "failed_pending_review"
                        sync.error_summary = type(error).__name__
                        sync.completed_at = utc_now()
                    if isinstance(error, AIGatewayError):
                        source_message = session.get(IncomingMessage, message_id)
                        if source_message is not None:
                            self._record_lead_processing_failed_notice(session, source_message)
                    audit_details: dict[str, object] = {
                        "attempt_id": attempt_id,
                        "segment_index": segment_index,
                        "error": retry_attempt.error_summary,
                        "operator_user_id": operator_id,
                    }
                    # 诊断仅落规范字段名；外部 errmsg、单元格值和 payload 永不持久化。
                    for source_name, audit_name in (
                        ("missing_or_mismatched_fields", "missing_fields"),
                        ("rejected_field_candidates", "rejected_field_candidates"),
                    ):
                        names = getattr(error, source_name, ())
                        if isinstance(names, (tuple, list)) and all(
                            isinstance(name, str) for name in names
                        ):
                            audit_details[audit_name] = list(names)
                    self._record_audit(
                        session,
                        event,
                        "lead_message_protected_retry_failed",
                        details=audit_details,
                    )
            logger.exception("lead_message_protected_retry_failed")
            return ProtectedSupplementResult(
                ProtectedSupplementStatus.FAILED_PENDING_REVIEW,
                message_id,
                lead_id=lead_id,
                attempt_id=attempt_id,
            )

        with self._session_factory.begin() as session:
            retry_attempt = session.get(MessageRetryAttempt, attempt_id)
            if retry_attempt is None:
                raise ValueError(f"补充重试事实不存在：{attempt_id}")
            retry_attempt.status = ProtectedSupplementStatus.SUCCEEDED.value
            retry_attempt.updated_fields = list(sync_result.updated_fields)
            retry_attempt.protected_fields = list(sync_result.protected_fields)
            retry_attempt.completed_at = utc_now()
            retry_attempt.processing_started_at = None
            retry_attempt.processing_lease_expires_at = None
            event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == message_id))
            if event is not None:
                # T09 返回前真实 Adapter 已核实远端字段；仅来源事件对应的同步行随事件收敛。
                event.status = "succeeded"
                event.failure_category = None
                event.failure_summary = None
                event.failed_at = None
                event.processing_started_at = None
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                if sync is not None and sync.source_message_id == message_id:
                    sync.status = "succeeded"
                    sync.error_summary = None
                    sync.completed_at = utc_now()
                self._record_audit(
                    session,
                    event,
                    "lead_message_protected_retry_succeeded",
                    details={
                        "attempt_id": attempt_id,
                        "segment_index": segment_index,
                        "updated_fields": list(sync_result.updated_fields),
                        "persisted_fields": list(sync_result.updated_fields),
                        "protected_fields": list(sync_result.protected_fields),
                        "operator_user_id": operator_id,
                    },
                )
        return ProtectedSupplementResult(
            ProtectedSupplementStatus.SUCCEEDED,
            message_id,
            lead_id=lead_id,
            attempt_id=attempt_id,
            updated_fields=sync_result.updated_fields,
            protected_fields=sync_result.protected_fields,
        )

    @staticmethod
    def _retry_text(text: str | None, segment_index: int) -> str | None:
        """从失败消息中取出指定分段供 AI 补充解析，避免把其他客户上下文带入重试。

        参数：text 为原始标准化消息；segment_index 为失败分段序号。
        返回值：指定分段原文；分段无法可靠定位时仅分段 0 使用完整原文，其余返回 None。
        异常：无。
        副作用：无；不访问数据库或外部服务。
        """
        segments = DeterministicFirstTextLeadExtractor().extract_many_with_segments(text)
        if len(segments) <= 1:
            return text if segment_index == 0 else None
        if segment_index >= len(segments):
            return None
        return segments[segment_index][0]

    @staticmethod
    def _retry_target_lead(
        session: Session,
        message: IncomingMessage,
        segment_index: int,
        operator_user_id: str,
        operator_is_administrator: bool,
    ) -> Lead | None:
        """按当前持久化归属或来源线索确定受保护重试目标。

        参数：session 为当前事务；message 为失败消息；segment_index 为失败分段；
        operator_user_id 为当前操作人；operator_is_administrator 表示是否为管理员。
        返回值：当前仍可由该操作人补充的 Lead；无法可靠确定时返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取当前事实，不读取可变销售上下文、不解析历史消息或写入归属关系。
        """
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == message.message_id,
                LeadMessageResolution.segment_index == segment_index,
            )
        )
        target_id = resolution.lead_id if resolution is not None else None
        target = (
            session.scalar(select(Lead).where(Lead.id == target_id).with_for_update())
            if target_id is not None
            else None
        )
        if target is None:
            target = session.scalar(
                select(Lead)
                .where(
                    Lead.source_message_id == message.message_id,
                    Lead.source_segment_index == segment_index,
                )
                .with_for_update()
            )
        if target is None or (
            not operator_is_administrator and target.smart_table_owner_user_id != operator_user_id
        ):
            return None
        if target.lifecycle_state == "discarded":
            return None
        return target

    def confirm_temporary_company(
        self, lead_id: str, sales_user_id: str, company_name: str
    ) -> LeadProcessingResult:
        """通过受控入口确认 temporary Lead 公司并复用首次入表前的公司决策收尾。

        参数：lead_id 为待确认临时线索；sales_user_id 为当前销售；company_name 为人工输入名称。
        返回值：保持原 Lead 生命周期的创建或更新结论。
        异常：非本人、非 temporary、缺少来源事件或公司服务异常时按真实原因抛出。
        副作用：先写人工确认来源与公司审计，再首次入表或经 T09 更新既有 record。
        """
        if self._company_lead_service is None:
            raise ValueError("缺少公司解析服务")
        company_result = self._company_lead_service.confirm_temporary_company(
            lead_id, sales_user_id, company_name
        )
        with self._session_factory() as session:
            lead = session.get(Lead, company_result.lead_id)
            if lead is None:
                raise ValueError(f"公司确认目标线索不存在：{company_result.lead_id}")
            if lead.source_message_id is None:
                raise ValueError("管理员补建线索不支持来源消息公司的确认流程")
            event_id = session.scalar(
                select(OutboxEvent.id).where(OutboxEvent.message_id == lead.source_message_id)
            )
            if event_id is None:
                raise ValueError(f"公司确认来源 Outbox 不存在：{lead.source_message_id}")
            command = CompanyUpsertCommand(
                source_message_id=lead.source_message_id,
                source_segment_index=lead.source_segment_index,
                sales_user_id=sales_user_id,
                fields=dict(lead.field_values),
                existing_lead_id=lead.id,
                user_confirmed_company=True,
                defer_smart_table_sync=True,
            )
        return self._finish_company_decision_before_smart_table(event_id, command, company_result)

    def _apply_company_resolution(
        self, outbox_event_id: int, result: LeadProcessingResult
    ) -> LeadProcessingResult:
        """将已同步的本消息线索交给 T10 公司服务进行保守升级或销售内合并。

        参数：outbox_event_id 用于读取当前消息事实；result 为首次消费的成功结果。
        返回值：公司服务完成后指向最终线索的处理结果。
        异常：公司服务的权限、数据库或表格异常向调用方传播。
        副作用：可能升级临时线索、合并当前销售同公司线索并修正消息归属和当前上下文。
        """
        if self._company_lead_service is None or result.status not in {
            LeadProcessingStatus.CREATED,
            LeadProcessingStatus.UPDATED,
        }:
            return result
        lead_ids = result.lead_ids or ((result.lead_id,) if result.lead_id is not None else ())
        if not lead_ids:
            return result
        with self._session_factory() as session:
            _, message = self._load_event_and_message(session, outbox_event_id)
            message_id = message.message_id
            sales_user_id = message.sales_user_id
            commands: list[tuple[str, CompanyUpsertCommand]] = []
            for lead_id in lead_ids:
                lead = session.get(Lead, lead_id)
                if lead is None:
                    raise ValueError(f"线索不存在：{lead_id}")
                # 当前 Outbox 消息是本轮补充的来源，不能把旧首条消息误记为公司核验来源。
                commands.append(
                    (
                        lead_id,
                        CompanyUpsertCommand(
                            source_message_id=message.message_id,
                            source_segment_index=lead.source_segment_index,
                            sales_user_id=message.sales_user_id,
                            fields=dict(lead.field_values),
                            existing_lead_id=lead.id,
                            region_evidence=CompanyRegionEvidence(
                                message_text=message.normalized_text,
                                company_name=lead.field_values.get("线索名称"),
                                email=lead.field_values.get("邮箱"),
                                phone=lead.field_values.get("手机")
                                or lead.field_values.get("电话"),
                            ),
                            defer_smart_table_sync=True,
                        ),
                    )
                )

        resolved = [
            (source_lead_id, self._company_lead_service.upsert(command))
            for source_lead_id, command in commands
        ]
        with self._session_factory.begin() as session:
            for source_lead_id, company_result in resolved:
                if source_lead_id == company_result.lead_id:
                    continue
                # 临时线索被合并后，消息审计与当前上下文都必须指向同销售的最终目标。
                for resolution in session.scalars(
                    select(LeadMessageResolution).where(
                        LeadMessageResolution.message_id == message_id,
                        LeadMessageResolution.lead_id == source_lead_id,
                    )
                ).all():
                    resolution.lead_id = company_result.lead_id
                for context in session.scalars(
                    select(SalesLeadContext).where(
                        SalesLeadContext.sales_user_id == sales_user_id,
                        SalesLeadContext.lead_id == source_lead_id,
                    )
                ).all():
                    context.lead_id = company_result.lead_id
        materialized_record_ids: dict[str, str | None] = {}
        for source_lead_id, company_result in resolved:
            materialized = self._materialize_company_resolved_lead(
                outbox_event_id,
                company_result.lead_id,
                company_result.standard_company_name,
                pending_confirmation_fields=company_result.pending_confirmation_fields,
            )
            if materialized.status is LeadProcessingStatus.SYNC_FAILED:
                return materialized
            record_id = company_result.smart_table_record_id or materialized.smart_table_record_id
            if company_result.smart_table_patch and record_id is not None:
                # AI 首次入表后才完成天眼查解析时，必须把公司标准名补丁继续交给 T09，
                # 不能只更新后台 Lead 而让智能表格停留在原始简称。
                command = next(
                    command
                    for candidate_lead_id, command in commands
                    if candidate_lead_id == source_lead_id
                )
                updated = self._update_smart_table_record(
                    ContextUpdateRequest(
                        source_message_id=command.source_message_id,
                        lead_id=company_result.lead_id,
                        record_id=record_id,
                        fields=_normalize_company_patch(company_result.smart_table_patch),
                        outbox_event_id=outbox_event_id,
                        segment_index=command.source_segment_index,
                        pending_confirmation_fields=company_result.pending_confirmation_fields,
                        pending_prefill_allowed_fields=(
                            company_result.pending_prefill_allowed_fields
                        ),
                    )
                )
                if updated.status is LeadProcessingStatus.SYNC_FAILED:
                    return updated
                record_id = updated.smart_table_record_id
            materialized_record_ids[source_lead_id] = record_id
        logger.info(
            "lead_company_resolution_completed",
            extra={
                "outbox_event_id": outbox_event_id,
                "resolved_lead_count": len(resolved),
            },
        )
        final_ids = tuple(company_result.lead_id for _, company_result in resolved)
        return LeadProcessingResult(
            result.status,
            lead_id=final_ids[0],
            smart_table_record_id=materialized_record_ids[resolved[0][0]],
            lead_ids=final_ids if len(final_ids) > 1 else (),
        )

    def _materialize_company_resolved_lead(
        self,
        outbox_event_id: int,
        lead_id: str,
        standard_company_name: str | None,
        segment_index: int = 0,
        pending_confirmation_fields: tuple[str, ...] = (),
    ) -> LeadProcessingResult:
        """为已获得可靠公司身份、但尚未入表的原 Lead 创建唯一审核记录。

        参数：outbox_event_id 为本次公司确认来源；lead_id 为保持生命周期的目标；
        standard_company_name 为公司服务给出的可靠去重键。
        返回值：返回已有、创建成功或同步失败的表格定位。
        异常：数据库事实缺失时抛出 ValueError；表格同步错误由既有创建路径转换为失败结果。
        副作用：首次需要入表时创建 SmartTableSync、调用表格并进入 T09。
        """
        with self._session_factory.begin() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError(f"公司解析目标线索不存在：{lead_id}")
            if lead.smart_table_record_id is not None:
                return LeadProcessingResult(
                    LeadProcessingStatus.UPDATED,
                    lead_id=lead.id,
                    smart_table_record_id=lead.smart_table_record_id,
                )
            if standard_company_name is None or lead.lifecycle_state == "merged":
                # 未核验公司继续保持 temporary；它不能读取或写入 Smart Table/T09。
                return LeadProcessingResult(LeadProcessingStatus.UPDATED, lead_id=lead.id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead.id))
            if sync is None:
                # 同一 Lead 只补一条同步事实，外部创建仍由 record_id 空值保证幂等检查点。
                session.add(
                    SmartTableSync(
                        lead_id=lead.id,
                        source_message_id=self._source_message_id(outbox_event_id),
                        source_segment_index=segment_index,
                    )
                )
            sales_user_id = lead.smart_table_owner_user_id
            fields = dict(lead.field_values)
        return self._create_smart_table_record(
            sales_user_id,
            fields,
            lead_id,
            outbox_event_id,
            segment_index,
            pending_confirmation_fields,
        )

    def _consume_initial_company_before_smart_table(
        self, outbox_event_id: int, command: CompanyUpsertCommand
    ) -> LeadProcessingResult:
        """在首次表格副作用前完成公司决策，并把消息归属到唯一目标 Lead。

        参数：outbox_event_id 为已取得销售顺序锁检查点的事件；command 为本轮公司候选。
        返回值：新建、更新或保留 temporary 的最终消费结果。
        异常：公司服务、表格或数据库异常按既有 Outbox 语义向调用方传播。
        副作用：可能创建或复用当前销售 Lead；正式 Lead 才会进入 T09 和 Smart Table。
        """
        if self._company_lead_service is None:
            raise ValueError("缺少公司解析服务")
        company_result = self._company_lead_service.upsert(command)
        return self._finish_company_decision_before_smart_table(
            outbox_event_id, command, company_result
        )

    def _finish_company_decision_before_smart_table(
        self,
        outbox_event_id: int,
        command: CompanyUpsertCommand,
        company_result: CompanyUpsertResult,
    ) -> LeadProcessingResult:
        """将已持久化的公司决策在首次表格副作用前收敛为唯一目标 Lead。

        参数：outbox_event_id 为来源事件；command 提供来源和分段；company_result 为公司服务结果。
        返回值：temporary、创建或更新后的可观察消费结论。
        异常：公司结果结构不完整或表格同步失败时按既有 Outbox 语义传播或返回失败。
        副作用：仅正式 Lead 创建记录；既有记录的增量始终进入 T09。
        """
        if company_result.standard_company_name is None:
            # 天眼查无结果或超时仍是 temporary，完成后台归属但绝不进入表格审核。
            self._complete_deferred_company_message(
                outbox_event_id, company_result.lead_id, command.source_segment_index
            )
            return LeadProcessingResult(
                LeadProcessingStatus.CREATED,
                lead_id=company_result.lead_id,
                company_resolution_applied=True,
            )

        materialized = self._materialize_company_resolved_lead(
            outbox_event_id,
            company_result.lead_id,
            company_result.standard_company_name,
            command.source_segment_index,
            company_result.pending_confirmation_fields,
        )
        if materialized.status is LeadProcessingStatus.SYNC_FAILED:
            return LeadProcessingResult(
                materialized.status,
                lead_id=materialized.lead_id,
                company_resolution_applied=True,
            )
        if (
            company_result.smart_table_record_id is None
            and materialized.status is LeadProcessingStatus.CREATED
        ):
            # 新 Lead 在公司决策后才首次创建 record，_create_smart_table_record 会完成消息归属。
            return LeadProcessingResult(
                materialized.status,
                lead_id=company_result.lead_id,
                smart_table_record_id=materialized.smart_table_record_id,
                company_resolution_applied=True,
            )
        record_id = company_result.smart_table_record_id or materialized.smart_table_record_id
        if record_id is None:
            # 新 Lead 在公司决策后才首次创建 record，_create_smart_table_record 会完成消息归属。
            return LeadProcessingResult(
                materialized.status,
                lead_id=company_result.lead_id,
                smart_table_record_id=materialized.smart_table_record_id,
                company_resolution_applied=True,
            )
        if company_result.smart_table_patch:
            # 命中当前销售已有记录时，只把公司服务判定安全的增量字段交给 T09。
            updated = self._update_smart_table_record(
                ContextUpdateRequest(
                    source_message_id=command.source_message_id,
                    lead_id=company_result.lead_id,
                    record_id=record_id,
                    fields=_normalize_company_patch(company_result.smart_table_patch),
                    outbox_event_id=outbox_event_id,
                    segment_index=command.source_segment_index,
                    pending_confirmation_fields=company_result.pending_confirmation_fields,
                    pending_prefill_allowed_fields=(company_result.pending_prefill_allowed_fields),
                )
            )
            return LeadProcessingResult(
                updated.status,
                lead_id=company_result.lead_id,
                smart_table_record_id=updated.smart_table_record_id,
                company_resolution_applied=True,
            )
        self._complete_deferred_company_message(
            outbox_event_id, company_result.lead_id, command.source_segment_index
        )
        return LeadProcessingResult(
            LeadProcessingStatus.UPDATED,
            lead_id=company_result.lead_id,
            smart_table_record_id=company_result.smart_table_record_id,
            company_resolution_applied=True,
        )

    def _complete_deferred_company_message(
        self, outbox_event_id: int, lead_id: str, segment_index: int = 0
    ) -> None:
        """完成无需 Smart Table 调用的 temporary 或无字段变化消息归属。

        参数：outbox_event_id 为待完成事件；lead_id 为同销售范围内已确定目标。
        返回值：无。
        异常：事件、消息或线索缺失时由既有查询抛出 ValueError。
        副作用：写入消息归属、上下文和可审计的公司决策完成事件。
        """
        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            if session.get(Lead, lead_id) is None:
                raise ValueError(f"公司决策目标线索不存在：{lead_id}")
            self._mark_assigned(session, event, lead_id, segment_index)
            self._refresh_context(session, message, lead_id)
            self._record_audit(session, event, "company_resolution_completed_before_smart_table")

    def _prepare_quote_route(
        self,
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        quote_result: QuoteResolutionResult,
        extractor: DeterministicFirstTextLeadExtractor,
    ) -> QuoteRouteDecision:
        """在当前事务中解析 quote 目标、冲突和 unassigned recovery。

        参数：session 为当前消息消费事务；event/message 为当前消息事实；quote_result 为已幂等
        写入的引用解析；extractor 只用于服务器侧可靠公司身份判断。
        返回值：返回可继承、等待 source 或 fail closed 的单一路由决策。
        异常：数据库错误或 AI 结构化提取错误向调用方传播并回滚整个事务。
        副作用：可能写入当前/来源消息归属、Lead、字段来源、上下文和恢复审计；不调用 Smart Table。
        """
        resolution = quote_result.resolution
        if resolution.resolution_status != MessageQuoteResolutionStatus.RESOLVED.value:
            self._finish_quote_unresolved(
                session,
                event,
                resolution,
                audit_type=(
                    "quote_routing_not_found"
                    if resolution.resolution_status
                    == MessageQuoteResolutionStatus.NOT_FOUND.value
                    else "quote_routing_ambiguous"
                ),
            )
            return QuoteRouteDecision(fail_closed=True)

        source = quote_result.source_message
        if source is None:
            self._finish_quote_unresolved(
                session,
                event,
                resolution,
                conflict_code="QUOTE_SOURCE_MISSING",
                audit_type="QUOTE_SOURCE_MISSING",
            )
            return QuoteRouteDecision(fail_closed=True)

        source_resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id == source.message_id)
            .with_for_update()
        ).all()
        distinct_lead_ids = {
            item.lead_id for item in source_resolutions if item.lead_id is not None
        }
        if len(distinct_lead_ids) > 1:
            self._finish_quote_unresolved(
                session,
                event,
                resolution,
                conflict_code="QUOTE_TARGET_AMBIGUOUS",
                audit_type="QUOTE_TARGET_AMBIGUOUS",
            )
            return QuoteRouteDecision(fail_closed=True)

        if distinct_lead_ids:
            target_id = next(iter(distinct_lead_ids))
            target = session.get(Lead, target_id)
            if target is None or target.lifecycle_state == "discarded":
                self._finish_quote_unresolved(
                    session,
                    event,
                    resolution,
                    conflict_code="QUOTE_TARGET_MISSING",
                    audit_type="QUOTE_TARGET_MISSING",
                )
                return QuoteRouteDecision(fail_closed=True)
            if target.smart_table_owner_user_id != message.sales_user_id:
                # 引用关系只能复用当前销售仍拥有的本地业务事实，不能绕过权限边界。
                self._finish_quote_unresolved(
                    session,
                    event,
                    resolution,
                    conflict_code="QUOTE_TARGET_FORBIDDEN",
                    audit_type="QUOTE_TARGET_FORBIDDEN",
                )
                return QuoteRouteDecision(fail_closed=True)
            current_company = self._extract_reliable_company_identity(message, extractor)
            if current_company is not None and not self._lead_matches_company(
                target, current_company
            ):
                # 只有服务器确定性识别出的当前公司才触发冲突；AI 候选不会改变 quote 路由。
                self._finish_quote_unresolved(
                    session,
                    event,
                    resolution,
                    conflict_code="QUOTE_IDENTITY_CONFLICT",
                    audit_type="QUOTE_IDENTITY_CONFLICT",
                )
                return QuoteRouteDecision(fail_closed=True)
            # processing 也是真实的本地 Lead identity；外部投影状态不参与目标判断。
            self._mark_processing_assignment(session, event, target.id)
            return QuoteRouteDecision(target_lead=target)

        source_event = session.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.message_id == source.message_id)
            .with_for_update()
        )
        source_resolution = next(
            (item for item in source_resolutions if item.segment_index == 0), None
        )
        source_is_explicitly_unassigned = (
            source_resolution is not None
            and source_resolution.status == "unassigned"
            and source_resolution.lead_id is None
            and bool(source_resolutions)
            and all(
                item.status == "unassigned" and item.lead_id is None
                for item in source_resolutions
            )
        )
        if source_event is not None and source_event.status in {
            "pending",
            "retrying",
            "processing",
        }:
            # source 仍可由正常 Worker 推进，reply 应等待 source 形成稳定业务结论。
            logger.info("quote_routing_waiting_for_source_resolution")
            return QuoteRouteDecision(waiting_for_source=True)
        if not (
            source_event is not None
            and source_event.status == "succeeded"
            and source_is_explicitly_unassigned
        ):
            # source 已经结束或状态不可识别，但没有唯一 Lead identity / 明确 unassigned。
            self._finish_quote_unresolved(
                session,
                event,
                resolution,
                conflict_code="QUOTE_SOURCE_UNROUTABLE",
                audit_type="QUOTE_SOURCE_UNROUTABLE",
            )
            return QuoteRouteDecision(fail_closed=True)

        # source 已明确为 unassigned 时，quote relationship 固定双片段恢复边界。
        recovery_request = QuoteRecoveryExtractionRequest(
            quoted_source=QuoteRecoverySegment(
                message_id=source.message_id,
                normalized_text=source.normalized_text or "",
            ),
            current_reply=QuoteRecoverySegment(
                message_id=message.message_id,
                normalized_text=message.normalized_text or "",
            ),
        )
        segment_results = self._extract_quote_recovery_fields(recovery_request, extractor)
        fields: dict[str, object] = {}
        field_sources: dict[str, str] = {}
        enrichment: dict[str, str] = {}
        for segment_result in segment_results:
            # 同字段按 quote source 先于 current reply 的固定顺序保留，禁止静默覆盖。
            for field_name, value in segment_result.fields.items():
                if field_name not in fields:
                    fields[field_name] = value
                    field_sources[field_name] = segment_result.message_id
            for field_name, value in segment_result.enrichment.items():
                enrichment.setdefault(field_name, value)
        company_name = fields.get("线索名称")
        if not isinstance(company_name, str) or not company_name.strip():
            self._finish_quote_unresolved(
                session,
                event,
                resolution,
                conflict_code="QUOTE_RECOVERY_INCOMPLETE",
                audit_type="QUOTE_RECOVERY_INCOMPLETE",
            )
            return QuoteRouteDecision(fail_closed=True)

        target = session.scalar(
            select(Lead)
            .where(
                Lead.source_message_id == source.message_id,
                Lead.source_segment_index == 0,
            )
            .with_for_update()
        )
        if target is None:
            # source 原 resolution 只有 unassigned 事实时才会进入这里，避免重复创建 Lead。
            target = Lead(
                source_message_id=source.message_id,
                source_segment_index=0,
                original_capturing_sales_user_id=message.sales_user_id,
                smart_table_owner_user_id=message.sales_user_id,
                field_values={"线索来源": "展会", **fields},
                enrichment_values=enrichment,
            )
            session.add(target)
            session.flush()
            session.add(
                SmartTableSync(
                    lead_id=target.id,
                    source_message_id=source.message_id,
                    source_segment_index=0,
                )
            )
            for field_name, value in fields.items():
                session.add(
                    LeadFieldProvenance(
                        lead_id=target.id,
                        source_message_id=field_sources[field_name],
                        field_name=field_name,
                        value=serialize_field_value(value),
                        last_ai_synced_value=serialize_field_value(value),
                    )
                )

        assert source_resolution is not None
        previous_source_status = source_resolution.status
        source_resolution.lead_id = target.id
        source_resolution.status = "assigned"
        self._mark_processing_assignment(session, event, target.id)
        self._refresh_context(session, message, target.id)
        self._record_audit(
            session,
            event,
            "quoted_unassigned_message_recovered",
            details={
                "quoted_source_message_id": source.message_id,
                "lead_id": target.id,
                "previous_source_status": previous_source_status,
                "new_source_status": "assigned",
            },
        )
        return QuoteRouteDecision(target_lead=target, recovery_completed=True)

    def _extract_quote_recovery_fields(
        self,
        request: QuoteRecoveryExtractionRequest,
        extractor: DeterministicFirstTextLeadExtractor,
    ) -> tuple[QuoteRecoverySegmentResult, ...]:
        """按两个独立 quote recovery 片段提取字段并保守合并。

        参数：request 明确包含 quoted source 与 current reply 两个片段；extractor 提供本地规则。
        返回值：每个片段独立的字段和备注补充素材，调用方再按固定顺序合并。
        异常：AI Gateway 结构或传输错误向调用方传播，保证事务回滚而不留半状态。
        副作用：配置 AI Gateway 时每个非空片段调用一次字段提取；不向模型传入 Lead
        或 quoted source ID。
        """
        results: list[QuoteRecoverySegmentResult] = []
        for segment in (request.quoted_source, request.current_reply):
            # 每个片段单独进入提取器，禁止把 quote 原文拼接成没有边界的普通输入。
            deterministic_fields = extractor.extract_patch(segment.normalized_text)
            segment_fields: dict[str, object] = dict(deterministic_fields)
            segment_enrichment: dict[str, str] = {}
            if self._ai_gateway is not None and segment.normalized_text:
                # 确定性字段只作为已验证事实；AI 仍必须补齐同一片段的其它合法字段。
                patch = self._ai_gateway.extract_fields(
                    segment.normalized_text,
                    source_message_id=segment.message_id,
                    lead_id=None,
                )
                for field_name, value in patch.fields.items():
                    # 同一片段内服务器确定性字段优先，模型只补充未覆盖字段。
                    segment_fields.setdefault(field_name, value)
                segment_enrichment = dict(patch.enrichment)
                for prepared_segment in patch.segments:
                    # 单个 recovery segment 仍可能被模型包装成语义子段，字段仍属于当前消息。
                    for field_name, value in prepared_segment.patch.fields.items():
                        segment_fields.setdefault(field_name, value)
                    for field_name, value in prepared_segment.patch.enrichment.items():
                        segment_enrichment.setdefault(field_name, value)
            results.append(
                QuoteRecoverySegmentResult(
                    message_id=segment.message_id,
                    fields=segment_fields,
                    enrichment=segment_enrichment,
                )
            )
        return tuple(results)

    @staticmethod
    def _extract_reliable_company_identity(
        message: IncomingMessage, extractor: DeterministicFirstTextLeadExtractor
    ) -> str | None:
        """只从当前消息的服务器确定性解析结果提取公司身份。

        参数：message 为当前 reply；extractor 为既有窄范围确定性解析器。
        返回值：可靠公司名称；AI 候选、模糊文本和联系方式均返回 None。
        异常：无。
        副作用：无，不调用 AI 或外部服务。
        """
        fields = extractor.extract_patch(message.normalized_text)
        company = fields.get("线索名称")
        if isinstance(company, str) and company.strip():
            return company.strip()
        return None

    @staticmethod
    def _lead_matches_company(lead: Lead, company_name: str) -> bool:
        """比较当前可靠公司身份与 quote 目标 Lead 的已知公司名称。

        参数：lead 为 quote 唯一目标；company_name 为当前消息确定性公司名。
        返回值：当前名称等于 Lead 展示名或标准名时返回 True。
        异常：无。
        副作用：无，仅读取本地 Lead 字段。
        """
        known_names = {
            str(lead.field_values.get("线索名称") or "").strip(),
            str(lead.standard_company_name or "").strip(),
        }
        return company_name.strip() in {name for name in known_names if name}

    def _finish_quote_unresolved(
        self,
        session: Session,
        event: OutboxEvent,
        resolution: object,
        *,
        conflict_code: str | None = None,
        audit_type: str,
    ) -> None:
        """原子收敛 quote fail-closed 状态并写入当前消息待归属事实。

        参数：session/event 为当前事务；resolution 为 MessageQuoteResolution；
        conflict_code 为确定性冲突码；audit_type 为受控审计事件名。
        返回值：无。
        异常：数据库错误向调用方传播并回滚 quote、归属和审计写入。
        副作用：当前消息变为 unassigned，事件越过检查点且不复用 active context。
        """
        if not isinstance(resolution, MessageQuoteResolution):
            raise TypeError("quote resolution 类型非法")
        if conflict_code is not None:
            resolution.resolution_status = MessageQuoteResolutionStatus.CONFLICT.value
            resolution.conflict_code = conflict_code
        self._mark_unassigned(session, event)
        self._record_audit(
            session,
            event,
            audit_type,
            details={"candidate_count": resolution.candidate_count},
        )

    def _consume_once(
        self,
        outbox_event_id: int,
        *,
        claimed_for_processing: bool = False,
        recover_expired_lease: bool = False,
    ) -> LeadProcessingResult:
        """消费一条 T02 Outbox 文本事件并将首次有效线索同步到共享审核表。

        参数：outbox_event_id 为 T02 已提交的待处理事件标识；claimed_for_processing 表示
        当前 Worker 已唯一接管 processing；recover_expired_lease 表示只进入失联恢复。
        返回值：返回创建、忽略、拒绝、重复或表格同步失败等确定性结论。
        异常：事件或来源消息丢失时抛出 ValueError；数据库异常向调用方传播。
        副作用：可能创建 Lead、字段来源、同步结果、审计事件及智能表格记录。
        """
        log_token = None
        context_update: ContextUpdateRequest | None = None
        ai_review: AIReviewRequest | tuple[AIReviewRequest, ...] | None = None
        temporary_capture: LeadProcessingResult | None = None
        company_initial_command: CompanyUpsertCommand | None = None
        multi_company_fields: list[dict[str, str]] | None = None
        smart_table_recovery_sync: SmartTableSync | None = None
        quote_target_lead: Lead | None = None
        quote_recovery_completed = False
        context_lead: Lead | None = None
        try:
            with self._session_factory.begin() as session:
                event, message = self._load_event_and_message(session, outbox_event_id)
                log_token = bind_log_context(
                    message_id=message.message_id, wecom_user_id=message.sales_user_id
                )
                if event.sales_user_id != message.sales_user_id:
                    # Outbox 是可靠传输事实而非身份权威，来源消息的企业微信成员才可拥有记录。
                    event.status = "invalid"
                    self._record_audit(session, event, "outbox_sales_identity_mismatch")
                    logger.error("outbox_sales_identity_mismatch")
                    return LeadProcessingResult(LeadProcessingStatus.INVALID_EVENT)
                self._lock_sales_processing_stream(session, message.sales_user_id)
                # 销售顺序锁等待期间，其他事务可能已改变本事件状态，必须重读后再决策。
                session.refresh(event)
                terminal_or_unknown_statuses = COMPLETED_CHECKPOINT_STATUSES | {"processing"}
                if recover_expired_lease:
                    if event.status != "processing":
                        return self._processed_result(session, event)
                    # 调度器已唯一认领失联租约；保留既有人工复核语义而不重放外部调用。
                    event.status = "failed_pending_review"
                    event.failure_category = "unknown"
                    event.failure_summary = "processing_lease_expired"
                    event.failed_at = utc_now()
                    self._record_audit(session, event, "lead_outbox_processing_lease_expired")
                    logger.error("lead_outbox_processing_lease_expired")
                    return self._processed_result(session, event)
                if not claimed_for_processing and self._processing_lease_expired(event):
                    # 失联 Worker 不得永久占住该销售队列；未知外部结果保留给人工核验而不重放。
                    event.status = "failed_pending_review"
                    event.failure_category = "unknown"
                    event.failure_summary = "processing_lease_expired"
                    event.failed_at = utc_now()
                    self._record_audit(session, event, "lead_outbox_processing_lease_expired")
                    logger.error("lead_outbox_processing_lease_expired")
                    return self._processed_result(session, event)
                if not claimed_for_processing and event.status in terminal_or_unknown_statuses:
                    # ponytail: Adapter 无创建幂等键；未知结果待人工核验，接口提供键后再安全重试。
                    return self._processed_result(session, event)

                # Worker 二次校验销售身份后，quote matcher 才能在顺序等待前建立安全目标。
                if not self._sales_identity_provider.is_active(session, message.sales_user_id):
                    event.status = "unauthorized"
                    self._record_audit(session, event, "lead_actor_inactive")
                    logger.warning("lead_outbox_actor_inactive")
                    return LeadProcessingResult(LeadProcessingStatus.UNAUTHORIZED)

                extractor = DeterministicFirstTextLeadExtractor()
                quote_result = resolve_message_quote(session, message)
                if quote_result is not None:
                    quote_decision = self._prepare_quote_route(
                        session, event, message, quote_result, extractor
                    )
                    quote_target_lead = quote_decision.target_lead
                    quote_recovery_completed = quote_decision.recovery_completed
                    if quote_decision.waiting_for_source:
                        self._release_processing_claim(event)
                        return LeadProcessingResult(LeadProcessingStatus.WAITING_FOR_PREVIOUS)
                    if quote_decision.fail_closed:
                        return LeadProcessingResult(LeadProcessingStatus.QUOTE_UNRESOLVED)

                # 只有无 quote 或 quote 尚未唯一解析时才受旧消息顺序阻塞；
                # processing Lead 的本地 identity 已可靠固定，不被外部投影失败拖住。
                if quote_target_lead is None and self._has_unfinished_previous_event(
                    session, event
                ):
                    # 同一销售只能按持久化 sequence 前进，不能由 Worker 实际完成快慢决定归属。
                    logger.info("lead_outbox_waiting_for_previous")
                    return LeadProcessingResult(LeadProcessingStatus.WAITING_FOR_PREVIOUS)

                if self._media_enrichment_is_pending(session, message):
                    # 媒体接收与 Outbox 调度并发时，附件可能尚未落库；释放本次认领，
                    # 由下一轮调度在附件就绪后执行 OCR，不能占用 processing 租约至超时。
                    self._release_processing_claim(event)
                    logger.info("lead_outbox_waiting_for_media_enrichment")
                    return LeadProcessingResult(LeadProcessingStatus.WAITING_FOR_PREVIOUS)

                multi_fields = extractor.extract_many(message.normalized_text)
                if quote_target_lead is not None and len(multi_fields) > 1:
                    # quote 关系不能替代当前消息明确拆出的多个客户，无法安全选择单一目标。
                    quote_result = resolve_message_quote(session, message)
                    assert quote_result is not None
                    self._finish_quote_unresolved(
                        session,
                        event,
                        quote_result.resolution,
                        conflict_code="QUOTE_TARGET_AMBIGUOUS",
                        audit_type="QUOTE_TARGET_AMBIGUOUS",
                    )
                    return LeadProcessingResult(LeadProcessingStatus.QUOTE_UNRESOLVED)
                if len(multi_fields) <= 1 and extractor.has_ambiguous_multiple_companies(
                    message.normalized_text
                ):
                    # 多个公司候选未能按明确边界拆开时，宁可待归属也不能以最后字段覆盖前段事实。
                    self._mark_unassigned(session, event)
                    return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                if len(multi_fields) <= 1:
                    # 已有表格记录但 T09 后续步骤失败时，先恢复该同步事实，不能被上下文解析短路。
                    smart_table_recovery_sync = session.scalar(
                        select(SmartTableSync)
                        .where(
                            SmartTableSync.source_message_id == message.message_id,
                            SmartTableSync.status != "succeeded",
                            SmartTableSync.smart_table_record_id.is_not(None),
                        )
                        .order_by(SmartTableSync.id.asc())
                        .limit(1)
                    )
                if len(multi_fields) > 1 and self._company_lead_service is None:
                    # 多客户消息先在同一事务内固定所有分段事实，再按既有失败检查点逐条同步。
                    multi_request = self._prepare_multi_leads(
                        session, event, message, multi_fields, outbox_event_id
                    )
                elif len(multi_fields) > 1:
                    # 有 T10 时各分段必须先完成公司目标定位，禁止预建 SmartTableSync。
                    multi_request = None
                    multi_company_fields = multi_fields
                    event.status = "processing"
                    event.processing_started_at = utc_now()
                else:
                    multi_request = None
                extracted_patch = extractor.extract_patch(message.normalized_text)
                # 自由表达的公司身份由 AI 按语义提议；本地快速路径只读显式标签。
                leading_company_hint = extractor.extract_leading_company_hint(
                    message.normalized_text
                )
                explicit_new_lead_signal = self._has_explicit_new_lead_signal(
                    message.normalized_text
                )
                explicit_company_identity_evidence = (
                    self._has_explicit_company_identity_evidence(message.normalized_text)
                )
                # 明确公司身份可以切换客户；无公司名时先尊重当前会话，再回溯历史联系方式。
                strong_identity_match = False
                if quote_target_lead is not None:
                    # quote 唯一解析出的 Lead 是服务器事实，不能被 active context、
                    # 联系方式或 AI 改路由。
                    context_lead = quote_target_lead
                    strong_identity_match = True
                elif smart_table_recovery_sync is not None:
                    # T09 失败重试必须回到原 Lead，随后复用同一来源消息重新取得 AI 补丁。
                    context_lead = session.get(Lead, smart_table_recovery_sync.lead_id)
                elif multi_request is not None or multi_company_fields is not None:
                    context_lead = None
                else:
                    incoming_company_name = str(
                        extracted_patch.get("线索名称") or leading_company_hint or ""
                    ).strip()
                    if incoming_company_name:
                        # 公司身份只按公司名精确查找，不能被同一消息中的联系方式劫持。
                        context_lead = self._get_strong_identity_lead(
                            session, message, {"线索名称": incoming_company_name}
                        )
                        strong_identity_match = context_lead is not None
                        if context_lead is None:
                            active_context = self._get_active_context_lead(session, message)
                            if active_context is not None and (
                                active_context.lifecycle_state == "temporary"
                                and active_context.standard_company_name is None
                                or self._lead_matches_company(active_context, incoming_company_name)
                            ):
                                # 只升级无公司临时草稿，或恢复同公司但表格建行失败的 Lead。
                                context_lead = active_context
                    elif not explicit_new_lead_signal and not explicit_company_identity_evidence:
                        active_context = self._get_active_context_lead(session, message)
                        if active_context is not None:
                            context_lead = active_context
                            self._record_audit(
                                session,
                                event,
                                "lead_context_continuation_selected",
                                {
                                    "lead_id": active_context.id,
                                    "reason": "active_context_without_company",
                                },
                            )
                            contact_fields = {
                                field_name: value
                                for field_name, value in extracted_patch.items()
                                if field_name in {"联系人", "手机", "电话", "邮箱"}
                            }
                            historical_match = self._get_strong_identity_lead(
                                session, message, contact_fields
                            ) if contact_fields else None
                            if (
                                historical_match is not None
                                and historical_match.id != active_context.id
                            ):
                                self._record_audit(
                                    session,
                                    event,
                                    "historical_contact_match_ignored_for_active_context",
                                    {"lead_id": active_context.id},
                                )
                        else:
                            # 没有有效会话时，联系方式才允许回溯历史线索。
                            contact_fields = {
                                field_name: value
                                for field_name, value in extracted_patch.items()
                                if field_name in {"联系人", "手机", "电话", "邮箱"}
                            }
                            context_lead = self._get_strong_identity_lead(
                                session, message, contact_fields
                            ) if contact_fields else None
                            strong_identity_match = context_lead is not None
                    else:
                        # 显式新客户但没有公司身份时，不能静默跳转到历史联系方式命中的线索。
                        context_lead = None
                if (
                    context_lead is None
                    and multi_request is None
                    and multi_company_fields is None
                    and leading_company_hint is None
                    and not explicit_new_lead_signal
                ):
                    active_context = self._get_active_context_lead(session, message)
                    # 无公司名时沿用当前上下文；有公司名时只允许补全尚未入表的 temporary 草稿，
                    # 已有正式记录必须创建新的智能表格线索，不能借上下文做公司名查重。
                    if not extracted_patch.get("线索名称") or (
                        active_context is not None
                        and active_context.lifecycle_state == "temporary"
                        and active_context.standard_company_name is None
                    ):
                        context_lead = active_context
                if (
                    context_lead is not None
                    and context_lead.source_message_id == message.message_id
                    and context_lead.smart_table_record_id is None
                ):
                    # 同一来源消息的首次表格创建失败时，恢复原 Lead 的建档路径，不能把已推进的
                    # context 当成普通补充而返回 UPDATED；后续新消息仍保留该 context。
                    context_lead = None
                if (
                    multi_request is None
                    and multi_company_fields is None
                    and (not extracted_patch or leading_company_hint is None)
                    and message.normalized_text
                    and self._ai_gateway is not None
                    and not quote_recovery_completed
                ):
                    # 显式公司标签和多客户走确定性路径；无公司标签的自由/混合文本交由 T08 理解身份。
                    ai_review, ai_result = self._prepare_ai_review(
                        session,
                        event,
                        message,
                        context_lead,
                        company_name_hint=leading_company_hint,
                        quote_target_lead_id=(
                            quote_target_lead.id if quote_target_lead is not None else None
                        ),
                    )
                    if ai_result is not None:
                        if quote_target_lead is not None and ai_result.status in {
                            LeadProcessingStatus.IGNORED,
                            LeadProcessingStatus.UNASSIGNED,
                        }:
                            # quote 已经确定业务目标；当前文本没有可写补丁时仍必须完成消息归属。
                            self._mark_assigned(session, event, quote_target_lead.id)
                            self._refresh_context(session, message, quote_target_lead.id)
                            self._record_audit(
                                session, event, "quote_routing_assigned_without_patch"
                            )
                            return LeadProcessingResult(
                                LeadProcessingStatus.UPDATED,
                                lead_id=quote_target_lead.id,
                                smart_table_record_id=quote_target_lead.smart_table_record_id,
                            )
                        return ai_result
                if (
                    ai_review is None
                    and quote_target_lead is None
                    and smart_table_recovery_sync is None
                    and multi_request is None
                    and multi_company_fields is None
                    and not extracted_patch.get("线索名称")
                    and leading_company_hint is None
                    and (
                        self._is_weak_identity_fragment(message.normalized_text)
                        or (context_lead is not None and bool(extracted_patch))
                    )
                ):
                    # 无 AI 结果时也必须使用同一安全门，且只有有效上下文中的原文安全补充可继续。
                    active_context = self._get_active_context_lead(session, message)
                    safe_context_supplement = self._is_weak_context_fragment(
                        message.normalized_text, extracted_patch
                    )
                    rejection_reason = self._context_supplement_rejection_reason(
                        session,
                        message,
                        active_context,
                        context_lead,
                        extracted_patch,
                        safe_context_supplement,
                    )
                    if rejection_reason is not None:
                        self._record_audit(
                            session,
                            event,
                            "lead_update_identity_unassigned",
                            details={"reason": rejection_reason},
                        )
                        self._mark_unassigned(session, event)
                        return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                    # 有效上下文优先；否则仅保留此前唯一联系方式命中的 Lead。
                    context_lead = active_context or context_lead
                if (
                    context_lead is not None
                    and not extracted_patch
                    and ai_review is None
                    and not self._is_weak_identity_fragment(message.normalized_text)
                    and quote_target_lead is None
                ):
                    # 提前固定的上下文不能把普通问候升级成待归属消息；只有弱补充才继承线索。
                    event.status = "ignored"
                    self._record_audit(session, event, "lead_text_ignored")
                    logger.info("lead_text_ignored")
                    return LeadProcessingResult(LeadProcessingStatus.IGNORED)
                same_context_company = context_lead is not None and extracted_patch.get(
                    "线索名称"
                ) == context_lead.field_values.get("线索名称")
                temporary_context = (
                    self._company_lead_service is not None
                    and context_lead is not None
                    and context_lead.lifecycle_state == "temporary"
                    and context_lead.standard_company_name is None
                )
                if (
                    multi_request is None
                    and multi_company_fields is None
                    and ai_review is None
                    and smart_table_recovery_sync is None
                    and context_lead is not None
                    and (
                        "线索名称" not in extracted_patch
                        or same_context_company
                        or temporary_context
                    )
                ):
                    # 临时线索首次获得公司名时升级当前草稿；正式线索只接受相同公司名的补充。
                    context_patch = {
                        field_name: value
                        for field_name, value in extracted_patch.items()
                        if field_name != "线索名称" or temporary_context
                    }
                    if (
                        not context_patch
                        and not same_context_company
                        and not temporary_context
                        and quote_target_lead is None
                    ):
                        self._mark_unassigned(session, event)
                        return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

                    # 既有非空值不允许被碎片消息静默覆盖，只向当前线索补充空字段。
                    safe_patch = {
                        **self._pending_snapshot_fields(session, context_lead.id),
                        **self._only_empty_fields(context_lead, context_patch),
                    }
                    repaired_stale_company_name = False
                    context_company_name = context_patch.get("线索名称")
                    stored_company_name = context_lead.field_values.get("线索名称")
                    if (
                        temporary_context
                        and context_company_name
                        and self._is_stale_multiline_company_name(stored_company_name)
                        and context_company_name != stored_company_name
                    ):
                        # 只修复早期解析把整段多行表单吞进公司字段的临时草稿；正常非空值仍不可覆盖。
                        safe_patch["线索名称"] = context_company_name
                        repaired_stale_company_name = True
                    if not safe_patch and context_lead.smart_table_record_id is not None:
                        bind_log_context(lead_id=context_lead.id)
                        self._mark_assigned(session, event, context_lead.id)
                        self._refresh_context(session, message, context_lead.id)
                        logger.info("lead_message_assigned_to_current_context")
                        return LeadProcessingResult(
                            LeadProcessingStatus.UPDATED,
                            lead_id=context_lead.id,
                            smart_table_record_id=context_lead.smart_table_record_id,
                        )
                    if context_lead.smart_table_record_id is None:
                        if temporary_context:
                            # temporary 尚未进入审核表，补充只写后台事实与来源，绝不能误调用 T09。
                            values = dict(context_lead.field_values)
                            values.update(safe_patch)
                            context_lead.field_values = values
                            for field_name, value in safe_patch.items():
                                session.add(
                                    LeadFieldProvenance(
                                        lead_id=context_lead.id,
                                        source_message_id=message.message_id,
                                        field_name=field_name,
                                        value=serialize_field_value(value),
                                    )
                                )
                            self._mark_assigned(session, event, context_lead.id)
                            self._refresh_context(session, message, context_lead.id)
                            self._record_audit(
                                session,
                                event,
                                "temporary_lead_updated_before_company_resolution",
                            )
                            if repaired_stale_company_name:
                                # 记录可审计的历史解析修复事实，不保存原始表单内容。
                                self._record_audit(
                                    session,
                                    event,
                                    "temporary_lead_stale_company_name_repaired",
                                )
                            return LeadProcessingResult(
                                LeadProcessingStatus.UPDATED,
                                lead_id=context_lead.id,
                            )
                        # 已有 Lead 但远端行缺失时，复用 T09 的建行与补写事务，不能永久化同步失败。
                        event.status = "processing"
                        event.processing_started_at = utc_now()
                        self._mark_processing_assignment(session, event, context_lead.id)
                        bind_log_context(lead_id=context_lead.id)
                        recovery_fields = _normalize_company_patch(
                            {**context_lead.field_values, **safe_patch}
                        )
                        ai_review = AIReviewRequest(
                            source_message_id=message.message_id,
                            sales_user_id=message.sales_user_id,
                            lead_id=context_lead.id,
                            outbox_event_id=outbox_event_id,
                            patch=ExtractedLeadPatch(
                                trace_id=f"deterministic-recovery-{outbox_event_id}",
                                analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                                fields=recovery_fields,
                                pending_confirmation_fields=(),
                                low_confidence_candidates={},
                            ),
                            creates_lead=False,
                        )
                    else:
                        event.status = "processing"
                        event.processing_started_at = utc_now()
                        self._mark_processing_assignment(session, event, context_lead.id)
                        bind_log_context(lead_id=context_lead.id)
                        # 外部表格调用必须等本事务提交后执行，避免在销售顺序锁内等待网络。
                        context_update = ContextUpdateRequest(
                            source_message_id=message.message_id,
                            lead_id=context_lead.id,
                            record_id=context_lead.smart_table_record_id,
                            fields=safe_patch,
                            outbox_event_id=outbox_event_id,
                        )

                if (
                    multi_request is None
                    and multi_company_fields is None
                    and context_update is None
                    and ai_review is None
                    and smart_table_recovery_sync is None
                    and self._company_lead_service is not None
                    and extracted_patch.get("线索名称")
                ):
                    # 有公司候选时先做 T10 销售内身份决策，禁止先生成临时 Lead 或表格 record。
                    company_initial_command = CompanyUpsertCommand(
                        source_message_id=message.message_id,
                        sales_user_id=message.sales_user_id,
                        fields={**extracted_patch, "线索来源": "展会"},
                        existing_lead_id=(
                            context_lead.id
                            if strong_identity_match and context_lead is not None
                            else None
                        ),
                        region_evidence=CompanyRegionEvidence(
                            message_text=message.normalized_text,
                            company_name=extracted_patch.get("线索名称"),
                            email=extracted_patch.get("邮箱"),
                            phone=extracted_patch.get("手机") or extracted_patch.get("电话"),
                        ),
                        defer_smart_table_sync=True,
                    )
                    event.status = "processing"
                    event.processing_started_at = utc_now()

                if (
                    multi_request is None
                    and multi_company_fields is None
                    and context_update is None
                    and ai_review is None
                    and company_initial_command is None
                ):
                    if explicit_new_lead_signal and not extracted_patch.get("线索名称"):
                        # 明确切换客户但没有公司身份时，联系方式不足以安全创建或定位线索。
                        self._mark_unassigned(session, event)
                        return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                    existing_lead = session.scalar(
                        select(Lead).where(Lead.source_message_id == message.message_id)
                    )
                    if existing_lead is None:
                        fields = extractor.extract(message.normalized_text)
                        if fields is None:
                            if extracted_patch:
                                # 联系人与联系方式同样是可审核客户事实，先保存为不能提交的临时线索。
                                fields = extracted_patch
                            elif self._is_weak_identity_fragment(message.normalized_text):
                                self._mark_unassigned(session, event)
                                return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                            else:
                                event.status = "ignored"
                                self._record_audit(session, event, "lead_text_ignored")
                                logger.info("lead_text_ignored")
                                return LeadProcessingResult(LeadProcessingStatus.IGNORED)

                        # 首次创建的两类销售归属同时固定为当前有效 actor，后续转交不在 T05 范围内。
                        lead = Lead(
                            source_message_id=message.message_id,
                            original_capturing_sales_user_id=message.sales_user_id,
                            smart_table_owner_user_id=message.sales_user_id,
                            field_values={**fields, "线索来源": "展会"},
                        )
                        session.add(lead)
                        session.flush()
                        for field_name, value in fields.items():
                            session.add(
                                LeadFieldProvenance(
                                    lead_id=lead.id,
                                    source_message_id=message.message_id,
                                    field_name=field_name,
                                    value=serialize_field_value(value),
                                    # 确定性首录同样由系统写表，保存基线。
                                    # T09 后续读取该基线以识别销售人工编辑。
                                    last_ai_synced_value=serialize_field_value(value),
                                )
                            )
                        self._record_audit(session, event, "lead_created")
                        if self._company_lead_service is not None and not fields.get("线索名称"):
                            # 无可靠公司身份只保留后台事实；A' 禁止提前产生表格副作用。
                            self._mark_assigned(session, event, lead.id)
                            self._refresh_context(session, message, lead.id)
                            self._record_audit(
                                session, event, "temporary_lead_smart_table_deferred"
                            )
                            temporary_capture = LeadProcessingResult(
                                LeadProcessingStatus.CREATED,
                                lead_id=lead.id,
                            )
                        else:
                            session.add(
                                SmartTableSync(
                                    lead_id=lead.id, source_message_id=message.message_id
                                )
                            )
                    else:
                        # 重试只使用持久化字段快照，禁止重新解析并改变首次线索的业务事实。
                        lead = existing_lead
                        fields = dict(lead.field_values)
                    if temporary_capture is None:
                        event.status = "processing"
                        event.processing_started_at = utc_now()
                        bind_log_context(lead_id=lead.id)
                        # 会话提交后 ORM 对象会脱离；只将下一步需要的不可变标识带出事务。
                        sales_user_id = message.sales_user_id
                        lead_id = lead.id

            if multi_company_fields is not None:
                return self._consume_multi_companies_before_smart_table(
                    outbox_event_id, multi_company_fields
                )
            if multi_request is not None:
                return self._create_multi_smart_table_records(multi_request)
            if ai_review is not None:
                if isinstance(ai_review, tuple):
                    return self._sync_ai_reviews(ai_review)
                return self._sync_ai_review(ai_review)
            if context_update is not None:
                return self._update_smart_table_record(context_update)
            if temporary_capture is not None:
                return temporary_capture
            if company_initial_command is not None:
                return self._consume_initial_company_before_smart_table(
                    outbox_event_id, company_initial_command
                )
            assert fields is not None
            return self._create_smart_table_record(sales_user_id, fields, lead_id, outbox_event_id)
        finally:
            if log_token is not None:
                reset_log_context(log_token)

    def _prepare_ai_review(
        self,
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        context_lead: Lead | None,
        company_name_hint: str | None = None,
        quote_target_lead_id: str | None = None,
    ) -> tuple[
        AIReviewRequest | tuple[AIReviewRequest, ...] | None,
        LeadProcessingResult | None,
    ]:
        """调用 T08 并以确定性规则决定安全的新增、更新或待归属结论。

        参数：session、event 和 message 为当前有序消费事实；
        context_lead 为 T07 已可靠定位的当前线索；公司提示仅来自显式公司标签；
        quote_target_lead_id 为已由服务器 quote relationship 确定的唯一目标。
        返回值：可在提交后执行的 T09 请求，或已完成的消费结果；两者不会同时存在。
        异常：无；T08 失败被转换为明确的失败待审事实。
        副作用：可能创建最小 Lead 草稿、登记审计并将事件置为 processing 或 failed_pending_review。
        """
        assert self._ai_gateway is not None
        # 保留 T07 在 AI 调用前确定的上下文；模型返回新身份候选时不能直接抹掉这份关系事实。
        active_context_lead = context_lead
        use_active_context = False
        try:
            patch = self._ai_gateway.extract_fields(
                message.normalized_text or "",
                source_message_id=message.message_id,
                lead_id=active_context_lead.id if active_context_lead is not None else None,
                context_fields=self._context_fields_for_ai(active_context_lead),
            )
            company_identity_claimed = self._ai_company_identity_claimed(patch)
            if company_name_hint:
                # 确定性原文公司身份始终优先，模型的 UPDATE_LEAD 或冲突候选不能改写客户边界。
                fields = {**patch.fields, "线索名称": company_name_hint}
                analysis = patch.analysis.model_copy(
                    update={
                        "crm_fields": {
                            **patch.analysis.crm_fields,
                            "线索名称": company_name_hint,
                        },
                        "confidence_by_field": {
                            **patch.analysis.confidence_by_field,
                            "线索名称": 1.0,
                        },
                    }
                )
                patch = replace(patch, analysis=analysis, fields=fields)
                company_identity_claimed = True
            patch = self._ground_source_identity_patch(patch, message.normalized_text)
        except AIGatewayError as error:
            # 网关已完成自身传输重试；此处绝不伪造建档成功，也不能阻塞该销售的后续消息。
            event.status = "failed_pending_review"
            event.failure_category = classify_task_failure(error).value
            event.failure_summary = safe_failure_summary(error)
            event.failed_at = utc_now()
            self._record_audit(session, event, "ai_gateway_failed_pending_review")
            self._record_lead_processing_failed_notice(session, message)
            logger.exception(
                "ai_gateway_first_text_failed", extra={"error_type": type(error).__name__}
            )
            return None, LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED)

        if (
            quote_target_lead_id is None
            and company_identity_claimed
            and not patch.fields.get("线索名称")
        ):
            # AI 提出了公司身份但当前消息无法验证时，不能把失败候选退化成 active context 补充。
            self._record_audit(
                session,
                event,
                "lead_company_identity_unverified_fail_closed",
                details={"reason": "company_candidate_not_grounded"},
            )
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

        explicit_new_lead_signal = self._has_explicit_new_lead_signal(message.normalized_text)
        if quote_target_lead_id is not None and (
            patch.segments
            or patch.analysis.intent in {"MULTI_LEAD", "MULTI_LEAD_AMBIGUOUS"}
        ):
            # quote 已固定单一 Lead；模型返回多客户结果不能扩展目标或创建其他 Lead。
            quote_resolution = session.scalar(
                select(MessageQuoteResolution).where(
                    MessageQuoteResolution.current_message_id == message.message_id
                )
            )
            if quote_resolution is not None:
                self._finish_quote_unresolved(
                    session,
                    event,
                    quote_resolution,
                    conflict_code="QUOTE_TARGET_AMBIGUOUS",
                    audit_type="QUOTE_TARGET_AMBIGUOUS",
                )
            return None, LeadProcessingResult(LeadProcessingStatus.QUOTE_UNRESOLVED)
        if patch.analysis.intent == "MULTI_LEAD_AMBIGUOUS":
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
        if patch.segments:
            # 多客户分段只携带候选事实，实际目标仍由本服务按公司、上下文和联系方式解析。
            segment_reviews = self._prepare_ai_segment_reviews(
                session,
                event,
                message,
                context_lead,
                patch,
                explicit_new_lead_signal,
            )
            if segment_reviews is None:
                self._mark_unassigned(session, event)
                return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
            return segment_reviews, None
        incoming_company_candidate = patch.fields.get("线索名称")
        if (
            quote_target_lead_id is None
            and self._has_explicit_company_identity_evidence(message.normalized_text)
            and not isinstance(incoming_company_candidate, str)
        ):
            # 文本明确指向公司但提取器与模型都未能确认名称时，不能用联系人或上下文猜目标。
            self._record_audit(
                session,
                event,
                "lead_company_identity_unresolved_fail_closed",
                details={"reason": "company_name_not_confirmed"},
            )
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
        has_reliable_company_evidence = (
            company_name_hint is not None
            or bool(incoming_company_candidate)
        )
        if (
            active_context_lead is not None
            and not explicit_new_lead_signal
            and not has_reliable_company_evidence
            and "线索名称" in patch.fields
        ):
            # 没有原文公司证据时，模型猜出的公司名不能抢占当前销售上下文。
            patch = self._drop_unreliable_company_candidate(patch, message.normalized_text)
        safe_context_supplement = self._is_weak_context_fragment(
            message.normalized_text, patch.fields, patch.enrichment
        )
        rejection_reason = (
            self._context_supplement_rejection_reason(
                session,
                message,
                active_context_lead,
                context_lead,
                patch.fields,
                safe_context_supplement,
            )
            if quote_target_lead_id is None and not patch.fields.get("线索名称")
            else None
        )
        if (
            quote_target_lead_id is None
            and not patch.fields.get("线索名称")
            and rejection_reason is not None
            and patch.analysis.intent in {"UPDATE_LEAD", "IGNORE"}
        ):
            # UPDATE/IGNORE 意图都不是归属证据；无公司身份时由同一安全门裁决目标。
            self._record_audit(
                session,
                event,
                "lead_update_identity_unassigned",
                details={"reason": rejection_reason},
            )
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
        media_context_continuation = (
            active_context_lead is not None
            and message.requires_media_enrichment
            and not explicit_new_lead_signal
            and not patch.fields.get("线索名称")
            and safe_context_supplement
        )
        if patch.analysis.intent == "IGNORE":
            if (
                active_context_lead is None
                or not safe_context_supplement
                or explicit_new_lead_signal
            ):
                event.status = "ignored"
                self._record_audit(session, event, "lead_text_ignored")
                logger.info("lead_text_ignored")
                return None, LeadProcessingResult(LeadProcessingStatus.IGNORED)
            # 当前客户上下文内的数量、预算和需求补充不能因模型过于保守而丢失。
            patch = self._promote_ignored_context_fragment(patch, message.normalized_text)
            use_active_context = True
            context_lead = active_context_lead
            logger.info("ai_context_continuation_selected", extra={"reason": "ignored_fragment"})
        if patch.analysis.intent in {"MULTI_LEAD", "MULTI_LEAD_AMBIGUOUS"}:
            # T07 只支持明确标签切分；模型声称多客户时不猜测边界或创建多条线索。
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

        fields = patch.fields
        # 正式公司名是确定性的新线索边界；不能由模型的 UPDATE_LEAD 意图覆盖。
        incoming_company_name = str(fields.get("线索名称") or "").strip()
        ai_identity_fields = {
            field_name: value
            for field_name, value in fields.items()
            if field_name in {"线索名称", "手机", "电话", "邮箱"} and value
        }
        contact_identity_fields = {
            field_name: value
            for field_name, value in ai_identity_fields.items()
            if field_name != "线索名称" and isinstance(value, str)
        }
        strong_identity_lead = None
        if (
            quote_target_lead_id is not None and active_context_lead is not None
        ) or media_context_continuation or (
            active_context_lead is not None
            and not explicit_new_lead_signal
            and not incoming_company_name
            and patch.analysis.intent == "UPDATE_LEAD"
        ):
            # 只有前置安全门认可的无身份字段补充才能沿用当前客户上下文。
            assert active_context_lead is not None
            context_lead = active_context_lead
            use_active_context = True
            if safe_context_supplement:
                # 弱片段中的“采购10台左右”等伪公司名不是当前消息事实，禁止覆盖真实线索名称。
                patch = self._drop_unreliable_company_candidate(patch, message.normalized_text)
                logger.info("ai_context_continuation_selected", extra={"reason": "weak_fragment"})
            elif media_context_continuation:
                # 名片通常是当前客户的联系方式补充；只有销售明确表达新客户时才启动新建分支。
                logger.info("ai_context_continuation_selected", extra={"reason": "media_message"})
            else:
                logger.info("ai_context_continuation_selected", extra={"reason": "ai_update"})
            if contact_identity_fields:
                historical_match = self._get_strong_identity_lead(
                    session, message, contact_identity_fields
                )
                if historical_match is not None and historical_match.id != active_context_lead.id:
                    self._record_audit(
                        session,
                        event,
                        "historical_contact_match_ignored_for_active_context",
                        {"lead_id": active_context_lead.id},
                    )
        elif incoming_company_name:
            # 普通文本明确公司身份时，联系方式不能改变公司身份的精确匹配结果。
            strong_identity_lead = self._get_strong_identity_lead(
                session, message, {"线索名称": incoming_company_name}
            )
            context_lead = strong_identity_lead
        elif (
            not explicit_new_lead_signal
            and patch.analysis.intent == "UPDATE_LEAD"
            and contact_identity_fields
        ):
            # 没有有效上下文时，联系方式才允许回溯当前销售历史线索。
            strong_identity_lead = self._get_strong_identity_lead(
                session, message, contact_identity_fields
            )
            if strong_identity_lead is not None:
                context_lead = strong_identity_lead
        elif ai_identity_fields:
            # 有明确 AI 身份且没有可靠历史命中时，继续按新线索处理，避免串入旧客户。
            if (
                active_context_lead is not None
                and incoming_company_name
                and not safe_context_supplement
                and not media_context_continuation
                and not explicit_new_lead_signal
            ):
                # 仅记录受控事实，不把公司名称或其他客户内容写入容器日志。
                self._record_audit(
                    session,
                    event,
                    "ai_company_identity_overrode_active_context",
                    {"active_context_present": True, "new_company_identity_present": True},
                )
            context_lead = None
        elif (
            active_context_lead is not None
            and quote_target_lead_id is None
            and not explicit_new_lead_signal
            and patch.analysis.intent != "IGNORE"
        ):
            # AI 已判为新客户但没有可靠公司身份时不能降级成旧客户补充。
            context_lead = None
        else:
            # 显式新客户但没有可核验公司身份时，不继承旧客户上下文。
            context_lead = None
        # 弱片段可能在上一步移除了模型伪造的身份字段，后续逻辑必须使用清理后的补丁。
        fields = patch.fields
        same_context_company = context_lead is not None and fields.get(
            "线索名称"
        ) == context_lead.field_values.get("线索名称")
        if (
            patch.analysis.intent == "UPDATE_LEAD"
            or same_context_company
            or use_active_context
            or strong_identity_lead is not None
        ):
            if context_lead is None:
                if not ai_identity_fields:
                    # 没有可核验的公司、联系方式时仍不能由模型猜测更新目标。
                    self._mark_unassigned(session, event)
                    return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                # AI 可能把没有历史上下文的自然语言首条消息标成 UPDATE；强身份未命中旧线索时，
                # 按新线索继续，避免要求销售使用固定话术，也避免把消息丢进待归属队列。
                self._record_audit(session, event, "ai_update_without_context_fallback_to_new_lead")
                logger.info("ai_update_without_context_fallback_to_new_lead")
            else:
                event.status = "processing"
                event.processing_started_at = utc_now()
                # 精确公司身份已定位到既有线索时，即使展示名已被 TYC 标准化，也必须沿更新边界继续。
                self._mark_processing_assignment(session, event, context_lead.id)
                bind_log_context(lead_id=context_lead.id)
                return (
                    AIReviewRequest(
                        source_message_id=message.message_id,
                        sales_user_id=message.sales_user_id,
                        lead_id=context_lead.id,
                        outbox_event_id=event.id,
                        patch=patch,
                        creates_lead=False,
                    ),
                    None,
                )

        if not fields.get("线索名称"):
            # NEW_LEAD 也必须有已通过 T08 校验且非低置信度的公司名，才允许正式创建草稿。
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

        lead = session.scalar(select(Lead).where(Lead.source_message_id == message.message_id))
        if lead is None:
            # 身份与负责人完全取自当前有效 actor；模型字段只会交给 T09 作为业务字段补丁。
            lead = Lead(
                source_message_id=message.message_id,
                original_capturing_sales_user_id=message.sales_user_id,
                smart_table_owner_user_id=message.sales_user_id,
                field_values={"线索来源": "展会"},
            )
            session.add(lead)
            session.flush()
            session.add(SmartTableSync(lead_id=lead.id, source_message_id=message.message_id))
            self._record_audit(session, event, "ai_lead_created")
        event.status = "processing"
        event.processing_started_at = utc_now()
        bind_log_context(lead_id=lead.id)
        return (
            AIReviewRequest(
                source_message_id=message.message_id,
                sales_user_id=message.sales_user_id,
                lead_id=lead.id,
                outbox_event_id=event.id,
                patch=patch,
                creates_lead=True,
            ),
            None,
        )

    def _prepare_ai_segment_reviews(
        self,
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        context_lead: Lead | None,
        patch: ExtractedLeadPatch,
        explicit_new_lead_signal: bool,
    ) -> tuple[AIReviewRequest, ...] | None:
        """把 AI 语义分段转换为多个独立请求，但不接受模型指定 Lead 目标。"""
        if len(patch.segments) > 1 and any(
            not segment.patch.fields.get("线索名称") for segment in patch.segments
        ):
            # 多客户中没有公司身份的分段无法安全继承同一个 context，整体 fail closed。
            return None
        requests: list[AIReviewRequest] = []
        for segment in sorted(patch.segments, key=lambda item: item.segment_index):
            segment_fields = dict(segment.patch.fields)
            company_name = str(segment_fields.get("线索名称") or "").strip()
            contact_fields = {
                field_name: value
                for field_name, value in segment_fields.items()
                if field_name in {"手机", "电话", "邮箱"} and isinstance(value, str)
            }
            target: Lead | None = None
            creates_lead = False
            if company_name:
                # 公司候选已由网关验证来自当前 segment 原文；服务器只按公司身份查历史目标。
                target = self._get_strong_identity_lead(
                    session, message, {"线索名称": company_name}
                )
            elif not explicit_new_lead_signal and len(patch.segments) == 1:
                # 单 segment 没有新公司时才允许沿用当前有效上下文。
                target = context_lead or (
                    self._get_strong_identity_lead(session, message, contact_fields)
                    if contact_fields
                    else None
                )
            elif not explicit_new_lead_signal and contact_fields:
                # 多 segment 无公司时只接受联系方式的唯一历史命中，不复制当前上下文。
                target = self._get_strong_identity_lead(session, message, contact_fields)
            if target is None:
                if not company_name:
                    return None
                target = session.scalar(
                    select(Lead).where(
                        Lead.source_message_id == message.message_id,
                        Lead.source_segment_index == segment.segment_index,
                    )
                )
                if target is None:
                    target = Lead(
                        source_message_id=message.message_id,
                        source_segment_index=segment.segment_index,
                        original_capturing_sales_user_id=message.sales_user_id,
                        smart_table_owner_user_id=message.sales_user_id,
                        field_values={"线索来源": "展会"},
                    )
                    session.add(target)
                    session.flush()
                    session.add(
                        SmartTableSync(
                            lead_id=target.id,
                            source_message_id=message.message_id,
                            source_segment_index=segment.segment_index,
                        )
                    )
                    self._record_audit(session, event, "ai_lead_segment_created")
                creates_lead = True
            else:
                self._mark_processing_assignment(
                    session, event, target.id, segment.segment_index
                )
            event.status = "processing"
            event.processing_started_at = utc_now()
            requests.append(
                AIReviewRequest(
                    source_message_id=message.message_id,
                    sales_user_id=message.sales_user_id,
                    lead_id=target.id,
                    outbox_event_id=event.id,
                    patch=segment.patch,
                    creates_lead=creates_lead,
                    segment_index=segment.segment_index,
                )
            )
        return tuple(requests)

    def _sync_ai_reviews(
        self, requests: tuple[AIReviewRequest, ...]
    ) -> LeadProcessingResult:
        """按 segment 顺序同步多个 AI 请求，并保留最后一个 segment 的会话上下文。"""
        results: list[LeadProcessingResult] = []
        for index, request in enumerate(requests):
            result = self._sync_ai_review(request, refresh_context=index == len(requests) - 1)
            if result.status is LeadProcessingStatus.SYNC_FAILED:
                return LeadProcessingResult(
                    LeadProcessingStatus.SYNC_FAILED,
                    lead_ids=tuple(
                        lead_id
                        for item in results
                        for lead_id in (item.lead_ids or ((item.lead_id,) if item.lead_id else ()))
                    ),
                )
            results.append(result)
        lead_ids = tuple(
            lead_id
            for item in results
            for lead_id in (item.lead_ids or ((item.lead_id,) if item.lead_id else ()))
        )
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED
            if any(item.status is LeadProcessingStatus.CREATED for item in results)
            else LeadProcessingStatus.UPDATED,
            lead_id=lead_ids[0] if lead_ids else None,
            smart_table_record_id=next(
                (item.smart_table_record_id for item in results if item.smart_table_record_id),
                None,
            ),
            lead_ids=lead_ids,
        )

    def _sync_ai_review(
        self, request: AIReviewRequest, *, refresh_context: bool = True
    ) -> LeadProcessingResult:
        """为 AI 已安全决定的目标创建审核记录（如需）并经 T09 写入字段补丁。

        参数：request 为事务提交后可执行的 AI 结果及真实销售身份。
        返回值：首次建档返回 CREATED，既有上下文补充返回 UPDATED，外部失败返回 SYNC_FAILED。
        异常：关键数据库事实缺失时抛出 ValueError；其他外部失败转为失败待审事实。
        副作用：可能创建智能表格记录、调用 T09，并完成消息归属和当前客户上下文。
        """
        # 先固定业务归属和销售会话，再允许 T09/Smart Table 产生任何外部副作用。
        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, request.outbox_event_id)
            self._pin_message_lead_before_external_sync(
                session,
                event,
                message,
                request.lead_id,
                request.segment_index,
                refresh_context=refresh_context,
            )
        record_id, initial_record, preserve_record = self._ensure_ai_review_record(request)
        if record_id is None:
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=request.lead_id)
        try:
            # T09 负责人工编辑保护、中置信度 AI待确认与增量写入；不得由本层直接写模型字段。
            self._sync_ai_review_patch(
                request,
                initial_record=initial_record,
                allow_recreate=not preserve_record,
            )
        except Exception as error:
            self._record_ai_review_sync_failure(
                request,
                error,
                field_patch_pending=preserve_record,
            )
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=request.lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, request.outbox_event_id)
            lead = session.get(Lead, request.lead_id)
            if lead is None:
                raise ValueError(f"AI 审核线索不存在：{request.lead_id}")
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead.id))
            if request.creates_lead and sync is None:
                raise ValueError(f"AI 审核同步事实不存在：{lead.id}")
            first_success = (
                sync is not None
                and sync.status != "succeeded"
                and sync.completed_at is None
            )
            if sync is not None and sync.status != "succeeded":
                sync.status = "succeeded"
                sync.completed_at = sync.completed_at or utc_now()
            if sync is not None:
                sync.error_summary = None
            self._mark_assigned(session, event, lead.id, request.segment_index)
            if refresh_context:
                self._refresh_context(session, message, lead.id)
            self._record_audit(session, event, "ai_review_fields_synced")
            # AI 审核只有在远端字段补丁和本地成功事实同事务完成后才可发行首次链接。
            if first_success and sync is not None:
                self._queue_first_success_notification(
                    session, event, message, lead, sync, request.segment_index
                )
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED if request.creates_lead else LeadProcessingStatus.UPDATED,
            lead_id=request.lead_id,
            smart_table_record_id=record_id,
        )

    def _sync_ai_review_patch(
        self,
        request: AIReviewRequest,
        *,
        protected_supplement: bool = False,
        acknowledged_but_unverified_fields: tuple[str, ...] = (),
        initial_record: SmartTableRecord | None = None,
        allow_recreate: bool = True,
    ) -> ReviewSyncResult:
        """以同一份已校验补丁完成一次有限的表格恢复与重试。

        参数：request 为已持久化来源消息、目标线索和 T08 补丁；initial_record 为刚创建的远端快照；
        allow_recreate 表示远端明确缺失时是否允许恢复创建。
        返回值：T09 实际写入和保护字段结果。
        异常：普通流程远端记录缺失时最多重建一次；受保护历史补充遇到远端行缺失时直接失败；
        暂态适配器失败时最多重试一次，其他异常由调用方记录失败事实。
        副作用：普通首次流程可能重建远端审核行；受保护补充仅更新已绑定的原行。
        """
        recovered_missing_record = False
        retried_transient_failure = False
        while True:
            try:
                return LeadReviewService(
                    self._session_factory,
                    self._smart_table_adapter,
                    robot_submission_confirmation_available=self._robot_submission_confirmation_available,
                ).sync_ai_patch(
                    request.lead_id,
                    request.source_message_id,
                    request.patch,
                    protected_supplement=protected_supplement,
                    acknowledged_but_unverified_fields=acknowledged_but_unverified_fields,
                    initial_record=initial_record,
                )
            except SmartTableRecordNotFoundError:
                if recovered_missing_record:
                    raise
                if protected_supplement or not allow_recreate:
                    # 历史消息恢复只能补同一行，远端行缺失时不得 delete/recreate。
                    raise
                # 本地仍有完整后台事实时，只对已确认远端不存在的行重建一次，再复用同一补丁。
                self._recreate_missing_ai_review_record(request)
                recovered_missing_record = True
            except RetryableTaskFailure:
                if retried_transient_failure:
                    raise
                # update/list 是幂等操作；直接重试同一补丁，避免因一次 CLI 进程抖动留下半行记录。
                retried_transient_failure = True
                # 首次 ACK 可能已经落地；重试必须重新读取远端当前事实，而不是复用旧快照。
                initial_record = None
                logger.warning("ai_review_sync_retrying_same_patch")

    def _recreate_missing_ai_review_record(self, request: AIReviewRequest) -> None:
        """重建本地仍有事实但远端已不存在的智能表格审核行。

        参数：request 为当前 AI 审核请求，用于定位线索、来源事件和销售身份。
        返回值：无。
        异常：线索或同步事实缺失、表格创建失败时抛出异常。
        副作用：以当前负责人重建一行，更新本地 record_id，并写入恢复审计。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, request.lead_id)
            sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
            )
            if lead is None or sync is None:
                raise ValueError(f"智能表格恢复事实不存在：{request.lead_id}")
            old_record_id = lead.smart_table_record_id
            fields: dict[str, object] = {
                **DEFAULT_SMART_TABLE_FIELD_VALUES,
                **lead.field_values,
                "线索来源": lead.field_values.get("线索来源", "展会"),
                # 机器人重建审核记录代表重新产生待提交变更，状态必须从头开始。
                "提交状态": "未提交",
                "创建人": lead.smart_table_owner_user_id,
                "负责人": lead.smart_table_owner_user_id,
            }

        record = self._smart_table_adapter.create_record(fields, actor=SmartTableActor.ROBOT)
        with self._session_factory.begin() as session:
            lead = session.get(Lead, request.lead_id)
            sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
            )
            if lead is None or sync is None:
                raise ValueError(f"智能表格恢复事实不存在：{request.lead_id}")
            lead.smart_table_record_id = record.record_id
            sync.smart_table_record_id = record.record_id
            sync.status = "processing"
            event, _ = self._load_event_and_message(session, request.outbox_event_id)
            self._record_audit(
                session,
                event,
                "smart_table_record_recreated_after_missing",
                details={"previous_record_id": old_record_id, "new_record_id": record.record_id},
            )
        logger.warning("smart_table_record_recreated_after_missing")

    def _record_ai_review_sync_failure(
        self,
        request: AIReviewRequest,
        error: Exception,
        *,
        field_patch_pending: bool = False,
    ) -> None:
        """记录 T09 失败并按异常类别决定重试或人工检查点。

        参数：request 为当前审核请求；error 为最终一次同步异常；
        field_patch_pending 表示远端行已绑定、仅待补写业务字段，必须保留为可恢复任务。
        返回值：无。
        异常：数据库写入失败时向调用方传播。
        副作用：更新 Outbox、智能表格同步状态和业务审计，不伪造成功。
        """
        with self._session_factory.begin() as session:
            event, _ = self._load_event_and_message(session, request.outbox_event_id)
            sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
            )
            remote_record_id = getattr(error, "remote_record_id", None)
            if isinstance(remote_record_id, str) and remote_record_id:
                # add 已由远端确认但字段核实失败时，冻结该 ID，后续只能恢复原行。
                lead = session.get(Lead, request.lead_id)
                if lead is not None:
                    lead.smart_table_record_id = remote_record_id
                if sync is not None:
                    sync.smart_table_record_id = remote_record_id
            failed_pending_review = self._record_sync_failure(
                session,
                event,
                retrying_event_type="ai_review_sync_retrying",
                failed_event_type="ai_review_sync_failed_pending_review",
                error=error,
                force_retrying=field_patch_pending,
                retry_reason="field_patch_pending" if field_patch_pending else None,
            )
            if sync is not None and sync.status != "succeeded":
                # 既有 Lead 也可能在本轮首次补建远端记录；失败状态不能因 creates_lead=False 被丢掉。
                sync.status = "retrying" if field_patch_pending else (
                    "failed_pending_review" if failed_pending_review else "retrying"
                )
                error_code = getattr(error, "error_code", None)
                sync.error_summary = (
                    "field_patch_pending"
                    if field_patch_pending
                    else error_code if isinstance(error_code, str) else type(error).__name__
                )
        logger.exception("ai_review_sync_failed", extra={"error_type": type(error).__name__})

    def _ensure_ai_review_record(
        self, request: AIReviewRequest
    ) -> tuple[str | None, SmartTableRecord | None, bool]:
        """为新 AI 草稿建立最小审核记录，避免模型字段绕过 T09 直接写入。

        参数：request 为目标 Lead、真实销售身份和待审核字段补丁。
        返回值：记录标识、刚创建的记录快照和是否必须保留该记录不重建；创建失败时记录标识为 None。
        异常：数据库事实缺失时抛出 ValueError。
        副作用：首次新建记录并保存可审计的表格定位信息。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, request.lead_id)
            if lead is None:
                raise ValueError(f"AI 审核线索不存在：{request.lead_id}")
            if lead.smart_table_record_id is not None:
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
                )
                return (
                    lead.smart_table_record_id,
                    None,
                    sync is not None
                    and sync.error_summary
                    in {"field_patch_pending", "write_verification_pending"},
                )
        try:
            # 创建人和负责人只使用接入层已授权的销售身份，模型无法影响权限关键字段。
            record_fields: dict[str, object] = {
                **DEFAULT_SMART_TABLE_FIELD_VALUES,
                "线索来源": "展会",
                "创建人": request.sales_user_id,
                "负责人": request.sales_user_id,
            }
            if not request.creates_lead:
                # 已有 Lead 缺少远端行时，把后台已确认字段随首个 create 一并落表，
                # 避免新行的空值被 T09 误判为销售清空而保护掉。
                record_fields.update(request.patch.fields)
            record = self._smart_table_adapter.create_record(
                record_fields,
                actor=SmartTableActor.ROBOT,
            )
        except Exception as error:
            self._mark_ai_review_failed(request, error)
            return None, None, False

        with self._session_factory.begin() as session:
            lead = session.get(Lead, request.lead_id)
            sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
            )
            if lead is None or sync is None:
                raise ValueError(f"AI 审核表格同步事实不存在：{request.lead_id}")
            lead.smart_table_record_id = record.record_id
            sync.smart_table_record_id = record.record_id
            sync.status = "processing"
        # 创建响应已确认远端行存在；后续字段补写必须复用同一行，不能因预读失败重建。
        return record.record_id, record, True

    @staticmethod
    def _record_lead_processing_failed_notice(session: Session, message: IncomingMessage) -> None:
        """为失败待人工处理的线索消息登记一次不含客户内容的销售通知。

        参数：session 为失败状态事务；message 为已持久化来源消息。
        返回值：无。
        异常：数据库写入异常由事务调用方处理。
        副作用：按销售、来源消息和通知类型写入可靠出站通知，不保存原文或模型错误正文。
        """
        notification_type = "lead_processing_failed"
        key = hashlib.sha256(
            f"{message.sales_user_id}:{message.message_id}:{notification_type}".encode()
        ).hexdigest()
        if session.get(NotificationRecord, key) is None:
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=message.sales_user_id,
                    source_message_id=message.message_id,
                    notification_type=notification_type,
                    content="这条线索消息未能完成解析，已进入待人工处理，请稍后重试或补充信息。",
                )
            )

    def _mark_ai_review_failed(self, request: AIReviewRequest, error: Exception) -> None:
        """记录 AI 审核表格建行失败，并按外部错误类别保留可恢复状态。

        参数：request 定位来源事件和线索；error 为已捕获的外部异常。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：增加失败尝试次数，更新 Outbox、同步事实和受控审计，不伪造成功。
        """
        with self._session_factory.begin() as session:
            event, _ = self._load_event_and_message(session, request.outbox_event_id)
            sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
            )
            failed_pending_review = self._record_sync_failure(
                session,
                event,
                retrying_event_type="ai_review_sync_retrying",
                # 保留旧审计事件名，兼容既有管理员查询和历史诊断。
                failed_event_type="ai_review_failed_pending_review",
                error=error,
            )
            if sync is not None:
                sync.status = "failed_pending_review" if failed_pending_review else "retrying"
                error_code = getattr(error, "error_code", None)
                sync.error_summary = (
                    error_code if isinstance(error_code, str) else type(error).__name__
                )
            remote_record_id = getattr(error, "remote_record_id", None)
            if isinstance(remote_record_id, str) and remote_record_id:
                # 即使 create_record 抛出核实异常，也要持久化 ACK 返回的原行身份。
                lead = session.get(Lead, request.lead_id)
                if lead is not None:
                    lead.smart_table_record_id = remote_record_id
                if sync is not None:
                    sync.smart_table_record_id = remote_record_id
        logger.exception("ai_review_sync_failed", extra={"error_type": type(error).__name__})

    def _consume_multi_companies_before_smart_table(
        self, outbox_event_id: int, fields_by_segment: list[dict[str, str]]
    ) -> LeadProcessingResult:
        """在多客户消息的任何表格副作用前逐段完成公司决策与销售内去重。

        参数：outbox_event_id 为已取得销售顺序检查点的事件；fields_by_segment 为原始分段字段。
        返回值：按消息顺序返回所有最终 Lead 标识；temporary 分段没有 Smart Table record。
        异常：公司解析、表格或数据库错误按既有 Outbox 重试语义传播或返回失败。
        副作用：只为正式且未命中既有记录的分段首次入表；所有归属仍保留原分段序号。
        """
        if self._company_lead_service is None:
            raise ValueError("缺少公司解析服务")
        with self._session_factory() as session:
            _, message = self._load_event_and_message(session, outbox_event_id)
            source_message_id = message.message_id
            sales_user_id = message.sales_user_id
            message_text = message.normalized_text

        decisions: list[tuple[CompanyUpsertCommand, CompanyUpsertResult]] = []
        for segment_index, fields in enumerate(fields_by_segment):
            command = CompanyUpsertCommand(
                source_message_id=source_message_id,
                source_segment_index=segment_index,
                sales_user_id=sales_user_id,
                fields={**fields, "线索来源": "展会"},
                region_evidence=CompanyRegionEvidence(
                    message_text=message_text,
                    company_name=fields.get("线索名称"),
                    email=fields.get("邮箱"),
                    phone=fields.get("手机") or fields.get("电话"),
                ),
                defer_smart_table_sync=True,
            )
            # 先固定全部分段的唯一目标，避免任一分段的表格副作用早于其他分段的去重决策。
            decisions.append((command, self._company_lead_service.upsert(command)))

        lead_ids: list[str] = []
        record_id: str | None = None
        for command, company_result in decisions:
            finalized = self._finish_company_decision_before_smart_table(
                outbox_event_id, command, company_result
            )
            if finalized.status is LeadProcessingStatus.SYNC_FAILED:
                return LeadProcessingResult(
                    LeadProcessingStatus.SYNC_FAILED,
                    lead_ids=tuple(lead_ids),
                    company_resolution_applied=True,
                )
            lead_ids.append(finalized.lead_id or company_result.lead_id)
            if record_id is None:
                record_id = finalized.smart_table_record_id
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED,
            lead_id=lead_ids[0],
            smart_table_record_id=record_id,
            lead_ids=tuple(lead_ids),
            company_resolution_applied=True,
        )

    def _prepare_multi_leads(
        self,
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        fields_by_segment: list[dict[str, str]],
        outbox_event_id: int,
    ) -> tuple[MultiLeadSyncRequest, ...]:
        """为同一消息的多个客户分段创建或恢复独立线索同步请求。

        参数：session、event 和 message 是当前有序消费事实；fields_by_segment 为按消息顺序的字段。
        返回值：每个分段的销售、线索、序号和持久化字段快照。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：创建独立 Lead、同步事实、来源记录和多分段审计，事件转为 processing。
        """
        requests: list[MultiLeadSyncRequest] = []
        for segment_index, fields in enumerate(fields_by_segment):
            lead = session.scalar(
                select(Lead).where(
                    Lead.source_message_id == message.message_id,
                    Lead.source_segment_index == segment_index,
                )
            )
            if lead is None:
                # 每个分段都有独立 Lead 和字段来源，禁止让一个客户继承另一个分段的字段。
                lead = Lead(
                    source_message_id=message.message_id,
                    source_segment_index=segment_index,
                    original_capturing_sales_user_id=message.sales_user_id,
                    smart_table_owner_user_id=message.sales_user_id,
                    field_values={**fields, "线索来源": "展会"},
                )
                session.add(lead)
                session.flush()
                for field_name, value in fields.items():
                    session.add(
                        LeadFieldProvenance(
                            lead_id=lead.id,
                            source_message_id=message.message_id,
                            field_name=field_name,
                            value=serialize_field_value(value),
                            # 多客户的确定性首录也需要同一份人工编辑比较基线。
                            last_ai_synced_value=serialize_field_value(value),
                        )
                    )
                session.add(
                    SmartTableSync(
                        lead_id=lead.id,
                        source_message_id=message.message_id,
                        source_segment_index=segment_index,
                    )
                )
                self._record_audit(session, event, "multi_lead_segment_created")
            requests.append(
                MultiLeadSyncRequest(
                    sales_user_id=message.sales_user_id,
                    lead_id=lead.id,
                    segment_index=segment_index,
                    fields=dict(lead.field_values),
                    outbox_event_id=outbox_event_id,
                )
            )
        event.status = "processing"
        event.processing_started_at = utc_now()
        return tuple(requests)

    def _create_multi_smart_table_records(
        self, requests: tuple[MultiLeadSyncRequest, ...]
    ) -> LeadProcessingResult:
        """按消息分段顺序创建独立表格记录，并沿用既有失败检查点语义。

        参数：requests 为已提交的多个分段同步请求。
        返回值：成功时返回所有 Lead 标识；任一外部失败时返回同步失败。
        异常：数据库错误向调用方传播。
        副作用：逐个调用智能表格适配器，失败时保留同一 Outbox 的可重试状态。
        """
        lead_ids: list[str] = []
        for request in requests:
            result = self._create_smart_table_record(
                request.sales_user_id,
                request.fields,
                request.lead_id,
                request.outbox_event_id,
                request.segment_index,
            )
            if result.status is LeadProcessingStatus.SYNC_FAILED:
                return LeadProcessingResult(
                    LeadProcessingStatus.SYNC_FAILED, lead_ids=tuple(lead_ids)
                )
            lead_ids.append(request.lead_id)
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED, lead_id=lead_ids[0], lead_ids=tuple(lead_ids)
        )

    def _consume_next_after_checkpoint(self, outbox_event_id: int) -> None:
        """在本事件成为检查点后串行消费同一销售的下一条待处理消息。

        参数：outbox_event_id 为刚完成或失败耗尽的来源 Outbox 事件。
        返回值：无。
        异常：数据库读取失败时由 SQLAlchemy 抛出；下一条消费的异常按其真实类型传播。
        副作用：可能递归消费同一销售按 sequence 排序的后续 pending 或 retrying 事件。
        """
        with self._session_factory() as session:
            event = session.get(OutboxEvent, outbox_event_id)
            if event is None or event.status not in COMPLETED_CHECKPOINT_STATUSES:
                return
            # 只挑选最早的后续事件，递归调用会在每个新检查点后继续推进该销售的队列。
            next_event_id = session.scalar(
                select(OutboxEvent.id)
                .where(
                    OutboxEvent.sales_user_id == event.sales_user_id,
                    OutboxEvent.sequence > event.sequence,
                    OutboxEvent.status.in_(("pending", "retrying")),
                )
                .order_by(OutboxEvent.sequence)
                .limit(1)
            )
        if next_event_id is not None:
            self.consume(next_event_id)

    def _processing_lease_expired(self, event: OutboxEvent) -> bool:
        """判断 processing 事件是否已超过可配置租约且应停止自动重放。

        参数：event 为待检查的 Outbox 事件。
        返回值：仅当事件处于 processing 且开始时间超过租约时返回 True。
        异常：无。
        副作用：无。
        """
        if event.status != "processing" or event.processing_started_at is None:
            return False
        return (
            self._as_utc(utc_now())
            > self._as_utc(event.processing_started_at) + self._lead_processing_timeout
        )

    @staticmethod
    def _context_fields_for_ai(lead: Lead | None) -> dict[str, str]:
        """提取仅供 AI 判断消息关系的当前线索非敏感字段。

        参数：lead 为 T07 已确定的当前销售线索；为空时表示没有可用上下文。
        返回值：仅包含公司、联系人、职务和业务分类的字符串字段。
        异常：无；线索字段值不是字符串时忽略该字段。
        副作用：无；不改变线索或调用外部服务。
        """
        if lead is None:
            return {}
        allowed_fields = {"线索名称", "联系人", "职务", "业务线", "客户行业", "工艺"}
        return {
            field_name: value
            for field_name, value in lead.field_values.items()
            if field_name in allowed_fields and isinstance(value, str) and value
        }

    @staticmethod
    def _has_explicit_company_identity_evidence(text: str | None) -> bool:
        """判断文本是否包含可确定解析的显式公司标签。

        参数：text 为当前消息标准化文本。
        返回值：显式标签解析出线索名称时返回 True。
        异常：无。
        副作用：无；仅运行显式标签解析。
        """
        return bool(DeterministicFirstTextLeadExtractor().extract_patch(text).get("线索名称"))

    @staticmethod
    def _ai_company_identity_claimed(patch: ExtractedLeadPatch) -> bool:
        """判断 AI 是否尝试给当前消息指定公司身份，包括未通过字段置信度的候选。

        参数：patch 为 AI Gateway 返回并尚未做来源 grounding 的字段补丁。
        返回值：正式线索名称字段或 company 类 customer_reference 非空时返回 True。
        异常：无。
        副作用：无；仅检查本次分析结果，不访问上下文或数据库。
        """
        if patch.fields.get("线索名称") or patch.analysis.crm_fields.get("线索名称"):
            return True
        company_reference_keys = {
            "company",
            "company_name",
            "企业",
            "企业名称",
            "公司",
            "公司名称",
            "线索名称",
        }
        return any(
            key.strip().lower() in company_reference_keys and value.strip()
            for key, value in patch.analysis.customer_reference.items()
        )

    @staticmethod
    def _has_explicit_contact_evidence(text: str | None) -> bool:
        """判断文本中是否出现手机号或邮箱等联系方式证据。

        参数：text 为当前消息标准化文本。
        返回值：出现手机号或邮箱时返回 True。
        异常：无。
        副作用：无；仅进行本地正则判断。
        """
        if not text:
            return False
        return bool(
            re.search(r"(?<!\d)(?:\+?86[ -]?)?1[3-9]\d{9}(?!\d)", text)
            or re.search(r"[^\s；;，,、]+@[^\s；;，,、]+\.[^\s；,、]+", text)
        )

    def _is_weak_context_fragment(
        self,
        text: str | None,
        fields: Mapping[str, LeadFieldValue],
        enrichment: Mapping[str, str] | None = None,
    ) -> bool:
        """只接受原文完全由联系方式或预算补充组成的上下文续写。

        参数：text 为当前来源原文；fields 与 enrichment 为 AI 已校验的本轮增量字段。
        返回值：字段值能在原文核验且移除字段与通用标签后无其他客户语义时返回 True。
        异常：无；不访问数据库或外部服务。
        副作用：无。
        """
        source = (text or "").strip()
        if not source or self._has_explicit_company_identity_evidence(source):
            return False
        contact_fields = {"手机", "电话", "邮箱"}
        normalized_enrichment = dict(enrichment or {})
        if fields and set(fields).issubset(contact_fields) and not normalized_enrichment:
            source_spans: list[tuple[int, int]] = []
            for field_name, raw_value in fields.items():
                if not isinstance(raw_value, str) or not raw_value.strip():
                    return False
                if field_name == "邮箱":
                    value = raw_value.strip()
                    matches = list(re.finditer(re.escape(value), source, flags=re.IGNORECASE))
                    if not matches:
                        return False
                    source_spans.extend((match.start(), match.end()) for match in matches)
                else:
                    normalized_value = AIGateway._normalize_contact_number(raw_value)
                    if normalized_value is None:
                        return False
                    phone_pattern = (
                        r"(?<![A-Za-z0-9_])(?:\+|00)?\d[\d \t().-]{5,}\d"
                        r"(?![A-Za-z0-9_])"
                    )
                    source_matches = [
                        match
                        for match in re.finditer(phone_pattern, source)
                        if AIGateway._normalize_contact_number(match.group()) == normalized_value
                    ]
                    if not source_matches:
                        return False
                    # 仅移除与当前候选归一化后相同的号码，其他数字和业务语义保留。
                    source_spans.extend((match.start(), match.end()) for match in source_matches)
            remainder = source
            for start, end in sorted(set(source_spans), reverse=True):
                remainder = remainder[:start] + remainder[end:]
            remainder = re.sub(
                r"补充|联系方式|联系电话|手机号|手机|电话|邮箱|email|e-mail|phone|tel|号码|是|为",
                "",
                remainder,
                flags=re.IGNORECASE,
            )
            return not re.sub(r"[\s,，。；;:：、()（）\-]+", "", remainder)

        budget_re = re.compile(r"(?<!\d)\d+(?:\.\d+)?\s*(?:万(?:元)?|千(?:元)?|元|[kKwW])")
        if fields or set(normalized_enrichment) - {"预算"}:
            return False
        source_amounts = [
            re.sub(r"\s+", "", item.group()).lower() for item in budget_re.finditer(source)
        ]
        if not source_amounts:
            return False
        if normalized_enrichment:
            budget = normalized_enrichment.get("预算", "")
            if not isinstance(budget, str) or not budget.strip():
                return False
            candidate_amounts = [
                re.sub(r"\s+", "", item.group()).lower()
                for item in budget_re.finditer(budget)
            ]
            candidate_remainder = budget_re.sub("", budget)
            candidate_remainder = re.sub(
                r"预算|大概|大约|约|左右|补充|金额|是|在", "", candidate_remainder
            )
            if (
                not candidate_amounts
                or not set(candidate_amounts).issubset(set(source_amounts))
                or re.sub(r"[\s,，。；;:：、()（）\-~～]+", "", candidate_remainder)
            ):
                return False
        remainder = budget_re.sub("", source)
        remainder = re.sub(r"预算|大概|大约|约|左右|补充|金额|是|在", "", remainder)
        return not re.sub(r"[\s,，。；;:：、()（）\-~～]+", "", remainder)

    @classmethod
    def validate_company_candidate_against_source(
        cls, candidate: str | None, normalized_text: str | None
    ) -> bool:
        """验证 AI 语义公司字段的候选文本确实来自当前消息。

        参数：candidate 为 AI 已按公司主体语义提取的候选；normalized_text 为当前消息原文。
        返回值：候选逐字存在于当前消息时返回 True；调用方还校验 AI 语义字段及字段间冲突。
        异常：无。
        副作用：无；只做来源字符串核验。
        """
        return bool(candidate and normalized_text and candidate.strip() in normalized_text)

    @classmethod
    def _ground_source_identity_patch(
        cls, patch: ExtractedLeadPatch, normalized_text: str | None
    ) -> ExtractedLeadPatch:
        """移除无法回指当前消息的公司与联系人候选，统一保护单客户路由。"""
        fields = dict(patch.fields)
        crm_fields = dict(patch.analysis.crm_fields)
        confidences = dict(patch.analysis.confidence_by_field)
        pending = tuple(patch.pending_confirmation_fields)
        low_candidates = dict(patch.low_confidence_candidates)
        semantic_company = patch.analysis.crm_fields.get("线索名称")
        identity_details = [
            value
            for field_name, value in (*fields.items(), *crm_fields.items())
            if field_name != "线索名称" and isinstance(value, str) and value.strip()
        ]
        identity_details.extend(
            value
            for value in patch.analysis.customer_reference.values()
            if isinstance(value, str) and value.strip()
        )
        business_details = [
            value
            for value in (*patch.enrichment.values(), *patch.analysis.enrichment.values())
            if isinstance(value, str) and value.strip()
        ]
        # ponytail: 当前用 4 字重叠差量拦截拼接候选；模型提供来源跨度后可替换该长度启发式。
        ambiguous_company_values = {
            candidate.strip()
            for candidate in (fields.get("线索名称"), crm_fields.get("线索名称"))
            if isinstance(candidate, str)
            and (
                any(
                    detail != candidate.strip()
                    and detail in candidate
                    and len(candidate) - len(detail) >= 4
                    for detail in identity_details
                )
                or any(
                    detail != candidate.strip()
                    and (
                        detail in candidate
                        and len(candidate) - len(detail) >= 4
                        or candidate.strip() in detail
                        and len(detail) - len(candidate.strip()) >= 4
                    )
                    for detail in business_details
                )
            )
        }
        for field_name in ("线索名称", "联系人"):
            field_value = fields.get(field_name)
            company_is_ambiguous = (
                field_name == "线索名称"
                and isinstance(field_value, str)
                and field_value.strip() in ambiguous_company_values
            )
            valid_source_value = (
                not company_is_ambiguous
                and isinstance(semantic_company, str)
                and isinstance(field_value, str)
                and field_value.strip() == semantic_company.strip()
                and cls.validate_company_candidate_against_source(field_value, normalized_text)
                if field_name == "线索名称" and isinstance(field_value, str)
                else isinstance(field_value, str)
                and normalized_text is not None
                and field_value.strip() in normalized_text
            )
            if isinstance(field_value, str) and not valid_source_value:
                fields.pop(field_name, None)
                pending = tuple(item for item in pending if item != field_name)
                low_candidates.pop(field_name, None)
            crm_value = crm_fields.get(field_name)
            valid_crm_source_value = (
                not (
                    field_name == "线索名称"
                    and isinstance(crm_value, str)
                    and crm_value.strip() in ambiguous_company_values
                )
                and cls.validate_company_candidate_against_source(crm_value, normalized_text)
                if field_name == "线索名称" and isinstance(crm_value, str)
                else isinstance(crm_value, str)
                and normalized_text is not None
                and crm_value.strip() in normalized_text
            )
            if isinstance(crm_value, str) and not valid_crm_source_value:
                crm_fields.pop(field_name, None)
                confidences.pop(field_name, None)
        if crm_fields == patch.analysis.crm_fields and fields == patch.fields:
            return patch
        return replace(
            patch,
            fields=fields,
            pending_confirmation_fields=pending,
            low_confidence_candidates=low_candidates,
            analysis=patch.analysis.model_copy(
                update={
                    "crm_fields": crm_fields,
                    "confidence_by_field": confidences,
                }
            ),
        )

    @classmethod
    def _drop_unreliable_company_candidate(
        cls, patch: ExtractedLeadPatch, text: str | None
    ) -> ExtractedLeadPatch:
        """从当前客户弱片段中移除模型误生成的公司名候选。

        参数：patch 为网关已校验补丁；text 为支持该补丁的当前原文。
        返回值：无明确公司证据时移除“线索名称”的补丁，否则原样返回。
        异常：无；只复制不可变补丁，不触发数据库或模型调用。
        副作用：无。
        """
        if "线索名称" not in patch.fields or cls._has_explicit_company_identity_evidence(text):
            return patch
        fields = dict(patch.fields)
        fields.pop("线索名称", None)
        pending = tuple(
            field_name
            for field_name in patch.pending_confirmation_fields
            if field_name != "线索名称"
        )
        low_candidates = dict(patch.low_confidence_candidates)
        low_candidates.pop("线索名称", None)
        return replace(
            patch,
            fields=fields,
            pending_confirmation_fields=pending,
            low_confidence_candidates=low_candidates,
        )

    @staticmethod
    def _has_explicit_new_lead_signal(text: str | None) -> bool:
        """判断销售是否明确表示当前消息属于另一个新客户。

        参数：text 为当前消息标准化文本。
        返回值：出现明确的新客户或新公司切换表达时返回 True。
        异常：无。
        副作用：无；仅进行本地短语判断。
        """
        if not text:
            return False
        # 仅拦截明确的切换表达，避免把“公司想采购”等普通业务描述误认为新线索信号。
        explicit_signals = (
            "另一个客户",
            "另外一个客户",
            "另一个公司",
            "另外一家公司",
            "新客户",
            "下一个客户",
            "刚又见了一个",
            "再录入一个",
        )
        return any(signal in text for signal in explicit_signals)

    @staticmethod
    def _promote_ignored_context_fragment(
        patch: ExtractedLeadPatch, text: str | None
    ) -> ExtractedLeadPatch:
        """将当前客户上下文中的保守 IGNORE 结果提升为可追溯补充补丁。

        参数：patch 为网关已完成结构和业务校验的结果；text 为当前消息原文。
        返回值：将意图改为 UPDATE_LEAD，并在模型未提取补充字段时保留原文素材的补丁。
        异常：无；只复制不可变补丁，不触发数据库或模型调用。
        副作用：无；后续 T09 会按人工编辑保护规则生成备注。
        """
        enrichment = dict(patch.enrichment)
        normalized_text = (text or "").strip()
        if normalized_text and not enrichment:
            # 原文是唯一可靠证据；数量、批量等没有独立 CRM 字段的信息进入备注素材。
            enrichment["特殊要求"] = normalized_text
        analysis = patch.analysis.model_copy(update={"intent": "UPDATE_LEAD"})
        return replace(patch, analysis=analysis, enrichment=enrichment)

    def _get_active_context_lead(self, session: Session, message: IncomingMessage) -> Lead | None:
        """读取尚未过期且属于当前销售的当前客户线索。

        参数：session 为当前事务；message 为待归属的持久化消息。
        返回值：上下文有效时返回对应 Lead，否则返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取销售上下文和线索事实。
        """
        context = session.get(SalesLeadContext, message.sales_user_id)
        if context is None:
            return None
        received_at = self._as_utc(message.received_at)
        context_at = self._as_utc(context.last_message_received_at)
        if received_at > context_at + self._lead_context_ttl:
            # 过期上下文不得作为弱身份消息的默认归属依据。
            return None
        lead = session.get(Lead, context.lead_id)
        if lead is None or lead.smart_table_owner_user_id != message.sales_user_id:
            # 上下文只能指向当前销售仍拥有的有效线索，异常事实一律不复用。
            return None
        return lead

    def _get_strong_identity_lead(
        self, session: Session, message: IncomingMessage, fields: dict[str, str]
    ) -> Lead | None:
        """在当前销售范围内按公司名优先、联系方式次之定位既有线索。

        参数：session 为当前事务；message 为待归属消息；fields 为确定性或 AI 提取的候选字段。
        返回值：公司名精确命中，或无公司名时联系人/联系方式恰好唯一命中时返回 Lead，否则返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取当前销售的线索草稿，不访问其他销售数据。
        """
        incoming_company_name = fields.get("线索名称", "").strip()
        if incoming_company_name:
            # 消息明确给出公司名时，必须先用公司身份定位；禁止共享手机号把不同公司串到旧线索。
            candidate_values = {"线索名称": incoming_company_name}
        else:
            # 没有公司名的补充消息才允许使用联系人、手机、电话或邮箱回溯既有线索。
            strong_fields = {"联系人", "手机", "电话", "邮箱"}
            candidate_values = {
                field_name: value
                for field_name, value in fields.items()
                if field_name in strong_fields
            }
        if not candidate_values:
            return None
        # ponytail: 当前按销售读取后比较 JSON；线索量成为瓶颈时改为已验证字段的索引列。
        candidates = session.scalars(
            select(Lead).where(
                Lead.smart_table_owner_user_id == message.sales_user_id,
                Lead.smart_table_record_id.is_not(None),
            )
        ).all()
        matches = []
        for lead in candidates:
            if incoming_company_name:
                # 公司名既比较当前展示值，也比较已核验标准名，保持精确匹配且不依赖联系方式。
                known_company_names = {
                    str(lead.field_values.get("线索名称") or "").strip(),
                    str(lead.standard_company_name or "").strip(),
                }
                # 企查查标准化后保留的原始 AI 来源值也是精确事实，允许同一原始名称重试时定位原线索。
                provenance_rows = session.scalars(
                    select(LeadFieldProvenance).where(
                        LeadFieldProvenance.lead_id == lead.id,
                        LeadFieldProvenance.field_name == "线索名称",
                        LeadFieldProvenance.is_user_modified.is_(False),
                    )
                ).all()
                known_company_names.update(
                    str(value).strip()
                    for row in provenance_rows
                    for value in (row.value, row.last_ai_synced_value)
                    if value
                )
                if incoming_company_name in known_company_names:
                    matches.append(lead)
                continue
            if any(
                lead.field_values.get(field_name) == value
                for field_name, value in candidate_values.items()
            ):
                matches.append(lead)
        # 多条记录命中时宁可保留待归属，也不能猜测应补充给哪一条线索。
        return matches[0] if len(matches) == 1 else None

    def _context_supplement_rejection_reason(
        self,
        session: Session,
        message: IncomingMessage,
        active_context: Lead | None,
        candidate_lead: Lead | None,
        fields: Mapping[str, LeadFieldValue],
        source_supported: bool,
    ) -> str | None:
        """统一裁决无公司身份补充是否可写入当前或唯一强身份目标。

        参数：session/message 为当前消息事实；active_context 为未过期上下文；candidate_lead
        为前序唯一身份匹配候选；fields 为核验字段；source_supported 为安全补充校验结果。
        返回值：允许继续时返回 None；否则返回受控审计原因码。
        异常：数据库读取错误由 SQLAlchemy 抛出。
        副作用：仅读取当前销售拥有的线索。
        """
        if not source_supported:
            return "no_grounded_identity_or_safe_supplement"
        if active_context is None and candidate_lead is None:
            return "no_grounded_identity_or_safe_supplement"
        contact_fields = {
            field_name: str(value)
            for field_name, value in fields.items()
            if field_name in {"联系人", "手机", "电话", "邮箱"} and isinstance(value, str)
        }
        if active_context is not None and contact_fields:
            historical_match = self._get_strong_identity_lead(session, message, contact_fields)
            if historical_match is not None and historical_match.id != active_context.id:
                return "contact_identity_conflicts_with_active_context"
        return None

    def _as_utc(self, value: datetime) -> datetime:
        """将 SQLite 等驱动返回的朴素时间统一视为 UTC 时间。

        参数：value 为数据库读取的时间。
        返回值：带 UTC 时区的等价时间。
        异常：无。
        副作用：无。
        """
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    def _is_weak_identity_fragment(self, text: str | None) -> bool:
        """判断文本是否像缺少公司身份的客户补充信息。

        参数：text 为已标准化文本。
        返回值：含客户补充语义时返回 True；仅用于识别无法安全归属的待处理消息，
        绝不能作为沿用 active context 的授权证据。
        异常：无。
        副作用：无。
        """
        weak_keywords = ("预算", "需求", "报价", "项目", "采购")
        return bool(text and any(keyword in text for keyword in weak_keywords))

    @staticmethod
    def _release_processing_claim(event: OutboxEvent) -> None:
        """释放当前 Worker claim，使等待处理的事件可由后续调度再次认领。

        参数：event 为当前 reply 的 Outbox 事件。
        返回值：无。
        异常：无。
        副作用：将事件重置为 pending 并清空 processing lease 起始时间。
        """
        event.status = "pending"
        event.processing_started_at = None

    @staticmethod
    def _media_enrichment_is_pending(session: Session, message: IncomingMessage) -> bool:
        """判断媒体消息是否仍在下载或 OCR/ASR 处理中。

        参数：session 为当前事务，message 为待消费来源消息。
        返回值：媒体标记存在且无附件或存在 pending 附件时返回 True。
        异常：数据库读取异常由 SQLAlchemy 抛出。
        副作用：无；调用方据此保留 Outbox 的 pending 状态。
        """
        if not message.requires_media_enrichment:
            return False
        statuses = session.scalars(
            select(MessageAttachment.processing_status).where(
                MessageAttachment.message_id == message.message_id
            )
        ).all()
        # 下载尚未创建附件元数据同样必须等待，避免媒体消息被错误提前忽略。
        return not statuses or "pending" in statuses

    def _mark_unassigned(self, session: Session, event: OutboxEvent) -> None:
        """将无法可靠归属的消息持久化为待归属，并越过首次消费检查点。

        参数：session 为当前事务；event 为来源 Outbox 事件。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增待归属结论、审计事件并将任务标记为 succeeded。
        """
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == event.message_id,
                LeadMessageResolution.segment_index == 0,
            )
        )
        if resolution is None:
            session.add(
                LeadMessageResolution(
                    message_id=event.message_id,
                    segment_index=0,
                    status="unassigned",
                )
            )
        event.status = "succeeded"
        self._record_audit(session, event, "lead_message_unassigned")
        logger.info("lead_message_unassigned")

    def _mark_assigned(
        self, session: Session, event: OutboxEvent, lead_id: str, segment_index: int = 0
    ) -> None:
        """保存消息已归属的结论，并完成无需表格写入的消费。

        参数：session 为当前事务；event 为来源 Outbox；lead_id 为归属线索；
        segment_index 为消息分段。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增或更新归属结论并将任务标记为 succeeded。
        """
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == event.message_id,
                LeadMessageResolution.segment_index == segment_index,
            )
        )
        if resolution is None:
            session.add(
                LeadMessageResolution(
                    message_id=event.message_id,
                    lead_id=lead_id,
                    status="assigned",
                    segment_index=segment_index,
                )
            )
        else:
            resolution.lead_id = lead_id
            resolution.status = "assigned"
        event.status = "succeeded"
        # 成功终态必须清理旧的暂态失败元数据，但保留 attempts 作为处理次数事实。
        event.failure_category = None
        event.failure_summary = None
        event.failed_at = None
        event.processing_started_at = None
        self._record_audit(session, event, "lead_message_assigned")
        logger.info("lead_message_assigned")

    def _mark_processing_assignment(
        self, session: Session, event: OutboxEvent, lead_id: str, segment_index: int = 0
    ) -> None:
        """在外部同步开始前持久化已确定的消息目标，供失败补充重试复用。

        参数：session 为当前处理事务；event 为消息 Outbox 事实；lead_id 为已由 T07 确定的线索；
        segment_index 为消息分段序号。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增或补全 processing 归属，不改变 Outbox 的任务状态或线索生命周期。
        """
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == event.message_id,
                LeadMessageResolution.segment_index == segment_index,
            )
        )
        if resolution is None:
            session.add(
                LeadMessageResolution(
                    message_id=event.message_id,
                    segment_index=segment_index,
                    lead_id=lead_id,
                    status="processing",
                )
            )
        elif resolution.lead_id is None:
            # 已有待归属事实没有目标时，只能补入本次确定的同一消息目标，不能覆盖已有归属。
            resolution.lead_id = lead_id
            resolution.status = "processing"
        elif resolution.lead_id == lead_id:
            # 重试同一外部同步时继续保留同一个业务目标，不允许状态回退造成重新猜测。
            resolution.status = "processing"

    def _pin_message_lead_before_external_sync(
        self,
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        lead_id: str,
        segment_index: int = 0,
        *,
        refresh_context: bool = True,
    ) -> None:
        """在首次 Smart Table 外部调用前固定消息归属和销售当前线索。

        参数：session 为当前事务；event 和 message 为同一来源事实；lead_id 为已确定目标；
        segment_index 为消息分段序号。
        返回值：无。
        异常：归属或上下文持久化失败时向调用方传播。
        副作用：写入 processing 归属、推进 SalesLeadContext，并记录受控审计事件；不调用外部系统。
        """
        self._mark_processing_assignment(session, event, lead_id, segment_index)
        if refresh_context:
            self._refresh_context(session, message, lead_id)
        self._record_audit(
            session,
            event,
            "lead_routing_context_pinned",
            details={"lead_id": lead_id},
        )

    def _pending_snapshot_fields(
        self, session: Session, lead_id: str
    ) -> dict[str, LeadFieldValue]:
        """读取尚未完成智能表格同步的后台字段计划，供后续消息补偿写回。

        参数：session 为当前消费事务；lead_id 为目标线索。
        返回值：字段名到待补写值的安全映射；无法恢复的历史值会被忽略。
        异常：数据库读取异常由 SQLAlchemy 向调用方传播。
        副作用：无，仅读取字段来源事实。
        """
        pending_sources = session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.last_ai_synced_value.is_(None),
            )
        ).all()
        pending_fields: dict[str, LeadFieldValue] = {}
        for source in pending_sources:
            value = deserialize_field_value(source.value)
            if isinstance(value, str) or (
                isinstance(value, list) and all(isinstance(item, str) for item in value)
            ):
                pending_fields[source.field_name] = value
        return pending_fields

    def _only_empty_fields(
        self, lead: Lead, fields: Mapping[str, LeadFieldValue]
    ) -> dict[str, LeadFieldValue]:
        """从字段补丁中保留当前线索尚无值的字段，避免覆盖既有或人工数据。

        参数：lead 为归属线索；fields 为本次确定性字段补丁。
        返回值：只包含 lead.field_values 中为空的字段补丁。
        异常：无。
        副作用：无。
        """
        # 所有上下文补充都复用同一保护规则，不能因同步前后两个阶段出现行为漂移。
        return {
            field_name: value
            for field_name, value in fields.items()
            if not lead.field_values.get(field_name)
        }

    @staticmethod
    def _is_stale_multiline_company_name(value: object) -> bool:
        """判断公司字段是否明显是旧解析误吞的多行标签文本。

        参数：value 为临时线索当前保存的公司字段。
        返回值：包含换行和标签分隔符时返回 True；普通公司名返回 False。
        异常：无。
        副作用：无。
        """
        if not isinstance(value, str):
            return False
        # 正常公司名称不应包含换行；同时要求出现标签分隔符，避免误伤特殊公司名文本。
        return ("\n" in value or "\r" in value) and "：" in value

    def _refresh_context(self, session: Session, message: IncomingMessage, lead_id: str) -> None:
        """按消息序号单调推进一名销售的当前线索上下文。

        参数：session 为当前事务；message 为已持久化消息；lead_id 为其归属线索。
        返回值：无。
        异常：销售 actor 缺失或同序号归属冲突时抛出 ValueError；数据库错误由 SQLAlchemy 抛出。
        副作用：在销售锁和上下文行锁保护下，按消息序号新增或更新当前客户上下文。
        """
        # 先锁 actor 行，和消息消费顺序锁保持同一并发边界；SQLite 会忽略行锁语义。
        authorization = session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == message.sales_user_id)
            .with_for_update()
        )
        if authorization is None:
            raise ValueError(f"actor registry 记录不存在：{message.sales_user_id}")
        # 再锁上下文行，避免两个 Worker 按完成时间交错覆盖销售当前线索。
        context = session.scalar(
            select(SalesLeadContext)
            .where(SalesLeadContext.sales_user_id == message.sales_user_id)
            .with_for_update()
        )
        if context is None:
            # 首条成功归属消息同时保存序号，建立后续单调推进的事实基线。
            session.add(
                SalesLeadContext(
                    sales_user_id=message.sales_user_id,
                    lead_id=lead_id,
                    last_message_received_at=message.received_at,
                    last_message_sequence=message.sequence,
                )
            )
            return

        current_sequence = context.last_message_sequence
        if current_sequence is None:
            # 历史 context 没有序号时，不能让更早收到的旧消息覆盖已有时间点。
            if message.received_at < context.last_message_received_at:
                logger.info(
                    "lead_context_stale_message_ignored",
                    extra={
                        "incoming_sequence": message.sequence,
                        "current_sequence": None,
                    },
                )
                return
        elif message.sequence < current_sequence:
            # 重试和旧 Worker 只能观察旧事实，绝不能回退当前销售上下文。
            logger.info(
                "lead_context_stale_message_ignored",
                extra={
                    "incoming_sequence": message.sequence,
                    "current_sequence": current_sequence,
                },
            )
            return
        elif message.sequence == current_sequence:
            if context.lead_id == lead_id:
                # 同一消息的重复完成是幂等重放，不需要再次写入时间点。
                return
            distinct_segment_leads = {
                resolved_lead_id
                for resolved_lead_id in session.scalars(
                    select(LeadMessageResolution.lead_id).where(
                        LeadMessageResolution.message_id == message.message_id,
                        LeadMessageResolution.lead_id.is_not(None),
                    )
                )
                if resolved_lead_id is not None
            }
            if len(distinct_segment_leads) > 1:
                # 同一消息的多客户分段没有唯一当前线索，保持现有上下文而不任选目标。
                logger.error(
                    "lead_context_same_sequence_conflict",
                    extra={
                        "incoming_sequence": message.sequence,
                        "current_sequence": current_sequence,
                        "multi_segment": True,
                    },
                )
                return
            # 同序号对应不同线索无法确定正确目标，必须阻止静默覆盖。
            logger.error(
                "lead_context_same_sequence_conflict",
                extra={
                    "incoming_sequence": message.sequence,
                    "current_sequence": current_sequence,
                },
            )
            raise ValueError("同一消息序号对应不同线索上下文")

        # 只有新消息序号，或尚未完成序号回填的非旧消息，才可推进当前上下文。
        context.lead_id = lead_id
        context.last_message_received_at = message.received_at
        context.last_message_sequence = message.sequence

    def _update_smart_table_record(self, request: ContextUpdateRequest) -> LeadProcessingResult:
        """将当前客户上下文中的安全字段补丁增量写入既有智能表格记录。

        参数：request 为提交后可执行的上下文更新请求。
        返回值：成功时返回 UPDATED，外部失败时返回 SYNC_FAILED。
        异常：关键持久化事实缺失时抛出 ValueError；数据库错误向调用方传播。
        副作用：调用 SmartTableAdapter，成功后保存字段来源、消息归属、上下文和审计。
        """
        logger.info("smart_table_context_update_started")
        # 即使后续 T09 读取或写入失败，当前销售也必须留在已确定的目标线索上。
        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, request.outbox_event_id)
            if message.message_id != request.source_message_id:
                raise ValueError(f"上下文更新来源消息不一致：{request.outbox_event_id}")
            self._pin_message_lead_before_external_sync(
                session, event, message, request.lead_id, request.segment_index
            )
        try:
            # 确定性补充也必须复用 T09：它负责人工保护、字段来源和基于安全快照的备注重建。
            LeadReviewService(
                self._session_factory,
                self._smart_table_adapter,
                robot_submission_confirmation_available=self._robot_submission_confirmation_available,
            ).sync_ai_patch(
                request.lead_id,
                request.source_message_id,
                ExtractedLeadPatch(
                    trace_id=f"deterministic-{request.outbox_event_id}",
                    analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                    fields=dict(request.fields),
                    pending_confirmation_fields=request.pending_confirmation_fields,
                    pending_prefill_allowed_fields=request.pending_prefill_allowed_fields,
                    low_confidence_candidates={},
                ),
            )
        except Exception as error:
            # 失败只留下可重试任务状态；retrying 不阻塞当前销售的后续消息。
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, request.outbox_event_id)
                failed_pending_review = self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_context_update_retrying",
                    failed_event_type="smart_table_context_update_failed_pending_review",
                    error=error,
                )
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
                )
                if sync is not None and sync.status != "succeeded":
                    sync.status = "failed_pending_review" if failed_pending_review else "retrying"
                    sync.error_summary = type(error).__name__
            logger.exception(
                "smart_table_context_update_failed",
                extra={"error_type": type(error).__name__},
            )
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=request.lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, request.outbox_event_id)
            if message.message_id != request.source_message_id:
                raise ValueError(f"上下文更新来源消息不一致：{request.outbox_event_id}")
            lead = session.get(Lead, request.lead_id)
            if lead is None:
                raise ValueError(f"上下文更新线索不存在：{request.lead_id}")
            self._mark_assigned(session, event, request.lead_id, request.segment_index)
            self._refresh_context(session, message, request.lead_id)
            self._record_audit(session, event, "smart_table_context_updated")
        logger.info("smart_table_context_updated")
        return LeadProcessingResult(
            LeadProcessingStatus.UPDATED,
            lead_id=request.lead_id,
            smart_table_record_id=request.record_id,
        )

    def _lock_sales_processing_stream(self, session: Session, sales_user_id: str) -> None:
        """锁定一名 actor 的注册行，以原子判断该销售的消息消费顺序。

        参数：session 为当前事务；sales_user_id 为待消费消息的发送销售。
        返回值：无。
        异常：actor 记录缺失时抛出 ValueError，避免未受保护的顺序判断。
        副作用：在当前事务提交前持有该 actor 注册行锁；不同销售不互相阻塞。
        """
        authorization = session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == sales_user_id)
            .with_for_update()
        )
        if authorization is None:
            raise ValueError(f"actor registry 记录不存在：{sales_user_id}")

    def _has_unfinished_previous_event(self, session: Session, event: OutboxEvent) -> bool:
        """判断同一销售是否还有未越过首次消费检查点的较早事件。

        参数：session 为当前事务；event 为准备消费的 Outbox 事件。
        返回值：存在未完成较早事件时返回 True，否则返回 False。
        异常：数据库查询失败时由 SQLAlchemy 抛出。
        副作用：仅读取同一销售且 sequence 更小的 Outbox 事件。
        """
        # retrying 已完成本轮外部调用且可安全重试，不能阻塞后续消息；pending/processing 仍保持顺序。
        previous_event_id = session.scalar(
            select(OutboxEvent.id)
            .where(
                OutboxEvent.sales_user_id == event.sales_user_id,
                OutboxEvent.sequence < event.sequence,
                OutboxEvent.status.not_in(COMPLETED_CHECKPOINT_STATUSES | {"retrying"}),
            )
            .order_by(OutboxEvent.sequence)
            .limit(1)
        )
        return previous_event_id is not None

    def _load_event_and_message(
        self, session: Session, outbox_event_id: int
    ) -> tuple[OutboxEvent, IncomingMessage]:
        """读取待消费事件及其 T02 已持久化来源消息。

        参数：session 为当前业务事务；outbox_event_id 为目标事件标识。
        返回值：事件与来源消息组成的元组。
        异常：任一事实缺失时抛出 ValueError，避免对不完整输入执行外部写入。
        副作用：仅读取数据库。
        """
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            raise ValueError(f"Outbox 事件不存在：{outbox_event_id}")
        message = session.get(IncomingMessage, event.message_id)
        if message is None:
            raise ValueError(f"Outbox 来源消息不存在：{event.message_id}")
        return event, message

    def _processed_result(self, session: Session, event: OutboxEvent) -> LeadProcessingResult:
        """将已终态事件转换为幂等响应，禁止再次写入智能表格。

        参数：session 为当前事务；event 为已不处于 pending 的 T02 事件。
        返回值：已有 Lead 和表格记录标识（若存在）的重复消费结果。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：无。
        """
        lead = session.scalar(select(Lead).where(Lead.source_message_id == event.message_id))
        return LeadProcessingResult(
            LeadProcessingStatus.ALREADY_PROCESSED,
            lead_id=lead.id if lead is not None else None,
            smart_table_record_id=lead.smart_table_record_id if lead is not None else None,
        )

    def _create_smart_table_record(
        self,
        sales_user_id: str,
        fields: dict[str, str],
        lead_id: str,
        outbox_event_id: int,
        segment_index: int = 0,
        pending_confirmation_fields: tuple[str, ...] = (),
    ) -> LeadProcessingResult:
        """以机器人身份新建销售可见表格记录，并持久化同步成功或失败事实。

        参数：sales_user_id 为当前有效 actor；fields 为确定性提取字段；
        lead_id、事件和分段标识用于回写。
        返回值：包含表格记录标识的创建结果，或表格失败结论。
        异常：数据库回写错误向调用方传播；表格适配器错误转换为可审计失败结果。
        副作用：调用 SmartTableAdapter，并更新 Lead、同步结果、Outbox 与审计。
        """
        existing_record_id: str | None = None
        with self._session_factory() as session:
            existing_lead = session.get(Lead, lead_id)
            if existing_lead is not None and existing_lead.smart_table_record_id is not None:
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                if sync is not None and sync.status == "succeeded":
                    # 同一消息的另一分段失败后重试时，已成功分段不得再次调用无幂等键的表格创建。
                    return LeadProcessingResult(
                        LeadProcessingStatus.CREATED,
                        lead_id=lead_id,
                        smart_table_record_id=existing_lead.smart_table_record_id,
                    )
                # 记录已经由前一次调用创建，但 T09 或回写阶段失败时只恢复后续短步骤。
                existing_record_id = existing_lead.smart_table_record_id
        # Smart Table 是外部投影；先提交业务归属和销售上下文，再执行创建/核实/更新。
        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            if message.sales_user_id != sales_user_id:
                raise ValueError(f"智能表格同步销售身份不一致：{outbox_event_id}")
            self._pin_message_lead_before_external_sync(
                session, event, message, lead_id, segment_index
            )
        # 创建人和负责人共同写为当前销售，绝不使用机器人、管理员或公共账号。
        record_fields: dict[str, object] = {
            **DEFAULT_SMART_TABLE_FIELD_VALUES,
            **fields,
            "线索来源": "展会",
            # 每次机器人首次写入审核行都从未提交开始，CRM 成功后再改为已提交。
            "提交状态": "未提交",
            "创建人": sales_user_id,
            "负责人": sales_user_id,
        }
        logger.info("smart_table_first_lead_sync_started")
        record_id = existing_record_id
        try:
            if record_id is None:
                record = self._smart_table_adapter.create_record(
                    record_fields, actor=SmartTableActor.ROBOT
                )
                record_id = record.record_id
        except Exception as error:
            # 失败保留线索和待审同步事实，避免将外部错误误记为销售已看到记录。
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, outbox_event_id)
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                remote_record_id = getattr(error, "remote_record_id", None)
                if isinstance(remote_record_id, str) and remote_record_id:
                    # add ACK 后回读失败仍保留唯一远端记录，保护后续重试不再 add。
                    lead = session.get(Lead, lead_id)
                    if lead is not None:
                        lead.smart_table_record_id = remote_record_id
                    if sync is not None:
                        sync.smart_table_record_id = remote_record_id
                if sync is not None:
                    sync.error_summary = type(error).__name__
                failed_pending_review = self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_sync_retrying",
                    failed_event_type="smart_table_sync_failed_pending_review",
                    error=error,
                )
                if sync is not None:
                    sync.status = "failed_pending_review" if failed_pending_review else "retrying"
            logger.exception(
                "smart_table_first_lead_sync_failed",
                extra={"error_type": type(error).__name__},
            )
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=lead_id)

        with self._session_factory.begin() as session:
            lead = session.get(Lead, lead_id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
            if lead is None or sync is None:
                raise ValueError(f"线索同步事实不存在：{lead_id}")
            # 表格成功结果是后续审核和 CRM 提交唯一可用的表格定位信息。
            lead.smart_table_record_id = record_id
            sync.smart_table_record_id = record_id
            sync.status = "processing"
        try:
            # 显式标签路径不调用 Qwen，仍必须经 T09 生成受人工保护的冻结备注和字段来源。
            LeadReviewService(
                self._session_factory,
                self._smart_table_adapter,
                robot_submission_confirmation_available=self._robot_submission_confirmation_available,
            ).sync_ai_patch(
                lead_id,
                self._source_message_id(outbox_event_id),
                ExtractedLeadPatch(
                    trace_id=f"deterministic-{outbox_event_id}",
                    analysis=LeadAnalysis(intent="NEW_LEAD"),
                    fields={},
                    pending_confirmation_fields=pending_confirmation_fields,
                    low_confidence_candidates={},
                ),
            )
        except Exception as error:
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, outbox_event_id)
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                failed_pending_review = self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_sync_retrying",
                    failed_event_type="smart_table_sync_failed_pending_review",
                    error=error,
                )
                if sync is not None and sync.status != "succeeded":
                    sync.status = "failed_pending_review" if failed_pending_review else "retrying"
                    sync.error_summary = type(error).__name__
            logger.exception("smart_table_first_lead_remark_failed")
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            lead = session.get(Lead, lead_id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
            if lead is None or sync is None:
                raise ValueError(f"线索同步事实不存在：{lead_id}")
            first_success = sync.status != "succeeded" and sync.completed_at is None
            sync.status = "succeeded"
            sync.completed_at = sync.completed_at or utc_now()
            self._mark_assigned(session, event, lead_id, segment_index)
            self._refresh_context(session, message, lead_id)
            self._record_audit(session, event, "smart_table_record_created")
            if first_success:
                # 首次成功态和唯一通知同事务提交；已有历史成功时间时不补发上线通知。
                self._queue_first_success_notification(
                    session, event, message, lead, sync, segment_index
                )
        assert record_id is not None
        bind_log_context(record_id=record_id)
        logger.info("smart_table_first_lead_created")
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED,
            lead_id=lead_id,
            smart_table_record_id=record_id,
        )

    @staticmethod
    def _queue_first_success_notification(
        session: Session,
        event: OutboxEvent,
        message: IncomingMessage,
        lead: Lead,
        sync: SmartTableSync,
        segment_index: int,
    ) -> None:
        """为销售第一次已核实的智能表格写入登记唯一链接通知。

        参数：session 为最终同步事务；event、message、lead、sync 为本次来源事实；
        segment_index 为已归属分段。
        返回值：无；不满足首次成功条件或未配置链接时静默跳过。
        异常：数据库错误向事务传播；事务失败不会把表格写入记为成功。
        副作用：可能新增一条待发送 NotificationRecord，与 SmartTableSync 成功态原子提交。
        """
        settings = get_settings()
        url = settings.lead_smart_table_url
        if (
            not settings.lead_first_success_link_enabled
            or not isinstance(url, str)
            or not url.strip().startswith("https://")
            or event.event_type != "message_received"
            or event.sales_user_id != message.sales_user_id
            or lead.original_capturing_sales_user_id != message.sales_user_id
            or sync.status != "succeeded"
            or not lead.smart_table_record_id
            or sync.smart_table_record_id != lead.smart_table_record_id
        ):
            return

        # 只有已持久化为 assigned 的消息分段才代表明确线索归属。
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == message.message_id,
                LeadMessageResolution.segment_index == segment_index,
            )
        )
        if resolution is None or resolution.status != "assigned" or resolution.lead_id != lead.id:
            return

        # 锁销售身份行串行化并发成功事务，停用成员只保留表格结果而不发送通知。
        actor = session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == message.sales_user_id)
            .with_for_update()
        )
        if actor is None or not actor.is_active:
            return

        # 固定用户键防止多 Lead、消息重放、重启和并发 Worker 重复发行逻辑通知。
        notification_key = hashlib.sha256(
            f"lead_first_smart_table_success:{message.sales_user_id}".encode()
        ).hexdigest()
        if session.get(NotificationRecord, notification_key) is not None:
            return

        # 已有任何有效成功记录的销售按历史用户处理，不因功能上线收到补发。
        prior_success = session.scalar(
            select(SmartTableSync.id)
            .join(Lead, Lead.id == SmartTableSync.lead_id)
            .where(
                Lead.original_capturing_sales_user_id == message.sales_user_id,
                Lead.id != lead.id,
                Lead.smart_table_record_id.is_not(None),
                SmartTableSync.status == "succeeded",
                SmartTableSync.smart_table_record_id.is_not(None),
            )
            .limit(1)
        )
        if prior_success is not None:
            return

        content = (
            "🎉 你的第一条客户需求已成功录入企业微信智能表格！\n\n"
            "点击下方链接，即可查看和完善客户信息：\n\n"
            f"[📋 打开需求登记智能表格]({url.strip()})\n\n"
            "后续可继续发送客户需求，我会自动录入。你可以随时打开智能表格查看和完善信息。"
        )
        session.add(
            NotificationRecord(
                notification_key=notification_key,
                sales_user_id=message.sales_user_id,
                source_message_id=message.message_id,
                notification_type="lead_first_smart_table_success",
                content=content,
            )
        )

    def _source_message_id(self, outbox_event_id: int) -> str:
        """读取审核同步所需的已持久化来源消息标识。

        参数：outbox_event_id 为当前 Outbox 事件标识。
        返回值：该事件关联的来源消息标识。
        异常：事件或消息缺失时抛出 ValueError。
        副作用：仅读取数据库，不执行外部调用。
        """
        with self._session_factory() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            if event.message_id != message.message_id:
                raise ValueError(f"Outbox 来源消息不一致：{outbox_event_id}")
            return message.message_id

    def _record_sync_failure(
        self,
        session: Session,
        event: OutboxEvent,
        *,
        retrying_event_type: str,
        failed_event_type: str,
        error: BaseException | None = None,
        force_retrying: bool = False,
        retry_reason: str | None = None,
    ) -> bool:
        """记录一次外部同步失败，并在重试耗尽时将其变为顺序检查点。

        参数：session 为当前事务；event 为失败来源事件；两个 event_type 分别记录可重试和耗尽结论；
        force_retrying 表示已绑定的字段补写不得收敛为人工检查点；retry_reason 为安全原因码。
        返回值：本次失败是否使事件进入 failed_pending_review。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：增加尝试次数，更新任务状态并写入对应业务审计事件。
        """
        event.attempts += 1
        failure_category: str | None = None
        failure_code: str | None = None
        if error is not None:
            failure_details = self._failure_audit_details(error)
            failure_category = str(failure_details["failure_category"])
            failure_code = str(failure_details["failure_code"])
            if force_retrying:
                # 已绑定远端记录的字段补写不能丢失；即使本轮预算耗尽也保留可恢复任务。
                failure_category = "transient"
                failure_code = retry_reason or failure_code
            event.failure_category = failure_category
            event.failure_summary = retry_reason or safe_failure_summary(error)
        audit_details: dict[str, object] | None = (
            {"failure_category": failure_category, "failure_code": failure_code}
            if failure_category is not None and failure_code is not None
            else None
        )
        if force_retrying:
            event.status = "retrying"
            self._record_audit(session, event, retrying_event_type, details=audit_details)
            logger.warning("lead_outbox_retrying", extra={"reason_code": retry_reason})
            return False
        if failure_category is not None and failure_category != "transient":
            # 永久或未知失败不重复调用确定性失败的外部接口，直接成为可人工处理的检查点。
            event.status = "failed_pending_review"
            event.failed_at = utc_now()
            self._record_audit(session, event, failed_event_type, details=audit_details)
            logger.error("lead_outbox_failed_pending_review")
            return True
        # 配置值表示额外重试次数：首次失败可重试，超过上限后才允许后续消息越过。
        if event.attempts > self._lead_message_retry_count:
            event.status = "failed_pending_review"
            event.failed_at = utc_now()
            self._record_audit(session, event, failed_event_type, details=audit_details)
            logger.error("lead_outbox_failed_pending_review")
            return True
        event.status = "retrying"
        self._record_audit(session, event, retrying_event_type, details=audit_details)
        logger.warning("lead_outbox_retrying")
        return False

    @staticmethod
    def _failure_audit_details(error: BaseException) -> dict[str, object]:
        """生成可写入业务审计的受控失败分类和代码。

        参数：error 为外部同步异常。
        返回值：只包含失败分类与安全失败代码，不包含异常正文、响应体或凭据。
        异常：无；未知异常使用异常类型名作为最后兜底。
        副作用：无。
        """
        failure_category = classify_task_failure(error).value
        external_error_code = getattr(error, "external_error_code", None)
        if type(external_error_code) is int:
            failure_code = str(external_error_code)
        else:
            internal_error_code = getattr(error, "error_code", None)
            failure_code = (
                internal_error_code[:64]
                if isinstance(internal_error_code, str) and internal_error_code
                else type(error).__name__[:64]
            )
        return {"failure_category": failure_category, "failure_code": failure_code}

    def _record_audit(
        self,
        session: Session,
        event: OutboxEvent,
        event_type: str,
        details: dict[str, object] | None = None,
    ) -> None:
        """为当前 Outbox 处理阶段添加唯一且可查询的业务审计事件。

        参数：session 为当前事务；event 为来源事件；event_type 为受控处理阶段名称；
        details 为可选审计详情。
        返回值：无。
        异常：数据库查询或写入失败时由 SQLAlchemy 抛出。
        副作用：首次出现的阶段向业务审计表新增一条记录。
        """
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == event.message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=event.message_id,
                    sales_user_id=event.sales_user_id,
                    event_type=event_type,
                    details=details or {},
                )
            )


class LeadReassignmentService:
    """以销售权限和字段来源约束执行消息分段的人工重新归属。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """注入人工重归属需要的事务边界。

        参数：session_factory 创建数据库事务。
        返回值：无。
        异常：无。
        副作用：仅保存会话工厂。
        """
        self._session_factory = session_factory

    def reassign(
        self,
        message_id: str,
        segment_index: int,
        new_lead_id: str,
        operator_user_id: str,
        reason: str,
        *,
        operation_id: str | None = None,
    ) -> None:
        """将销售自己的消息分段重新归属，并只安全补充来源字段。

        参数：message_id 和 segment_index 定位来源；new_lead_id 为新目标；
        operator_user_id 为操作销售；reason 为原因。
        返回值：无。
        异常：消息、归属或目标不存在时抛出 ValueError；
        越权或空原因时抛出 PermissionError/ValueError。
        副作用：创建重归属审计，成功后更新归属和安全来源记录。
        """
        if not reason.strip():
            raise ValueError("人工重归属必须填写原因")
        request = self._prepare_reassignment(
            message_id,
            segment_index,
            new_lead_id,
            operator_user_id,
            reason.strip(),
            operation_id=operation_id,
        )
        with self._session_factory.begin() as session:
            # T07 尚未读取表格人工编辑状态，因此重归属绝不直接写入表格字段。
            resolution = self._get_resolution(session, request.message_id, request.segment_index)
            target = session.get(Lead, request.new_lead_id)
            audit = session.get(MessageReassignmentAudit, request.audit_id)
            if resolution is None or target is None or audit is None:
                raise ValueError("重归属完成时缺少归属结论、目标线索或审计事实")
            if audit.status == "succeeded":
                return
            safe_fields = {
                field_name: value
                for field_name, value in request.safe_fields.items()
                if not target.field_values.get(field_name)
            }
            resolution.lead_id = request.new_lead_id
            resolution.status = "assigned"
            if safe_fields:
                # 第二次校验后仍为空的字段才进入后台事实，防止覆盖同期新增值。
                target.field_values = {**target.field_values, **safe_fields}
                for field_name, value in safe_fields.items():
                    session.add(
                        LeadFieldProvenance(
                            lead_id=target.id,
                            source_message_id=request.message_id,
                            field_name=field_name,
                            value=serialize_field_value(value),
                        )
                    )
            audit.status = "succeeded"
            audit.error_summary = None
            self._record_reassignment_audit(session, request.message_id)
        logger.info(
            "lead_message_reassigned",
            extra={"message_id": request.message_id, "lead_id": request.new_lead_id},
        )

    def _prepare_reassignment(
        self,
        message_id: str,
        segment_index: int,
        new_lead_id: str,
        operator_user_id: str,
        reason: str,
        *,
        operation_id: str | None = None,
    ) -> ReassignmentRequest:
        """校验权限并持久化一条可恢复的人工重归属请求。

        参数：各参数共同定位来源分段、新目标、操作人和受审计原因。
        返回值：完成外部表格增量写入所需的不可变请求。
        异常：事实缺失或权限不足时抛出 ValueError 或 PermissionError。
        副作用：新增 processing 状态的重归属审计，不改变当前归属。
        """
        with self._session_factory.begin() as session:
            message = session.get(IncomingMessage, message_id)
            resolution = self._get_resolution(session, message_id, segment_index)
            target = session.get(Lead, new_lead_id)
            operator = session.get(SalesAuthorization, operator_user_id)
            if message is None or resolution is None or target is None or operator is None:
                raise ValueError("消息分段、归属结论、新目标线索或操作人不存在")
            if not operator.is_active:
                raise PermissionError("操作人没有可用的 actor")
            previous = (
                session.get(Lead, resolution.lead_id) if resolution.lead_id is not None else None
            )
            if resolution.lead_id is not None and previous is None:
                raise ValueError("原目标线索不存在")
            if not operator.is_administrator and (
                message.sales_user_id != operator_user_id
                or target.smart_table_owner_user_id != operator_user_id
                or (previous is not None and previous.smart_table_owner_user_id != operator_user_id)
            ):
                # 普通销售必须同时拥有来源消息、原目标和新目标；管理员由授权目录显式放行。
                raise PermissionError("销售只能重新归属自己的消息和线索")
            if operation_id is not None:
                existing_audit = session.scalar(
                    select(MessageReassignmentAudit)
                    .where(MessageReassignmentAudit.operation_id == operation_id)
                    .with_for_update()
                )
                if existing_audit is not None:
                    return ReassignmentRequest(
                        audit_id=existing_audit.id,
                        message_id=message_id,
                        segment_index=segment_index,
                        new_lead_id=existing_audit.new_lead_id,
                        safe_fields={},
                    )
            safe_fields = self._safe_provenance_fields(
                session, message_id, resolution.lead_id, target
            )
            audit = MessageReassignmentAudit(
                message_id=message_id,
                segment_index=segment_index,
                previous_lead_id=resolution.lead_id,
                new_lead_id=new_lead_id,
                operator_user_id=operator_user_id,
                operator_role="administrator" if operator.is_administrator else "sales",
                reason=reason,
                operation_id=operation_id,
            )
            session.add(audit)
            session.flush()
            return ReassignmentRequest(
                audit_id=audit.id,
                message_id=message_id,
                segment_index=segment_index,
                new_lead_id=new_lead_id,
                safe_fields=safe_fields,
            )

    def _get_resolution(
        self, session: Session, message_id: str, segment_index: int
    ) -> LeadMessageResolution | None:
        """按消息和分段读取唯一的当前归属结论。

        参数：session 为事务；message_id 和 segment_index 定位分段。
        返回值：存在时返回归属结论，否则返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：无。
        """
        return session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == message_id,
                LeadMessageResolution.segment_index == segment_index,
            )
        )

    def _safe_provenance_fields(
        self,
        session: Session,
        message_id: str,
        previous_lead_id: str | None,
        target: Lead,
    ) -> dict[str, str]:
        """计算可从原归属消息安全补充到新目标的字段来源。

        参数：session 为事务；message_id 为来源；previous_lead_id 为原目标；target 为新目标。
        返回值：仅包含新目标为空且由该消息确实贡献的字段。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：无。
        """
        if previous_lead_id is None:
            return {}
        provenances = session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == previous_lead_id,
                LeadFieldProvenance.source_message_id == message_id,
            )
        ).all()
        # 字段来源是唯一可移动的事实；没有来源记录的当前值不能因人工操作被猜测搬运。
        return {
            item.field_name: item.value
            for item in provenances
            if not target.field_values.get(item.field_name)
        }

    def _record_reassignment_audit(self, session: Session, message_id: str) -> None:
        """为人工重归属保存一次独立于自动处理阶段的业务审计。

        参数：session 为当前事务；message_id 为来源消息标识。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：首次操作时新增业务审计事件。
        """
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == message_id))
        if event is None:
            return
        exists = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == event.message_id,
                BusinessAuditEvent.event_type == "lead_message_reassigned",
            )
        )
        if exists is None:
            session.add(
                BusinessAuditEvent(
                    message_id=event.message_id,
                    sales_user_id=event.sales_user_id,
                    event_type="lead_message_reassigned",
                )
            )
