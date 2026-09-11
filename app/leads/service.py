"""将 T02 文本 Outbox 事件写入销售个人智能表格审核工作区。"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.ai.gateway import AIGateway, AIGatewayError
from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.companies.models import CompanyRegionEvidence, CompanyUpsertCommand
from app.companies.service import CompanyLeadService
from app.core.config import get_settings
from app.core.logging import bind_log_context, reset_log_context
from app.leads.identity import DatabaseSalesIdentityProvider, SalesIdentityProvider
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageReassignmentAudit,
    SalesLeadContext,
    SmartTableSync,
)
from app.leads.review import LeadReviewService
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    MessageAttachment,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import SmartTableActor, SmartTableAdapter

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
    SYNC_FAILED = "sync_failed"


@dataclass(frozen=True)
class LeadProcessingResult:
    """返回 Outbox 消费结果及已创建的线索和智能表格记录标识。"""

    status: LeadProcessingStatus
    lead_id: str | None = None
    smart_table_record_id: str | None = None
    lead_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ContextUpdateRequest:
    """描述提交事务后可安全执行的一次既有智能表格字段补丁。"""

    source_message_id: str
    lead_id: str
    record_id: str
    fields: dict[str, str]
    outbox_event_id: int


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


@dataclass(frozen=True)
class ReassignmentRequest:
    """描述完成权限校验后可安全写入新目标表格的人工重归属请求。"""

    audit_id: int
    message_id: str
    segment_index: int
    new_lead_id: str
    safe_fields: dict[str, str]


class DeterministicFirstTextLeadExtractor:
    """仅识别显式标签文本，作为 T05 不调用 LLM 的临时确定性提取器。"""

    _label_to_field = {
        "客户": "线索名称",
        "公司": "线索名称",
        "联系人": "联系人",
        "手机": "手机",
        "手机号": "手机",
        "电话": "电话",
        "邮箱": "邮箱",
    }
    _process_values = ("装配", "码垛", "视觉检测", "贴标", "开箱机", "自助加油/充电", "商业应用")

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
        # 仅接受人工可读的“标签：值”片段，避免将普通聊天猜测为客户信息。
        for segment in re.split(r"[；;，,、]", text):
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
                for segment in re.split(r"[；;，,、]", text)
                for label, separator, value in [segment.partition("：")]
                if separator and label.strip() == "需求"
            ),
            "",
        )
        for process in self._process_values:
            if process in demand:
                fields["工艺"] = process
                break

        return fields

    def extract_many(self, text: str | None) -> list[dict[str, str]]:
        """从明确客户标签分段中提取一条或多条独立字段补丁。

        参数：text 为已标准化文本。
        返回值：按原消息顺序返回含线索名称的字段补丁；普通文本返回空列表。
        异常：无；不调用外部服务。
        副作用：无。
        """
        if text is None:
            return []
        # 仅在明确字段标签前切分，兼容销售常用的换行、分号和逗号而不猜测自由文本边界。
        segments = re.split(r"(?:\r?\n|[；;，,、]\s*(?=(?:客户|公司)\s*[:：]))", text)
        return [fields for segment in segments if (fields := self.extract(segment)) is not None]

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
    ) -> None:
        """注入数据库、表格和销售身份边界，避免业务层依赖真实 CLI 或 Qwen。

        参数：session_factory 创建事务；smart_table_adapter 写销售审核表；身份提供器可替换测试实现；
        lead_context_ttl_minutes 可覆盖环境中的上下文有效期；
        ai_gateway 为可替换的 T08 网关；company_lead_service 负责 T10 公司核验与销售内去重。
        返回值：无。
        异常：无；依赖错误在消费时按其真实类型处理。
        副作用：仅保存依赖引用，不读写数据库或智能表格。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._sales_identity_provider = sales_identity_provider or DatabaseSalesIdentityProvider()
        self._ai_gateway = ai_gateway
        self._company_lead_service = company_lead_service
        settings = get_settings()
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
        result = self._apply_company_resolution(outbox_event_id, result)
        # 只在本事件已越过首次消费检查点后继续，防止 retrying/processing 事件被错误跳过。
        self._consume_next_after_checkpoint(outbox_event_id)
        return result

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
            smart_table_record_id=resolved[0][1].smart_table_record_id,
            lead_ids=final_ids if len(final_ids) > 1 else (),
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
        ai_review: AIReviewRequest | None = None
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
                    self._record_audit(session, event, "lead_outbox_processing_lease_expired")
                    logger.error("lead_outbox_processing_lease_expired")
                    return self._processed_result(session, event)
                if not claimed_for_processing and self._processing_lease_expired(event):
                    # 失联 Worker 不得永久占住该销售队列；未知外部结果保留给人工核验而不重放。
                    event.status = "failed_pending_review"
                    self._record_audit(session, event, "lead_outbox_processing_lease_expired")
                    logger.error("lead_outbox_processing_lease_expired")
                    return self._processed_result(session, event)
                if not claimed_for_processing and event.status in terminal_or_unknown_statuses:
                    # ponytail: Adapter 无创建幂等键；未知结果待人工核验，接口提供键后再安全重试。
                    return self._processed_result(session, event)
                if self._has_unfinished_previous_event(session, event):
                    # 同一销售只能按持久化 sequence 前进，不能由 Worker 实际完成快慢决定归属。
                    logger.info("lead_outbox_waiting_for_previous")
                    return LeadProcessingResult(LeadProcessingStatus.WAITING_FOR_PREVIOUS)

                # Worker 在 T02 之后再次经过权威销售目录，异常 Outbox 不可绕过身份门禁。
                if not self._sales_identity_provider.is_authorized(session, message.sales_user_id):
                    event.status = "unauthorized"
                    self._record_audit(session, event, "lead_unauthorized")
                    logger.warning("lead_outbox_unauthorized")
                    return LeadProcessingResult(LeadProcessingStatus.UNAUTHORIZED)

                if self._media_enrichment_is_pending(session, message):
                    # 附件下载和识别独立失败；仅在尚未取得其终态时保持 Outbox pending。
                    logger.info("lead_outbox_waiting_for_media_enrichment")
                    return LeadProcessingResult(LeadProcessingStatus.WAITING_FOR_PREVIOUS)

                extractor = DeterministicFirstTextLeadExtractor()
                multi_fields = extractor.extract_many(message.normalized_text)
                if len(multi_fields) <= 1 and extractor.has_ambiguous_multiple_companies(
                    message.normalized_text
                ):
                    # 多个公司候选未能按明确边界拆开时，宁可待归属也不能以最后字段覆盖前段事实。
                    self._mark_unassigned(session, event)
                    return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
                if len(multi_fields) > 1:
                    # 多客户消息先在同一事务内固定所有分段事实，再按既有失败检查点逐条同步。
                    multi_request = self._prepare_multi_leads(
                        session, event, message, multi_fields, outbox_event_id
                    )
                else:
                    multi_request = None
                extracted_patch = extractor.extract_patch(message.normalized_text)
                # 强身份优先于当前上下文，避免销售补充历史客户时把字段串到最近客户。
                context_lead = (
                    None
                    if multi_request is not None
                    else self._get_strong_identity_lead(session, message, extracted_patch)
                )
                if context_lead is None and multi_request is None:
                    context_lead = self._get_active_context_lead(session, message)
                if (
                    multi_request is None
                    and not extracted_patch
                    and message.normalized_text
                    and self._ai_gateway is not None
                ):
                    # 显式标签和多客户仍走既有确定性路径；仅自由文本在归属判定后进入 T08。
                    ai_review, ai_result = self._prepare_ai_review(
                        session, event, message, context_lead
                    )
                    if ai_result is not None:
                        return ai_result
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
                    and ai_review is None
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
                    if not context_patch and not same_context_company and not temporary_context:
                        self._mark_unassigned(session, event)
                        return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

                    # 既有非空值不允许被碎片消息静默覆盖，只向当前线索补充空字段。
                    safe_patch = self._only_empty_fields(context_lead, context_patch)
                    if not safe_patch:
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
                        raise ValueError(f"当前线索缺少智能表格记录：{context_lead.id}")
                    event.status = "processing"
                    event.processing_started_at = utc_now()
                    bind_log_context(lead_id=context_lead.id)
                    # 外部表格调用必须等本事务提交后执行，避免在销售顺序锁内等待网络。
                    context_update = ContextUpdateRequest(
                        source_message_id=message.message_id,
                        lead_id=context_lead.id,
                        record_id=context_lead.smart_table_record_id,
                        fields=safe_patch,
                        outbox_event_id=outbox_event_id,
                    )

                if multi_request is None and context_update is None and ai_review is None:
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

                        # 首次创建的两类销售归属同时固定为当前授权销售，后续转交不在 T05 范围内。
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
                                value=value,
                                # 确定性首录同样由系统写表，保存基线以便 T09 正确识别后续人工编辑。
                                last_ai_synced_value=value,
                                )
                            )
                        session.add(
                            SmartTableSync(lead_id=lead.id, source_message_id=message.message_id)
                        )
                        self._record_audit(session, event, "lead_created")
                    else:
                        # 重试只使用持久化字段快照，禁止重新解析并改变首次线索的业务事实。
                        lead = existing_lead
                        fields = dict(lead.field_values)
                    event.status = "processing"
                    event.processing_started_at = utc_now()
                    bind_log_context(lead_id=lead.id)
                    # 会话提交后 ORM 对象会脱离；只将下一步需要的不可变标识带出事务。
                    sales_user_id = message.sales_user_id
                    lead_id = lead.id

            if multi_request is not None:
                return self._create_multi_smart_table_records(multi_request)
            if ai_review is not None:
                return self._sync_ai_review(ai_review)
            if context_update is not None:
                return self._update_smart_table_record(context_update)
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
    ) -> tuple[AIReviewRequest | None, LeadProcessingResult | None]:
        """调用 T08 并以确定性规则决定安全的新增、更新或待归属结论。

        参数：session、event 和 message 为当前有序消费事实；
        context_lead 为 T07 已可靠定位的当前线索。
        返回值：可在提交后执行的 T09 请求，或已完成的消费结果；两者不会同时存在。
        异常：无；T08 失败被转换为明确的失败待审事实。
        副作用：可能创建最小 Lead 草稿、登记审计并将事件置为 processing 或 failed_pending_review。
        """
        assert self._ai_gateway is not None
        try:
            patch = self._ai_gateway.extract_fields(message.normalized_text or "")
        except AIGatewayError as error:
            # 网关已完成自身传输重试；此处绝不伪造建档成功，也不能阻塞该销售的后续消息。
            event.status = "failed_pending_review"
            self._record_audit(session, event, "ai_gateway_failed_pending_review")
            logger.exception(
                "ai_gateway_first_text_failed", extra={"error_type": type(error).__name__}
            )
            return None, LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED)

        if patch.analysis.intent == "IGNORE":
            event.status = "ignored"
            self._record_audit(session, event, "lead_text_ignored")
            logger.info("lead_text_ignored")
            return None, LeadProcessingResult(LeadProcessingStatus.IGNORED)
        if patch.analysis.intent == "MULTI_LEAD":
            # T07 只支持明确标签切分；模型声称多客户时不猜测边界或创建多条线索。
            self._mark_unassigned(session, event)
            return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

        fields = patch.fields
        same_context_company = context_lead is not None and fields.get(
            "线索名称"
        ) == context_lead.field_values.get("线索名称")
        if patch.analysis.intent == "UPDATE_LEAD" or same_context_company:
            if context_lead is None:
                # UPDATE 缺少 T07 已可靠的目标时必须进入待归属，不能由模型指定或猜测线索。
                self._mark_unassigned(session, event)
                return None, LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
            event.status = "processing"
            event.processing_started_at = utc_now()
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
            # 身份与负责人完全取自当前已授权销售；模型字段只会交给 T09 作为业务字段补丁。
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

    def _sync_ai_review(self, request: AIReviewRequest) -> LeadProcessingResult:
        """为 AI 已安全决定的目标创建审核记录（如需）并经 T09 写入字段补丁。

        参数：request 为事务提交后可执行的 AI 结果及真实销售身份。
        返回值：首次建档返回 CREATED，既有上下文补充返回 UPDATED，外部失败返回 SYNC_FAILED。
        异常：关键数据库事实缺失时抛出 ValueError；其他外部失败转为失败待审事实。
        副作用：可能创建智能表格记录、调用 T09，并完成消息归属和当前客户上下文。
        """
        record_id = self._ensure_ai_review_record(request)
        if record_id is None:
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=request.lead_id)
        try:
            # T09 负责人工编辑保护、中置信度 AI待确认与增量写入；不得由本层直接写模型字段。
            LeadReviewService(self._session_factory, self._smart_table_adapter).sync_ai_patch(
                request.lead_id, request.source_message_id, request.patch
            )
        except Exception as error:
            self._mark_ai_review_failed(request, error)
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=request.lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, request.outbox_event_id)
            lead = session.get(Lead, request.lead_id)
            if lead is None:
                raise ValueError(f"AI 审核线索不存在：{request.lead_id}")
            if request.creates_lead:
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead.id)
                )
                if sync is None:
                    raise ValueError(f"AI 审核同步事实不存在：{lead.id}")
                sync.status = "succeeded"
                sync.completed_at = utc_now()
                sync.error_summary = None
            self._mark_assigned(session, event, lead.id)
            self._refresh_context(session, message, lead.id)
            self._record_audit(session, event, "ai_review_fields_synced")
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED if request.creates_lead else LeadProcessingStatus.UPDATED,
            lead_id=request.lead_id,
            smart_table_record_id=record_id,
        )

    def _ensure_ai_review_record(self, request: AIReviewRequest) -> str | None:
        """为新 AI 草稿建立最小审核记录，避免模型字段绕过 T09 直接写入。

        参数：request 为目标 Lead、真实销售身份和待审核字段补丁。
        返回值：可供 T09 重读的表格记录标识；创建失败时返回 None。
        异常：数据库事实缺失时抛出 ValueError。
        副作用：首次新建记录并保存可审计的表格定位信息。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, request.lead_id)
            if lead is None:
                raise ValueError(f"AI 审核线索不存在：{request.lead_id}")
            if lead.smart_table_record_id is not None:
                return lead.smart_table_record_id
        try:
            # 创建人和负责人只使用接入层已授权的销售身份，模型无法影响权限关键字段。
            record = self._smart_table_adapter.create_record(
                {
                    "线索来源": "展会",
                    "创建人": request.sales_user_id,
                    "负责人": request.sales_user_id,
                },
                actor=SmartTableActor.ROBOT,
            )
        except Exception as error:
            self._mark_ai_review_failed(request, error)
            return None

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
        return record.record_id

    def _mark_ai_review_failed(self, request: AIReviewRequest, error: Exception) -> None:
        """把 AI 或审核表格失败固定为失败待审，避免重放模型结果或伪造成功。

        参数：request 定位来源事件和线索；error 为已捕获的外部异常。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：事件进入 failed_pending_review，并在新线索时更新同步失败事实。
        """
        with self._session_factory.begin() as session:
            event, _ = self._load_event_and_message(session, request.outbox_event_id)
            event.status = "failed_pending_review"
            if request.creates_lead:
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == request.lead_id)
                )
                if sync is not None:
                    sync.status = "failed_pending_review"
                    sync.error_summary = type(error).__name__
            self._record_audit(session, event, "ai_review_failed_pending_review")
        logger.exception("ai_review_sync_failed", extra={"error_type": type(error).__name__})

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
                                value=value,
                                # 多客户的确定性首录也需要同一份人工编辑比较基线。
                                last_ai_synced_value=value,
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
        """在当前销售范围内以唯一明确身份字段定位过期上下文后的既有线索。

        参数：session 为当前事务；message 为待归属消息；fields 为确定性提取的候选字段。
        返回值：公司、手机或邮箱恰好唯一命中时返回 Lead，否则返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取当前销售的线索草稿，不访问其他销售数据。
        """
        strong_fields = {"线索名称", "手机", "邮箱"}
        candidate_values = {
            field_name: value for field_name, value in fields.items() if field_name in strong_fields
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
        matches = [
            lead
            for lead in candidates
            if any(
                lead.field_values.get(field_name) == value
                for field_name, value in candidate_values.items()
            )
        ]
        # 多条记录命中时宁可保留待归属，也不能猜测应补充给哪一条线索。
        return matches[0] if len(matches) == 1 else None

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
        返回值：含预算、需求或项目等弱身份关键词时返回 True。
        异常：无。
        副作用：无。
        """
        weak_keywords = ("预算", "需求", "报价", "项目", "采购")
        return bool(text and any(keyword in text for keyword in weak_keywords))

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
        event.status = "succeeded"
        self._record_audit(session, event, "lead_message_assigned")
        logger.info("lead_message_assigned")

    def _only_empty_fields(self, lead: Lead, fields: dict[str, str]) -> dict[str, str]:
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

    def _refresh_context(self, session: Session, message: IncomingMessage, lead_id: str) -> None:
        """将一条成功处理消息设为该销售当前线索上下文的最新时间点。

        参数：session 为当前事务；message 为成功消息；lead_id 为其归属线索。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增或更新销售当前客户上下文。
        """
        context = session.get(SalesLeadContext, message.sales_user_id)
        if context is None:
            # 首条成功归属消息为该销售创建独立上下文，不与其他销售共享。
            session.add(
                SalesLeadContext(
                    sales_user_id=message.sales_user_id,
                    lead_id=lead_id,
                    last_message_received_at=message.received_at,
                )
            )
            return
        # 同一销售的后续成功消息才推进上下文，保留其当前线索和接收时间。
        context.lead_id = lead_id
        context.last_message_received_at = message.received_at

    def _update_smart_table_record(self, request: ContextUpdateRequest) -> LeadProcessingResult:
        """将当前客户上下文中的安全字段补丁增量写入既有智能表格记录。

        参数：request 为提交后可执行的上下文更新请求。
        返回值：成功时返回 UPDATED，外部失败时返回 SYNC_FAILED。
        异常：关键持久化事实缺失时抛出 ValueError；数据库错误向调用方传播。
        副作用：调用 SmartTableAdapter，成功后保存字段来源、消息归属、上下文和审计。
        """
        logger.info("smart_table_context_update_started")
        try:
            # 确定性补充也必须复用 T09：它负责人工保护、字段来源和基于安全快照的备注重建。
            LeadReviewService(self._session_factory, self._smart_table_adapter).sync_ai_patch(
                request.lead_id,
                request.source_message_id,
                ExtractedLeadPatch(
                    trace_id=f"deterministic-{request.outbox_event_id}",
                    analysis=LeadAnalysis(intent="UPDATE_LEAD"),
                    fields=request.fields,
                    pending_confirmation_fields=(),
                    low_confidence_candidates={},
                ),
            )
        except Exception as error:
            # 失败只留下可重试任务状态，当前销售的后续消息会等待或在失败终态后继续。
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, request.outbox_event_id)
                self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_context_update_retrying",
                    failed_event_type="smart_table_context_update_failed_pending_review",
                )
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
            self._mark_assigned(session, event, request.lead_id)
            self._refresh_context(session, message, request.lead_id)
            self._record_audit(session, event, "smart_table_context_updated")
        logger.info("smart_table_context_updated")
        return LeadProcessingResult(
            LeadProcessingStatus.UPDATED,
            lead_id=request.lead_id,
            smart_table_record_id=request.record_id,
        )

    def _lock_sales_processing_stream(self, session: Session, sales_user_id: str) -> None:
        """锁定一名销售的授权行，以原子判断该销售的消息消费顺序。

        参数：session 为当前事务；sales_user_id 为待消费消息的发送销售。
        返回值：无。
        异常：授权记录缺失时抛出 ValueError，避免未受保护的顺序判断。
        副作用：在当前事务提交前持有该销售授权行锁；不同销售不互相阻塞。
        """
        authorization = session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == sales_user_id)
            .with_for_update()
        )
        if authorization is None:
            raise ValueError(f"销售授权记录不存在：{sales_user_id}")

    def _has_unfinished_previous_event(self, session: Session, event: OutboxEvent) -> bool:
        """判断同一销售是否还有未越过首次消费检查点的较早事件。

        参数：session 为当前事务；event 为准备消费的 Outbox 事件。
        返回值：存在未完成较早事件时返回 True，否则返回 False。
        异常：数据库查询失败时由 SQLAlchemy 抛出。
        副作用：仅读取同一销售且 sequence 更小的 Outbox 事件。
        """
        # failed_pending_review 是失败重试耗尽后的顺序检查点，后续消息不能被它永久阻塞。
        previous_event_id = session.scalar(
            select(OutboxEvent.id)
            .where(
                OutboxEvent.sales_user_id == event.sales_user_id,
                OutboxEvent.sequence < event.sequence,
                OutboxEvent.status.not_in(COMPLETED_CHECKPOINT_STATUSES),
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
    ) -> LeadProcessingResult:
        """以机器人身份新建销售可见表格记录，并持久化同步成功或失败事实。

        参数：sales_user_id 为当前授权销售；fields 为确定性提取字段；
        lead_id、事件和分段标识用于回写。
        返回值：包含表格记录标识的创建结果，或表格失败结论。
        异常：数据库回写错误向调用方传播；表格适配器错误转换为可审计失败结果。
        副作用：调用 SmartTableAdapter，并更新 Lead、同步结果、Outbox 与审计。
        """
        with self._session_factory() as session:
            existing_lead = session.get(Lead, lead_id)
            if existing_lead is not None and existing_lead.smart_table_record_id is not None:
                # 同一消息的另一分段失败后重试时，已成功分段不得再次调用无幂等键的表格创建。
                return LeadProcessingResult(
                    LeadProcessingStatus.CREATED,
                    lead_id=lead_id,
                    smart_table_record_id=existing_lead.smart_table_record_id,
                )
        # 创建人和负责人共同写为当前销售，绝不使用机器人、管理员或公共账号。
        record_fields: dict[str, object] = {
            **fields,
            "线索来源": "展会",
            "创建人": sales_user_id,
            "负责人": sales_user_id,
        }
        logger.info("smart_table_first_lead_sync_started")
        try:
            record = self._smart_table_adapter.create_record(
                record_fields, actor=SmartTableActor.ROBOT
            )
        except Exception as error:
            # 失败保留线索和待审同步事实，避免将外部错误误记为销售已看到记录。
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, outbox_event_id)
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                if sync is not None:
                    sync.error_summary = type(error).__name__
                failed_pending_review = self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_sync_retrying",
                    failed_event_type="smart_table_sync_failed_pending_review",
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
            lead.smart_table_record_id = record.record_id
            sync.smart_table_record_id = record.record_id
            sync.status = "processing"
        try:
            # 显式标签路径不调用 Qwen，仍必须经 T09 生成受人工保护的冻结备注和字段来源。
            LeadReviewService(self._session_factory, self._smart_table_adapter).sync_ai_patch(
                lead_id,
                self._source_message_id(outbox_event_id),
                ExtractedLeadPatch(
                    trace_id=f"deterministic-{outbox_event_id}",
                    analysis=LeadAnalysis(intent="NEW_LEAD"),
                    fields={},
                    pending_confirmation_fields=(),
                    low_confidence_candidates={},
                ),
            )
        except Exception as error:
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, outbox_event_id)
                sync = session.scalar(
                    select(SmartTableSync).where(SmartTableSync.lead_id == lead_id)
                )
                if sync is not None:
                    sync.error_summary = type(error).__name__
                    sync.status = "failed_pending_review"
                self._record_sync_failure(
                    session,
                    event,
                    retrying_event_type="smart_table_sync_retrying",
                    failed_event_type="smart_table_sync_failed_pending_review",
                )
            logger.exception("smart_table_first_lead_remark_failed")
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
            if sync is None:
                raise ValueError(f"线索同步事实不存在：{lead_id}")
            sync.status = "succeeded"
            sync.completed_at = utc_now()
            self._mark_assigned(session, event, lead_id, segment_index)
            self._refresh_context(session, message, lead_id)
            self._record_audit(session, event, "smart_table_record_created")
        bind_log_context(record_id=record.record_id)
        logger.info("smart_table_first_lead_created")
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED,
            lead_id=lead_id,
            smart_table_record_id=record.record_id,
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
    ) -> bool:
        """记录一次外部同步失败，并在重试耗尽时将其变为顺序检查点。

        参数：session 为当前事务；event 为失败来源事件；两个 event_type 分别记录可重试和耗尽结论。
        返回值：本次失败是否使事件进入 failed_pending_review。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：增加尝试次数，更新任务状态并写入对应业务审计事件。
        """
        event.attempts += 1
        # 配置值表示额外重试次数：首次失败可重试，超过上限后才允许后续消息越过。
        if event.attempts > self._lead_message_retry_count:
            event.status = "failed_pending_review"
            self._record_audit(session, event, failed_event_type)
            logger.error("lead_outbox_failed_pending_review")
            return True
        event.status = "retrying"
        self._record_audit(session, event, retrying_event_type)
        logger.warning("lead_outbox_retrying")
        return False

    def _record_audit(self, session: Session, event: OutboxEvent, event_type: str) -> None:
        """为当前 Outbox 处理阶段添加唯一且可查询的业务审计事件。

        参数：session 为当前事务；event 为来源事件；event_type 为受控处理阶段名称。
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
            message_id, segment_index, new_lead_id, operator_user_id, reason.strip()
        )
        with self._session_factory.begin() as session:
            # T07 尚未读取表格人工编辑状态，因此重归属绝不直接写入表格字段。
            resolution = self._get_resolution(session, request.message_id, request.segment_index)
            target = session.get(Lead, request.new_lead_id)
            audit = session.get(MessageReassignmentAudit, request.audit_id)
            if resolution is None or target is None or audit is None:
                raise ValueError("重归属完成时缺少归属结论、目标线索或审计事实")
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
                            value=value,
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
            if not operator.is_active or not (operator.is_authorized or operator.is_administrator):
                raise PermissionError("操作人没有可用的重归属权限")
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
