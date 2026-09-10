"""将 T02 文本 Outbox 事件写入销售个人智能表格审核工作区。"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.logging import bind_log_context, reset_log_context
from app.leads.identity import DatabaseSalesIdentityProvider, SalesIdentityProvider
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    SalesLeadContext,
    SmartTableSync,
)
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import SmartTableActor, SmartTableAdapter

logger = logging.getLogger(__name__)


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


class DeterministicFirstTextLeadExtractor:
    """仅识别显式标签文本，作为 T05 不调用 LLM 的临时确定性提取器。"""

    _label_to_field = {"客户": "线索名称", "公司": "线索名称", "联系人": "联系人"}
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
        for segment in re.split(r"[；;]", text):
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
                for segment in re.split(r"[；;]", text)
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


class FirstTextLeadWorkspaceService:
    """消费 T02 已持久化文本事件，创建一条销售个人审核线索和表格记录。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        sales_identity_provider: SalesIdentityProvider | None = None,
        lead_context_ttl_minutes: int | None = None,
    ) -> None:
        """注入数据库、表格和销售身份边界，避免业务层依赖真实 CLI 或 Qwen。

        参数：session_factory 创建事务；smart_table_adapter 写销售审核表；身份提供器可替换测试实现；
        lead_context_ttl_minutes 可覆盖环境中的上下文有效期。
        返回值：无。
        异常：无；依赖错误在消费时按其真实类型处理。
        副作用：仅保存依赖引用，不读写数据库或智能表格。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._sales_identity_provider = sales_identity_provider or DatabaseSalesIdentityProvider()
        configured_ttl_minutes = (
            get_settings().lead_context_ttl_minutes
            if lead_context_ttl_minutes is None
            else lead_context_ttl_minutes
        )
        if configured_ttl_minutes <= 0:
            raise ValueError("当前客户上下文有效期必须大于 0")
        self._lead_context_ttl = timedelta(minutes=configured_ttl_minutes)

    def consume(self, outbox_event_id: int) -> LeadProcessingResult:
        """消费一条 T02 Outbox 文本事件并将首次有效线索同步到共享审核表。

        参数：outbox_event_id 为 T02 已提交的待处理事件标识。
        返回值：返回创建、忽略、拒绝、重复或表格同步失败等确定性结论。
        异常：事件或来源消息丢失时抛出 ValueError；数据库异常向调用方传播。
        副作用：可能创建 Lead、字段来源、同步结果、审计事件及智能表格记录。
        """
        log_token = None
        context_update: tuple[str, str, str, dict[str, str], int] | None = None
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
                terminal_or_unknown_statuses = {
                    "succeeded",
                    "ignored",
                    "unauthorized",
                    "invalid",
                    "processing",
                    "failed_pending_review",
                }
                if event.status in terminal_or_unknown_statuses:
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

                extractor = DeterministicFirstTextLeadExtractor()
                extracted_patch = extractor.extract_patch(message.normalized_text)
                context_lead = self._get_active_context_lead(session, message)
                if context_lead is not None and "线索名称" not in extracted_patch:
                    if not extracted_patch:
                        self._mark_unassigned(session, event)
                        return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)

                    # 既有非空值不允许被碎片消息静默覆盖，只向当前线索补充空字段。
                    safe_patch = {
                        field_name: value
                        for field_name, value in extracted_patch.items()
                        if not context_lead.field_values.get(field_name)
                    }
                    if not safe_patch:
                        self._mark_assigned(session, event, context_lead.id)
                        self._refresh_context(session, message, context_lead.id)
                        return LeadProcessingResult(
                            LeadProcessingStatus.UPDATED,
                            lead_id=context_lead.id,
                            smart_table_record_id=context_lead.smart_table_record_id,
                        )
                    if context_lead.smart_table_record_id is None:
                        raise ValueError(f"当前线索缺少智能表格记录：{context_lead.id}")
                    event.status = "processing"
                    bind_log_context(lead_id=context_lead.id)
                    # 外部表格调用必须等本事务提交后执行，避免在销售顺序锁内等待网络。
                    context_update = (
                        message.message_id,
                        context_lead.id,
                        context_lead.smart_table_record_id,
                        safe_patch,
                        outbox_event_id,
                    )

                if context_update is None:
                    existing_lead = session.scalar(
                        select(Lead).where(Lead.source_message_id == message.message_id)
                    )
                    if existing_lead is None:
                        fields = extractor.extract(message.normalized_text)
                        if fields is None:
                            if extracted_patch or self._is_weak_identity_fragment(
                                message.normalized_text
                            ):
                                self._mark_unassigned(session, event)
                                return LeadProcessingResult(LeadProcessingStatus.UNASSIGNED)
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
                    bind_log_context(lead_id=lead.id)
                    # 会话提交后 ORM 对象会脱离；只将下一步需要的不可变标识带出事务。
                    sales_user_id = message.sales_user_id
                    lead_id = lead.id

            if context_update is not None:
                return self._update_smart_table_record(*context_update)
            assert fields is not None
            return self._create_smart_table_record(sales_user_id, fields, lead_id, outbox_event_id)
        finally:
            if log_token is not None:
                reset_log_context(log_token)

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
            return None
        lead = session.get(Lead, context.lead_id)
        if lead is None or lead.smart_table_owner_user_id != message.sales_user_id:
            return None
        return lead

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

    def _mark_unassigned(self, session: Session, event: OutboxEvent) -> None:
        """将无法可靠归属的消息持久化为待归属，并越过首次消费检查点。

        参数：session 为当前事务；event 为来源 Outbox 事件。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增待归属结论、审计事件并将任务标记为 succeeded。
        """
        resolution = session.get(LeadMessageResolution, event.message_id)
        if resolution is None:
            session.add(LeadMessageResolution(message_id=event.message_id, status="unassigned"))
        event.status = "succeeded"
        self._record_audit(session, event, "lead_message_unassigned")
        logger.info("lead_message_unassigned")

    def _mark_assigned(self, session: Session, event: OutboxEvent, lead_id: str) -> None:
        """保存消息已归属的结论，并完成无需表格写入的消费。

        参数：session 为当前事务；event 为来源 Outbox；lead_id 为归属线索。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增或更新归属结论并将任务标记为 succeeded。
        """
        resolution = session.get(LeadMessageResolution, event.message_id)
        if resolution is None:
            session.add(
                LeadMessageResolution(
                    message_id=event.message_id,
                    lead_id=lead_id,
                    status="assigned",
                )
            )
        event.status = "succeeded"
        self._record_audit(session, event, "lead_message_assigned")

    def _refresh_context(self, session: Session, message: IncomingMessage, lead_id: str) -> None:
        """将一条成功处理消息设为该销售当前线索上下文的最新时间点。

        参数：session 为当前事务；message 为成功消息；lead_id 为其归属线索。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：新增或更新销售当前客户上下文。
        """
        context = session.get(SalesLeadContext, message.sales_user_id)
        if context is None:
            session.add(
                SalesLeadContext(
                    sales_user_id=message.sales_user_id,
                    lead_id=lead_id,
                    last_message_received_at=message.received_at,
                )
            )
            return
        context.lead_id = lead_id
        context.last_message_received_at = message.received_at

    def _update_smart_table_record(
        self,
        source_message_id: str,
        lead_id: str,
        record_id: str,
        fields: dict[str, str],
        outbox_event_id: int,
    ) -> LeadProcessingResult:
        """将当前客户上下文中的安全字段补丁增量写入既有智能表格记录。

        参数：source_message_id 为来源消息；lead_id 和 record_id 定位既有记录；fields 为补丁；
        outbox_event_id 用于持久化任务结果。
        返回值：成功时返回 UPDATED，外部失败时返回 SYNC_FAILED。
        异常：关键持久化事实缺失时抛出 ValueError；数据库错误向调用方传播。
        副作用：调用 SmartTableAdapter，成功后保存字段来源、消息归属、上下文和审计。
        """
        logger.info("smart_table_context_update_started")
        try:
            self._smart_table_adapter.update_record(record_id, fields)
        except Exception as error:
            # 失败只留下可重试任务状态，当前销售的后续消息会等待或在失败终态后继续。
            with self._session_factory.begin() as session:
                event, _ = self._load_event_and_message(session, outbox_event_id)
                event.status = "retrying"
                self._record_audit(session, event, "smart_table_context_update_retrying")
            logger.exception(
                "smart_table_context_update_failed",
                extra={"error_type": type(error).__name__},
            )
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            if message.message_id != source_message_id:
                raise ValueError(f"上下文更新来源消息不一致：{outbox_event_id}")
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError(f"上下文更新线索不存在：{lead_id}")
            # 再次只追加空字段，避免未来并发路径把较新业务事实或人工修改覆盖回去。
            safe_fields = {
                field_name: value
                for field_name, value in fields.items()
                if not lead.field_values.get(field_name)
            }
            if safe_fields:
                lead.field_values = {**lead.field_values, **safe_fields}
                for field_name, value in safe_fields.items():
                    session.add(
                        LeadFieldProvenance(
                            lead_id=lead_id,
                            source_message_id=source_message_id,
                            field_name=field_name,
                            value=value,
                        )
                    )
            self._mark_assigned(session, event, lead_id)
            self._refresh_context(session, message, lead_id)
            self._record_audit(session, event, "smart_table_context_updated")
        logger.info("smart_table_context_updated")
        return LeadProcessingResult(
            LeadProcessingStatus.UPDATED,
            lead_id=lead_id,
            smart_table_record_id=record_id,
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
        completed_checkpoint_statuses = {
            "succeeded",
            "ignored",
            "unauthorized",
            "invalid",
            "failed_pending_review",
        }
        # failed_pending_review 是失败重试耗尽后的顺序检查点，后续消息不能被它永久阻塞。
        previous_event_id = session.scalar(
            select(OutboxEvent.id)
            .where(
                OutboxEvent.sales_user_id == event.sales_user_id,
                OutboxEvent.sequence < event.sequence,
                OutboxEvent.status.not_in(completed_checkpoint_statuses),
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
        self, sales_user_id: str, fields: dict[str, str], lead_id: str, outbox_event_id: int
    ) -> LeadProcessingResult:
        """以机器人身份新建销售可见表格记录，并持久化同步成功或失败事实。

        参数：sales_user_id 为当前授权销售；fields 为确定性提取字段；lead_id 和事件标识用于回写。
        返回值：包含表格记录标识的创建结果，或表格失败结论。
        异常：数据库回写错误向调用方传播；表格适配器错误转换为可审计失败结果。
        副作用：调用 SmartTableAdapter，并更新 Lead、同步结果、Outbox 与审计。
        """
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
                    sync.status = "retrying"
                    sync.error_summary = type(error).__name__
                event.status = "retrying"
                self._record_audit(session, event, "smart_table_sync_retrying")
            logger.exception(
                "smart_table_first_lead_sync_failed",
                extra={"error_type": type(error).__name__},
            )
            return LeadProcessingResult(LeadProcessingStatus.SYNC_FAILED, lead_id=lead_id)

        with self._session_factory.begin() as session:
            event, message = self._load_event_and_message(session, outbox_event_id)
            lead = session.get(Lead, lead_id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
            if lead is None or sync is None:
                raise ValueError(f"线索同步事实不存在：{lead_id}")
            # 表格成功结果是后续审核和 CRM 提交唯一可用的表格定位信息。
            lead.smart_table_record_id = record.record_id
            sync.smart_table_record_id = record.record_id
            sync.status = "succeeded"
            sync.completed_at = utc_now()
            self._mark_assigned(session, event, lead_id)
            self._refresh_context(session, message, lead_id)
            self._record_audit(session, event, "smart_table_record_created")
        bind_log_context(record_id=record.record_id)
        logger.info("smart_table_first_lead_created")
        return LeadProcessingResult(
            LeadProcessingStatus.CREATED,
            lead_id=lead_id,
            smart_table_record_id=record.record_id,
        )

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
