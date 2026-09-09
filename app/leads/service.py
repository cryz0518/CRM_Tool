"""将 T02 文本 Outbox 事件写入销售个人智能表格审核工作区。"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.logging import bind_log_context, reset_log_context
from app.leads.identity import DatabaseSalesIdentityProvider, SalesIdentityProvider
from app.leads.models import Lead, LeadFieldProvenance, SmartTableSync
from app.messaging.models import BusinessAuditEvent, IncomingMessage, OutboxEvent, utc_now
from app.smart_table.adapter import SmartTableActor, SmartTableAdapter

logger = logging.getLogger(__name__)


class LeadProcessingStatus(StrEnum):
    """描述一次 T02 Outbox 文本消费的可观察业务结论。"""

    CREATED = "created"
    IGNORED = "ignored"
    UNAUTHORIZED = "unauthorized"
    INVALID_EVENT = "invalid_event"
    ALREADY_PROCESSED = "already_processed"
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
        if text is None:
            return None

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

        return fields if fields.get("线索名称") else None


class FirstTextLeadWorkspaceService:
    """消费 T02 已持久化文本事件，创建一条销售个人审核线索和表格记录。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        sales_identity_provider: SalesIdentityProvider | None = None,
    ) -> None:
        """注入数据库、表格和销售身份边界，避免业务层依赖真实 CLI 或 Qwen。

        参数：session_factory 创建事务；smart_table_adapter 写销售审核表；身份提供器可替换测试实现。
        返回值：无。
        异常：无；依赖错误在消费时按其真实类型处理。
        副作用：仅保存依赖引用，不读写数据库或智能表格。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._sales_identity_provider = sales_identity_provider or DatabaseSalesIdentityProvider()

    def consume(self, outbox_event_id: int) -> LeadProcessingResult:
        """消费一条 T02 Outbox 文本事件并将首次有效线索同步到共享审核表。

        参数：outbox_event_id 为 T02 已提交的待处理事件标识。
        返回值：返回创建、忽略、拒绝、重复或表格同步失败等确定性结论。
        异常：事件或来源消息丢失时抛出 ValueError；数据库异常向调用方传播。
        副作用：可能创建 Lead、字段来源、同步结果、审计事件及智能表格记录。
        """
        log_token = None
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
                terminal_or_unknown_statuses = {
                    "succeeded",
                    "ignored",
                    "unauthorized",
                    "invalid",
                    "processing",
                }
                if event.status in terminal_or_unknown_statuses:
                    # ponytail: Adapter 无创建幂等键；未知结果待人工核验，接口提供键后再安全重试。
                    return self._processed_result(session, event)

                # Worker 在 T02 之后再次经过权威销售目录，异常 Outbox 不可绕过身份门禁。
                if not self._sales_identity_provider.is_authorized(session, message.sales_user_id):
                    event.status = "unauthorized"
                    self._record_audit(session, event, "lead_unauthorized")
                    logger.warning("lead_outbox_unauthorized")
                    return LeadProcessingResult(LeadProcessingStatus.UNAUTHORIZED)

                existing_lead = session.scalar(
                    select(Lead).where(Lead.source_message_id == message.message_id)
                )
                if existing_lead is None:
                    fields = DeterministicFirstTextLeadExtractor().extract(message.normalized_text)
                    if fields is None:
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

            return self._create_smart_table_record(sales_user_id, fields, lead_id, outbox_event_id)
        finally:
            if log_token is not None:
                reset_log_context(log_token)

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
            event, _ = self._load_event_and_message(session, outbox_event_id)
            lead = session.get(Lead, lead_id)
            sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
            if lead is None or sync is None:
                raise ValueError(f"线索同步事实不存在：{lead_id}")
            # 表格成功结果是后续审核和 CRM 提交唯一可用的表格定位信息。
            lead.smart_table_record_id = record.record_id
            sync.smart_table_record_id = record.record_id
            sync.status = "succeeded"
            sync.completed_at = utc_now()
            event.status = "succeeded"
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
