"""将 T08 AI 字段补丁安全同步至 T03 智能表格审核工作区。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.ai.models import ExtractedLeadPatch
from app.core.config import get_settings
from app.leads.models import Lead, LeadFieldProvenance, UserConfirmationEvent
from app.messaging.models import BusinessAuditEvent, IncomingMessage
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.models import SmartTableRecord

logger = logging.getLogger(__name__)

CRM_REQUIRED_CORE_FIELDS = frozenset({"线索名称", "业务线"})
CRM_CONTACT_FIELDS = frozenset({"手机", "电话", "邮箱"})
AI_CONFIRMATION_FIELD = "AI待确认"


@dataclass(frozen=True)
class ReviewSyncResult:
    """描述一次 AI 审核同步实际写入与被人工保护的字段。"""

    updated_fields: tuple[str, ...]
    protected_fields: tuple[str, ...]


@dataclass(frozen=True)
class SubmissionConfirmationState:
    """描述提交前仍需机器人显式确认的 CRM 必填字段。"""

    blocking_fields: tuple[str, ...]

    @property
    def can_submit(self) -> bool:
        """返回当前审核状态是否已不再被 AI 待确认字段阻塞。

        返回值：不存在阻塞字段时返回 True。
        异常：无。
        副作用：无。
        """
        return not self.blocking_fields


class LeadReviewService:
    """在 AI 写入和 CRM 提交前保护销售表格编辑并维护确认事实。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        robot_submission_confirmation_available: bool | None = None,
    ) -> None:
        """注入 T05 持久化边界和 T03 智能表格适配器。

        参数：session_factory 创建数据库事务；smart_table_adapter 提供表格重读和增量写入；
        robot_submission_confirmation_available 可覆盖机器人卡片能力配置。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不读写外部系统。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        settings = get_settings()
        self._robot_submission_confirmation_available = (
            settings.robot_submission_confirmation_available
            if robot_submission_confirmation_available is None
            else robot_submission_confirmation_available
        )

    def sync_ai_patch(
        self, lead_id: str, source_message_id: str, patch: ExtractedLeadPatch
    ) -> ReviewSyncResult:
        """重读智能表格后同步 T08 的合法字段，并永久保护人工编辑字段。

        参数：lead_id 为目标线索；source_message_id 为已持久化 AI 来源消息；
        patch 为 T08 结果。
        返回值：实际写入与被保护字段的确定性结果。
        异常：线索、消息或表格记录缺失时抛出 ValueError；适配器错误向调用方传播。
        副作用：可能更新表格、字段来源、人工编辑标记和审计记录。
        """
        with self._session_factory() as session:
            lead = self._require_lead(session, lead_id)
            self._require_source_message(session, source_message_id)
            if lead.smart_table_record_id is None:
                raise ValueError(f"线索缺少智能表格记录：{lead_id}")
            record_id = lead.smart_table_record_id

        # 每次 AI 写入前都从 T03 Adapter 重读，不能使用过期的后台字段快照判断人工编辑。
        record = self._smart_table_adapter.get_record(record_id)
        if record is None:
            raise ValueError(f"智能表格记录不存在：{record_id}")
        current_fields = dict(record.fields)

        with self._session_factory.begin() as session:
            lead = self._require_lead(session, lead_id)
            provenance = self._latest_provenance_by_field(session, lead_id)
            pending = self._confirmation_names(current_fields.get(AI_CONFIRMATION_FIELD))
            protected = self._detect_user_edits(
                session, lead, provenance, current_fields, pending, source_message_id
            )
            pending.difference_update(protected)
            fields_to_write: dict[str, object] = {}
            written_names: list[str] = []
            # T08 已完成结构和业务校验；本层仅决定是否可安全写入，不重新解释 AI 内容。
            for field_name, value in patch.fields.items():
                field_provenance = provenance.get(field_name)
                current_value = current_fields.get(field_name)
                is_pending = field_name in patch.pending_confirmation_fields
                if is_pending and self._must_not_prefill_without_confirmation(field_name):
                    # 没有可靠卡片时，冻结规则要求必填中置信度候选仅留在后台，不写正式字段。
                    continue
                if field_provenance is not None and field_provenance.is_user_modified:
                    protected.add(field_name)
                    pending.discard(field_name)
                    continue
                if current_value not in (None, "") and (
                    field_provenance is None
                    or current_value != field_provenance.last_ai_synced_value
                ):
                    # 未由 AI 写入过的非空值同样不能被本轮建议静默覆盖。
                    protected.add(field_name)
                    continue
                if current_value == value:
                    if is_pending:
                        pending.add(field_name)
                    else:
                        pending.discard(field_name)
                    continue
                fields_to_write[field_name] = value
                written_names.append(field_name)
                if is_pending:
                    pending.add(field_name)
                else:
                    pending.discard(field_name)

            new_pending = sorted(pending)
            existing_pending = self._confirmation_names(current_fields.get(AI_CONFIRMATION_FIELD))
            if existing_pending != pending:
                fields_to_write[AI_CONFIRMATION_FIELD] = new_pending

        # 数据库事务不包裹外部调用；Adapter 只收到确有变化的字段补丁。
        if fields_to_write:
            self._smart_table_adapter.update_record(record_id, fields_to_write)

        with self._session_factory.begin() as session:
            lead = self._require_lead(session, lead_id)
            provenance = self._latest_provenance_by_field(session, lead_id)
            for field_name in written_names:
                value = patch.fields[field_name]
                source = provenance.get(field_name)
                if source is None:
                    session.add(
                        LeadFieldProvenance(
                            lead_id=lead_id,
                            source_message_id=source_message_id,
                            field_name=field_name,
                            value=value,
                            last_ai_synced_value=value,
                        )
                    )
                else:
                    source.value = value
                    source.last_ai_synced_value = value
            if protected:
                self._record_audit(
                    session,
                    source_message_id,
                    lead.smart_table_owner_user_id,
                    "user_edit_detected",
                )
            if written_names:
                self._record_audit(
                    session,
                    source_message_id,
                    lead.smart_table_owner_user_id,
                    "ai_review_fields_synced",
                )

        logger.info("lead_review_patch_synced", extra={"lead_id": lead_id, "record_id": record_id})
        return ReviewSyncResult(tuple(written_names), tuple(sorted(protected)))

    def get_submission_confirmation_state(self, lead_id: str) -> SubmissionConfirmationState:
        """重读审核表并返回当前 CRM 最小必填集中仍待显式确认的字段。

        参数：lead_id 为准备进入确定性 CRM 提交阶段的线索。
        返回值：仅含阻塞字段的提交前确认状态；本方法不调用 CRM。
        异常：线索或表格记录缺失时抛出 ValueError；适配器读取异常向调用方传播。
        副作用：发现销售修改时会持久化人工确认状态并维护 AI待确认。
        """
        record, lead = self._read_record_for_lead(lead_id)
        protected = self._reconcile_user_edits(lead, record.fields, lead.source_message_id)
        pending = self._confirmation_names(record.fields.get(AI_CONFIRMATION_FIELD))
        pending.difference_update(protected)
        blocking = self._required_pending_fields(pending, record.fields)
        return SubmissionConfirmationState(tuple(sorted(blocking)))

    def confirm_submission_fields(
        self, lead_id: str, sales_user_id: str, field_names: tuple[str, ...]
    ) -> SubmissionConfirmationState:
        """记录销售对机器人列出的待确认字段的显式确认，并移除相应表格元数据。

        参数：lead_id 为目标线索；sales_user_id 为确认销售；field_names 为卡片提交的字段名。
        返回值：确认后的提交前阻塞状态；本方法不调用 CRM。
        异常：销售非表格负责人、字段非当前阻塞项或表格缺失时抛出 ValueError。
        副作用：更新 AI待确认、字段来源和人工确认事件。
        """
        record, lead = self._read_record_for_lead(lead_id)
        self._reconcile_user_edits(lead, record.fields, lead.source_message_id)
        record, lead = self._read_record_for_lead(lead_id)
        if lead.smart_table_owner_user_id != sales_user_id:
            raise ValueError("只有当前智能表格负责人可以确认待提交字段")
        current_pending = self._confirmation_names(record.fields.get(AI_CONFIRMATION_FIELD))
        blocking = self._required_pending_fields(current_pending, record.fields)
        requested = set(field_names)
        if not requested or not requested.issubset(blocking):
            raise ValueError("确认字段必须是当前阻塞的 AI待确认 字段")

        remaining = sorted(current_pending - requested)
        self._smart_table_adapter.update_record(
            record.record_id, {AI_CONFIRMATION_FIELD: remaining}
        )
        with self._session_factory.begin() as session:
            provenance = self._latest_provenance_by_field(session, lead_id)
            for field_name in sorted(requested):
                value = record.fields.get(field_name)
                if not isinstance(value, str) or not value:
                    raise ValueError(f"确认字段缺少合法表格值：{field_name}")
                source = provenance.get(field_name)
                if source is not None:
                    source.is_user_confirmed = True
                session.add(
                    UserConfirmationEvent(
                        lead_id=lead_id,
                        field_name=field_name,
                        confirmed_value=value,
                        operator_sales_user_id=sales_user_id,
                    )
                )
            self._record_audit(
                session,
                lead.source_message_id,
                sales_user_id,
                "submission_ai_fields_confirmed",
            )
        return self.get_submission_confirmation_state(lead_id)

    def _read_record_for_lead(self, lead_id: str) -> tuple[SmartTableRecord, Lead]:
        """读取目标线索及其当前智能表格快照。

        参数：lead_id 为目标线索标识。
        返回值：智能表格记录和脱离会话的 Lead 事实。
        异常：线索或记录定位缺失时抛出 ValueError。
        副作用：读取数据库和智能表格。
        """
        with self._session_factory() as session:
            lead = self._require_lead(session, lead_id)
            if lead.smart_table_record_id is None:
                raise ValueError(f"线索缺少智能表格记录：{lead_id}")
            record_id = lead.smart_table_record_id
            session.expunge(lead)
        record = self._smart_table_adapter.get_record(record_id)
        if record is None:
            raise ValueError(f"智能表格记录不存在：{record_id}")
        return record, lead

    def _reconcile_user_edits(
        self, lead: Lead, current_fields: Mapping[str, object], source_message_id: str
    ) -> set[str]:
        """将提交前重读发现的人工编辑持久化，并移除对应待确认元数据。

        参数：lead 为目标线索；current_fields 为表格快照；source_message_id 为审计来源消息。
        返回值：本次识别出的人工保护字段名称。
        异常：数据库或表格更新失败时向调用方传播。
        副作用：更新字段来源、AI待确认与审计事件。
        """
        with self._session_factory.begin() as session:
            provenance = self._latest_provenance_by_field(session, lead.id)
            pending = self._confirmation_names(current_fields.get(AI_CONFIRMATION_FIELD))
            protected = self._detect_user_edits(
                session, lead, provenance, current_fields, pending, source_message_id
            )
            if not protected:
                return set()
            remaining = sorted(pending - protected)
        # 销售不维护 AI待确认；系统只因实际业务字段偏离才移除对应标记。
        self._smart_table_adapter.update_record(
            lead.smart_table_record_id or "", {AI_CONFIRMATION_FIELD: remaining}
        )
        return protected

    def _detect_user_edits(
        self,
        session: Session,
        lead: Lead,
        provenance: Mapping[str, LeadFieldProvenance],
        current_fields: Mapping[str, object],
        pending: set[str],
        source_message_id: str,
    ) -> set[str]:
        """识别与最后 AI 同步值不同的字段并永久标记人工修改和确认。

        参数：session 为数据库事务；lead 为目标线索；provenance 为最新字段来源；
        其余参数为表格与审计上下文。
        返回值：本次新识别或已有的人工保护字段名称。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：更新字段来源状态并写入人工编辑审计。
        """
        protected: set[str] = set()
        for field_name, source in provenance.items():
            if source.is_user_modified:
                protected.add(field_name)
                continue
            if source.last_ai_synced_value is None:
                continue
            if current_fields.get(field_name) != source.last_ai_synced_value:
                # 当前值偏离 AI 写入值是唯一的自动确认依据，时间流逝或查看记录均不构成确认。
                source.is_user_modified = True
                source.is_user_confirmed = True
                protected.add(field_name)
                self._record_audit(
                    session,
                    source_message_id,
                    lead.smart_table_owner_user_id,
                    "user_edit_detected",
                )
        return protected

    def _latest_provenance_by_field(
        self, session: Session, lead_id: str
    ) -> dict[str, LeadFieldProvenance]:
        """按字段读取最新来源记录，兼容 T05 已保存的历史来源事实。

        参数：session 为数据库事务；lead_id 为目标线索。
        返回值：字段名到最新来源记录的映射。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取字段来源。
        """
        sources = session.scalars(
            select(LeadFieldProvenance)
            .where(LeadFieldProvenance.lead_id == lead_id)
            .order_by(LeadFieldProvenance.id.desc())
        ).all()
        latest: dict[str, LeadFieldProvenance] = {}
        for source in sources:
            # 查询已按倒序排列，首个来源记录才是该字段的当前保护事实。
            latest.setdefault(source.field_name, source)
        return latest

    def _required_pending_fields(
        self, pending: set[str], current_fields: Mapping[str, object]
    ) -> set[str]:
        """按项目 CRM 最小必填集计算哪些待确认字段会阻塞确定性提交。

        参数：pending 为表格 AI待确认 名称；current_fields 为当前业务字段快照。
        返回值：仍需机器人显式确认的字段集合。
        异常：无。
        副作用：无。
        """
        blocking = pending & CRM_REQUIRED_CORE_FIELDS
        # 只要存在一个非待确认联系方式，其他待确认联系方式不应阻塞项目级“至少一种”规则。
        has_confirmed_contact = any(
            current_fields.get(field_name) not in (None, "") and field_name not in pending
            for field_name in CRM_CONTACT_FIELDS
        )
        if not has_confirmed_contact:
            blocking.update(
                field_name
                for field_name in CRM_CONTACT_FIELDS
                if field_name in pending and current_fields.get(field_name) not in (None, "")
            )
        return blocking

    def _must_not_prefill_without_confirmation(self, field_name: str) -> bool:
        """判断卡片不可用时某个中置信度字段是否必须按降级策略留空。

        参数：field_name 为已通过 T08 校验的业务字段名称。
        返回值：机器人无法可靠确认且字段属于项目 CRM 最小必填集时返回 True。
        异常：无。
        副作用：无。
        """
        return (
            not self._robot_submission_confirmation_available
            and field_name in CRM_REQUIRED_CORE_FIELDS | CRM_CONTACT_FIELDS
        )

    def _confirmation_names(self, value: object) -> set[str]:
        """将适配器返回的 AI待确认 多选值规范化为字段名称集合。

        参数：value 为智能表格字段原始值。
        返回值：非空字符串字段名集合。
        异常：无。
        副作用：无。
        """
        if not isinstance(value, list):
            return set()
        return {item for item in value if isinstance(item, str) and item}

    def _require_lead(self, session: Session, lead_id: str) -> Lead:
        """读取必需线索事实，避免后续流程以缺失记录继续执行。

        参数：session 为数据库会话；lead_id 为线索标识。
        返回值：已持久化的 Lead。
        异常：线索不存在时抛出 ValueError。
        副作用：仅读取数据库。
        """
        lead = session.get(Lead, lead_id)
        if lead is None:
            raise ValueError(f"线索不存在：{lead_id}")
        return lead

    def _require_source_message(self, session: Session, message_id: str) -> None:
        """确认 AI 建议可追溯到已持久化的来源消息。

        参数：session 为数据库会话；message_id 为来源消息标识。
        返回值：无。
        异常：消息不存在时抛出 ValueError。
        副作用：仅读取数据库。
        """
        if session.get(IncomingMessage, message_id) is None:
            raise ValueError(f"来源消息不存在：{message_id}")

    def _record_audit(
        self, session: Session, message_id: str, sales_user_id: str, event_type: str
    ) -> None:
        """保存可查询且幂等的 T09 人工保护或确认业务审计事件。

        参数：session 为事务；message_id 和 sales_user_id 提供审计归属；event_type 为受控事件名。
        返回值：无。
        异常：数据库读写失败时由 SQLAlchemy 抛出。
        副作用：首次事件写入业务审计表。
        """
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=message_id,
                    sales_user_id=sales_user_id,
                    event_type=event_type,
                )
            )
