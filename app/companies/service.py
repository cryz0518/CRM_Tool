"""公司地域、企查查核验和销售内保守合并服务。"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.companies.models import (
    CompanyRegion,
    CompanyRegionEvidence,
    CompanyResolution,
    CompanyUpsertCommand,
    CompanyUpsertResult,
    CompanyVerificationStatus,
    QCCCandidate,
    QCCLookupResult,
)
from app.leads.identity import DatabaseSalesIdentityProvider, SalesIdentityProvider
from app.leads.models import Lead, LeadFieldProvenance
from app.messaging.models import BusinessAuditEvent, IncomingMessage
from app.smart_table.adapter import SmartTableActor, SmartTableAdapter

logger = logging.getLogger(__name__)


class CompanyRegionResolver:
    """仅依据明确证据区分国内、国外与未知公司。"""

    _foreign_markers = (
        "国外客户",
        "海外客户",
        "境外客户",
        "外国客户",
        "外资公司",
        "海外公司",
    )
    _domestic_markers = ("国内客户", "国内公司", "中国客户", "中国公司")
    _foreign_countries = (
        "美国",
        "德国",
        "法国",
        "英国",
        "日本",
        "韩国",
        "加拿大",
        "澳大利亚",
        "新加坡",
        "意大利",
        "西班牙",
        "荷兰",
        "瑞士",
    )

    def resolve(self, evidence: CompanyRegionEvidence) -> CompanyRegion:
        """基于销售确认或消息中的明确地域表述返回公司地域。

        参数：evidence 为消息与人工确认携带的可审计证据。
        返回值：仅在有充分依据时返回 domestic 或 foreign，否则返回 unknown。
        异常：无。
        副作用：无；不会查询任何外部服务。
        """
        # 销售明确确认的地域优先于文本，避免 AI 或名称风格反向推翻人工结论。
        if evidence.sales_confirmed_region is not None:
            return evidence.sales_confirmed_region
        # 上游完成实体关联后的明确国外证据可以安全跳过企查查。
        if evidence.explicit_foreign:
            return CompanyRegion.FOREIGN
        message_text = evidence.message_text or ""
        # 只识别销售对客户/公司的直接地域表述，邮箱、电话和英文名称不会参与判断。
        if any(marker in message_text for marker in self._foreign_markers):
            return CompanyRegion.FOREIGN
        if any(marker in message_text for marker in self._domestic_markers):
            return CompanyRegion.DOMESTIC
        # 国家名必须与公司实体词同时出现，避免把展会地点或个人行程误当作公司地域。
        if any(
            re.search(rf"{country}(?:的)?(?:公司|企业|客户)", message_text)
            for country in self._foreign_countries
        ):
            return CompanyRegion.FOREIGN
        return CompanyRegion.UNKNOWN


class QCCAdapter(Protocol):
    """隔离真实企查查接口的稳定查询契约。"""

    def lookup(self, company_name: str) -> QCCLookupResult:
        """按原始公司名称查询企查查候选。

        参数：company_name 为待核验的销售或 AI 提取名称。
        返回值：唯一匹配、无结果或多候选的受控结果。
        异常：网络超时等适配器故障由具体实现抛出。
        副作用：具体实现可能调用外部服务。
        """
        ...


class QCCAdapterError(RuntimeError):
    """表示真实企查查适配器已分类的可恢复调用失败。"""


class MockQCCAdapter:
    """用显式预置结果模拟企查查，绝不猜测真实接口或公司事实。"""

    def __init__(self, responses: Mapping[str, QCCLookupResult] | None = None) -> None:
        """初始化原始名称到受控查询结果的映射。

        参数：responses 为测试或 PoC 明确提供的查询结果。
        返回值：无。
        异常：无。
        副作用：复制输入映射，避免调用方修改影响后续查询。
        """
        self._responses = dict(responses or {})

    def lookup(self, company_name: str) -> QCCLookupResult:
        """返回预置结果；未预置公司一律视为无结果。

        参数：company_name 为原始公司名称。
        返回值：预置的查询结论或 not_found。
        异常：无。
        副作用：无；不访问真实企查查。
        """
        return self._responses.get(company_name, QCCLookupResult.not_found())


class CompanyLeadService:
    """以公司事实将临时线索升级，并严格限制在当前销售内去重。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        qcc_adapter: QCCAdapter,
        region_resolver: CompanyRegionResolver | None = None,
        sales_identity_provider: SalesIdentityProvider | None = None,
    ) -> None:
        """注入数据库、表格和可替换的企查查适配器。

        参数：session_factory 管理事务；smart_table_adapter 同步增量字段；
        qcc_adapter 查询工商候选；region_resolver 可替换地域策略；
        sales_identity_provider 校验销售录入授权。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不访问数据库或外部系统。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._qcc_adapter = qcc_adapter
        self._region_resolver = region_resolver or CompanyRegionResolver()
        self._sales_identity_provider = sales_identity_provider or DatabaseSalesIdentityProvider()

    def upsert(self, command: CompanyUpsertCommand) -> CompanyUpsertResult:
        """创建、升级或销售内合并一条线索，并仅同步安全字段补丁。

        参数：command 为来源消息、销售、候选字段和人工确认事实。
        返回值：最终目标线索的公司、生命周期和表格定位。
        异常：来源消息、既有线索或销售边界不一致时抛出 ValueError 或 PermissionError；
        表格适配器异常向调用方传播。
        副作用：可能创建或更新 Lead、字段来源、审计和智能表格记录。
        """
        company_name = command.fields.get("线索名称", "").strip()
        # 将原始消息转为最小地域证据；名称、邮箱和电话仅传递，绝不单独推导国外身份。
        evidence = command.region_evidence or CompanyRegionEvidence(
            company_name=company_name,
            email=command.fields.get("邮箱"),
            phone=command.fields.get("手机") or command.fields.get("电话"),
        )
        region = self._region_resolver.resolve(evidence)
        logger.info(
            "company_resolution_started",
            extra={
                "message_id": command.source_message_id,
                "wecom_user_id": command.sales_user_id,
                "company_region": region.value,
            },
        )
        resolution = self._resolve_company(command, company_name, region)
        return self._persist_resolution(command, company_name, region, resolution)

    def _resolve_company(
        self, command: CompanyUpsertCommand, company_name: str, region: CompanyRegion
    ) -> CompanyResolution:
        """在事务外查询企查查或处理人工确认，返回不含数据库副作用的公司事实。

        参数：company_name 为候选名称；region 为确定性地域；user_confirmed_company 表示销售确认。
        返回值：标准名、企查查标识、核验状态和候选集合。
        异常：无；适配器超时被转换为未核验结论，避免阻塞采集。
        副作用：国内或未知地域可能调用一次 QCC Adapter。
        """
        if not company_name:
            return CompanyResolution(None, None, CompanyVerificationStatus.INCOMPLETE_COMPANY)
        if region is CompanyRegion.FOREIGN:
            # 明确国外证据直接进入基础规范化，禁止为其调用国内工商查询。
            return CompanyResolution(
                self._normalize_company_name_for_comparison(company_name),
                None,
                CompanyVerificationStatus.FOREIGN_DECLARED,
            )
        failure_event_type: str | None = None
        try:
            # domestic 和 unknown 都可查询 QCC；unknown 的失败仍绝不推导为 foreign。
            lookup = self._qcc_adapter.lookup(company_name)
        except (TimeoutError, QCCAdapterError) as error:
            # 只有已分类的可恢复调用故障才降级为未核验，其他编程错误必须显式失败。
            logger.warning(
                "company_qcc_lookup_failed",
                extra={
                    "message_id": command.source_message_id,
                    "wecom_user_id": command.sales_user_id,
                    "error_type": type(error).__name__,
                },
            )
            lookup = QCCLookupResult.not_found()
            failure_event_type = "company_qcc_lookup_timeout"
        if lookup.status == "matched" and len(lookup.candidates) == 1:
            candidate = lookup.candidates[0]
            return CompanyResolution(
                candidate.standard_company_name,
                candidate.company_id,
                CompanyVerificationStatus.QCC_VERIFIED,
                lookup.candidates,
            )
        candidates = lookup.candidates
        if command.user_confirmed_company:
            # 销售确认可作为当前销售去重与后续提交的标准名，但不伪装成工商核验成功。
            return CompanyResolution(
                self._normalize_company_name_for_comparison(company_name),
                None,
                CompanyVerificationStatus.USER_CONFIRMED_UNVERIFIED,
                candidates,
                failure_event_type,
            )
        return CompanyResolution(
            None,
            None,
            CompanyVerificationStatus.COMPANY_UNVERIFIED,
            candidates,
            failure_event_type,
        )

    def _persist_resolution(
        self,
        command: CompanyUpsertCommand,
        company_name: str,
        region: CompanyRegion,
        resolution: CompanyResolution,
    ) -> CompanyUpsertResult:
        """在当前销售事务内保存公司结论、保守合并及可审计字段来源。

        参数：command 为输入命令；其余参数为已在事务外得到的确定性公司结论。
        返回值：更新或合并后目标线索的观察结果。
        异常：销售越权、消息缺失或数据库唯一约束冲突时向调用方传播。
        副作用：更新 Lead、字段来源和审计；随后创建或增量更新智能表格记录。
        """
        standard_name = resolution.standard_company_name
        qcc_company_id = resolution.qcc_company_id
        verification_status = resolution.verification_status
        candidates = resolution.candidates
        with self._session_factory.begin() as session:
            message = session.get(IncomingMessage, command.source_message_id)
            if message is None:
                raise ValueError(f"来源消息不存在：{command.source_message_id}")
            if message.sales_user_id != command.sales_user_id:
                raise PermissionError("来源消息不属于当前销售")
            # 企业微信机器人可见范围不能替代后端销售授权，解析服务也必须独立守住入口。
            if not self._sales_identity_provider.is_authorized(session, command.sales_user_id):
                raise PermissionError("销售未获授权，不能创建或更新公司线索")
            if resolution.qcc_failure_event_type is not None:
                # 将外部超时与“查无结果”区分为可审计事件，供运维和销售审核追溯。
                self._record_audit(session, command, resolution.qcc_failure_event_type)
            # 所有目标定位都带销售条件，避免表格阶段读取或推断其他销售的同公司信息。
            lead = self._get_requested_or_matching_lead(session, command, standard_name)
            merge_target = self._get_temporary_merge_target(
                session, lead, command.sales_user_id, standard_name
            )
            if merge_target is not None and lead is not None:
                # 临时草稿升级前先转入当前销售已有的同标准名目标，避免违反销售内唯一约束。
                transferred_fields = {**lead.field_values, **command.fields}
                table_patch = self._merge_patch(
                    session, merge_target, transferred_fields, command.source_message_id
                )
                self._apply_company_state(
                    merge_target,
                    company_name,
                    standard_name,
                    region,
                    verification_status,
                    qcc_company_id,
                    candidates,
                    command.user_confirmed_company,
                    table_patch,
                )
                lead.lifecycle_state = "merged"
                self._record_audit(session, command, "temporary_lead_merged")
                logger.info(
                    "temporary_lead_merged",
                    extra={"message_id": command.source_message_id, "lead_id": merge_target.id},
                )
                lead = merge_target
                created = False
            if lead is None:
                lead = self._create_lead(
                    session,
                    command,
                    standard_name,
                    region,
                    verification_status,
                    qcc_company_id,
                    candidates,
                )
                table_patch = dict(lead.field_values)
                created = True
            elif merge_target is None:
                self._ensure_sales_boundary(lead, command.sales_user_id)
                if (
                    lead.company_confirmed_by_user
                    and lead.standard_company_name is not None
                    and standard_name is not None
                    and standard_name != lead.standard_company_name
                ):
                    # 已确认名称与后续工商结果冲突时保留销售事实，交由人工判断更名或错误修正。
                    lead.company_verification_status = (
                        CompanyVerificationStatus.VERIFICATION_CONFLICT.value
                    )
                    self._record_audit(session, command, "company_verification_conflict")
                    return self._result(lead)
                if (
                    lead.lifecycle_state == "synced"
                    and standard_name is not None
                    and lead.standard_company_name is not None
                    and standard_name != lead.standard_company_name
                ):
                    # 已同步线索的公司身份变化绝不是普通字段更新，必须停止自动化变更。
                    lead.lifecycle_state = "company_identity_change_pending_review"
                    lead.company_verification_status = (
                        CompanyVerificationStatus.VERIFICATION_CONFLICT.value
                    )
                    self._record_audit(session, command, "company_identity_change_pending_review")
                    return self._result(lead)
                table_patch = self._merge_patch(
                    session, lead, command.fields, command.source_message_id
                )
                if not (
                    lead.company_confirmed_by_user
                    and lead.standard_company_name is not None
                    and standard_name is None
                ):
                    self._apply_company_state(
                        lead,
                        company_name,
                        standard_name,
                        region,
                        verification_status,
                        qcc_company_id,
                        candidates,
                        command.user_confirmed_company,
                        table_patch,
                    )
                self._record_audit(session, command, "company_lead_updated")
                created = False
            session.flush()
            lead_id = lead.id

        return self._sync_smart_table(lead_id, table_patch, created)

    def _get_requested_or_matching_lead(
        self, session: Session, command: CompanyUpsertCommand, standard_name: str | None
    ) -> Lead | None:
        """只在当前销售范围内定位显式目标或相同标准名线索。

        参数：session 为事务；command 提供销售与可选目标；standard_name 为可靠去重键。
        返回值：当前销售唯一可操作的线索；无可靠目标时返回 None。
        异常：显式目标不存在或不属于当前销售时抛出异常。
        副作用：仅读取当前销售范围内的 Lead，绝不查询其他销售数据。
        """
        if command.existing_lead_id is not None:
            lead = session.get(Lead, command.existing_lead_id)
            if lead is None:
                raise ValueError(f"线索不存在：{command.existing_lead_id}")
            self._ensure_sales_boundary(lead, command.sales_user_id)
            return lead
        if standard_name is None:
            return None
        return session.scalar(
            select(Lead).where(
                Lead.smart_table_owner_user_id == command.sales_user_id,
                Lead.standard_company_name == standard_name,
                Lead.lifecycle_state != "merged",
            )
        )

    @staticmethod
    def _get_temporary_merge_target(
        session: Session, lead: Lead | None, sales_user_id: str, standard_name: str | None
    ) -> Lead | None:
        """为有可靠名称的临时草稿定位同一销售已有的正式目标。

        参数：session 为事务；lead 为显式升级的临时线索；sales_user_id 限制查找范围；
        standard_name 为唯一可靠的公司去重键。
        返回值：可合并的当前销售目标；不存在或非临时升级时返回 None。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：只读取当前销售的 Lead，绝不访问其他销售记录。
        """
        if lead is None or standard_name is None or lead.standard_company_name is not None:
            return None
        return session.scalar(
            select(Lead).where(
                Lead.smart_table_owner_user_id == sales_user_id,
                Lead.standard_company_name == standard_name,
                Lead.id != lead.id,
                Lead.lifecycle_state != "merged",
            )
        )

    def _create_lead(
        self,
        session: Session,
        command: CompanyUpsertCommand,
        standard_name: str | None,
        region: CompanyRegion,
        verification_status: CompanyVerificationStatus,
        qcc_company_id: str | None,
        candidates: tuple[QCCCandidate, ...],
    ) -> Lead:
        """创建一条归属当前销售的临时或已核验线索草稿。

        参数：各参数均为已验证的消息、销售、公司及字段事实。
        返回值：已加入当前事务的新 Lead。
        异常：数据库约束错误由 SQLAlchemy 抛出。
        副作用：增加线索、字段来源和公司处理审计。
        """
        # 临时线索也保存全部可用补丁并进入表格审核，但没有可靠标准名时不会参与去重。
        fields = {**command.fields, "线索来源": "展会"}
        if (
            standard_name is not None
            and verification_status is CompanyVerificationStatus.QCC_VERIFIED
        ):
            fields["线索名称"] = standard_name
        lead = Lead(
            source_message_id=command.source_message_id,
            original_capturing_sales_user_id=command.sales_user_id,
            smart_table_owner_user_id=command.sales_user_id,
            lifecycle_state=self._lifecycle_state(fields, standard_name),
            field_values=fields,
            standard_company_name=standard_name,
            company_region=region.value,
            company_verification_status=verification_status.value,
            qcc_company_id=qcc_company_id,
            qcc_candidates=self._candidate_dicts(candidates),
            company_confirmed_by_user=command.user_confirmed_company,
        )
        session.add(lead)
        session.flush()
        # 每个首次字段都建立可追溯来源，后续人工保护与保守合并依赖这份事实。
        for field_name, value in fields.items():
            session.add(
                LeadFieldProvenance(
                    lead_id=lead.id,
                    source_message_id=command.source_message_id,
                    field_name=field_name,
                    value=value,
                )
            )
        self._record_audit(
            session,
            command,
            "temporary_lead_created" if standard_name is None else "company_lead_created",
        )
        logger.info(
            "company_lead_created",
            extra={"message_id": command.source_message_id, "lead_id": lead.id},
        )
        return lead

    def _merge_patch(
        self, session: Session, lead: Lead, incoming: Mapping[str, str], source_message_id: str
    ) -> dict[str, str]:
        """只将空字段补入目标线索，并把不同非空值保存为补充信息。

        参数：session 为事务；lead 为当前销售目标；incoming 为本轮字段；source_message_id 追溯来源。
        返回值：可安全同步到智能表格的最小字段补丁。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：更新后台字段、补充信息与字段来源。
        """
        patch: dict[str, str] = {}
        protected = self._user_protected_fields(session, lead.id)
        values = dict(lead.field_values)
        enrichment = dict(lead.enrichment_values)
        # 逐字段合并仅允许补空；非空冲突和人工保护值均转为补充信息而非覆盖表格。
        for field_name, value in incoming.items():
            if not value or field_name == "线索名称":
                continue
            old_value = values.get(field_name)
            if field_name in protected:
                # 人工编辑保护优先于所有公司、AI 或消息补充规则。
                self._append_enrichment(enrichment, field_name, value)
                continue
            if not old_value:
                values[field_name] = value
                patch[field_name] = value
                session.add(
                    LeadFieldProvenance(
                        lead_id=lead.id,
                        source_message_id=source_message_id,
                        field_name=field_name,
                        value=value,
                    )
                )
            elif old_value != value:
                self._append_enrichment(enrichment, field_name, value)
        lead.field_values = values
        lead.enrichment_values = enrichment
        logger.info(
            "company_lead_merge_applied",
            extra={"lead_id": lead.id, "updated_field_count": len(patch)},
        )
        return patch

    def _apply_company_state(
        self,
        lead: Lead,
        company_name: str,
        standard_name: str | None,
        region: CompanyRegion,
        verification_status: CompanyVerificationStatus,
        qcc_company_id: str | None,
        candidates: tuple[QCCCandidate, ...],
        user_confirmed_company: bool,
        table_patch: dict[str, str],
    ) -> None:
        """将新的公司核验事实写入未同步或可安全更新的线索。

        参数：lead 为目标；其余参数为已解析的公司事实和表格字段补丁。
        返回值：无。
        异常：无。
        副作用：可能更新公司元数据、展示名称、生命周期及表格名称补丁。
        """
        if company_name and standard_name is not None:
            display_name = (
                standard_name
                if verification_status is CompanyVerificationStatus.QCC_VERIFIED
                else company_name
            )
            values = dict(lead.field_values)
            if values.get("线索名称") != display_name:
                values["线索名称"] = display_name
                table_patch["线索名称"] = display_name
            lead.field_values = values
            lead.standard_company_name = standard_name
        # 查询候选和人工确认均为后台审计信息，不能混入 CRM 业务字段。
        lead.company_region = region.value
        lead.company_verification_status = verification_status.value
        lead.qcc_company_id = qcc_company_id
        lead.qcc_candidates = self._candidate_dicts(candidates)
        lead.company_confirmed_by_user = lead.company_confirmed_by_user or user_confirmed_company
        lead.lifecycle_state = self._lifecycle_state(lead.field_values, lead.standard_company_name)

    def _sync_smart_table(
        self, lead_id: str, patch: Mapping[str, str], created: bool
    ) -> CompanyUpsertResult:
        """在事务提交后创建或增量更新智能表格，并回写记录定位。

        参数：lead_id 为目标；patch 为安全字段；created 指示是否需要创建记录。
        返回值：完成表格同步后的最新线索结果。
        异常：表格适配器异常向调用方传播。
        副作用：调用 SmartTableAdapter，并在首次创建后写入 record_id。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError(f"线索不存在：{lead_id}")
            record_id = lead.smart_table_record_id
            fields = dict(lead.field_values)
            owner = lead.smart_table_owner_user_id
        if created:
            logger.info("company_smart_table_create_started", extra={"lead_id": lead_id})
            try:
                record = self._smart_table_adapter.create_record(
                    {**fields, "创建人": owner, "负责人": owner}, actor=SmartTableActor.ROBOT
                )
            except Exception as error:
                logger.exception(
                    "company_smart_table_create_failed",
                    extra={"lead_id": lead_id, "error_type": type(error).__name__},
                )
                raise
            with self._session_factory.begin() as session:
                lead = session.get(Lead, lead_id)
                if lead is None:
                    raise ValueError(f"线索不存在：{lead_id}")
                lead.smart_table_record_id = record.record_id
            record_id = record.record_id
        elif patch and record_id is not None:
            # 再次写入前读取销售当前表格值；不同的非空值一律按人工编辑保护处理。
            current_record = self._smart_table_adapter.get_record(record_id)
            if current_record is None:
                raise ValueError(f"智能表格记录不存在：{record_id}")
            last_synced_values = self._last_synced_values(lead_id, set(patch))
            safe_patch = {
                field_name: value
                for field_name, value in patch.items()
                if not current_record.fields.get(field_name)
                or current_record.fields.get(field_name) == value
                or current_record.fields.get(field_name) == last_synced_values.get(field_name)
            }
            protected_fields = set(patch) - set(safe_patch)
            if protected_fields:
                self._mark_user_protected_fields(lead_id, protected_fields)
                logger.info(
                    "company_smart_table_user_edit_detected",
                    extra={
                        "lead_id": lead_id,
                        "record_id": record_id,
                        "protected_fields": sorted(protected_fields),
                    },
                )
            if not safe_patch:
                return self._result_for_id(lead_id)
            logger.info(
                "company_smart_table_update_started",
                extra={
                    "lead_id": lead_id,
                    "record_id": record_id,
                    "updated_field_count": len(safe_patch),
                },
            )
            try:
                self._smart_table_adapter.update_record(record_id, safe_patch)
            except Exception as error:
                logger.exception(
                    "company_smart_table_update_failed",
                    extra={
                        "lead_id": lead_id,
                        "record_id": record_id,
                        "error_type": type(error).__name__,
                    },
                )
                raise
            self._record_last_ai_synced_values(lead_id, safe_patch)
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError(f"线索不存在：{lead_id}")
            return self._result(lead)

    @staticmethod
    def _normalize_company_name_for_comparison(company_name: str) -> str:
        """执行允许自由填写名称的空白与大小写基础规范化。

        参数：company_name 为销售提供的显示名称。
        返回值：供比较的规范化名称；原展示文本仍存于 field_values。
        异常：无。
        副作用：无。
        """
        return re.sub(r"\s+", " ", company_name).strip().casefold()

    @staticmethod
    def _candidate_dicts(candidates: tuple[QCCCandidate, ...]) -> list[dict[str, str]]:
        """将企查查候选转换为可持久化 JSON 审计结构。

        参数：candidates 为适配器返回的候选。
        返回值：不包含未声明字段的候选字典列表。
        异常：无。
        副作用：无。
        """
        return [
            {"standard_company_name": item.standard_company_name, "company_id": item.company_id}
            for item in candidates
        ]

    @staticmethod
    def _append_enrichment(enrichment: dict[str, str], field_name: str, value: str) -> None:
        """去重追加一个不可自动覆盖的补充信息值。

        参数：enrichment 为可变补充信息；field_name 和 value 为冲突候选。
        返回值：无。
        异常：无。
        副作用：可能修改 enrichment。
        """
        existing = enrichment.get(field_name)
        # 主线的补充信息契约是字符串；同一冲突字段按换行去重串接，保留所有候选事实。
        if existing is None:
            enrichment[field_name] = value
        elif value not in existing.split("\n"):
            enrichment[field_name] = f"{existing}\n{value}"

    @staticmethod
    def _lifecycle_state(fields: Mapping[str, str], standard_company_name: str | None) -> str:
        """根据公司与项目级最小必填集推导可提交前的线索生命周期。

        参数：fields 为当前字段；standard_company_name 为可靠或销售确认后的去重键。
        返回值：完整最小集时为 pending_create，其余为 temporary。
        异常：无。
        副作用：无。
        """
        has_contact = any(fields.get(name) for name in ("手机", "电话", "邮箱"))
        return (
            "pending_create"
            if standard_company_name and fields.get("业务线") and has_contact
            else "temporary"
        )

    @staticmethod
    def _ensure_sales_boundary(lead: Lead, sales_user_id: str) -> None:
        """拒绝跨销售读取、升级或合并线索。

        参数：lead 为目标线索；sales_user_id 为请求销售。
        返回值：无。
        异常：目标不属于销售时抛出 PermissionError。
        副作用：无。
        """
        if lead.smart_table_owner_user_id != sales_user_id:
            raise PermissionError("智能表格阶段禁止跨销售操作同公司线索")

    @staticmethod
    def _user_protected_fields(session: Session, lead_id: str) -> set[str]:
        """读取已被 T09 标记为人工编辑的字段集合。

        参数：session 为事务；lead_id 为目标线索。
        返回值：不可由本服务覆盖的字段名称集合。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取字段来源。
        """
        return set(
            session.scalars(
                select(LeadFieldProvenance.field_name).where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.is_user_modified.is_(True),
                )
            ).all()
        )

    @staticmethod
    def _record_audit(session: Session, command: CompanyUpsertCommand, event_type: str) -> None:
        """保存当前公司处理阶段的幂等业务审计事件。

        参数：session 为事务；command 提供消息和销售；event_type 为受控事件名。
        返回值：无。
        异常：数据库写入失败时由 SQLAlchemy 抛出。
        副作用：首次事件会写入业务审计表。
        """
        existing = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == command.source_message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing is None:
            session.add(
                BusinessAuditEvent(
                    message_id=command.source_message_id,
                    sales_user_id=command.sales_user_id,
                    event_type=event_type,
                )
            )

    @staticmethod
    def _result(lead: Lead) -> CompanyUpsertResult:
        """将 ORM 线索转换为可在会话外使用的应用服务结果。

        参数：lead 为已持久化线索。
        返回值：只含调用方需要的稳定字段。
        异常：核验状态异常时由枚举转换抛出 ValueError。
        副作用：无。
        """
        return CompanyUpsertResult(
            lead.id,
            lead.smart_table_record_id,
            lead.lifecycle_state,
            lead.standard_company_name,
            CompanyVerificationStatus(lead.company_verification_status),
        )

    def _mark_user_protected_fields(self, lead_id: str, field_names: set[str]) -> None:
        """将智能表格中发现差异的字段标记为销售人工确认。

        参数：lead_id 为当前线索；field_names 为本轮禁止覆盖的字段集合。
        返回值：无。
        异常：数据库读取或提交失败时由 SQLAlchemy 抛出。
        副作用：更新字段来源的人工修改和人工确认状态。
        """
        with self._session_factory.begin() as session:
            provenances = session.scalars(
                select(LeadFieldProvenance).where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.field_name.in_(field_names),
                )
            ).all()
            # 已有来源记录是 T09 的保护事实；未记录字段不凭空创建 AI 来源。
            for provenance in provenances:
                provenance.is_user_modified = True
                provenance.is_user_confirmed = True

    def _last_synced_values(self, lead_id: str, field_names: set[str]) -> dict[str, str]:
        """读取每个待写字段最后由机器人同步到表格的值。

        参数：lead_id 为当前线索；field_names 为即将写入的字段集合。
        返回值：字段到最后机器人同步值的映射。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：仅读取字段来源；历史版本未回填同步值时使用其原始值作为保守基线。
        """
        with self._session_factory() as session:
            provenances = session.scalars(
                select(LeadFieldProvenance)
                .where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.field_name.in_(field_names),
                )
                .order_by(LeadFieldProvenance.id.desc())
            ).all()
        values: dict[str, str] = {}
        # 同一字段可能来自多条消息，按最新来源优先并兼容 T10 前未回填的同步值。
        for provenance in provenances:
            values.setdefault(
                provenance.field_name, provenance.last_ai_synced_value or provenance.value
            )
        return values

    def _record_last_ai_synced_values(self, lead_id: str, patch: Mapping[str, str]) -> None:
        """回写本轮成功写入表格的机器人字段值。

        参数：lead_id 为当前线索；patch 为已成功写入的最小字段补丁。
        返回值：无。
        异常：数据库提交失败时由 SQLAlchemy 抛出。
        副作用：更新相应字段来源的最后 AI 同步值。
        """
        with self._session_factory.begin() as session:
            provenances = session.scalars(
                select(LeadFieldProvenance)
                .where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.field_name.in_(set(patch)),
                )
                .order_by(LeadFieldProvenance.id.desc())
            ).all()
            updated_fields: set[str] = set()
            # 每个字段只更新最新来源，保证下一次比较使用最近一次成功同步的基线。
            for provenance in provenances:
                if provenance.field_name not in updated_fields:
                    provenance.last_ai_synced_value = patch[provenance.field_name]
                    updated_fields.add(provenance.field_name)

    def _result_for_id(self, lead_id: str) -> CompanyUpsertResult:
        """读取并返回指定线索的会话外稳定结果。

        参数：lead_id 为目标线索标识。
        返回值：当前线索的应用服务结果。
        异常：线索不存在时抛出 ValueError。
        副作用：仅读取数据库。
        """
        with self._session_factory() as session:
            lead = session.get(Lead, lead_id)
            if lead is None:
                raise ValueError(f"线索不存在：{lead_id}")
            return self._result(lead)
