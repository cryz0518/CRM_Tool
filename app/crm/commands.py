"""消费已持久化 CRM 提交命令并生成脱敏销售汇总。"""

from __future__ import annotations

import hashlib
import logging
import re

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.crm.adapter import CRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionBatchResult, SubmissionCommand
from app.leads.models import CrmSyncRecord, Lead, new_lead_id
from app.leads.review import LeadReviewService
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.wecom_bot.actions import (
    CardCapabilityUnavailable,
    WecomActionService,
)

_LOGGER = logging.getLogger(__name__)

CRM_SUBMISSION_COMMANDS = frozenset(
    {"提交今天的线索", "提交我所有线索", "帮我提交放弃提交的线索", "提交我的更新"}
)
_COMPANY_SUBMISSION_PATTERN = re.compile(r"^请帮我提交(?P<company>[^\r\n。]{1,128})这条线索$")
_PREVIEW_FIELD_NAMES = (
    "业务线",
    "线索名称",
    "线索来源",
    "联系人",
    "职务",
    "沟通方式",
    "手机",
    "电话",
    "邮箱",
    "客户行业",
    "客户级别",
    "工艺",
    "下次联系时间",
    "备注",
    "是否为国际客户",
    "负责人",
    "提交状态",
)


def parse_company_submission_request(text: str) -> str | None:
    """严格解析按公司名称定位线索的固定机器人请求。

    参数：text 为销售消息正文。
    返回值：去除句式包装后的公司名称；不符合固定句式时返回 None。
    异常：无。
    副作用：无；不调用模型、不查询外部系统。
    """

    match = _COMPANY_SUBMISSION_PATTERN.fullmatch(text)
    if match is None:
        return None
    company = match.group("company").strip()
    return company or None


def parse_crm_submission_command(text: str) -> str | None:
    """识别批量命令或固定公司定位提交句式。

    参数：text 为销售消息正文；不自动 trim、不做模糊匹配、不调用 LLM。
    返回值：合法命令原文；其他文本返回 None。公司定位句式只进入预览边界。
    异常：无。
    副作用：无。
    """
    # 精确相等是 CRM 写操作授权边界，任何相似自然语言都不能触发提交。
    if text in CRM_SUBMISSION_COMMANDS or parse_company_submission_request(text) is not None:
        return text
    return None


def consume_submission_command(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    crm_adapter: CRMAdapter | None,
    outbox_event_id: int,
) -> str:
    """消费一条已认领命令 Outbox，并返回不含敏感数据的销售汇总文本。

    参数：前三项为业务依赖，outbox_event_id 为现有可靠消息事件标识。
    返回值：可安全发送给当前销售的确定性中文汇总。
    异常：消息或事件事实缺失时抛出 ValueError；业务依赖异常被转换为可靠重试状态。
    副作用：调用 T12 submission service，写入脱敏通知，并结束或重试本命令事件。
    """
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None or event.event_type != "crm_submission_command":
            raise ValueError("不是 CRM 提交命令 Outbox")
        message = session.get(IncomingMessage, event.message_id)
        if message is None:
            raise ValueError("CRM 提交命令缺少来源消息")
        command = SubmissionCommand(
            text=message.normalized_text or "",
            sales_user_id=message.sales_user_id,
            request_message_id=message.message_id,
        )
    company_name = parse_company_submission_request(command.text)
    if company_name is not None:
        reply = prepare_company_submission_preview(
            session_factory,
            smart_table_adapter,
            command,
            company_name,
        )
        with session_factory.begin() as session:
            event = session.get(OutboxEvent, outbox_event_id)
            if event is not None:
                event.status = "succeeded"
            key = notification_key_for_message(command.request_message_id)
            if session.get(NotificationRecord, key) is None:
                session.add(
                    NotificationRecord(
                        notification_key=key,
                        sales_user_id=command.sales_user_id,
                        source_message_id=command.request_message_id,
                        notification_type="crm_submission_preview",
                        content=reply,
                    )
                )
        return reply
    if crm_adapter is None:
        raise ValueError("批量 CRM 命令缺少 CRM Adapter")
    if command.text in {"提交我所有线索", "帮我提交放弃提交的线索"}:
        try:
            reply = prepare_batch_submission_selection(
                session_factory, smart_table_adapter, crm_adapter, command
            )
        except Exception:
            # 候选读取或卡片发行失败同样必须释放消息顺序检查点，并遵循统一重试策略。
            return _record_command_failure(session_factory, outbox_event_id, command)
        with session_factory.begin() as session:
            event = session.get(OutboxEvent, outbox_event_id)
            if event is not None:
                event.status = "succeeded"
            key = notification_key_for_message(command.request_message_id)
            if session.get(NotificationRecord, key) is None:
                session.add(
                    NotificationRecord(
                        notification_key=key,
                        sales_user_id=command.sales_user_id,
                        source_message_id=command.request_message_id,
                        notification_type="crm_submission_selection",
                        content=reply,
                    )
                )
        return reply
    try:
        service = CrmSubmissionService(
            session_factory,
            smart_table_adapter,
            crm_adapter,
            robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
        )
        result = service.submit(command)
    except Exception:
        # 命令编排失败需要有界结束，不能永久占住同销售的消息顺序检查点。
        return _record_command_failure(session_factory, outbox_event_id, command)
    # 重放时必须将本请求已成功的同步事实重新计入汇总，不能因 Lead 已 synced 漏报成功。
    result = _include_persisted_results(session_factory, command, result)
    _issue_field_confirmation_cards(session_factory, smart_table_adapter, command, result)
    _issue_duplicate_confirmation_card(command, result, session_factory)
    reply = format_submission_reply(result)
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is not None:
            retrying_or_processing = session.scalar(
                select(CrmSyncRecord.id)
                .where(
                    CrmSyncRecord.status.in_(("retrying", "processing")),
                    # 仅以本命令冻结的同步记录决定其 Outbox 状态，禁止串到同销售的其他命令。
                    CrmSyncRecord.request_message_id == command.request_message_id,
                )
                .limit(1)
            )
            event.status = "retrying" if retrying_or_processing is not None else "succeeded"
        key = notification_key_for_message(command.request_message_id)
        if session.get(NotificationRecord, key) is None:
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=command.sales_user_id,
                    source_message_id=command.request_message_id,
                    notification_type="crm_submission_summary",
                    content=reply,
                )
            )
    return reply


def prepare_batch_submission_selection(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    crm_adapter: CRMAdapter,
    command: SubmissionCommand,
) -> str:
    """为批量提交命令发行服务端候选卡，不在发卡阶段调用 CRM。

    参数：前三项为既有 CRM 提交依赖；command 为已鉴权的固定命令。
    返回值：可直接回复销售的候选卡状态文本。
    异常：卡片能力或服务端状态异常向调用方传播；不吞掉安全拒绝。
    副作用：可能写入一张仅允许当前销售勾选的模板卡动作。
    """
    service = CrmSubmissionService(
        session_factory,
        smart_table_adapter,
        crm_adapter,
        robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
    )
    candidates = service.list_submission_candidates(command.text, command.sales_user_id)
    if not candidates:
        if command.text == "帮我提交放弃提交的线索":
            return "当前没有可重新提交的放弃提交线索。"
        return "当前没有可提交的未提交线索。"
    action_service = WecomActionService(
        session_factory,
        card_callback_ready=get_settings().wecom_card_callback_ready(),
    )
    try:
        page_size = 20
        pages = [
            candidates[start : start + page_size]
            for start in range(0, len(candidates), page_size)
        ]
        for page_number, page in enumerate(pages, start=1):
            # 每一页都由服务端冻结候选 ID；销售只能在对应卡片内选择，不能传任意 offset。
            action_service.issue_batch_submission_action(
                actor_user_id=command.sales_user_id,
                request_message_id=command.request_message_id,
                command_text=command.text,
                candidates=tuple(
                    {
                        "lead_id": item.lead_id,
                        "company_name": item.company_name,
                        "display_text": item.display_text or item.company_name,
                    }
                    for item in page
                ),
                page=page_number,
                page_count=len(pages),
            )
    except CardCapabilityUnavailable:
        return "候选线索已找到，但当前机器人卡片能力未就绪，请先完成卡片配置。"
    title = "重新提交放弃线索" if command.text == "帮我提交放弃提交的线索" else "选择要提交的线索"
    page_suffix = f"，共 {len(pages)} 张候选卡" if len(pages) > 1 else ""
    return f"{title}：已发送候选卡，请勾选后确认提交（共 {len(candidates)} 条{page_suffix}）。"


def prepare_company_submission_preview(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    command: SubmissionCommand,
    company_name: str,
) -> str:
    """按公司名称精确或包含匹配定位线索并发行全字段确认卡，不调用 CRM。

    参数：command 提供销售身份和消息幂等键；company_name 为固定句式解析结果。
    返回值：可直接回复销售的定位状态文本。
    异常：表格、数据库或卡片持久化异常向调用方传播。
    副作用：读取智能表格和 Lead；唯一匹配时新增一张待确认卡片。
    """

    records, contains_match = _find_company_records(smart_table_adapter, company_name)
    if not records:
        return "未找到该公司名称的线索，请确认智能表格中的线索名称后重试。"

    record_ids = tuple(record.record_id for record in records)
    lead_ids_by_record: dict[str, str] = {}
    with session_factory.begin() as session:
        leads = list(
            session.scalars(
                select(Lead).where(
                    Lead.smart_table_record_id.in_(record_ids),
                    Lead.smart_table_owner_user_id == command.sales_user_id,
                    Lead.lifecycle_state == "pending_create",
                )
            )
        )
        lead_ids_by_record.update(
            {
                record_id: lead.id
                for lead in leads
                if (record_id := lead.smart_table_record_id) is not None
            }
        )
        # 新数据库可能没有旧表格记录对应的后台 Lead；仅在当前销售是表格负责人时建立受控映射。
        # 跨负责人记录不会被导入，也不会借此改变原有权限边界。
        for record in records:
            if record.record_id in lead_ids_by_record:
                continue
            linked = _link_existing_smart_table_record(session, record, command)
            if linked is not None:
                lead_ids_by_record[record.record_id] = linked.id
    with session_factory() as session:
        leads = list(session.scalars(select(Lead).where(Lead.id.in_(lead_ids_by_record.values()))))
    by_record_id = {lead.smart_table_record_id: lead for lead in leads}
    eligible = [
        (record, by_record_id[record.record_id])
        for record in records
        if record.record_id in by_record_id
        and (record.fields.get("负责人") in (None, command.sales_user_id))
    ]
    if not eligible:
        return "已找到同名表格记录，但当前账号没有可提交的本人待提交线索。"

    action_service = WecomActionService(
        session_factory,
        card_callback_ready=get_settings().wecom_card_callback_ready(),
    )
    if len(eligible) > 1:
        candidates = tuple(
            {
                "lead_id": lead.id,
                "company_name": f"{record.fields.get('线索名称') or company_name}（候选{index}）",
            }
            for index, (record, lead) in enumerate(eligible, start=1)
        )
        try:
            action_service.issue_company_candidate_confirmation_action(
                actor_user_id=command.sales_user_id,
                request_message_id=command.request_message_id,
                company_name=company_name,
                candidates=candidates,
                contains_match=contains_match,
            )
        except CardCapabilityUnavailable:
            return "找到多个同名线索，但当前机器人卡片能力未就绪，请先在智能表格中确认唯一记录。"
        return f"找到 {len(eligible)} 条同名线索，请在卡片中选择要提交的一条。"

    record, lead = eligible[0]
    display_fields = {
        name: record.fields.get(name)
        for name in _PREVIEW_FIELD_NAMES
    }
    display_fields.update(
        {
            name: value
            for name, value in record.fields.items()
            if name not in display_fields
        }
    )
    # 成员字段的 userId 仍保留在业务快照中；确认卡只展示同一响应附带的真实成员显示名。
    for member_field in ("负责人", "创建人"):
        display_name = record.member_names.get(member_field)
        if isinstance(display_name, str) and display_name.strip():
            display_fields[member_field] = display_name.strip()
    try:
        action_service.issue_company_submission_confirmation_action(
            actor_user_id=command.sales_user_id,
            lead_id=lead.id,
            request_message_id=command.request_message_id,
            company_name=str(record.fields.get("线索名称") or company_name),
            field_values=display_fields,
        )
    except CardCapabilityUnavailable:
        return "已精确找到线索，但当前机器人卡片能力未就绪；请先完成卡片配置后再确认提交。"
    match_description = "按名称包含关系找到候选" if contains_match else "精确找到线索"
    return (
        f"已{match_description}，已发送包含全部字段的确认卡；"
        "如需修改，请先编辑智能表格后再点击确认提交。"
    )


def _link_existing_smart_table_record(
    session: Session, record: SmartTableRecord, command: SubmissionCommand
) -> Lead | None:
    """把当前销售负责的既有表格记录安全登记为待提交 Lead。

    参数：session 为当前事务；record 为智能表格快照；command 提供当前销售和审计消息。
    返回值：新建或已存在的 Lead；负责人不匹配、销售未授权或公司名无效时返回 None。
    异常：数据库约束异常向调用方传播；不调用外部 CRM。
    副作用：写入一条 pending_create Lead 和一条“既有表格记录已绑定”审计事件。
    """
    owner = record.fields.get("负责人")
    company_name = record.fields.get("线索名称")
    if owner != command.sales_user_id or not isinstance(company_name, str) or not company_name:
        return None
    authorization = session.get(SalesAuthorization, command.sales_user_id)
    if (
        authorization is None
        or not authorization.is_authorized
        or not authorization.is_active
    ):
        return None
    existing = session.scalar(
        select(Lead).where(Lead.smart_table_record_id == record.record_id).with_for_update()
    )
    if existing is not None:
        if (
            existing.smart_table_owner_user_id == command.sales_user_id
            and existing.lifecycle_state == "pending_create"
        ):
            return existing
        return None
    # 只按稳定表格记录标识绑定，不按名称猜测或把另一条 Lead 强行合并进来。
    field_values = {name: value for name, value in record.fields.items() if value is not None}
    lead = Lead(
        id=new_lead_id(),
        source_message_id=None,
        original_capturing_sales_user_id=command.sales_user_id,
        smart_table_owner_user_id=command.sales_user_id,
        smart_table_record_id=record.record_id,
        lifecycle_state="pending_create",
        field_values=field_values,
        standard_company_name=company_name,
        company_region="unknown",
        company_verification_status="user_confirmed_unverified",
        company_confirmed_by_user=True,
    )
    session.add(lead)
    session.add(
        BusinessAuditEvent(
            message_id=command.request_message_id,
            sales_user_id=command.sales_user_id,
            event_type="smart_table_existing_lead_linked",
            details={"mapping_type": "existing_record_to_pending_create"},
        )
    )
    session.flush()
    return lead


def _find_company_records(
    smart_table_adapter: SmartTableAdapter, company_name: str
) -> tuple[list[SmartTableRecord], bool]:
    """先精确查询，失败后按公司名称包含关系返回候选记录。

    参数：smart_table_adapter 为智能表格读取边界；company_name 为机器人解析出的公司名称。
    返回值：记录列表及是否使用了包含匹配；精确命中时标记为 False。
    异常：智能表格读取失败时由适配器向上抛出。
    副作用：精确无结果时读取当前子表快照，不执行任何写入或 CRM 调用。
    """
    exact_records = smart_table_adapter.find_records({"线索名称": company_name})
    if exact_records:
        return exact_records, False

    normalized_query = _normalize_company_lookup_text(company_name)
    if len(normalized_query) < 2:
        return [], False
    contains_records: list[SmartTableRecord] = []
    for record in smart_table_adapter.get_records():
        value = record.fields.get("线索名称")
        if not isinstance(value, str):
            continue
        normalized_value = _normalize_company_lookup_text(value)
        if normalized_value and (
            normalized_query in normalized_value or normalized_value in normalized_query
        ):
            contains_records.append(record)
    return contains_records, bool(contains_records)


def _normalize_company_lookup_text(value: str) -> str:
    """规范化公司名称查找文本，仅移除空白并统一大小写。

    参数：value 为待查找的公司名称文本。
    返回值：用于确定性包含匹配的规范文本。
    异常：无。
    副作用：无。
    """
    return "".join(value.split()).casefold()


def _issue_field_confirmation_cards(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    command: SubmissionCommand,
    result: SubmissionBatchResult,
) -> None:
    """为 CRM 必填 AI待确认 字段发行服务端动作卡，不调用 LLM 或直接写 CRM。

    参数：session_factory 与 smart_table_adapter 提供最新审核快照；command 为精确提交命令；
    result 提供本轮待完善线索集合。
    返回值：无。
    异常：卡片能力未就绪时静默保留现有表格 fallback；其他数据库错误向 Worker 传播。
    副作用：能力就绪时持久化 field-confirmation action 与可靠 template-card 通知。
    """
    settings = get_settings()
    if not settings.wecom_card_callback_ready():
        # 未配置 provider 时必须走 T09 既有 Smart Table 人工确认，不生成不可点击的旧卡。
        return
    review = LeadReviewService(
        session_factory,
        smart_table_adapter,
        robot_submission_confirmation_available=True,
    )
    action_service = WecomActionService(
        session_factory,
        card_callback_ready=True,
    )
    for lead_id in result.incomplete_lead_ids:
        try:
            state = review.get_submission_confirmation_state(lead_id)
            if not state.blocking_fields:
                continue
            snapshot = review.reconcile_submission(lead_id)
            action_service.issue_field_confirmation_action(
                actor_user_id=command.sales_user_id,
                lead_id=lead_id,
                field_names=state.blocking_fields,
                command_text=command.text,
                request_message_id=command.request_message_id,
                field_values={
                    field: value
                    for field, value in snapshot.fields.items()
                    if field in state.blocking_fields and isinstance(value, str)
                },
            )
        except CardCapabilityUnavailable:
            # readiness 在事务间变化时同样 fail closed，销售仍可在表格完成确认。
            return
        except ValueError:
            # 线索状态已在重读期间变化时不发行过期卡；最终摘要仍由既有命令通知发送。
            _LOGGER.info("wecom_field_confirmation_card_skipped", extra={"lead_id": lead_id})


def _issue_duplicate_confirmation_card(
    command: SubmissionCommand,
    result: SubmissionBatchResult,
    session_factory: sessionmaker[Session],
) -> None:
    """为 CRM 查重命中的本批次线索发行一次批量继续/停止卡片。

    参数：command 提供销售和提交请求身份；result 提供查重命中集合；session_factory 提供动作持久化。
    返回值：无。
    异常：卡片能力未就绪或动作上下文不合法时保留待确认状态并记录日志。
    副作用：可能写入一张企业微信模板卡动作和对应通知 outbox。
    """
    if not result.duplicate_confirmations or not get_settings().wecom_card_callback_ready():
        return
    action_service = WecomActionService(session_factory, card_callback_ready=True)
    try:
        action_service.issue_duplicate_confirmation_action(
            actor_user_id=command.sales_user_id,
            request_message_id=command.request_message_id,
            duplicates=result.duplicate_confirmations,
        )
    except CardCapabilityUnavailable:
        # readiness 在事务间变化时保留未提交状态，销售可稍后重新发起提交。
        return
    except ValueError:
        _LOGGER.info(
            "wecom_duplicate_confirmation_card_skipped",
            extra={"request_message_id": command.request_message_id},
        )


def notification_key_for_message(message_id: str) -> str:
    """为 CRM 提交通知生成带命名空间的固定长度 SHA-256 键。

    参数：message_id 为企业微信来源消息标识。
    返回值：不超过 notification_key 列限制的 64 位十六进制键。
    异常：无。
    副作用：无。
    """
    return hashlib.sha256(f"crm_submission_notification:{message_id}".encode()).hexdigest()


def terminal_failure_notification_key_for_message(message_id: str) -> str:
    """为命令终态失败通知生成与成功汇总隔离的固定长度键。"""
    return hashlib.sha256(f"crm_submission_terminal_failure:{message_id}".encode()).hexdigest()


def _include_persisted_results(
    session_factory: sessionmaker[Session],
    command: SubmissionCommand,
    result: SubmissionBatchResult,
) -> SubmissionBatchResult:
    """将同一请求已冻结的 CRM 同步状态补入重放汇总。

    参数：session_factory 读取持久化事实；command 标识本次提交；result 为本轮处理结果。
    返回值：首次或恢复执行均可使用的确定性汇总。
    异常：数据库读取错误向调用方传播。
    副作用：仅读取 CRM 同步记录。
    """
    with session_factory() as session:
        sync_results = (
            session.execute(
                select(CrmSyncRecord.status, CrmSyncRecord.failure_code).where(
                    CrmSyncRecord.request_message_id == command.request_message_id
                )
            )
            .tuples()
            .all()
        )
    statuses = [status for status, _ in sync_results]
    # 映射缺失已有独立计数，不能同时归为笼统的待人工处理失败。
    generic_terminal_failure_count = sum(
        status == "failed_pending_review" and failure_code != "mapping_missing"
        for status, failure_code in sync_results
    )
    # 本轮实际成功已经在 result 中，不应被同一持久化记录再次累计。
    if result.succeeded or result.updated:
        return result
    return SubmissionBatchResult(
        succeeded=statuses.count("succeeded"),
        incomplete=result.incomplete,
        retrying=max(result.retrying, statuses.count("retrying")),
        processing=max(result.processing, statuses.count("processing")),
        failed_pending_review=max(result.failed_pending_review, generic_terminal_failure_count),
        updates_not_implemented=result.updates_not_implemented,
        incomplete_lead_ids=result.incomplete_lead_ids,
        updated=result.updated,
        unchanged=result.unchanged,
        company_identity_review=result.company_identity_review,
        mapping_missing=result.mapping_missing,
        duplicate_confirmations=result.duplicate_confirmations,
    )


def _record_command_failure(
    session_factory: sessionmaker[Session], outbox_event_id: int, command: SubmissionCommand
) -> str:
    """记录一次命令编排失败，并在重试耗尽后释放销售顺序检查点。

    参数：session_factory 提供事务；outbox_event_id 为已认领的命令事件。
    返回值：可安全发送给销售的失败摘要。
    异常：数据库写入错误向 Worker 传播。
    副作用：增加尝试次数并置为 retrying 或 failed_pending_review。
    """
    with session_factory.begin() as session:
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.id == outbox_event_id).with_for_update()
        )
        if event is None:
            raise ValueError("CRM 提交命令 Outbox 不存在")
        event.attempts += 1
        # 配置值表示额外重试次数；耗尽后该命令成为完成检查点，后续消息可继续。
        if event.attempts <= get_settings().lead_message_retry_count:
            event.status = "retrying"
            return "CRM 提交任务暂时失败，系统将自动重试；请勿重复提交。"
        succeeded = session.scalar(
            select(func.count()).where(
                CrmSyncRecord.request_message_id == command.request_message_id,
                CrmSyncRecord.status == "succeeded",
            )
        )
        reply = (
            "本次线索提交未能完成，需要人工处理。"
            f"已成功提交：{succeeded or 0} 条；待完善：0 条；需人工处理：1 条。"
        )
        key = terminal_failure_notification_key_for_message(command.request_message_id)
        if session.get(NotificationRecord, key) is None:
            # 通知插入与命令终态在同一事务；插入失败会回滚，命令仍可由 lease 恢复。
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=command.sales_user_id,
                    source_message_id=command.request_message_id,
                    notification_type="crm_submission_summary",
                    content=reply,
                )
            )
        event.status = "failed_pending_review"
        return reply


def format_submission_reply(result: SubmissionBatchResult) -> str:
    """将批次结果格式化为只含计数的销售回复。

    参数：result 为 T12 application service 返回的批次汇总。
    返回值：不包含线索名称、联系方式、payload 或异常堆栈的中文文本。
    异常：无。
    副作用：无。
    """
    if result.updates_not_implemented:
        return "提交我的更新将在 T13 实现；本次未调用 CRM。"
    return (
        f"CRM 提交结果：创建成功 {result.succeeded} 条；更新成功 {result.updated} 条；"
        f"无变化 {result.unchanged} 条；"
        f"公司身份变化待人工审查 {result.company_identity_review} 条；"
        f"待完善或待明确确认 {result.incomplete} 条；"
        f"重复待确认 {len(result.duplicate_confirmations)} 条；"
        f"CRM 用户映射缺失 {result.mapping_missing} 条；"
        f"提交处理中 {result.processing} 条；可重试失败 {result.retrying} 条；"
        f"需人工处理失败 {result.failed_pending_review} 条。"
    )
