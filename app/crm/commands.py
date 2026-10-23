"""消费已持久化 CRM 提交命令并生成脱敏销售汇总。"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.crm.adapter import CRMAdapter
from app.crm.service import (
    CROSS_OWNER_SUBMISSION_COMMAND,
    CreateSubmissionOutcome,
    CrmSubmissionService,
    SubmissionBatchResult,
    SubmissionCommand,
    SubmissionItemResult,
    _append_create_result,
)
from app.leads.models import CrmSyncRecord, Lead, latest_crm_create_sync, new_lead_id
from app.leads.review import LeadReviewService
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    WecomAction,
)
from app.messaging.wecom_directory import WecomDirectoryError, resolve_wecom_member
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.wecom_bot.actions import (
    ACTION_TYPE_CRM_BATCH_SUBMISSION,
    ACTION_TYPE_CRM_COMPANY_CONFIRMATION,
    SUBMISSION_PREVIEW_FIELDS,
    CardCapabilityUnavailable,
    WecomActionService,
    _split_result_notifications,
    build_batch_submission_markdown,
)

_LOGGER = logging.getLogger(__name__)

CRM_SUBMISSION_COMMANDS = frozenset(
    {
        "提交今天的线索",
        "提交我所有线索",
        "帮我提交放弃提交的线索",
        "提交我的更新",
        CROSS_OWNER_SUBMISSION_COMMAND,
    }
)
_CROSS_OWNER_ALL_TARGET = "*"
_CROSS_OWNER_DATE_PATTERN = re.compile(
    r"^提交(?:(?P<year>\d{4})年)?(?P<month>\d{1,2})月(?P<day>\d{1,2})(?:日|号)"
    r"(?P<owner>[^\r\n:：]{1,128})的线索$"
)
_COMPANY_SUBMISSION_PATTERN = re.compile(r"^请帮我提交(?P<company>[^\r\n。]{1,128})这条线索$")
_SUBMISSION_COMMAND_ALIASES = {
    "提交今天线索": "提交今天的线索",
    "提交我今天的线索": "提交今天的线索",
    "帮我提交今天的线索": "提交今天的线索",
    "提交所有线索": "提交我所有线索",
    "提交我所有的线索": "提交我所有线索",
    "提交我的所有线索": "提交我所有线索",
    "帮我提交所有线索": "提交我所有线索",
    "帮我提交放弃的线索": "帮我提交放弃提交的线索",
    "提交放弃的线索": "帮我提交放弃提交的线索",
    "提交放弃提交的线索": "帮我提交放弃提交的线索",
    "重新提交放弃提交的线索": "帮我提交放弃提交的线索",
    "帮我提交我的更新": "提交我的更新",
    "提交更新": "提交我的更新",
}
_INQUIRY_MARKERS = ("吗", "了吗", "是否", "有没有", "是不是", "能否", "可以吗", "?", "？")
_NEGATIVE_SUBMISSION_MARKERS = (
    "不要",
    "别",
    "不用",
    "无需",
    "不想",
    "不需要",
    "暂不",
    "暂时不",
    "先不",
    "先别",
    "禁止",
    "取消",
)


def _company_submission_display_text(fields: Mapping[str, object], lead: Lead) -> str:
    """从发卡时 Smart Table 快照生成公司、联系人和日期的结果展示名。

    参数：fields 为同次表格读取的记录字段；lead 提供服务端创建日期。
    返回值：按公司、联系人、上海时区日期组成的安全展示字符串。
    异常：无。
    副作用：无，不访问表格或修改线索。
    """
    company_name = fields.get("线索名称") or lead.standard_company_name
    contact_name = fields.get("联系人")
    created_at = lead.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    created_text = created_at.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y年%m月%d日")
    return "｜".join(
        part
        for part in (
            company_name.strip() if isinstance(company_name, str) else "",
            contact_name.strip() if isinstance(contact_name, str) else "",
            created_text,
        )
        if part
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
    """识别提交意图并归一化为内部安全命令。

    参数：text 为销售消息正文。
    返回值：内部规范命令；其他文本返回 None。公司定位句式只进入预览边界。
    异常：无。
    副作用：无；仅执行有限的确定性短语归一化，不调用 LLM 或外部系统。
    """
    # 只接受明确执行请求；不能通过删除问号把疑问句变成写操作。
    candidate = text.strip().rstrip("。").strip()
    if not is_explicit_submission_request(candidate):
        return None
    if candidate in CRM_SUBMISSION_COMMANDS:
        return candidate
    alias = _SUBMISSION_COMMAND_ALIASES.get(candidate)
    if alias is not None:
        return alias
    if parse_cross_owner_submission_request(candidate) is not None:
        return CROSS_OWNER_SUBMISSION_COMMAND
    if _CROSS_OWNER_DATE_PATTERN.fullmatch(candidate) is not None:
        # 识别日期句式但日期本身无效时仍进入跨员工拒绝路径，避免 LLM 降级成普通提交命令。
        return CROSS_OWNER_SUBMISSION_COMMAND
    if parse_company_submission_request(candidate) is not None:
        return candidate
    return None


def parse_cross_owner_submission_target(text: str) -> str | None:
    """解析明确的跨员工提交命令及其负责人姓名。

    参数：text 为用户原始命令。
    返回值：目标企微姓名/花名、全员范围哨兵值或 None。
    异常：无；空姓名、过长姓名和含控制分隔符的文本返回 None。
    副作用：无，不查询数据库或触发提交。
    """
    request = parse_cross_owner_submission_request(text)
    return request[0] if request is not None else None


def parse_cross_owner_submission_request(
    text: str, *, today: date | None = None
) -> tuple[str, date | None] | None:
    """解析普通或指定日期的跨员工提交命令，并将短日期归一化到最近的过去日期。

    参数：text 为用户原始命令；today 仅供确定性测试覆盖上海当前日期。
    返回值：负责人姓名/全员哨兵及可选上海自然日；格式无效返回 None。
    异常：无；越界日期作为无效文本返回 None。
    副作用：无；不访问通讯录、数据库或 CRM。
    """
    candidate = text.strip().rstrip("。").strip()
    if candidate in {"提交所有员工的线索", "提交所有人的线索", "提交全部员工的线索"}:
        return _CROSS_OWNER_ALL_TARGET, None
    if candidate in CRM_SUBMISSION_COMMANDS or candidate in _SUBMISSION_COMMAND_ALIASES:
        return None
    dated = _CROSS_OWNER_DATE_PATTERN.fullmatch(candidate)
    if dated is not None:
        owner = dated.group("owner").strip()
        if not owner:
            return None
        month, day = int(dated.group("month")), int(dated.group("day"))
        year_text = dated.group("year")
        try:
            if year_text is not None:
                capture_date = date(int(year_text), month, day)
            else:
                shanghai_today = today or datetime.now(ZoneInfo("Asia/Shanghai")).date()
                # 日期落在今年未来时取上一年；闰日等日期逐年回溯到最近一次有效日期。
                if (month, day) > (shanghai_today.month, shanghai_today.day):
                    first_year = shanghai_today.year - 1
                else:
                    first_year = shanghai_today.year
                capture_date = next(
                    date(year, month, day)
                    for year in range(first_year, first_year - 9, -1)
                    if _valid_calendar_date(year, month, day)
                )
        except (ValueError, StopIteration):
            return None
        return owner, capture_date
    suffix = "的线索"
    prefix = next(
        (value for value in ("提交负责人", "提交") if candidate.startswith(value)), None
    )
    if prefix is None or not candidate.endswith(suffix):
        return None
    employee_name = candidate[len(prefix) : -len(suffix)].strip()
    if (
        not employee_name
        or employee_name == _CROSS_OWNER_ALL_TARGET
        or len(employee_name) > 128
        or any(char in employee_name for char in "\r\n:：")
    ):
        return None
    return employee_name, None


def _valid_calendar_date(year: int, month: int, day: int) -> bool:
    """判断年月日是否是有效公历日期。

    参数：year、month、day 分别是公历年、月、日。
    返回值：可构造公历日期时返回 True，超出日历范围时返回 False。
    异常：无；日期构造抛出的 ValueError 被转换为 False。
    副作用：无。
    """
    try:
        date(year, month, day)
    except ValueError:
        return False
    return True


def looks_like_submission_intent(text: str) -> bool:
    """判断消息是否值得交给模型做提交意图分类。

    参数：text 为销售消息正文。
    返回值：含提交动作且同时出现线索范围、更新或公司定位词时返回 True。
    异常：无。
    副作用：无；仅作异步分类前置筛选，不授权任何 CRM 操作。
    """
    candidate = text.strip()
    return "提交" in candidate and any(
        marker in candidate
        for marker in (
            "线索",
            "更新",
            "今天",
            "所有",
            "全部",
            "放弃",
            "这条",
            "这家公司",
            "公司",
            "企业",
            "重新提交",
            "待完善",
        )
    )


def is_explicit_submission_request(text: str) -> bool:
    """判断文本是否明确要求执行提交动作，而不是询问提交状态。

    参数：text 为销售原始消息文本。
    返回值：存在提交动作且没有明显疑问结构时返回 True。
    异常：无。
    副作用：无，不调用模型或外部系统。
    """
    candidate = text.strip()
    if not candidate or any(marker in candidate for marker in _INQUIRY_MARKERS):
        return False
    # 否定或取消表达优先于模型意图，防止“不要提交”进入任何 CRM 工作流。
    if any(marker in candidate for marker in _NEGATIVE_SUBMISSION_MARKERS):
        return False
    return "提交" in candidate and any(
        marker in candidate
        for marker in (
            "线索",
            "更新",
            "今天",
            "所有",
            "全部",
            "放弃",
            "这条",
            "这家公司",
            "重新提交",
            "待完善",
        )
    )


def consume_submission_command(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    crm_adapter: CRMAdapter | None,
    outbox_event_id: int,
    command_text: str | None = None,
) -> str:
    """消费一条已认领命令 Outbox，并返回不含敏感数据的销售汇总文本。

    参数：前三项为业务依赖，outbox_event_id 为现有可靠消息事件标识；command_text 为可选的
    模型意图归一化命令，仅允许映射到既有安全命令集合。
    返回值：可安全发送给当前销售的确定性中文汇总。
    异常：消息或事件事实缺失时抛出 ValueError；业务依赖异常被转换为可靠重试状态。
    副作用：调用 T12 submission service，写入脱敏通知，并结束或重试本命令事件。
    """
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None or event.event_type not in {
            "crm_submission_command",
            "crm_submission_intent",
        }:
            raise ValueError("不是 CRM 提交命令 Outbox")
        message = session.get(IncomingMessage, event.message_id)
        if message is None:
            raise ValueError("CRM 提交命令缺少来源消息")
        raw_text = message.normalized_text or ""
        # 消费端再次归一化，确保自然说法仍进入既有候选卡流程，而不是绕过确认卡直写 CRM。
        resolved_command_text = command_text or parse_crm_submission_command(raw_text) or raw_text
        cross_owner_request = (
            parse_cross_owner_submission_request(raw_text)
            if resolved_command_text == CROSS_OWNER_SUBMISSION_COMMAND
            else None
        )
        command = SubmissionCommand(
            text=resolved_command_text,
            sales_user_id=message.sales_user_id,
            request_message_id=message.message_id,
            target_owner_name=cross_owner_request[0] if cross_owner_request else None,
            target_capture_date=cross_owner_request[1] if cross_owner_request else None,
        )
    company_name = (
        None
        if command.text == CROSS_OWNER_SUBMISSION_COMMAND
        else parse_company_submission_request(command.text)
    )
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
    if command.text == "重新提交待完善的线索":
        try:
            key = notification_key_for_message(command.request_message_id)
            with session_factory.begin() as session:
                notice = session.get(NotificationRecord, key)
                if notice is None:
                    targets, labels = _latest_incomplete_submission_targets(
                        session_factory, command.sales_user_id
                    )
                    # 外部调用前冻结本命令目标；重放不能换成后来发行或选择的候选。
                    notice = NotificationRecord(
                        notification_key=key,
                        sales_user_id=command.sales_user_id,
                        source_message_id=command.request_message_id,
                        notification_type="crm_submission_retry_summary",
                        status="preparing",
                        payload={"target_lead_ids": list(targets), "lead_labels": labels},
                    )
                    session.add(notice)
                saved = dict(notice.payload or {})
                if saved.get("finished"):
                    # 已结算命令只回显冻结结果；CRM 和发送均由各自原有幂等边界保护。
                    return str(saved["reply"])
                targets = tuple(saved.get("target_lead_ids", ()))
                labels = saved.get("lead_labels", {})
                item_results = {
                    item["lead_id"]: item for item in saved.get("item_results", [])
                }
            pending = False
            if not targets:
                reply = "当前没有上次已选择且待完善的线索需要重新提交。"
            else:
                if crm_adapter is None:
                    raise ValueError("CRM 重提命令缺少 CRM Adapter")
                service = CrmSubmissionService(
                    session_factory,
                    smart_table_adapter,
                    crm_adapter,
                    robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
                )
                # 重放仅继续未完成项；成功、待完善和终态失败的原结果不能被后续轮次改写。
                retry_targets = tuple(
                    lead_id for lead_id in targets
                    if lead_id not in item_results
                    or item_results[lead_id]["status"] in {"retrying", "processing"}
                )
                result = service.submit_incomplete_retry(command, retry_targets)
                item_results.update({item.lead_id: asdict(item) for item in result.items})
                result = SubmissionBatchResult()
                for lead_id in targets:
                    # 复用已有逐条汇总转换，保持缺项、诊断和真实状态来自同一服务结果。
                    item = dict(item_results[lead_id])
                    item.pop("lead_id")
                    if item["status"] == "created":
                        item["status"] = "succeeded"
                    item["missing_fields"] = tuple(item["missing_fields"])
                    result = _append_create_result(
                        result, lead_id, CreateSubmissionOutcome(**item)
                    )
                pending = bool(result.retrying or result.processing)
                reply = format_submission_reply(
                    result, lead_labels=labels, selected_count=len(targets)
                ).replace(
                    f"CRM 提交结果（已选择 {len(targets)} 条）",
                    f"重新提交结果（共 {len(targets)} 条）",
                    1,
                )
        except Exception:
            return _record_command_failure(session_factory, outbox_event_id, command)
        with session_factory.begin() as session:
            event = session.get(OutboxEvent, outbox_event_id)
            if event is not None:
                # 复用其它请求的冻结同步任务时，也必须等实际逐条结果进入终态。
                event.status = "retrying" if pending else "succeeded"
            notice = session.get(NotificationRecord, key)
            if notice is None:
                raise ValueError("重提命令缺少冻结通知")
            saved = dict(notice.payload or {})
            was_pending = bool(saved.get("awaiting_final_result"))
            # 进度和最终结果各发一次；通知重试只重发原正文，不重新触发 CRM。
            phase = "final" if was_pending and not pending else "initial"
            if notice.status == "preparing" or (was_pending and not pending):
                for index, content in enumerate(_split_result_notifications(reply)):
                    chunk_key = key if phase == "initial" and index == 0 else hashlib.sha256(
                        f"crm_submission_retry:{command.request_message_id}:{phase}:{index}".encode()
                    ).hexdigest()
                    chunk = session.get(NotificationRecord, chunk_key)
                    if chunk is None:
                        chunk = NotificationRecord(
                            notification_key=chunk_key,
                            sales_user_id=command.sales_user_id,
                            source_message_id=command.request_message_id,
                            notification_type="crm_submission_retry_summary",
                            content=content,
                            status="pending",
                        )
                        session.add(chunk)
                    elif chunk_key == key and notice.status == "preparing":
                        # 仅首次启用冻结通知；已发送或已认领的分片绝不能重置为 pending。
                        chunk.content = content
                        chunk.status = "pending"
            notice.payload = {
                **saved,
                "reply": reply,
                "awaiting_final_result": pending,
                "finished": not pending,
                "item_results": list(item_results.values()),
            }
        return reply
    if crm_adapter is None:
        raise ValueError("批量 CRM 命令缺少 CRM Adapter")
    if command.text in {
        "提交今天的线索",
        "提交我所有线索",
        "帮我提交放弃提交的线索",
        CROSS_OWNER_SUBMISSION_COMMAND,
    }:
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
    if _submission_delivery_was_prepared(session_factory, command):
        # 重放只复用已冻结通知，不能因表格快照改变而再发行卡片或追加新页。
        _queue_submission_table_link(session_factory, command)
        return "本次提交确认消息已登记，请在原确认卡中操作。"
    if command.text == CROSS_OWNER_SUBMISSION_COMMAND:
        scoped_command, scope_error = _resolve_cross_owner_submission_scope(
            session_factory, command
        )
        if scope_error is not None:
            return scope_error
        if scoped_command is None:
            raise ValueError("跨员工提交负责人范围解析失败")
        command = scoped_command
    service = CrmSubmissionService(
        session_factory,
        smart_table_adapter,
        crm_adapter,
        robot_submission_confirmation_available=get_settings().wecom_card_callback_ready(),
    )
    smart_table_records: tuple[SmartTableRecord, ...] | None = None
    if command.text in {"提交今天的线索", "提交我所有线索", CROSS_OWNER_SUBMISSION_COMMAND}:
        # 批量提交先补齐负责人范围内的未提交表格行，包括人工录入且未关联消息的行。
        smart_table_records = _link_unsubmitted_smart_table_records(
            session_factory, smart_table_adapter, command
        )
    candidates = service.list_submission_candidates(
        command.text,
        command.sales_user_id,
        owner_scope_user_ids=command.owner_scope_user_ids,
        target_capture_date=command.target_capture_date,
        owner_display_names=(
            {command.owner_scope_user_ids[0]: command.target_owner_name}
            if command.target_capture_date is not None
            and command.owner_scope_user_ids
            and command.target_owner_name
            else None
        ),
        smart_table_records=smart_table_records,
    )
    if not candidates:
        if command.text == "帮我提交放弃提交的线索":
            return "当前没有可重新提交的放弃提交线索。"
        return "当前没有可提交的未提交线索。"
    action_service = WecomActionService(
        session_factory,
        card_callback_ready=get_settings().wecom_card_callback_ready(),
    )
    try:
        # 投票卡选项上限为 20，保留一项给可见的全选入口。
        page_size = 19
        pages = [
            candidates[start : start + page_size] for start in range(0, len(candidates), page_size)
        ]
        for page_number, page in enumerate(pages, start=1):
            # 每一页都由服务端冻结候选 ID；销售只能在对应卡片内选择，不能传任意 offset。
            page_candidates = tuple(
                {
                    "lead_id": item.lead_id,
                    "company_name": item.company_name,
                }
                for item in page
            )
            page_snapshots = tuple(
                {
                    "lead_id": item.lead_id,
                    "record_id": item.record.record_id,
                    "fields": dict(item.record.fields),
                    "member_names": dict(item.record.member_names),
                }
                for item in page
                if item.record is not None
            )
            if len(page_snapshots) != len(page_candidates):
                raise ValueError("提交候选缺少首次批量查询快照")
            page_owner_scope = tuple(
                dict.fromkeys(
                    item.owner_user_id for item in page if isinstance(item.owner_user_id, str)
                )
            )
            if command.text == CROSS_OWNER_SUBMISSION_COMMAND and not page_owner_scope:
                raise ValueError("跨员工候选缺少负责人身份")
            action_service.issue_batch_submission_action(
                actor_user_id=command.sales_user_id,
                request_message_id=command.request_message_id,
                command_text=command.text,
                candidates=page_candidates,
                candidate_snapshots=page_snapshots,
                preview_markdown_chunks=build_batch_submission_markdown(
                    page_candidates,
                    page=page_number,
                    page_count=len(pages),
                    name_only=True,
                ),
                page=page_number,
                page_count=len(pages),
                owner_scope_user_ids=(
                    page_owner_scope
                    if command.text == CROSS_OWNER_SUBMISSION_COMMAND
                    else None
                ),
                target_capture_date=command.target_capture_date,
                defer_delivery=True,
            )
    except CardCapabilityUnavailable:
        return "候选线索已找到，但当前机器人卡片能力未就绪，请先完成卡片配置。"
    _queue_submission_table_link(session_factory, command)
    title = "重新提交放弃线索" if command.text == "帮我提交放弃提交的线索" else "选择要提交的线索"
    page_suffix = f"，共 {len(pages)} 张候选卡" if len(pages) > 1 else ""
    return f"{title}：已发送候选卡，请勾选后确认提交（共 {len(candidates)} 条{page_suffix}）。"


def _resolve_cross_owner_submission_scope(
    session_factory: sessionmaker[Session], command: SubmissionCommand
) -> tuple[SubmissionCommand | None, str | None]:
    """校验操作人权限并将姓名目标解析成服务端冻结的负责人 user_id 范围。

    参数：session_factory 为数据库会话工厂；command 包含操作人和原始目标姓名。
    返回值：成功返回带冻结范围的命令；失败返回用户可读的拒绝原因。
    异常：数据库读取失败时传播 SQLAlchemy 异常。
    副作用：仅读取权限目录和线索。
    """
    target_name = command.target_owner_name
    # 没有目标就无法安全确定负责人的筛选范围，因此在发行候选动作前拒绝。
    if target_name is None:
        return None, "没有解析到负责人姓名，请使用“提交负责人姓名的线索”或“提交所有员工的线索”。"
    owner_user_ids: tuple[str, ...] | None = None
    resolved_owner_name: str
    with session_factory() as session:
        actor = session.get(SalesAuthorization, command.sales_user_id)
        # 操作人必须是启用成员且持有专用跨员工提交授权，不能由命令文本授予权限。
        if actor is None or not actor.is_active:
            return None, "当前企微账号未启用，不能提交 CRM 线索。"
        if not actor.can_submit_other_sales_leads:
            return None, "当前账号没有跨员工 CRM 提交授权。"
        if target_name == _CROSS_OWNER_ALL_TARGET:
            # 将“所有员工”限制为数据库中已有线索的负责人，不把通配符传入提交服务。
            owner_user_ids = tuple(
                sorted(set(session.scalars(select(Lead.smart_table_owner_user_id)).all()))
            )
            resolved_owner_name = target_name
    if owner_user_ids is None:
        # 通过完整通讯录解析本名和花名；不依赖员工预登记，也不在姓名重合时猜测。
        settings = get_settings()
        try:
            member = resolve_wecom_member(
                settings.wecom_cli_command,
                target_name,
                settings.wecom_cli_timeout_seconds,
            )
        except ValueError as error:
            return None, f"{error}，未发起提交。"
        except WecomDirectoryError:
            return None, "企微通讯录当前不可查询，未发起提交，请稍后重试。"
        owner_user_ids = (member.user_id,)
        resolved_owner_name = member.name
        # 企微通讯录可能返回“本名(花名)”，而智能表格负责人预筛使用本名。
        combined_name = re.fullmatch(r"(.+?)[(（]([^()（）]+)[)）]", resolved_owner_name.strip())
        if combined_name is not None:
            resolved_owner_name = combined_name.group(1).strip()
    return (
        replace(
            command,
            target_owner_name=resolved_owner_name,
            owner_scope_user_ids=owner_user_ids,
        ),
        None,
    )


def _link_unsubmitted_smart_table_records(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    command: SubmissionCommand,
) -> tuple[SmartTableRecord, ...]:
    """为批量候选补齐负责人范围内仍未提交的表格行映射。

    参数：session_factory 为数据库会话工厂；smart_table_adapter 为真实表格边界；
    command 提供操作者、负责人范围、目标日期和审计消息标识。
    返回值：负责人范围内本次批量读取的表格快照，供候选筛选和提交回调复用。
    异常：表格协议、数据库或授权错误向调用方传播；不调用 CRM。
    副作用：仅按稳定 record_id 新增缺失 Lead 和绑定审计事实，不覆盖已有 Lead。
    """
    cross_owner = command.text == CROSS_OWNER_SUBMISSION_COMMAND
    cross_owner_date = cross_owner and command.target_capture_date is not None
    if cross_owner:
        owner_user_ids = command.owner_scope_user_ids or ()
        if not owner_user_ids or (
            cross_owner_date and (len(owner_user_ids) != 1 or not command.target_owner_name)
        ):
            return ()
        owner_names = (
            {owner_user_ids[0]: command.target_owner_name}
            if cross_owner_date and command.target_owner_name
            else None
        )
    else:
        owner_user_ids = (command.sales_user_id,)
        with session_factory() as session:
            actor = session.get(SalesAuthorization, command.sales_user_id)
            display_name = actor.display_name if actor is not None else None
        owner_names = (
            {command.sales_user_id: display_name} if display_name else None
        )
    capture_date = command.target_capture_date if cross_owner_date else None
    if command.text == "提交今天的线索":
        capture_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    # 先按负责人和未提交状态做服务端筛选；日期命令再合并上海自然日条件。
    records = smart_table_adapter.get_records(
        owner_user_ids=owner_user_ids,
        owner_display_names=owner_names,
        submission_status="未提交",
        capture_date=capture_date,
    )
    day_messages: list[IncomingMessage] = []
    if capture_date is not None:
        local_start = datetime.combine(
            capture_date, datetime.min.time(), tzinfo=ZoneInfo("Asia/Shanghai")
        )
        utc_start = local_start.astimezone(UTC)
        utc_end = (local_start + timedelta(days=1)).astimezone(UTC)
        with session_factory() as session:
            # 仅读取负责人范围内指定上海自然日的来源消息，供无表格时间的旧记录回链。
            day_messages = list(
                session.scalars(
                    select(IncomingMessage).where(
                        IncomingMessage.sales_user_id.in_(owner_user_ids),
                        IncomingMessage.received_at >= utc_start,
                        IncomingMessage.received_at < utc_end,
                    )
                )
            )
    with session_factory.begin() as session:
        record_ids = tuple(record.record_id for record in records)
        linked_record_ids = set(
            session.scalars(
                select(Lead.smart_table_record_id).where(
                    Lead.smart_table_record_id.in_(record_ids)
                )
            )
        ) if record_ids else set()
        # 锁定负责人范围内尚未绑定表格记录的待提交 Lead，避免人工行再造同公司副本。
        day_leads = list(
            session.scalars(
                select(Lead)
                .where(
                    Lead.smart_table_owner_user_id.in_(owner_user_ids),
                    Lead.smart_table_record_id.is_(None),
                    Lead.lifecycle_state.in_(("pending_create", "temporary")),
                )
                .with_for_update()
            )
        )
        lead_source_ids = {
            lead.source_message_id for lead in day_leads if lead.source_message_id is not None
        }
        source_received_at = (
            dict(
                session.execute(
                    select(IncomingMessage.message_id, IncomingMessage.received_at).where(
                        IncomingMessage.message_id.in_(lead_source_ids)
                    )
                ).all()
            )
            if lead_source_ids and capture_date is not None
            else {}
        )
        linked_count = 0
        source_matches: dict[str, str] = {}
        source_match_counts: dict[str, int] = {}
        if capture_date is not None:
            # 没有表格录入日期时，才通过原始消息全文唯一匹配来源日期。
            for record in records:
                company_name = record.fields.get("线索名称")
                remark = record.fields.get("备注")
                original_message = (
                    remark.rsplit("原始消息：", 1)[-1].strip()
                    if isinstance(remark, str) and "原始消息：" in remark
                    else None
                )
                if (
                    record.fields.get("负责人") not in owner_user_ids
                    or record.fields.get("提交状态") not in (None, "", "未提交")
                    or not isinstance(company_name, str)
                    or not company_name.strip()
                    or not original_message
                ):
                    continue
                normalized_name = company_name.strip()
                matching_messages = []
                for message in day_messages:
                    # 公司名不能区分跨日重名；要求表格原始消息与目标日来源消息完全一致。
                    if original_message != (message.normalized_text or "").strip():
                        continue
                    first_line = next(
                        (
                            line.strip()
                            for line in (message.normalized_text or "").splitlines()
                            if line.strip()
                        ),
                        "",
                    )
                    # 名称后的首字符必须是边界，避免把更长的另一个公司名当成命中。
                    has_name_prefix = (
                        first_line[: len(normalized_name)].casefold()
                        == normalized_name.casefold()
                    )
                    has_boundary = (
                        len(first_line) == len(normalized_name)
                        or first_line[len(normalized_name) : len(normalized_name) + 1]
                        in " \t,，:：;；(（"
                    )
                    if has_name_prefix and has_boundary:
                        matching_messages.append(message)
                if len(matching_messages) == 1:
                    message_id = matching_messages[0].message_id
                    source_matches[record.record_id] = message_id
                    source_match_counts[message_id] = source_match_counts.get(message_id, 0) + 1
        # 只补负责人范围内未提交或空状态记录，不把已提交行混入候选。
        for record in records:
            record_owner_id = record.fields.get("负责人")
            if (
                record_owner_id not in owner_user_ids
                or record.fields.get("提交状态") not in (None, "", "未提交")
            ):
                continue
            if record.record_id in linked_record_ids:
                continue
            company_name_value = record.fields.get("线索名称")
            if company_name_value is not None and not isinstance(company_name_value, str):
                continue
            company_name = (
                company_name_value.strip() if isinstance(company_name_value, str) else None
            )
            source_message_id: str | None = None
            row_date = CrmSubmissionService._parse_capture_date(
                record.fields.get("录入时间") or record.fields.get("创建时间")
            )
            if capture_date is not None:
                # 表格时间可以直接证明手工行日期；无时间时仍要求原始消息唯一匹配。
                if row_date is not None and row_date != capture_date:
                    continue
                if row_date is None:
                    source_message_id = source_matches.get(record.record_id)
                    if (
                        source_message_id is None
                        or source_match_counts.get(source_message_id) != 1
                    ):
                        continue
            if capture_date is not None and source_message_id is not None:
                # 已有同公司、同负责人、同日期且未绑定表格记录的 Lead 优先一对一补链。
                matching_leads = [
                    lead
                    for lead in day_leads
                    if company_name is not None
                    and lead.smart_table_record_id is None
                    and lead.source_message_id == source_message_id
                    and lead.smart_table_owner_user_id == record_owner_id
                    and lead.lifecycle_state in {"pending_create", "temporary"}
                    and (
                        lead.standard_company_name == company_name
                        or lead.field_values.get("线索名称") == company_name
                    )
                ]
                if len(matching_leads) == 1:
                    matching_leads[0].smart_table_record_id = record.record_id
                    linked_count += 1
                    continue
                if len(matching_leads) > 1:
                    continue
                # 只有来源消息与表格行一对一对应时才允许按该行创建本地映射。
                source_leads = [
                    lead for lead in day_leads if lead.source_message_id == source_message_id
                ]
                matching_source_leads = [
                    lead
                    for lead in source_leads
                    if company_name is not None
                    and lead.smart_table_record_id is None
                    and (
                        lead.standard_company_name == company_name
                        or lead.field_values.get("线索名称") == company_name
                    )
                ]
                if len(matching_source_leads) == 1:
                    matching_source_leads[0].smart_table_record_id = record.record_id
                    linked_count += 1
                    continue
                if source_leads:
                    # 同一消息已有其他 Lead 时不猜测分段，也不制造来源唯一键冲突。
                    continue
            else:
                # 用负责人、公司名和已核实日期唯一回链；无日期的“全部”也复用唯一同公司草稿。
                matching_leads = [
                    lead
                    for lead in day_leads
                    if company_name is not None
                    and lead.smart_table_owner_user_id == record_owner_id
                    and (
                        lead.standard_company_name == company_name
                        or lead.field_values.get("线索名称") == company_name
                    )
                    and (
                        capture_date is None
                        or CrmSubmissionService._is_capture_date_owned_candidate(
                            lead,
                            (record_owner_id,),
                            capture_date,
                            source_received_at.get(lead.source_message_id),
                        )
                    )
                ]
                if len(matching_leads) == 1:
                    matching_leads[0].smart_table_record_id = record.record_id
                    linked_count += 1
                    continue
                if matching_leads:
                    # 多条同名后台草稿时不猜测绑定对象，避免二次提交同一公司。
                    continue
            linked = _link_existing_smart_table_record(
                session,
                record,
                command,
                owner_user_id=record_owner_id,
                source_message_id=source_message_id,
                write_audit=False,
            )
            if linked is not None:
                linked_count += 1
                linked_record_ids.add(record.record_id)
                if all(lead.id != linked.id for lead in day_leads):
                    day_leads.append(linked)
        if linked_count and session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == command.request_message_id,
                BusinessAuditEvent.event_type == "smart_table_existing_leads_linked",
            )
        ) is None:
            # 同一批量命令只登记一次汇总事件，避免 message_id/event_type 唯一约束冲突。
            session.add(
                BusinessAuditEvent(
                    message_id=command.request_message_id,
                    sales_user_id=command.sales_user_id,
                    event_type="smart_table_existing_leads_linked",
                    details={
                        "mapping_type": "existing_records_to_pending_create",
                        "linked_count": linked_count,
                    },
                )
            )
    return tuple(records)


def prepare_company_submission_preview(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    command: SubmissionCommand,
    company_name: str,
) -> str:
    """按公司名称精确或包含匹配定位线索并发行精简确认卡，不调用 CRM。

    参数：command 提供销售身份和消息幂等键；company_name 为固定句式解析结果。
    返回值：可直接回复销售的定位状态文本。
    异常：表格、数据库或卡片持久化异常向调用方传播。
    副作用：读取智能表格和 Lead；唯一匹配时新增一张待确认卡片。
    """

    if _submission_delivery_was_prepared(session_factory, command):
        # 重放只复用已冻结通知，不能因表格快照改变而再发行卡片或追加新页。
        _queue_submission_table_link(session_factory, command)
        return "本次提交确认消息已登记，请在原确认卡中操作。"
    records, contains_match = _find_company_records(
        smart_table_adapter, company_name, owner_user_ids=(command.sales_user_id,)
    )
    if not records:
        return "未找到该公司名称的线索，请确认智能表格中的线索名称后重试。"
    # 指定线索候选必须仍是未提交；字段完整性不在预览阶段筛除。
    records = [
        record
        for record in records
        if record.fields.get("提交状态") in (None, "", "未提交")
    ]
    if not records:
        return "已找到该公司记录，但当前没有可提交的未提交线索。"

    record_ids = tuple(record.record_id for record in records)
    lead_ids_by_record: dict[str, str] = {}
    with session_factory.begin() as session:
        leads = list(
            session.scalars(
                select(Lead).where(
                    Lead.smart_table_record_id.in_(record_ids),
                    Lead.smart_table_owner_user_id == command.sales_user_id,
                    Lead.lifecycle_state.in_(("pending_create", "temporary")),
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
    eligible: list[tuple[SmartTableRecord, Lead]] = []
    with session_factory() as session:
        for record in records:
            lead = by_record_id.get(record.record_id)
            if (
                lead is None
                or lead.smart_table_owner_user_id != command.sales_user_id
                or record.fields.get("负责人") not in (None, command.sales_user_id)
            ):
                continue
            sync = latest_crm_create_sync(session, lead.id)
            if sync is not None and sync.status in {"succeeded", "abandoned"}:
                continue
            eligible.append((record, lead))
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
                "display_text": _company_submission_display_text(record.fields, lead),
            }
            for index, (record, lead) in enumerate(eligible, start=1)
        )
        candidate_snapshots = tuple(
            {
                "lead_id": lead.id,
                "record_id": record.record_id,
                "fields": dict(record.fields),
                "member_names": dict(record.member_names),
            }
            for record, lead in eligible
        )
        try:
            action_service.issue_company_candidate_confirmation_action(
                actor_user_id=command.sales_user_id,
                request_message_id=command.request_message_id,
                company_name=company_name,
                candidates=candidates,
                candidate_snapshots=candidate_snapshots,
                contains_match=contains_match,
                preview_markdown_chunks=build_batch_submission_markdown(
                    tuple(
                        {
                            "lead_id": lead.id,
                            "company_name": str(record.fields.get("线索名称")),
                            "field_values": dict(record.fields),
                            "missing_fields": (),
                        }
                        for record, lead in eligible
                    ),
                    page=1,
                    page_count=1,
                ),
                defer_delivery=True,
            )
        except CardCapabilityUnavailable:
            return "找到多个同名线索，但当前机器人卡片能力未就绪，请先在智能表格中确认唯一记录。"
        _queue_submission_table_link(session_factory, command)
        return f"找到 {len(eligible)} 条同名线索，请在卡片中选择要提交的一条。"

    record, lead = eligible[0]
    display_fields = {name: record.fields.get(name) for name in SUBMISSION_PREVIEW_FIELDS}
    try:
        action_service.issue_company_submission_confirmation_action(
            actor_user_id=command.sales_user_id,
            lead_id=lead.id,
            request_message_id=command.request_message_id,
            company_name=str(record.fields.get("线索名称") or company_name),
            display_text=_company_submission_display_text(record.fields, lead),
            field_values=display_fields,
            candidate_snapshot={
                "lead_id": lead.id,
                "record_id": record.record_id,
                "fields": dict(record.fields),
                "member_names": dict(record.member_names),
            },
            defer_delivery=True,
        )
    except CardCapabilityUnavailable:
        return "已精确找到线索，但当前机器人卡片能力未就绪；请先完成卡片配置后再确认提交。"
    _queue_submission_table_link(session_factory, command)
    match_description = "按名称包含关系找到候选" if contains_match else "精确找到线索"
    return (
        f"已{match_description}，已发送提交前确认卡；如需修改，请先编辑智能表格后再点击确认提交。"
    )


def _submission_delivery_was_prepared(
    session_factory: sessionmaker[Session],
    command: SubmissionCommand,
) -> bool:
    """检查该销售命令是否已原子冻结全部投递依赖；只读数据库，错误向调用方传播。"""
    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.sales_user_id == command.sales_user_id,
                NotificationRecord.source_message_id == command.request_message_id,
                NotificationRecord.notification_type == "wecom_action_card",
            )
        )
        return any(
            isinstance(notice.payload, dict) and "depends_on" in notice.payload
            for notice in notices
        )


def _queue_submission_table_link(
    session_factory: sessionmaker[Session],
    command: SubmissionCommand,
) -> None:
    """原子激活提交明细、卡片和一次表格链接的严格投递依赖。

    参数：session_factory 为通知事务工厂；command 提供销售与命令幂等标识。
    返回值：无。异常：数据库错误向调用方传播，任务重试复用原通知。
    副作用：只登记 NotificationRecord；缺少链接时记录脱敏配置错误，不伪造成功。
    """
    with session_factory.begin() as session:
        # 同销售命令串行收敛，避免重放为已投递通知重新创建链接。
        session.scalar(
            select(SalesAuthorization)
            .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
            .with_for_update()
        )
        notices = list(
            session.scalars(
                select(NotificationRecord)
                .where(
                    NotificationRecord.sales_user_id == command.sales_user_id,
                    NotificationRecord.source_message_id == command.request_message_id,
                    NotificationRecord.notification_type.in_(
                        ("wecom_action_preview", "wecom_action_card")
                    ),
                )
                .order_by(NotificationRecord.created_at, NotificationRecord.notification_key)
            )
        )
        if not notices:
            return
        # 所有页的明细先完成，再发送所有页卡片；前一条实际成功才解锁后一条。
        ordered = sorted(notices, key=lambda item: item.notification_type == "wecom_action_card")
        dependencies: list[str] = []
        for notice in ordered:
            notice.payload = {
                **(notice.payload or {}),
                "delivery_pending": False,
                "depends_on": list(dependencies),
            }
            dependencies = [notice.notification_key]
        key = hashlib.sha256(
            f"crm_submission_table_link:{command.sales_user_id}:"
            f"{command.request_message_id}".encode()
        ).hexdigest()
        if session.get(NotificationRecord, key) is not None:
            return
        url = (get_settings().lead_smart_table_url or "").strip()
        if not url:
            _LOGGER.error(
                "crm_submission_table_link_configuration_missing",
                extra={"configuration_key": "LEAD_SMART_TABLE_URL"},
            )
            return
        session.add(
            NotificationRecord(
                notification_key=key,
                sales_user_id=command.sales_user_id,
                source_message_id=command.request_message_id,
                notification_type="crm_submission_table_link",
                content=f"请在智能表格中审核线索：[打开线索表]({url})",
                payload={"depends_on": dependencies},
            )
        )


def _link_existing_smart_table_record(
    session: Session,
    record: SmartTableRecord,
    command: SubmissionCommand,
    *,
    owner_user_id: str | None = None,
    source_message_id: str | None = None,
    write_audit: bool = True,
) -> Lead | None:
    """把负责人范围内的既有表格记录安全登记为待提交 Lead。

    参数：session 为当前事务；record 为智能表格快照；command 提供当前销售和审计消息。
    返回值：新建或已存在的 Lead；负责人不匹配或公司名类型异常时返回 None。
    异常：数据库约束异常向调用方传播；不调用外部 CRM。
    副作用：写入一条 pending_create Lead；write_audit 为 True 时追加绑定审计事件。
    """
    expected_owner_id = owner_user_id or command.sales_user_id
    owner = record.fields.get("负责人")
    company_name_value = record.fields.get("线索名称")
    if owner != expected_owner_id or (
        company_name_value is not None and not isinstance(company_name_value, str)
    ):
        return None
    company_name = (
        company_name_value.strip() if isinstance(company_name_value, str) else None
    )
    existing = session.scalar(
        select(Lead).where(Lead.smart_table_record_id == record.record_id).with_for_update()
    )
    if existing is not None:
        if (
            existing.smart_table_owner_user_id == expected_owner_id
            and existing.lifecycle_state in {"pending_create", "temporary"}
        ):
            if existing.source_message_id is None and source_message_id is not None:
                existing.source_message_id = source_message_id
            return existing
        return None
    # 表格成员字段已给出企微成员身份；缺少本地引用行时只登记成员事实，不授予提交权限。
    if session.get(SalesAuthorization, expected_owner_id) is None:
        try:
            with session.begin_nested():
                session.add(
                    SalesAuthorization(
                        wecom_user_id=expected_owner_id,
                        display_name=record.member_names.get("负责人"),
                        is_authorized=False,
                        is_active=True,
                    )
                )
                session.flush()
        except IntegrityError:
            # 并发请求若已插入同一成员行，继续使用已有事实；其他完整性错误仍应失败。
            if session.get(SalesAuthorization, expected_owner_id) is None:
                raise
    # 只按稳定表格记录标识绑定，不按名称猜测或把另一条 Lead 强行合并进来。
    field_values = {name: value for name, value in record.fields.items() if value is not None}
    # 表格中的人工空名行先建立映射，最终 CRM 必填校验会将其拦截为待完善。
    lead = Lead(
        id=new_lead_id(),
        source_message_id=source_message_id,
        original_capturing_sales_user_id=expected_owner_id,
        smart_table_owner_user_id=expected_owner_id,
        smart_table_record_id=record.record_id,
        lifecycle_state="pending_create",
        field_values=field_values,
        standard_company_name=company_name or None,
        company_region="unknown",
        company_verification_status=(
            "user_confirmed_unverified" if company_name else "incomplete_company"
        ),
        company_confirmed_by_user=bool(company_name),
    )
    session.add(lead)
    if write_audit:
        # 单条公司定位沿用原事件名；批量补齐由调用方写一条汇总审计，避免唯一键冲突。
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
    smart_table_adapter: SmartTableAdapter,
    company_name: str,
    *,
    owner_user_ids: tuple[str, ...],
) -> tuple[list[SmartTableRecord], bool]:
    """只在指定负责人范围内按公司名称精确或包含匹配表格记录。

    参数：smart_table_adapter 为智能表格读取边界；company_name 为解析出的公司名称；
    owner_user_ids 为当前已授权提交范围。
    返回值：记录列表及是否使用了包含匹配；精确命中时标记为 False。
    异常：智能表格读取失败时由适配器向上抛出。
    副作用：仅读取指定负责人记录，不执行任何写入或 CRM 调用。
    """
    scoped_records = smart_table_adapter.get_records(owner_user_ids=owner_user_ids)
    exact_records = [
        record for record in scoped_records if record.fields.get("线索名称") == company_name
    ]
    if exact_records:
        return exact_records, False

    normalized_query = _normalize_company_lookup_text(company_name)
    if len(normalized_query) < 2:
        return [], False
    contains_records: list[SmartTableRecord] = []
    for record in scoped_records:
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
        incomplete_missing_fields=result.incomplete_missing_fields,
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
        items=result.items,
        not_submitted=result.not_submitted,
    )


def _latest_incomplete_submission_targets(
    session_factory: sessionmaker[Session], sales_user_id: str
) -> tuple[tuple[str, ...], dict[str, str]]:
    """从最近已完成的提交命令组读取当时被选择且待完善的 Lead。

    参数：session_factory 提供历史动作读取；sales_user_id 限定当前销售本人。
    返回值：最近一个含不完整结果的命令组目标 ID 及发行时冻结展示标签。
    异常：数据库读取错误向调用方传播；畸形旧 context 被安全忽略。
    副作用：只读动作历史，不改写旧 action 或调用外部系统。
    """
    action_types = (
        ACTION_TYPE_CRM_BATCH_SUBMISSION,
        ACTION_TYPE_CRM_COMPANY_CONFIRMATION,
    )
    with session_factory() as session:
        actions = list(
            session.scalars(
                select(WecomAction)
                .where(
                    WecomAction.bound_actor_wecom_user_id == sales_user_id,
                    WecomAction.action_type.in_(action_types),
                )
                .order_by(WecomAction.created_at.desc(), WecomAction.id.desc())
            )
        )
    # 查询已按创建时间倒序；字典保留插入顺序，同时把同一 request 的所有卡页聚合。
    groups: dict[str, list[WecomAction]] = {}
    for action in actions:
        request_id = action.context.get("request_message_id")
        if isinstance(request_id, str) and request_id:
            groups.setdefault(request_id, []).append(action)
    for group in groups.values():
        labels: dict[str, str] = {}
        incomplete_ids: list[str] = []
        has_completed_selection = False
        for action in group:
            context = action.context
            display_text = context.get("display_text")
            if isinstance(display_text, str) and display_text:
                labels[action.target_id] = display_text
            candidates = context.get("candidate_leads")
            if isinstance(candidates, list):
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    lead_id = candidate.get("lead_id")
                    label = candidate.get("display_text") or candidate.get("company_name")
                    if isinstance(lead_id, str) and isinstance(label, str) and label:
                        labels[lead_id] = label
            if action.status != "succeeded":
                continue
            saved_results = context.get("submission_results")
            if not isinstance(saved_results, list):
                continue
            # 批量只接受服务端已勾选目标，单条只接受服务端最终选择的目标。
            selected = context.get("selected_lead_ids", [])
            if action.action_type == ACTION_TYPE_CRM_COMPANY_CONFIRMATION:
                selected = [context.get("selected_lead_id", action.target_id)]
            if not isinstance(selected, list) or not selected:
                continue
            has_completed_selection = True
            for item in saved_results:
                if (
                    isinstance(item, dict)
                    and item.get("status") == "incomplete"
                    and isinstance(item.get("lead_id"), str)
                    and item["lead_id"] in selected
                    and item["lead_id"] not in incomplete_ids
                ):
                    incomplete_ids.append(item["lead_id"])
        if has_completed_selection:
            # 最近实际选择已无待完善项时，不回退到更早的提交组扩大重提范围。
            for lead_id in incomplete_ids:
                labels.setdefault(lead_id, "线索")
            return tuple(incomplete_ids), labels
    return (), {}


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


def _crm_duplicate_search_failure_detail(item: SubmissionItemResult) -> str | None:
    """根据 CRM 查重失败的受控字段生成不含远端正文的逐条提示。

    参数：item 为单条 CRM 提交状态和适配器白名单错误元数据。
    返回值：查重失败说明；非查重错误返回 None，由既有文案处理。
    异常：无。
    副作用：无。
    """
    if item.reason_code not in {
        "crm_duplicate_search_failed",
        "crm_duplicate_search_retrying",
        "duplicate_target_unavailable",
    }:
        return None
    detail = {
        "authentication": "CRM 鉴权失败",
        "transport": "CRM 网络连接或超时异常",
        "malformed_response": "CRM 返回格式异常",
        "business": "CRM 返回业务错误",
        "business_rejection": "CRM 返回业务错误",
        "gateway": "CRM 网关异常",
        "duplicate_target_unavailable": "CRM 检测到重复线索但未返回可操作的线索 ID",
    }.get(item.adapter_category or "", "CRM 查重失败，需要人工处理")
    if item.reason_code == "duplicate_target_unavailable":
        return detail
    if item.adapter_category == "transport":
        return detail
    suffix: list[str] = []
    if item.http_status is not None:
        suffix.append(f"HTTP {item.http_status}")
    if item.failure_code:
        suffix.append(f"错误码：{item.failure_code}")
    return f"{detail}（{'，'.join(suffix)}）" if suffix else detail


def format_submission_reply(
    result: SubmissionBatchResult,
    *,
    lead_labels: Mapping[str, str] | None = None,
    selected_count: int | None = None,
) -> str:
    """将批次结果格式化为汇总加逐条明细的销售回复。

    参数：result 为确定性提交结果；lead_labels 为服务端冻结的线索展示名称；
    selected_count 为 callback 已确认的选择数量。
    返回值：不包含异常正文、凭据或原始 CRM 响应的中文 Markdown 文本。
    异常：无。
    副作用：无。
    """

    if result.updates_not_implemented:
        return "提交我的更新将在 T13 实现；本次未调用 CRM。"
    labels = lead_labels or {}
    chosen = selected_count if selected_count is not None else (len(result.items) or None)
    title = f"CRM 提交结果（已选择 {chosen} 条）" if chosen is not None else "CRM 提交结果"
    failed_count = (
        result.failed_pending_review
        + result.mapping_missing
        + result.company_identity_review
    )
    success_count = result.succeeded + result.updated
    completed_count = success_count + result.followed_up
    processing_count = result.processing + sum(
        getattr(item.status, "value", str(item.status)) == "duplicate_confirmation"
        for item in result.items
    )
    lines = [
        title,
        "",
        f"✅ CRM 新增/更新成功 {success_count} 条",
        f"🔁 CRM 已存在并跟进 {result.followed_up} 条",
        f"✅ 无变化 {result.unchanged} 条",
        f"⚠️ 待完善 {result.incomplete} 条",
        f"⏳ 处理中 {processing_count} 条",
        f"⚪ 未提交 {result.not_submitted} 条",
        f"🔄 重试中 {result.retrying} 条",
        f"需人工处理 {failed_count} 条",
        "",
        (
            f"汇总：提交完成 {completed_count}｜新增/更新成功 {success_count}｜"
            f"已存在并跟进 {result.followed_up}｜"
            f"待完善 {result.incomplete}｜处理中 {processing_count}｜"
            f"未提交 {result.not_submitted}｜失败 {failed_count}"
        ),
    ]
    detail_lines: list[str] = []
    status_titles = {
        "unchanged": "✅ 无变化",
        "incomplete": "⚠️ 待完善",
        "followed_up": "🔁 已存在并跟进",
        "processing": "⏳ 处理中",
        "retrying": "🔄 重试中",
        "duplicate_confirmation": "⏳ 处理中",
        "mapping_missing": "CRM 用户映射缺失",
        "company_identity_review": "需人工处理",
        "failed_pending_review": "需人工处理",
        "not_submitted": "⚪ 未提交",
    }
    reason_text = {
        "crm_enum_mapping_missing": "智能表格选项缺少 CRM 字典映射，请联系管理员配置后重新提交",
        "crm_user_mapping_missing": "当前负责人无法映射 CRM 用户，请联系管理员",
        "crm_cross_owner_submission_not_authorized": "跨员工提交权限已撤销或不可用，本次未提交",
        "company_identity_conflict": "公司身份记录冲突，需要人工处理",
        "company_identity_reserved": "已有提交任务正在处理，本次未重复创建",
        "sync_processing": "已有提交任务正在处理，本次未重复创建",
        "wecom_smart_table_retryable": (
            "企微智能表格暂时限流或网络异常，本次未发起 CRM 提交，请稍后重新提交"
        ),
        "pre_crm_transient_failure": "提交前依赖暂时不可用，本次未发起 CRM 提交，请稍后重新提交",
        "smart_table_snapshot_failed": (
            "企微智能表格读取失败，本次未发起 CRM 提交，请检查表格权限或系统配置后重试"
        ),
        "crm_create_retrying": "CRM 提交暂时失败，系统将自动重试",
        "crm_create_failed_pending_review": "CRM 提交失败，需要人工处理",
        "crm_followed_up": "CRM 中已存在该线索，已进行跟进",
        "crm_update_retrying": "CRM 提交暂时失败，系统将自动重试",
        "crm_update_failed_pending_review": "CRM 提交失败，需要人工处理",
        "crm_update_incomplete": "CRM 提交前校验未通过，请检查当前线索信息",
        "company_identity_change_pending_review": "公司名称发生变化，需要人工处理",
        "candidate_state_changed": "线索状态已变化，请重新发起提交",
        "already_submitted": "该线索已完成提交，本次未重复创建",
    }
    successful_items = [
        item
        for item in result.items
        if getattr(item.status, "value", str(item.status)) in {"created", "updated"}
        and item.lead_id in labels
    ]
    if successful_items:
        detail_lines.append(f"✅ CRM 新增/更新成功 {len(successful_items)} 条")
        detail_lines.extend(f"- {labels[item.lead_id]}" for item in successful_items)
    grouped_statuses = (
        "unchanged",
        "incomplete",
        "followed_up",
        "duplicate_confirmation",
        "mapping_missing",
        "processing",
        "retrying",
        "company_identity_review",
        "failed_pending_review",
        "not_submitted",
    )
    for status in grouped_statuses:
        items = [
            item
            for item in result.items
            if getattr(item.status, "value", str(item.status)) == status
            and item.lead_id in labels
        ]
        if not items:
            continue
        detail_lines.append(f"{status_titles[status]} {len(items)} 条")
        for item in items:
            label = labels[item.lead_id]
            if status == "incomplete" and item.missing_fields:
                detail_lines.append(f"- {label}：缺少「{'、'.join(item.missing_fields)}」")
            else:
                detail = _crm_duplicate_search_failure_detail(item) or reason_text.get(
                    item.reason_code or "", ""
                )
                detail_lines.append(f"- {label}：{detail}".rstrip("："))
    if detail_lines:
        lines.extend(["", "明细：", *detail_lines])
    elif not result.items and result.incomplete_missing_fields:
        # 保留旧调用方的兼容汇总；真实批量结果始终走上面的逐条字段来源。
        lines.extend(
            [
                "",
                f"待完善汇总：缺少必填字段：{'、'.join(result.incomplete_missing_fields)}",
            ]
        )
    return "\n".join(lines)
