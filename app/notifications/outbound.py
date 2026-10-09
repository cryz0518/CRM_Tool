"""由既有企业微信 Bot 长连接主动发送可靠通知。"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.leads.models import Lead, LeadMessageResolution, SmartTableSync
from app.leads.reminders import (
    DAILY_UNSUBMITTED_REMINDER_CONTENT,
    daily_reminder_is_current,
)
from app.messaging.models import NotificationRecord, OutboxEvent, SalesAuthorization, utc_now

logger = logging.getLogger(__name__)
_NOTIFICATION_LEASE = timedelta(minutes=5)
_SDK_ERROR_CODE = re.compile(r"errcode=(\d+)")
_NON_RETRYABLE_PROVIDER_CODES = frozenset(
    {f"420{code}" for code in range(27, 52)}
)
_RECEIPT_TERMINAL_OUTBOX_STATUSES = frozenset(
    {"succeeded", "ignored", "unauthorized", "invalid", "failed_pending_review"}
)

_SUPPORTED_NOTIFICATION_TYPES = frozenset(
    {
        "lead_intake_receipt",
        "lead_progress_summary",
        "daily_unsubmitted_lead_reminder",
        "lead_first_smart_table_success",
        "crm_submission_summary",
        "crm_submission_preview",
        "crm_submission_intent_unrecognized",
        "lead_processing_failed",
        "wecom_action_preview",
        "sales_authorization_denied",
        "media_text_input_required",
        "wecom_action_card",
        "wecom_action_result",
    }
)


class WecomMessageClient(Protocol):
    """描述已认证 WSClient 的最小主动发送能力。"""

    async def send_message(self, userid_or_chatid: str, body: dict[str, object]) -> Any:
        """向单聊 userid 或群聊 chatid 主动发送消息。"""
        ...


class WecomOutboundNotificationSender:
    """只使用 Bot 进程已持有的 WSClient 消费可靠通知。"""

    def __init__(self, session_factory: sessionmaker[Session], client: WecomMessageClient) -> None:
        """保存通知数据库与现有已认证客户端，不创建任何 WebSocket。"""
        self._session_factory = session_factory
        self._client = client

    async def send_pending_once(self) -> int:
        """发送当前待投递通知，失败保留 retrying 而不触碰 CRM。"""
        with self._session_factory() as session:
            notices = session.scalars(
                select(NotificationRecord).where(
                    NotificationRecord.notification_type.in_(_SUPPORTED_NOTIFICATION_TYPES),
                    or_(
                        NotificationRecord.status.in_(("pending", "retrying")),
                        and_(
                            NotificationRecord.status == "processing",
                            NotificationRecord.processing_lease_expires_at <= utc_now(),
                        ),
                    ),
                )
                .order_by(NotificationRecord.created_at, NotificationRecord.notification_key)
            ).all()
        sent = 0
        for notice in notices:
            claimed_notice: tuple[str, str | None, dict[str, object] | None] | None = None
            with self._session_factory.begin() as session:
                current = session.scalar(
                    select(NotificationRecord)
                    .where(NotificationRecord.notification_key == notice.notification_key)
                    .with_for_update()
                )
                lease_expired = (
                    current is not None
                    and current.status == "processing"
                    and current.processing_lease_expires_at is not None
                    and _as_utc(current.processing_lease_expires_at) <= utc_now()
                )
                if current is None or (
                    current.status not in {"pending", "retrying"} and not lease_expired
                ):
                    continue
                if current.notification_type == "lead_progress_summary":
                    authorization = session.get(SalesAuthorization, current.sales_user_id)
                    if (
                        not get_settings().lead_progress_enabled
                        or authorization is None
                        or not authorization.is_active
                    ):
                        # 停用销售或功能关闭时不发送排队中的主动进度汇报。
                        current.status = "suppressed"
                        current.processing_started_at = None
                        current.processing_lease_expires_at = None
                        current.processing_claim_token = None
                        continue
                if current.notification_type == "daily_unsubmitted_lead_reminder":
                    business_date = (
                        current.payload.get("business_date")
                        if isinstance(current.payload, dict)
                        else None
                    )
                    if not daily_reminder_is_current(
                        session,
                        current.sales_user_id,
                        business_date,
                    ):
                        # 已提交、跨日、停用或失去 CRM 映射的提醒不再发给销售。
                        current.status = "suppressed"
                        current.processing_started_at = None
                        current.processing_lease_expires_at = None
                        current.processing_claim_token = None
                        continue
                if current.notification_type == "lead_intake_receipt" and _receipt_is_deferred(
                    current.payload, utc_now()
                ):
                    # 合并窗口内只累加消息数，等窗口结束再发送一次确认。
                    continue
                # 先原子认领，避免多个 Bot 循环重复发送同一通知。
                claim_token = uuid4().hex
                current.status = "processing"
                current.processing_started_at = utc_now()
                current.processing_lease_expires_at = (
                    current.processing_started_at + _NOTIFICATION_LEASE
                )
                current.processing_claim_token = claim_token
                claimed_notice = (
                    current.sales_user_id,
                    current.content,
                    dict(current.payload) if current.payload is not None else None,
                )
            if claimed_notice is None:
                continue
            try:
                # AI Bot 主动发送只支持 markdown 或模板卡片；旧通知可能仍保存 text，统一在边界转换。
                sales_user_id, content, payload = claimed_notice
                if notice.notification_type == "lead_intake_receipt":
                    receipt_content, receipt_counts, should_suppress = self._receipt_content(
                        sales_user_id, notice.source_message_id, payload
                    )
                    if should_suppress:
                        # 仅在独立失败通知已承担全部反馈时收敛，避免重复刷屏。
                        with self._session_factory.begin() as session:
                            current = session.get(NotificationRecord, notice.notification_key)
                            if (
                                current is not None
                                and current.processing_claim_token == claim_token
                            ):
                                current.status = "suppressed"
                                current.payload = {
                                    **(current.payload or {}),
                                    "suppression_reason": (
                                        "all_failed_messages_have_independent_failure_notice"
                                    ),
                                    "receipt_outcome_counts": receipt_counts,
                                }
                                current.processing_started_at = None
                                current.processing_lease_expires_at = None
                                current.processing_claim_token = None
                        continue
                    content = receipt_content
                body: dict[str, object]
                if notice.notification_type == "daily_unsubmitted_lead_reminder":
                    # 每日提醒仅发送固定 Markdown 正文，不把审计载荷字段交给 SDK。
                    body = {
                        "msgtype": "markdown",
                        "markdown": {"content": DAILY_UNSUBMITTED_REMINDER_CONTENT},
                    }
                else:
                    body = _build_supported_body(payload, content)
                await self._client.send_message(sales_user_id, body)
            except Exception as exc:
                provider_error_code = _provider_error_code(exc)
                retryable = provider_error_code not in _NON_RETRYABLE_PROVIDER_CODES
                with self._session_factory.begin() as session:
                    current = session.get(NotificationRecord, notice.notification_key)
                    if current is not None and current.processing_claim_token == claim_token:
                        current.status = "retrying" if retryable else "failed"
                        current.processing_started_at = None
                        current.processing_lease_expires_at = None
                        current.processing_claim_token = None
                        current.attempts += 1
                        # 日志正文只保留异常类型和纯数字错误码，禁止透传回执原文。
                        logger.warning(
                            "notification_send_failed class=%s code=%s",
                            type(exc).__name__,
                            provider_error_code or "unknown",
                            extra={
                                "notification_key": notice.notification_key,
                                "event": "notification_send_failed",
                            },
                        )
                        logger.info(
                            "notification_retry",
                            extra={
                                "notification_key": notice.notification_key,
                                "event": "notification_retry",
                            },
                        )
                continue
            with self._session_factory.begin() as session:
                current = session.get(NotificationRecord, notice.notification_key)
                if current is not None and current.processing_claim_token == claim_token:
                    current.status = "succeeded"
                    current.processing_started_at = None
                    current.processing_lease_expires_at = None
                    current.processing_claim_token = None
                    current.attempts += 1
                    current.sent_at = utc_now()
                    logger.info(
                        "notification_sent",
                        extra={
                            "notification_key": notice.notification_key,
                            "event": "notification_sent",
                        },
                    )
            sent += 1
        return sent

    def _receipt_content(
        self,
        sales_user_id: str,
        source_message_id: str,
        payload: dict[str, object] | None,
    ) -> tuple[str, dict[str, int], bool]:
        """按接收通知关联的消息读取实时处理状态并生成不误报的提示。

        参数：sales_user_id 为收件人；source_message_id 为旧载荷兼容来源；payload 保存消息集合。
        返回值：准确的 Markdown 文本、按结果分类的数量及是否可由独立失败通知抑制。
        异常：数据库读取错误向调用方传播，原通知仍由现有失败/重试逻辑管理。
        副作用：仅查询消息 Outbox 与归属结论，不更新业务状态。
        """
        raw_ids = payload.get("message_ids") if isinstance(payload, dict) else None
        message_ids = (
            list(dict.fromkeys(item for item in raw_ids if isinstance(item, str)))
            if isinstance(raw_ids, list)
            else []
        )
        if not message_ids:
            message_ids = [source_message_id]

        with self._session_factory() as session:
            events = session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.message_id.in_(message_ids),
                    OutboxEvent.sales_user_id == sales_user_id,
                    OutboxEvent.event_type == "message_received",
                )
            ).all()
            resolutions = session.scalars(
                select(LeadMessageResolution).where(
                    LeadMessageResolution.message_id.in_(message_ids)
                )
            ).all()
            syncs = session.scalars(
                select(SmartTableSync).where(SmartTableSync.source_message_id.in_(message_ids))
            ).all()
            failure_notices = session.scalars(
                select(NotificationRecord).where(
                    NotificationRecord.sales_user_id == sales_user_id,
                    NotificationRecord.source_message_id.in_(message_ids),
                    NotificationRecord.notification_type == "lead_processing_failed",
                    NotificationRecord.status.in_(
                        ("pending", "processing", "retrying", "succeeded")
                    ),
                )
            ).all()
            lead_ids = {resolution.lead_id for resolution in resolutions if resolution.lead_id}
            leads = (
                session.scalars(select(Lead).where(Lead.id.in_(lead_ids))).all()
                if lead_ids
                else []
            )

        events_by_id = {event.message_id: event for event in events}
        resolutions_by_id: dict[str, list[LeadMessageResolution]] = {}
        for resolution in resolutions:
            resolutions_by_id.setdefault(resolution.message_id, []).append(resolution)
        syncs_by_segment = {
            (sync.source_message_id, sync.source_segment_index): sync for sync in syncs
        }
        leads_by_id = {lead.id: lead for lead in leads}
        failed_notice_message_ids = {notice.source_message_id for notice in failure_notices}

        counts: dict[str, int] = {}
        all_finished = True
        for message_id in message_ids:
            event = events_by_id.get(message_id)
            if event is None:
                category = "unknown"
                all_finished = False
            elif event.status in {"pending", "processing"}:
                category = "processing"
                all_finished = False
            elif event.status == "retrying":
                category = "retrying"
                all_finished = False
            elif event.status in _RECEIPT_TERMINAL_OUTBOX_STATUSES:
                message_resolutions = resolutions_by_id.get(message_id, [])
                states = {resolution.status for resolution in message_resolutions}
                if event.status == "failed_pending_review":
                    category = (
                        "failed_notified"
                        if message_id in failed_notice_message_ids
                        else "failed"
                    )
                elif event.status == "ignored":
                    category = "ignored"
                elif event.status == "succeeded" and states & {
                    "unassigned",
                    "quote_unresolved",
                }:
                    category = "unassigned"
                elif event.status == "succeeded" and "assigned" in states:
                    assigned = [
                        resolution
                        for resolution in message_resolutions
                        if resolution.status == "assigned"
                    ]
                    # Outbox 成功只代表消费结束；只有对应 Lead 与远端同步记录都吻合才报告已录入。
                    category = (
                        "recorded"
                        if assigned
                        and all(
                            resolution.lead_id is not None
                            and (sync := syncs_by_segment.get(
                                (message_id, resolution.segment_index)
                            )) is not None
                            and sync.status == "succeeded"
                            and bool(sync.smart_table_record_id)
                            and (lead := leads_by_id.get(resolution.lead_id)) is not None
                            and lead.smart_table_record_id == sync.smart_table_record_id
                            for resolution in assigned
                        )
                        else "completed"
                    )
                else:
                    category = "finished"
            else:
                category = "unknown"
                all_finished = False
            counts[category] = counts.get(category, 0) + 1

        count = len(message_ids)
        if counts == {"processing": count}:
            return f"✅ 已收到你的 {count} 条消息，正在识别并录入。", counts, False

        # 失败状态已有独立、可重试的通知记录时，整组全失败可抑制重复提醒。
        if all_finished and counts == {"failed_notified": count}:
            return "", counts, True

        labels = {
            "processing": "仍在识别并录入",
            "recorded": "已确认录入智能表格",
            "completed": "已完成处理，表格录入尚未核实",
            "unassigned": "无法可靠归属客户，请补充公司信息",
            "retrying": "等待重试",
            "failed": "待人工处理",
            "failed_notified": "待人工处理（另有失败通知）",
            "ignored": "已忽略（未录入）",
            "finished": "处理已结束，录入状态未核实",
            "unknown": "状态待核实",
        }
        details = "，".join(
            f"{amount} 条{labels[category]}"
            for category, amount in counts.items()
            if amount
        )
        return f"✅ 已收到你的 {count} 条消息：{details}。", counts, False


def _as_utc(value: datetime) -> datetime:
    """将数据库返回的时间统一解释为 UTC，以兼容 SQLite 测试存储。"""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _receipt_is_deferred(payload: dict[str, object] | None, now: datetime) -> bool:
    """判断接收提示的持久化合并窗口是否尚未到期。

    参数：payload 为通知记录载荷；now 为当前时间。
    返回值：截止时间有效且晚于当前时间时返回 True。
    异常：无；损坏或缺失的截止字段按不延迟处理。
    副作用：无。
    """
    until = payload.get("coalesce_until") if isinstance(payload, dict) else None
    if not isinstance(until, str):
        return False
    try:
        deadline = datetime.fromisoformat(until)
    except ValueError:
        return False
    return _as_utc(deadline) > _as_utc(now)


def _build_supported_body(
    payload: dict[str, object] | None, content: str | None
) -> dict[str, object]:
    """构造企业微信 AI Bot 主动发送支持的消息体。

    参数：payload 为通知持久化的白名单消息体；content 为旧通知的纯文本内容。
    返回值：保留模板卡片等已支持消息，或返回 Markdown 文本消息体。
    异常：非法 payload 不会透传，降级为安全的固定文本消息。
    副作用：无，不修改数据库中的原始通知载荷。
    """
    if isinstance(payload, dict):
        msgtype = payload.get("msgtype")
        if msgtype == "template_card":
            return payload
        if msgtype == "markdown":
            markdown = payload.get("markdown")
            if isinstance(markdown, dict) and isinstance(markdown.get("content"), str):
                return payload
        if msgtype == "text":
            text = payload.get("text")
            if isinstance(text, dict) and isinstance(text.get("content"), str):
                return {"msgtype": "markdown", "markdown": {"content": text["content"]}}
    return {"msgtype": "markdown", "markdown": {"content": content or "系统通知"}}


def _provider_error_code(error: BaseException) -> str | None:
    """从 SDK 异常中提取纯数字错误码，避免把回执原文写入日志。

    参数：error 为企业微信 SDK 抛出的异常。
    返回值：匹配到的数字错误码；无法安全提取时返回 None。
    异常：无；正则处理失败时按无错误码处理。
    副作用：无，不读取或记录异常消息之外的敏感内容。
    """
    match = _SDK_ERROR_CODE.search(str(error))
    return match.group(1) if match else None
