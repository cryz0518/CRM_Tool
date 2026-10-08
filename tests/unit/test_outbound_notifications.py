"""T12 销售提交出站通知测试。"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import Base, NotificationRecord, utc_now
from app.notifications.outbound import WecomOutboundNotificationSender


class FakeClient:
    """记录 Bot 进程使用的主动消息调用。"""

    def __init__(self) -> None:
        """初始化空调用历史。"""
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def send_message(self, userid_or_chatid: str, body: dict[str, object]) -> dict[str, str]:
        """记录单聊目标和正文，模拟已认证 WSClient。"""
        self.calls.append((userid_or_chatid, body))
        return {"msgid": "reply-1"}


class FailingClient(FakeClient):
    """模拟一次主动推送失败，验证通知重试不会触发 CRM 路径。"""

    async def send_message(self, userid_or_chatid: str, body: dict[str, object]) -> dict[str, str]:
        """记录调用后抛出传输错误，不改变任何业务同步记录。"""
        self.calls.append((userid_or_chatid, body))
        raise ConnectionError("temporary")


class InvalidCardClient(FakeClient):
    """模拟企业微信拒绝不合法模板卡片的非重试错误。"""

    async def send_message(self, userid_or_chatid: str, body: dict[str, object]) -> dict[str, str]:
        """抛出仅含非敏感错误码的 SDK 异常。"""
        self.calls.append((userid_or_chatid, body))
        raise RuntimeError("Reply ack error: errcode=42035")


def test_bot_sender_uses_submitting_sales_userid_and_marks_notice_sent() -> None:
    """验证现有 Bot 客户端消费通知时以销售 userid 主动推送。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="n-1",
                sales_user_id="sales-1",
                source_message_id="message-1",
                notification_type="crm_submission_summary",
                content="CRM 提交结果：创建成功 1 条。",
            )
        )
    client = FakeClient()

    asyncio.run(WecomOutboundNotificationSender(factory, client).send_pending_once())

    assert client.calls == [
        (
            "sales-1",
            {"msgtype": "markdown", "markdown": {"content": "CRM 提交结果：创建成功 1 条。"}},
        )
    ]
    with factory() as session:
        notice = session.get(NotificationRecord, "n-1")
        assert notice is not None and notice.status == "succeeded"


def test_first_smart_table_notice_sends_clickable_markdown_once_across_bot_restart() -> None:
    """验证首次录入通知以 Markdown 链接单聊发送，成功状态阻止重启后重发。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    content = (
        "🎉 你的第一条客户需求已成功录入企业微信智能表格！\n\n"
        "点击下方链接，即可查看和完善客户信息：\n\n"
        "[📋 打开需求登记智能表格](https://example.test/smart-table?view=leads)\n\n"
        "后续可继续发送客户需求，我会自动录入并定期汇报处理进度。"
    )
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="first-success-sales-1",
                sales_user_id="sales-1",
                source_message_id="message-1",
                notification_type="lead_first_smart_table_success",
                content=content,
            )
        )
    client = FakeClient()

    first_sender = WecomOutboundNotificationSender(factory, client)
    assert asyncio.run(first_sender.send_pending_once()) == 1
    restarted_sender = WecomOutboundNotificationSender(factory, client)
    assert asyncio.run(restarted_sender.send_pending_once()) == 0

    assert client.calls == [
        (
            "sales-1",
            {"msgtype": "markdown", "markdown": {"content": content}},
        )
    ]
    with factory() as session:
        notice = session.get(NotificationRecord, "first-success-sales-1")
    assert notice is not None and notice.status == "succeeded"


def test_receipt_sender_waits_for_coalesce_window_then_sends_merged_count() -> None:
    """验证接收通知在持久化窗口内暂缓发送，窗口结束后使用最新累计消息数。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="receipt-sales-1",
                sales_user_id="sales-1",
                source_message_id="message-1",
                notification_type="lead_intake_receipt",
                content="✅ 已收到你的 2 条消息，正在识别并录入。",
                payload={
                    "receipt_count": 2,
                    "coalesce_until": (utc_now() + timedelta(seconds=30)).isoformat(),
                },
            )
        )
    client = FakeClient()
    sender = WecomOutboundNotificationSender(factory, client)

    assert asyncio.run(sender.send_pending_once()) == 0
    assert client.calls == []
    with factory.begin() as session:
        notice = session.get(NotificationRecord, "receipt-sales-1")
        assert notice is not None and notice.payload is not None
        expired_payload = dict(notice.payload)
        expired_payload["coalesce_until"] = (utc_now() - timedelta(seconds=1)).isoformat()
        notice.payload = expired_payload

    assert asyncio.run(sender.send_pending_once()) == 1
    assert client.calls == [
        (
            "sales-1",
            {
                "msgtype": "markdown",
                "markdown": {"content": "✅ 已收到你的 2 条消息，正在识别并录入。"},
            },
        )
    ]


def test_bot_sender_delivers_lead_processing_failure_notice() -> None:
    """验证安全的线索解析失败通知由现有出站器主动发送。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    content = "这条线索消息未能完成解析，已进入待人工处理，请稍后重试或补充信息。"
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="lead-failure-notice",
                sales_user_id="sales-failure",
                source_message_id="source-failure",
                notification_type="lead_processing_failed",
                content=content,
            )
        )
    client = FakeClient()

    asyncio.run(WecomOutboundNotificationSender(factory, client).send_pending_once())

    assert client.calls[0][0] == "sales-failure"
    assert client.calls[0][1]["markdown"] == {"content": content}
    with factory() as session:
        notice = session.get(NotificationRecord, "lead-failure-notice")
        assert notice is not None and notice.status == "succeeded"


def test_notification_failure_is_retryable_without_duplicate_business_work() -> None:
    """验证发送异常只将通知置为 retrying，下一次可由同一 Bot 继续消费。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="n-2",
                sales_user_id="sales-2",
                source_message_id="message-2",
                notification_type="crm_submission_summary",
                content="CRM 提交结果：创建成功 1 条。",
            )
        )

    asyncio.run(WecomOutboundNotificationSender(factory, FailingClient()).send_pending_once())

    with factory() as session:
        notice = session.get(NotificationRecord, "n-2")
        assert notice is not None
        assert notice.status == "retrying"
        assert notice.attempts == 1


def test_invalid_template_card_error_is_terminal_without_infinite_retry() -> None:
    """验证企业微信卡片协议错误不会被当作可重试网络故障。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="invalid-card",
                sales_user_id="sales-card",
                source_message_id="message-card",
                notification_type="wecom_action_card",
                content="确认",
                payload={
                    "msgtype": "template_card",
                    "template_card": {"card_type": "button_interaction"},
                },
            )
        )

    asyncio.run(WecomOutboundNotificationSender(factory, InvalidCardClient()).send_pending_once())

    with factory() as session:
        notice = session.get(NotificationRecord, "invalid-card")
        assert notice is not None
        assert notice.status == "failed"
        assert notice.attempts == 1


def test_expired_processing_notification_is_reclaimed() -> None:
    """验证 Bot 崩溃遗留的过期通知可重新发送。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="n-3",
                sales_user_id="sales-3",
                source_message_id="message-3",
                notification_type="crm_submission_summary",
                status="processing",
                processing_lease_expires_at=utc_now() - timedelta(minutes=1),
                content="完成",
            )
        )
    sender = WecomOutboundNotificationSender(factory, FakeClient())
    assert asyncio.run(sender.send_pending_once()) == 1


def test_stale_sender_completion_cannot_overwrite_takeover() -> None:
    """验证通知 sender 租约被接管后，旧 sender 的晚到成功不能覆盖新 owner。"""

    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="n-stale",
                sales_user_id="sales-stale",
                source_message_id="message-stale",
                notification_type="crm_submission_summary",
                content="完成",
            )
        )

    class TakeoverClient(FakeClient):
        """在旧 sender 外部发送期间模拟新 sender 接管数据库租约。"""

        async def send_message(
            self, userid_or_chatid: str, body: dict[str, object]
        ) -> dict[str, str]:
            """记录发送后把同一通知标记为新 claimant。"""

            self.calls.append((userid_or_chatid, body))
            with factory.begin() as session:
                current = session.get(NotificationRecord, "n-stale")
                assert current is not None
                current.processing_claim_token = "new-sender-token"
            return {"status": "ok"}

    assert asyncio.run(
        WecomOutboundNotificationSender(factory, TakeoverClient()).send_pending_once()
    ) == 1
    with factory() as session:
        notice = session.get(NotificationRecord, "n-stale")
        assert notice is not None
        assert notice.status == "processing"
        assert notice.processing_claim_token == "new-sender-token"


def test_action_card_payload_is_sent_without_creating_business_retry() -> None:
    """验证 T18 template card 通知走主动 send_message，重试只影响通知状态。"""

    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="card-notice",
                sales_user_id="sales-card",
                source_message_id="action-1",
                notification_type="wecom_action_card",
                content="请确认",
                payload={
                    "msgtype": "template_card",
                    "template_card": {"task_id": "t18_1", "card_type": "text_notice"},
                },
            )
        )
    client = FakeClient()

    assert asyncio.run(WecomOutboundNotificationSender(factory, client).send_pending_once()) == 1
    assert client.calls == [
        (
            "sales-card",
            {
                "msgtype": "template_card",
                "template_card": {"task_id": "t18_1", "card_type": "text_notice"},
            },
        )
    ]


def test_company_submission_preview_notification_is_sent() -> None:
    """验证按公司定位生成的预览回复会进入 Bot 主动发送白名单。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="company-preview",
                sales_user_id="sales-preview",
                source_message_id="message-preview",
                notification_type="crm_submission_preview",
                content="已定位，请确认。",
            )
        )
    client = FakeClient()

    assert asyncio.run(WecomOutboundNotificationSender(factory, client).send_pending_once()) == 1
    assert client.calls == [
        (
            "sales-preview",
            {"msgtype": "markdown", "markdown": {"content": "已定位，请确认。"}},
        )
    ]


def test_legacy_text_payload_is_converted_to_supported_markdown() -> None:
    """验证历史 text 通知在主动发送边界转换为 AI Bot 支持的 Markdown。"""
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)
    factory = sessionmaker(engine)
    Base.metadata.create_all(engine)
    with factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="legacy-text",
                sales_user_id="sales-legacy",
                source_message_id="message-legacy",
                notification_type="crm_submission_summary",
                content="旧内容",
                payload={"msgtype": "text", "text": {"content": "历史通知"}},
            )
        )
    client = FakeClient()

    assert asyncio.run(WecomOutboundNotificationSender(factory, client).send_pending_once()) == 1
    assert client.calls == [
        (
            "sales-legacy",
            {"msgtype": "markdown", "markdown": {"content": "历史通知"}},
        )
    ]
