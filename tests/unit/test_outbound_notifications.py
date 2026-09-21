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
        ("sales-1", {"msgtype": "text", "text": {"content": "CRM 提交结果：创建成功 1 条。"}})
    ]
    with factory() as session:
        notice = session.get(NotificationRecord, "n-1")
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
