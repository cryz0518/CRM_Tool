"""T12 销售提交出站通知测试。"""

from __future__ import annotations

import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import Base, NotificationRecord
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
