"""由既有企业微信 Bot 长连接主动发送可靠通知。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.messaging.models import NotificationRecord, utc_now


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
                    NotificationRecord.notification_type == "crm_submission_summary",
                    or_(
                        NotificationRecord.status.in_(("pending", "retrying")),
                        and_(
                            NotificationRecord.status == "processing",
                            NotificationRecord.processing_lease_expires_at <= utc_now(),
                        ),
                    ),
                )
            ).all()
        sent = 0
        for notice in notices:
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
                # 先原子认领，避免多个 Bot 循环重复发送同一通知。
                current.status = "processing"
                current.processing_started_at = utc_now()
                current.processing_lease_expires_at = current.processing_started_at + timedelta(
                    minutes=5
                )
            try:
                await self._client.send_message(
                    notice.sales_user_id,
                    {"msgtype": "text", "text": {"content": notice.content or "系统通知"}},
                )
            except Exception:
                with self._session_factory.begin() as session:
                    current = session.get(NotificationRecord, notice.notification_key)
                    if current is not None:
                        current.status = "retrying"
                        current.processing_started_at = None
                        current.processing_lease_expires_at = None
                        current.attempts += 1
                continue
            with self._session_factory.begin() as session:
                current = session.get(NotificationRecord, notice.notification_key)
                if current is not None:
                    current.status = "succeeded"
                    current.processing_started_at = None
                    current.processing_lease_expires_at = None
                    current.attempts += 1
                    current.sent_at = utc_now()
            sent += 1
        return sent


def _as_utc(value: datetime) -> datetime:
    """将数据库返回的时间统一解释为 UTC，以兼容 SQLite 测试存储。"""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
