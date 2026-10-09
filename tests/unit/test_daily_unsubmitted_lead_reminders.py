"""每日未提交线索提醒的时间、资格、幂等和发送重试测试。"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.leads.models import CrmSyncRecord, Lead
from app.leads.reminders import DailyUnsubmittedLeadReminderService
from app.messaging.models import (
    Base,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.notifications.outbound import WecomOutboundNotificationSender
from workers.celery_app import build_beat_schedule

_SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def session_factory(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    """创建隔离 SQLite 会话工厂，避免接触项目 PostgreSQL 容器。

    参数：tmp_path 为 pytest 创建的临时目录。
    返回值：测试使用的 SQLAlchemy 会话工厂。
    异常：SQLite 建表或连接失败时由 SQLAlchemy 抛出。
    副作用：仅在临时目录创建数据库文件，fixture 结束时关闭连接池。
    """
    engine = create_engine(
        f"sqlite+pysqlite:///{tmp_path / 'daily-reminders.sqlite'}",
        connect_args={"timeout": 15},
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(engine)
    engine.dispose()


def seed_sales(
    session_factory: sessionmaker[Session],
    user_id: str,
    *,
    authorized: bool = True,
    active: bool = True,
    administrator: bool = False,
    crm_user_id: str | None = "crm-user",
) -> None:
    """登记每日提醒资格测试使用的 Actor Registry。

    参数：session_factory 为隔离数据库会话工厂；user_id 为成员标识；其余参数控制身份状态。
    返回值：无。
    异常：数据库约束或写入失败时由 SQLAlchemy 抛出。
    副作用：只写入当前测试数据库。
    """
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id=user_id,
                is_authorized=authorized,
                is_active=active,
                is_administrator=administrator,
                crm_user_id=crm_user_id,
            )
        )


def seed_lead(
    session_factory: sessionmaker[Session],
    lead_id: str,
    sales_user_id: str,
    created_at: datetime,
    *,
    lifecycle_state: str = "pending_create",
    crm_sync_status: str | None = None,
    message_received: bool = False,
) -> None:
    """创建线索及可选 CRM 或消息处理状态。

    参数：session_factory 为隔离数据库会话工厂；lead_id、sales_user_id、created_at 定位线索；
    lifecycle_state、crm_sync_status 和 message_received 控制模拟业务状态。
    返回值：无。
    异常：数据库约束或写入失败时由 SQLAlchemy 抛出。
    副作用：只在当前测试数据库新增线索和显式指定的模拟状态。
    """
    with session_factory.begin() as session:
        if message_received:
            # 模拟消息接收已完成，但这不构成 CRM 提交成功。
            session.add(
                IncomingMessage(
                    message_id=f"message-{lead_id}",
                    sales_user_id=sales_user_id,
                    sequence=1,
                    raw_payload={"msgtype": "text"},
                    received_at=created_at,
                )
            )
            session.add(
                OutboxEvent(
                    message_id=f"message-{lead_id}",
                    sales_user_id=sales_user_id,
                    sequence=1,
                    status="succeeded",
                )
            )
        session.add(
            Lead(
                id=lead_id,
                source_message_id=f"message-{lead_id}" if message_received else None,
                original_capturing_sales_user_id=sales_user_id,
                smart_table_owner_user_id=sales_user_id,
                smart_table_record_id=f"record-{lead_id}",
                lifecycle_state=lifecycle_state,
                field_values={},
                created_at=created_at,
            )
        )
        if crm_sync_status is not None:
            # 明确写入 CRM 创建状态，只有 succeeded 才能排除提醒。
            session.add(
                CrmSyncRecord(
                    lead_id=lead_id,
                    operation="create",
                    generation=1,
                    smart_table_record_id=f"record-{lead_id}",
                    idempotency_key=f"idempotency-{lead_id}",
                    canonical_payload={},
                    snapshot_hash="0" * 64,
                    request_message_id=f"submit-{lead_id}",
                    submitting_sales_user_id=sales_user_id,
                    status=crm_sync_status,
                )
            )


def notification_count(session_factory: sessionmaker[Session]) -> int:
    """返回当前测试库中已登记的每日提醒总数。

    参数：session_factory 为隔离数据库会话工厂。
    返回值：每日提醒 Outbox 记录数量。
    异常：数据库读取失败时由 SQLAlchemy 抛出。
    副作用：仅读取测试数据库。
    """
    with session_factory() as session:
        return int(session.scalar(select(func.count(NotificationRecord.notification_key))) or 0)


def test_beat_runs_daily_reminder_at_2000_shanghai() -> None:
    """验证 Beat 每天在上海时间 20:00 执行提醒任务。

    参数：无。
    返回值：断言任务名、小时和分钟均符合要求。
    异常：配置与预期不符时由 pytest 断言失败。
    副作用：只构造内存 Beat 配置。
    """
    entry = build_beat_schedule(Settings())["schedule-daily-unsubmitted-lead-reminders"]
    schedule = entry["schedule"]

    assert entry["task"] == "workers.schedule_daily_unsubmitted_lead_reminders"
    assert getattr(schedule, "hour") == {20}
    assert getattr(schedule, "minute") == {0}


def test_1959_waits_2000_queues_once_and_ignores_message_receipt(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 19:59 不排程、20:00 排程，重复扫描不产生重复通知。

    参数：session_factory 为隔离 SQLite 会话工厂。
    返回值：断言排程数量、消息接收状态和 Outbox 唯一性。
    异常：业务或断言错误时由 pytest 报告。
    副作用：只在临时数据库登记测试授权、线索和提醒。
    """
    service = DailyUnsubmittedLeadReminderService(session_factory)
    seed_sales(session_factory, "sales-a")
    seed_lead(
        session_factory,
        "pending-lead",
        "sales-a",
        datetime(2026, 10, 9, 10, 0, tzinfo=UTC),
        lifecycle_state="temporary",
        crm_sync_status="processing",
        message_received=True,
    )

    assert service.schedule_due_reminders(now=datetime(2026, 10, 9, 11, 59, tzinfo=UTC)) == 0
    assert service.schedule_due_reminders(now=datetime(2026, 10, 9, 12, 0, tzinfo=UTC)) == 1
    assert service.schedule_due_reminders(now=datetime(2026, 10, 9, 12, 5, tzinfo=UTC)) == 0
    assert notification_count(session_factory) == 1


def test_only_successful_crm_create_suppresses_reminder(
    session_factory: sessionmaker[Session],
) -> None:
    """验证仅成功 CRM create 排除提醒，处理中记录和草稿仍按未提交处理。

    参数：session_factory 为隔离 SQLite 会话工厂。
    返回值：断言只为待提交线索生成提醒并正确统计数量。
    异常：业务或断言错误时由 pytest 报告。
    副作用：只在临时数据库登记测试授权、线索和 CRM 同步状态。
    """
    seed_sales(session_factory, "sales-a")
    seed_lead(
        session_factory,
        "submitted-lead",
        "sales-a",
        datetime(2026, 10, 9, 10, 0, tzinfo=UTC),
        crm_sync_status="succeeded",
    )
    seed_lead(
        session_factory,
        "processing-lead",
        "sales-a",
        datetime(2026, 10, 9, 10, 5, tzinfo=UTC),
        lifecycle_state="temporary",
        crm_sync_status="processing",
    )

    assert (
        DailyUnsubmittedLeadReminderService(session_factory).schedule_due_reminders(
            now=datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
        )
        == 1
    )
    with session_factory() as session:
        notice = session.scalar(select(NotificationRecord))
    assert notice is not None and "1 条线索" in (notice.content or "")


def test_shanghai_midnight_and_next_day_are_independent(
    session_factory: sessionmaker[Session],
) -> None:
    """验证上海午夜的 UTC 半开区间和跨日后的重新资格计算。

    参数：session_factory 为隔离 SQLite 会话工厂。
    返回值：断言业务日边界、逐日提醒键和跨日无旧线索时不提醒。
    异常：业务或断言错误时由 pytest 报告。
    副作用：只在临时数据库登记测试授权、线索和提醒。
    """
    seed_sales(session_factory, "sales-a")
    seed_lead(
        session_factory,
        "midnight-lead",
        "sales-a",
        datetime(2026, 10, 8, 16, 0, tzinfo=UTC),
    )
    service = DailyUnsubmittedLeadReminderService(session_factory)
    assert service.schedule_due_reminders(now=datetime(2026, 10, 9, 12, 0, tzinfo=UTC)) == 1
    # 次日已没有当天线索，不重复使用前一业务日的资格。
    assert service.schedule_due_reminders(now=datetime(2026, 10, 10, 12, 0, tzinfo=UTC)) == 0

    seed_lead(
        session_factory,
        "next-day-lead",
        "sales-a",
        datetime(2026, 10, 9, 16, 0, tzinfo=UTC),
    )
    assert service.schedule_due_reminders(now=datetime(2026, 10, 10, 12, 0, tzinfo=UTC)) == 1
    assert notification_count(session_factory) == 2


def test_ineligible_and_zero_lead_users_are_not_reminded(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未授权、停用、管理员、未映射及零线索账号不会收到提醒。

    参数：session_factory 为隔离 SQLite 会话工厂。
    返回值：断言不符合资格的账号没有对应通知。
    异常：业务或断言错误时由 pytest 报告。
    副作用：只在临时数据库登记不同资格状态的成员和线索。
    """
    for user_id, options in (
        ("unauthorized", {"authorized": False}),
        ("inactive", {"active": False}),
        ("admin", {"administrator": True}),
        ("unmapped", {"crm_user_id": None}),
        ("no-lead", {}),
    ):
        seed_sales(session_factory, user_id, **options)
    for user_id in ("unauthorized", "inactive", "admin", "unmapped"):
        seed_lead(
            session_factory,
            f"lead-{user_id}",
            user_id,
            datetime(2026, 10, 9, 10, 0, tzinfo=UTC),
        )

    assert (
        DailyUnsubmittedLeadReminderService(session_factory).schedule_due_reminders(
            now=datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
        )
        == 0
    )
    assert notification_count(session_factory) == 0


def test_parallel_schedules_create_one_daily_notification(
    session_factory: sessionmaker[Session],
) -> None:
    """验证两个并发扫描通过数据库唯一键只登记一条提醒。

    参数：session_factory 为文件型隔离 SQLite 会话工厂。
    返回值：断言并发调用合计只新增一条 Outbox 记录。
    异常：数据库并发或断言错误时由 pytest 报告。
    副作用：只在临时数据库并发写入同一业务日提醒。
    """
    seed_sales(session_factory, "sales-a")
    seed_lead(
        session_factory,
        "parallel-lead",
        "sales-a",
        datetime(2026, 10, 9, 10, 0, tzinfo=UTC),
    )
    now = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    service = DailyUnsubmittedLeadReminderService(session_factory)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.schedule_due_reminders(now=now), range(2)))

    assert sum(results) == 1
    assert notification_count(session_factory) == 1


class RecordingClient:
    """用内存记录消息，按配置模拟发送成功或网络失败。

    参数：无。
    返回值：无。
    异常：无。
    副作用：保存假客户端收到的消息，不访问企业微信。
    """

    def __init__(self, fail: bool = False) -> None:
        """初始化消息记录和失败开关。

        参数：fail 为 True 时每次发送均模拟连接失败。
        返回值：无。
        异常：无。
        副作用：创建空的内存调用列表。
        """
        self.fail = fail
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def send_message(
        self, userid_or_chatid: str, body: dict[str, object]
    ) -> dict[str, str]:
        """记录模拟发送；失败模式抛出可重试连接错误。

        参数：userid_or_chatid 为模拟接收人；body 为消息体。
        返回值：模拟成功时返回假消息标识。
        异常：fail 为 True 时抛出 ConnectionError。
        副作用：追加一条内存调用记录，不访问企业微信。
        """
        self.calls.append((userid_or_chatid, body))
        if self.fail:
            raise ConnectionError("fake notification failure")
        return {"msgid": f"fake-{len(self.calls)}"}


def test_failed_send_retries_only_notification_outbox(
    session_factory: sessionmaker[Session],
) -> None:
    """验证发送失败沿用 Outbox 重试且业务线索状态保持不变。

    参数：session_factory 为隔离 SQLite 会话工厂。
    返回值：断言失败后重试成功、通知尝试次数及线索/CRM 状态均正确。
    异常：业务或断言错误时由 pytest 报告。
    副作用：通过假客户端发送消息并只修改临时 Outbox 状态。
    """
    now = datetime.now(UTC)
    business_date = now.astimezone(_SHANGHAI).date()
    seed_sales(session_factory, "sales-a")
    seed_lead(session_factory, "retry-lead", "sales-a", now, crm_sync_status="processing")
    with session_factory.begin() as session:
        session.add(
            NotificationRecord(
                notification_key="a" * 64,
                sales_user_id="sales-a",
                source_message_id="a" * 64,
                notification_type="daily_unsubmitted_lead_reminder",
                content="提醒",
                payload={
                    "msgtype": "markdown",
                    "markdown": {"content": "提醒"},
                    "business_date": business_date.isoformat(),
                },
            )
        )

    first_client = RecordingClient(fail=True)
    assert asyncio.run(
        WecomOutboundNotificationSender(session_factory, first_client).send_pending_once()
    ) == 0
    second_client = RecordingClient()
    assert asyncio.run(
        WecomOutboundNotificationSender(session_factory, second_client).send_pending_once()
    ) == 1

    with session_factory() as session:
        notice = session.get(NotificationRecord, "a" * 64)
        lead = session.get(Lead, "retry-lead")
        sync = session.scalar(select(CrmSyncRecord).where(CrmSyncRecord.lead_id == "retry-lead"))
    assert notice is not None and notice.status == "succeeded" and notice.attempts == 2
    assert lead is not None and lead.lifecycle_state == "pending_create"
    assert sync is not None and sync.status == "processing"
    assert first_client.calls[0][0] == second_client.calls[0][0] == "sales-a"
