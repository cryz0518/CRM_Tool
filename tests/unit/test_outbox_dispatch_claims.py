"""Outbox 调度认领语义的单元测试。"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization, utc_now
from workers.tasks import _take_lead_outbox_claim, claim_dispatchable_lead_outbox_events


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    """创建隔离的 Outbox 认领测试数据库。

    参数：无。
    返回：绑定 SQLite 内存数据库的会话工厂。
    异常：无。
    副作用：测试结束后释放临时数据库资源。
    """
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        yield sessionmaker(engine)
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def persist_event(
    session_factory: sessionmaker[Session],
    *,
    sales_user_id: str,
    sequence: int,
    status: str = "pending",
    processing_started_at: object | None = None,
) -> int:
    """持久化一条最小销售消息及其 Outbox 事件。

    参数：sales_user_id 为销售身份；sequence 为同销售顺序；status 与处理开始时间描述任务状态。
    返回：已保存的 Outbox 事件标识。
    异常：数据库约束失败时由 SQLAlchemy 抛出。
    副作用：写入授权目录、来源消息和 Outbox 事件。
    """
    message_id = f"message-{sales_user_id}-{sequence}"
    with session_factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id=sales_user_id,
                    is_authorized=True,
                    is_active=True,
                )
            )
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={"kind": "test"},
                normalized_text="客户信息",
            )
        )
        event = OutboxEvent(
            message_id=message_id,
            sales_user_id=sales_user_id,
            sequence=sequence,
            status=status,
            processing_started_at=processing_started_at,
        )
        session.add(event)
        session.flush()
        return event.id


def test_unexpired_processing_event_is_not_dispatched_again(
    session_factory: sessionmaker[Session],
) -> None:
    """验证仍在租约内的 processing 事件不会被调度器重复认领。

    参数：session_factory 为测试数据库会话工厂。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建一条 processing Outbox 事件。
    """
    persist_event(
        session_factory,
        sales_user_id="sales-1",
        sequence=1,
        status="processing",
        processing_started_at=utc_now(),
    )

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert claims == []


def test_expired_processing_event_is_claimed_for_lease_recovery(
    session_factory: sessionmaker[Session],
) -> None:
    """验证过期 processing 事件仍会被认领并标记为租约恢复。

    参数：session_factory 为测试数据库会话工厂。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建一条过期 processing Outbox 事件。
    """
    event_id = persist_event(
        session_factory,
        sales_user_id="sales-1",
        sequence=1,
        status="processing",
        processing_started_at=utc_now() - timedelta(minutes=6),
    )

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert [(claim.event_id, claim.recover_expired_lease) for claim in claims] == [
        (event_id, True)
    ]


def test_second_scan_cannot_claim_an_already_claimed_pending_event(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已被首轮调度认领的 pending 事件不会被后续扫描重复投递。

    参数：session_factory 为测试数据库会话工厂。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建并依次扫描一条 pending Outbox 事件。
    """
    persist_event(session_factory, sales_user_id="sales-1", sequence=1)

    first_claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )
    second_claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert len(first_claims) == 1
    assert second_claims == []


def test_retrying_event_is_claimed_and_only_one_worker_can_take_it(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 retrying 事件仍可消费且重复 Worker 只有一个能进入外部调用阶段。

    参数：session_factory 为测试数据库会话工厂。
    返回值：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建并认领一条 retrying Outbox 事件。
    """
    persist_event(session_factory, sales_user_id="sales-1", sequence=1, status="retrying")

    [claim] = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert _take_lead_outbox_claim(
        session_factory, claim.event_id, claim.claimed_at.isoformat()
    )
    assert not _take_lead_outbox_claim(
        session_factory, claim.event_id, claim.claimed_at.isoformat()
    )


def test_same_sales_claims_only_the_earliest_unfinished_sequence(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一销售只认领最早未完成 sequence，保持消息串行。

    参数：session_factory 为测试数据库会话工厂。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建同销售的两条 pending Outbox 事件。
    """
    first_event_id = persist_event(session_factory, sales_user_id="sales-1", sequence=1)
    persist_event(session_factory, sales_user_id="sales-1", sequence=2)

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert [claim.event_id for claim in claims] == [first_event_id]


def test_different_sales_can_be_claimed_in_parallel(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不同销售各自最早事件可以在同一轮调度中并行认领。

    参数：session_factory 为测试数据库会话工厂。
    返回：无。
    异常：断言失败时由 pytest 报告。
    副作用：创建两名销售各自的一条 pending Outbox 事件。
    """
    first_event_id = persist_event(session_factory, sales_user_id="sales-1", sequence=1)
    second_event_id = persist_event(session_factory, sales_user_id="sales-2", sequence=1)

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )

    assert {claim.event_id for claim in claims} == {first_event_id, second_event_id}
