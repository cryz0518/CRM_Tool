"""验证 PostgreSQL Outbox 认领与活跃 Worker 锁的串行边界。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization, utc_now
from workers.tasks import (
    _acquire_sales_processing_lock,
    _release_sales_processing_lock,
    claim_dispatchable_lead_outbox_events,
)


@pytest.fixture
def postgres_outbox_factory() -> Generator[tuple[Engine, sessionmaker[Session]], None, None]:
    """创建独立 PostgreSQL schema，测试结束后删除且不触碰业务表或固定卷。"""
    engine = create_engine(get_settings().database_url)
    schema_name = f"outbox_serial_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要隔离 PostgreSQL 执行 Outbox 并发测试")
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    schema_engine = engine.execution_options(schema_translate_map={None: schema_name})
    Base.metadata.create_all(schema_engine)
    try:
        yield schema_engine, sessionmaker(schema_engine)
    finally:
        schema_engine.dispose()
        with engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        engine.dispose()


def _persist_message_event(
    session_factory: sessionmaker[Session],
    *,
    sales_user_id: str,
    sequence: int,
    status: str = "pending",
    processing_started_at: datetime | None = None,
) -> int:
    """写入最小销售、消息和 Outbox 事实供 PostgreSQL 并发测试使用。

    参数：销售身份、持久化顺序、可选 Outbox 状态及处理起始时间。
    返回值：新建 Outbox 事件标识。
    异常：数据库约束或连接失败时由 SQLAlchemy 抛出。
    副作用：写入测试 schema 中的授权、消息和 Outbox 行。
    """
    message_id = f"{sales_user_id}-{sequence}-{uuid4().hex}"
    with session_factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id=sales_user_id,
                    is_authorized=True,
                    is_active=True,
                )
            )
            session.flush()
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={"kind": "integration-test"},
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


def test_live_worker_lock_blocks_expired_lease_recovery(
    postgres_outbox_factory: tuple[Engine, sessionmaker[Session]],
) -> None:
    """验证活跃 Worker 持锁期间，过期租约不会被另一个 Worker 接管。"""
    engine, session_factory = postgres_outbox_factory
    event_id = _persist_message_event(
        session_factory,
        sales_user_id="sales-a",
        sequence=1,
        status="processing",
        processing_started_at=utc_now() - timedelta(hours=1),
    )
    _persist_message_event(session_factory, sales_user_id="sales-a", sequence=2)

    with engine.connect() as worker_connection:
        sales_user_id = worker_connection.scalar(
            select(OutboxEvent.sales_user_id).where(OutboxEvent.id == event_id)
        )
        worker_connection.commit()
        assert sales_user_id == "sales-a"
        _acquire_sales_processing_lock(worker_connection, sales_user_id)

        claims = claim_dispatchable_lead_outbox_events(
            session_factory, lease_timeout=timedelta(minutes=5)
        )
        assert claims == []
        with session_factory() as session:
            event = session.get(OutboxEvent, event_id)
            assert event is not None
            assert event.status == "processing"
            assert event.processing_started_at < utc_now() - timedelta(minutes=5)

        assert _release_sales_processing_lock(worker_connection, sales_user_id)

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )
    assert [(claim.event_id, claim.recover_expired_lease) for claim in claims] == [
        (event_id, True)
    ]


def test_next_message_waits_for_worker_exit_but_other_sales_can_progress(
    postgres_outbox_factory: tuple[Engine, sessionmaker[Session]],
) -> None:
    """验证同销售下一条消息等待整条 Worker 退出，其他销售仍可并行认领。"""
    engine, session_factory = postgres_outbox_factory
    first_event_id = _persist_message_event(
        session_factory,
        sales_user_id="sales-a",
        sequence=1,
        status="processing",
        processing_started_at=utc_now(),
    )
    second_event_id = _persist_message_event(
        session_factory, sales_user_id="sales-a", sequence=2
    )
    other_sales_event_id = _persist_message_event(
        session_factory, sales_user_id="sales-b", sequence=1
    )

    initial_claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )
    assert [claim.event_id for claim in initial_claims] == [other_sales_event_id]

    with engine.connect() as worker_connection:
        _acquire_sales_processing_lock(worker_connection, "sales-a")
        with engine.connect() as other_worker_connection:
            _acquire_sales_processing_lock(other_worker_connection, "sales-b")
            assert _release_sales_processing_lock(other_worker_connection, "sales-b")

        with session_factory.begin() as session:
            first_event = session.get(OutboxEvent, first_event_id)
            assert first_event is not None
            first_event.status = "succeeded"
            first_event.processing_started_at = None

        # 远端工作已结束但 Worker 仍处于消息收尾区时，调度器仍不得放行下一条。
        assert claim_dispatchable_lead_outbox_events(
            session_factory, lease_timeout=timedelta(minutes=5)
        ) == []
        assert _release_sales_processing_lock(worker_connection, "sales-a")

    claims = claim_dispatchable_lead_outbox_events(
        session_factory, lease_timeout=timedelta(minutes=5)
    )
    assert [claim.event_id for claim in claims] == [second_event_id]
