"""T18 真实 PostgreSQL action claim、double-click、replay 和 lease 测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Event, Lock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.messaging.models import (
    Base,
    SalesAuthorization,
    WecomAction,
    WecomActionOutbox,
    WecomActionStatus,
    utc_now,
)
from app.wecom_bot.actions import (
    CARD_EVENT_KEY_DISCARD_CONFIRM,
    WecomActionService,
)


@pytest.fixture
def postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    """创建随机 PostgreSQL schema，保证 T18 锁测试不污染默认数据库。"""

    engine = create_engine(get_settings().database_url)
    schema_name = f"t18_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要 Docker Compose PostgreSQL 执行 T18 并发测试")
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    schema_engine = engine.execution_options(schema_translate_map={None: schema_name})
    Base.metadata.create_all(schema_engine)
    try:
        yield sessionmaker(schema_engine)
    finally:
        schema_engine.dispose()
        with engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        engine.dispose()


def _seed_action(session_factory: sessionmaker[Session]) -> WecomAction:
    """创建一条待点击的销售动作。"""

    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-a", is_authorized=True, is_active=True))
    action = WecomActionService(session_factory, card_callback_ready=True).issue_action(
        actor_user_id="sales-a",
        action_type="lead_discard_confirmation",
        target_type="lead",
        target_id="lead-1",
        expected_action_key=CARD_EVENT_KEY_DISCARD_CONFIRM,
        context={"reason": "并发测试"},
        title="确认废弃",
        description="请确认",
    )
    return action


def _callback_frame(action: WecomAction, msgid: str) -> dict[str, object]:
    """构造严格契约 callback，除 transport msgid 外不携带业务目标。"""

    return {
        "cmd": "aibot_event_callback",
        "headers": {"req_id": f"req-{msgid}"},
        "body": {
            "msgtype": "event",
            "msgid": msgid,
            "from": {"userid": "sales-a"},
            "event": {
                "eventtype": "template_card_event",
                "template_card_event": {
                    "event_key": CARD_EVENT_KEY_DISCARD_CONFIRM,
                    "task_id": action.task_id,
                },
            },
        },
    }


def test_postgres_concurrent_double_click_claims_one_business_action(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 行锁保证不同 provider msgid 也只有一个 action outbox。"""

    action = _seed_action(postgres_session_factory)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    barrier = Barrier(2)

    def claim(msgid: str) -> str:
        """在独立 SQLAlchemy session 中并发处理一次 callback。"""

        barrier.wait(timeout=5)
        return service.claim_callback(_callback_frame(action, msgid)).code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future.result(timeout=10)
            for future in (
                executor.submit(claim, "provider-pg-1"),
                executor.submit(claim, "provider-pg-2"),
            )
        ]

    assert sorted(results) == ["action_processing", "claimed"]
    with postgres_session_factory() as session:
        stored = session.get(WecomAction, action.id)
        outboxes = session.scalars(select(WecomActionOutbox)).all()
        assert stored is not None and stored.status == WecomActionStatus.PROCESSING.value
        assert len(outboxes) == 1


def test_postgres_expired_processing_lease_does_not_duplicate_domain_execution(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 下过期 lease 可恢复，但并发 Worker 仍只有一次 domain 调用。"""

    action = _seed_action(postgres_session_factory)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    assert service.claim_callback(_callback_frame(action, "provider-pg-lease")).code == "claimed"
    with postgres_session_factory.begin() as session:
        outbox = session.scalar(
            select(WecomActionOutbox).where(WecomActionOutbox.action_id == action.id)
        )
        assert outbox is not None
        outbox.status = "processing"
        outbox.processing_lease_expires_at = utc_now() - timedelta(seconds=1)

    entered = Event()
    release = Event()
    lock = Lock()
    calls = 0

    def executor(_snapshot: object) -> tuple[str, str]:
        """阻塞首个 domain 调用，确保第二个 Worker 观察到新 lease。"""

        nonlocal calls
        with lock:
            calls += 1
        entered.set()
        assert release.wait(timeout=5)
        return "discarded", "已处理"

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.execute_action, action.id, executor)
        assert entered.wait(timeout=5)
        second = pool.submit(service.execute_action, action.id, executor)
        second_result = second.result(timeout=5)
        release.set()
        first_result = first.result(timeout=5)

    assert first_result.executed is True
    assert second_result.code == "already_processing"
    assert calls == 1
