"""T18 真实 PostgreSQL action claim、double-click、replay 和 lease 测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Event, Lock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.leads.models import Lead
from app.messaging.models import (
    Base,
    NotificationRecord,
    SalesAuthorization,
    WecomAction,
    WecomActionOutbox,
    WecomActionStatus,
    WecomCallbackDelivery,
    utc_now,
)
from app.wecom_bot.actions import (
    ACTION_TYPE_CRM_FIELD_CONFIRMATION,
    ACTION_TYPE_REASSIGN_CONFIRMATION,
    CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    CARD_EVENT_KEY_DISCARD_CONFIRM,
    CARD_EVENT_KEY_REASSIGN_CONFIRM,
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


def _seed_action(
    session_factory: sessionmaker[Session],
    action_type: str = "lead_discard_confirmation",
) -> WecomAction:
    """创建一条待点击的销售动作及字段确认所需的负责人事实。"""

    with session_factory.begin() as session:
        if session.get(SalesAuthorization, "sales-a") is None:
            session.add(
                SalesAuthorization(wecom_user_id="sales-a", is_authorized=True, is_active=True)
            )
            session.flush()
        if action_type == ACTION_TYPE_CRM_FIELD_CONFIRMATION:
            session.add(
                Lead(
                    id="lead-1",
                    original_capturing_sales_user_id="sales-a",
                    smart_table_owner_user_id="sales-a",
                    lifecycle_state="pending_create",
                    field_values={"业务线": "协作机器人"},
                )
            )
    event_key = {
        "lead_discard_confirmation": CARD_EVENT_KEY_DISCARD_CONFIRM,
        ACTION_TYPE_REASSIGN_CONFIRMATION: CARD_EVENT_KEY_REASSIGN_CONFIRM,
        ACTION_TYPE_CRM_FIELD_CONFIRMATION: CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    }[action_type]
    context: dict[str, object] = {"reason": "并发测试"}
    if action_type == ACTION_TYPE_REASSIGN_CONFIRMATION:
        context.update({"message_id": "message-1", "segment_index": 0})
    if action_type == ACTION_TYPE_CRM_FIELD_CONFIRMATION:
        context.update(
            {
                "field_names": ["业务线"],
                "command_text": "提交今天的线索",
                "request_message_id": "request-1",
            }
        )
    action = WecomActionService(session_factory, card_callback_ready=True).issue_action(
        actor_user_id="sales-a",
        action_type=action_type,
        target_type="lead",
        target_id="lead-1",
        expected_action_key=event_key,
        context=context,
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
                    "card_type": "button_interaction",
                    "event_key": action.expected_action_key,
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


def test_postgres_same_provider_msgid_race_persists_one_delivery(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL 唯一键吸收同 msgid 并发 delivery race。"""

    action = _seed_action(postgres_session_factory)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    barrier = Barrier(2)

    def claim() -> str:
        """在独立会话中并发提交同一个 transport delivery。"""

        barrier.wait(timeout=5)
        return service.claim_callback(_callback_frame(action, "provider-pg-same")).code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [executor.submit(claim), executor.submit(claim)]
        codes = sorted(future.result(timeout=10) for future in results)

    assert codes == ["claimed", "duplicate_delivery"]
    with postgres_session_factory() as session:
        assert len(session.scalars(select(WecomCallbackDelivery)).all()) == 1
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_postgres_different_msgid_loser_finishes_claim_check_before_winner(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """不同 msgid 的 loser 在 winner 领域调用阻塞期间即可退出，不等待业务完成。"""

    action = _seed_action(postgres_session_factory)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    assert service.claim_callback(_callback_frame(action, "provider-pg-winner")).code == "claimed"
    entered = Event()
    release = Event()
    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """登记 operation 后阻塞 winner 的领域完成。"""

        nonlocal calls
        service.begin_domain_operation(snapshot.id, snapshot.claim_token)
        calls += 1
        entered.set()
        assert release.wait(timeout=5)
        return "completed", "已处理"

    with ThreadPoolExecutor(max_workers=2) as pool:
        winner = pool.submit(service.execute_action, action.id, executor)
        assert entered.wait(timeout=5)
        loser = pool.submit(service.execute_action, action.id, executor)
        loser_result = loser.result(timeout=2)
        release.set()
        winner_result = winner.result(timeout=5)

    assert loser_result.code == "already_processing"
    assert winner_result.executed is True
    assert calls == 1


def test_postgres_stale_finalize_cannot_overwrite_takeover_terminal_state(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证旧 token 的成功或失败晚到结果都不能覆盖 takeover 的 succeeded。"""

    action = _seed_action(postgres_session_factory)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    assert service.claim_callback(_callback_frame(action, "provider-pg-finalize")).code == "claimed"
    with postgres_session_factory.begin() as session:
        outbox = session.scalar(
            select(WecomActionOutbox)
            .where(WecomActionOutbox.action_id == action.id)
            .with_for_update()
        )
        assert outbox is not None
        outbox.claim_token = "takeover-token"
        outbox.status = "processing"
        stored = session.get(WecomAction, action.id)
        assert stored is not None
        stored.status = WecomActionStatus.SUCCEEDED.value
        stored.result_code = "winner"
        stored.result_summary = "takeover 已完成"
        outbox.status = "succeeded"

    stale_success = service._finish_action(
        action.id,
        claim_token="old-token",
        status=WecomActionStatus.SUCCEEDED.value,
        outbox_status="succeeded",
        result_code="old-success",
        result_summary="旧 Worker 成功",
    )
    stale_failure = service._finish_action(
        action.id,
        claim_token="old-token",
        status=WecomActionStatus.PENDING_RECOVERY.value,
        outbox_status="failed",
        result_code="old-failure",
        result_summary="旧 Worker 失败",
    )

    assert stale_success.code == "stale_claim"
    assert stale_failure.code == "stale_claim"
    with postgres_session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None
        assert stored.status == WecomActionStatus.SUCCEEDED.value
        assert stored.result_code == "winner"


def test_postgres_concurrent_action_issuance_has_one_action_and_card(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL issuance unique key 收敛并发发行到一张卡。"""

    with postgres_session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-a", is_authorized=True, is_active=True))
    barrier = Barrier(2)

    def issue() -> WecomAction:
        """并发发行相同 server-side business request。"""

        barrier.wait(timeout=5)
        return WecomActionService(
            postgres_session_factory, card_callback_ready=True
        ).issue_discard_action(
            actor_user_id="sales-a",
            lead_id="lead-issued-once",
            reason="相同请求",
            source_message_id="request-issued-once",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [executor.submit(issue), executor.submit(issue)]
        actions = [future.result(timeout=10) for future in results]

    assert actions[0].id == actions[1].id
    with postgres_session_factory() as session:
        assert len(session.scalars(select(WecomAction)).all()) == 1
        assert len(session.scalars(select(NotificationRecord)).all()) == 1


def test_postgres_t18_status_checks_reject_arbitrary_strings(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """真实 PostgreSQL CHECK 约束拒绝 action、outbox 和 callback 未注册状态。"""

    with postgres_session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-a", is_authorized=True, is_active=True))
    with pytest.raises(IntegrityError):
        with postgres_session_factory.begin() as session:
            session.execute(
                text(
                    "INSERT INTO wecom_actions "
                    "(id, task_id, action_type, bound_actor_wecom_user_id, target_type, target_id, "
                    "expected_action_key, status, expires_at, context, created_at, updated_at) "
                    "VALUES ('invalid-action', 'invalid-task', 'lead_discard_confirmation', "
                    "'sales-a', 'lead', 'lead-1', 'lead.discard.confirm', 'bogus', "
                    "CURRENT_TIMESTAMP, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                )
            )

    action = _seed_action(postgres_session_factory)
    with pytest.raises(IntegrityError):
        with postgres_session_factory.begin() as session:
            session.execute(
                text(
                    "INSERT INTO wecom_action_outbox "
                    "(action_id, status, attempts, created_at) "
                    "VALUES (:action_id, 'bogus', 0, CURRENT_TIMESTAMP)"
                ),
                {"action_id": action.id},
            )
    with pytest.raises(IntegrityError):
        with postgres_session_factory.begin() as session:
            session.execute(
                text(
                    "INSERT INTO wecom_callback_deliveries "
                    "(provider_msgid, req_id, actor_user_id, event_key, task_id, "
                    "processing_status, received_at) VALUES "
                    "('invalid-provider', 'req-1', 'sales-a', 'lead.discard.confirm', "
                    "'invalid-task', 'bogus', CURRENT_TIMESTAMP)"
                )
            )


@pytest.mark.parametrize(
    "action_type",
    (
        "lead_discard_confirmation",
        ACTION_TYPE_REASSIGN_CONFIRMATION,
        ACTION_TYPE_CRM_FIELD_CONFIRMATION,
    ),
)
def test_postgres_takeover_fencing_blocks_stale_worker_before_domain_call(
    postgres_session_factory: sessionmaker[Session],
    action_type: str,
) -> None:
    """真实 PostgreSQL takeover 后旧 Worker 不能取得 operation 或覆盖终态。"""

    action = _seed_action(postgres_session_factory, action_type)
    service = WecomActionService(postgres_session_factory, card_callback_ready=True)
    assert (
        service.claim_callback(_callback_frame(action, f"provider-pg-{action_type}")).code
        == "claimed"
    )

    entered = Event()
    release_old_worker = Event()
    takeover_completed = Event()
    lock = Lock()
    calls = 0
    first_token: str | None = None

    def executor(snapshot: object) -> tuple[str, str]:
        """让旧 Worker 在 begin operation 前停住，再由 takeover Worker 获胜。"""

        nonlocal calls, first_token
        assert hasattr(snapshot, "claim_token")
        token = snapshot.claim_token
        if first_token is None:
            first_token = token
            entered.set()
            assert release_old_worker.wait(timeout=5)
            service.begin_domain_operation(snapshot.id, token)
        else:
            service.begin_domain_operation(snapshot.id, token)
            with lock:
                calls += 1
            takeover_completed.set()
        return "completed", "已处理"

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.execute_action, action.id, executor)
        assert entered.wait(timeout=5)
        with postgres_session_factory.begin() as session:
            outbox = session.scalar(
                select(WecomActionOutbox)
                .where(WecomActionOutbox.action_id == action.id)
                .with_for_update()
            )
            assert outbox is not None
            outbox.processing_lease_expires_at = utc_now() - timedelta(seconds=1)
        second = pool.submit(service.execute_action, action.id, executor)
        assert takeover_completed.wait(timeout=5)
        second_result = second.result(timeout=5)
        release_old_worker.set()
        first_result = first.result(timeout=5)

    assert first_result.code == "stale_claim"
    assert second_result.executed is True
    assert calls == 1
    with postgres_session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == WecomActionStatus.SUCCEEDED.value
