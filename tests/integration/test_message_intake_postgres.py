"""可靠消息接收的 PostgreSQL 并发集成测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
from app.messaging.models import Base, SalesAuthorization
from app.messaging.service import IncomingMessageCommand, MessageIntakeResult, MessageIntakeService


@pytest.fixture
def postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    """创建仅用于单个测试的 PostgreSQL schema 与会话工厂。

    参数：无。
    返回值：绑定临时 schema 的 SQLAlchemy 会话工厂。
    异常：Docker PostgreSQL 不可达时跳过测试，其他 DDL 错误向上抛出。
    副作用：测试前创建、测试后级联删除临时 schema，不触碰业务 public schema。
    """
    engine = create_engine(get_settings().database_url)
    schema_name = f"t02_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要 Docker Compose PostgreSQL 执行并发集成测试")

    # 每个测试独享 schema，保证并发锁和唯一约束来自真实 PostgreSQL 而非 SQLite 模拟。
    with engine.begin() as connection:
        connection.execute(CreateSchema(schema_name))
    schema_engine = engine.execution_options(schema_translate_map={None: schema_name})
    Base.metadata.create_all(schema_engine)
    try:
        yield sessionmaker(schema_engine)
    finally:
        schema_engine.dispose()
        # CASCADE 仅删除随机测试 schema 内对象，防止测试残留影响下一次 Docker 验证。
        with engine.begin() as connection:
            connection.execute(DropSchema(schema_name, cascade=True))
        engine.dispose()


def authorize_salesperson(session_factory: sessionmaker[Session], user_id: str) -> None:
    """在临时 PostgreSQL schema 中登记一名可录入销售。

    参数：session_factory 为测试会话工厂，user_id 为企业微信销售标识。
    返回值：无。
    异常：数据库写入错误向上抛出。
    副作用：创建授权目录记录。
    """
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id=user_id, is_authorized=True, is_active=True))


def test_notification_record_schema_includes_retry_columns() -> None:
    """验证已升级的 T02 数据库具备通知重试元数据列。

    参数：无。
    返回值：无。
    异常：数据库不可达时由 SQLAlchemy 抛出，断言失败由 pytest 报告。
    副作用：只读取当前数据库的 information_schema，不修改业务表。
    """
    engine = create_engine(get_settings().database_url)
    try:
        with engine.connect() as connection:
            # 以当前数据库 schema 为准，避免将临时测试 schema 或其他表误判为生产目标。
            rows = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() "
                    "AND table_name = 'notification_records'"
                )
            )
            column_names = {row[0] for row in rows}
    finally:
        engine.dispose()

    assert {"attempts", "provider_message_id", "sent_at"} <= column_names


def test_business_audit_event_schema_exists() -> None:
    """验证已升级的 T02 数据库具备可靠接收所需的审计事件表。

    参数：无。
    返回值：无。
    异常：数据库不可达时由 SQLAlchemy 抛出，断言失败由 pytest 报告。
    副作用：只读取当前数据库的 metadata，不修改业务表。
    """
    engine = create_engine(get_settings().database_url)
    try:
        # 审计事件与原始消息、Outbox 位于同一事务，缺表会使真实入站消息整体回滚。
        inspector = inspect(engine)
        assert "business_audit_events" in inspector.get_table_names()
        column_names = {
            column["name"] for column in inspector.get_columns("business_audit_events")
        }
    finally:
        engine.dispose()

    assert {"id", "message_id", "sales_user_id", "event_type", "created_at"} <= column_names


def receive_at_the_same_time(
    service: MessageIntakeService, command: IncomingMessageCommand
) -> list[MessageIntakeResult]:
    """使用两个独立线程并发调用同一消息接收服务。

    参数：service 为共享的无状态接收服务，command 为两次相同输入。
    返回值：两个线程各自返回的接收结果。
    异常：任一线程数据库错误会由 Future.result 向上抛出。
    副作用：同时开启两个独立数据库事务，验证真实行锁和唯一约束。
    """
    barrier = Barrier(2)

    def receive() -> MessageIntakeResult:
        """等待另一线程就绪后提交同一条消息。"""
        barrier.wait()
        return service.receive(command)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(receive) for _ in range(2)]
        return [future.result() for future in futures]


def test_concurrent_authorized_duplicate_message_returns_one_duplicate(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证同一销售并发重投相同消息时，授权行锁产生一条接收和一个幂等结果。"""
    authorize_salesperson(postgres_session_factory, "sales-1")
    results = receive_at_the_same_time(
        MessageIntakeService(postgres_session_factory),
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="sales-1",
            raw_payload={"text": "客户需要码垛机器人"},
        ),
    )

    assert sorted(result.duplicate for result in results) == [False, True]
    assert all(result.accepted for result in results)


def test_concurrent_unauthorized_message_returns_one_duplicate_notice(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """验证无授权目录成员的并发重复消息只保留一个待发送权限通知。"""
    results = receive_at_the_same_time(
        MessageIntakeService(postgres_session_factory),
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="visitor-1",
            raw_payload={"text": "测试消息"},
        ),
    )

    assert sorted(result.duplicate for result in results) == [False, True]
    assert not any(result.accepted for result in results)
