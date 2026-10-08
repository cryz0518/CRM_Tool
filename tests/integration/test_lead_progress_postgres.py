"""需求进度调度器的隔离 PostgreSQL 行锁与唯一通知集成测试。"""

from __future__ import annotations

import os
import re
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.leads.models import (
    Lead,
    LeadMessageResolution,
    LeadProgressSession,
    SmartTableSync,
)
from app.leads.progress import LeadProgressService
from app.messaging.models import (
    Base,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.models import SmartTableRecord


def _validated_test_database_url(
    environment: dict[str, str] | None = None,
) -> tuple[str, str, str]:
    """解析并验证本轮随机的一次性测试 DSN。

    参数：environment 为显式测试环境映射；省略时只读取测试 DSN、身份及匹配的 DATABASE_URL。
    返回值：通过验证的 TEST_DATABASE_URL、32 位运行标识和随机数据库用户名。
    异常：缺少变量、URL 不合法或身份与 T15 隔离约定不符时抛出 ValueError。
    副作用：无，不建立连接或执行 SQL。
    """
    values = os.environ if environment is None else environment
    raw_url = values.get("TEST_DATABASE_URL", "")
    run_id = values.get("TEST_DATABASE_ID", "")
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ValueError("TEST_DATABASE_ID 必须是本次一次性测试运行的随机标识")
    try:
        from sqlalchemy.engine import make_url

        url = make_url(raw_url)
    except Exception as error:
        raise ValueError("必须显式提供有效的 TEST_DATABASE_URL") from error
    # 若应用连接变量同时存在，必须指向完全相同的测试库，避免迁移误用 DATABASE_URL。
    database_url = values.get("DATABASE_URL")
    database_user = values.get("TEST_DATABASE_USER", url.username or "")
    allowed_users = {"t15_migration", f"t15_{run_id}"}
    if (
        url.drivername != "postgresql+psycopg"
        or url.host != "postgres"
        or url.port != 5432
        or database_user not in allowed_users
        or url.username != database_user
        or url.password != f"t15_{run_id}"
        or url.database != f"crm_lead_test_{run_id}"
        or url.query
        or (database_url is not None and database_url != raw_url)
    ):
        raise ValueError("TEST_DATABASE_URL 未指向本次随机的一次性 T15 PostgreSQL")
    return raw_url, run_id, database_user


class ConcurrentSnapshotAdapter:
    """为并发排程提供无外部连接的只读成功记录。"""

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """返回指定记录的完整测试快照。"""
        return SmartTableRecord(
            record_id,
            {
                "业务线": "协作机器人",
                "线索名称": "测试公司",
                "线索来源": "展会",
                "联系人": "测试联系人",
                "职务": "采购经理",
                "沟通方式": "微信",
                "手机": "13800000000",
                "备注": "测试需求",
            },
        )


@pytest.fixture
def postgres_session_factory() -> Generator[sessionmaker[Session], None, None]:
    """仅在一次性 tmpfs 数据库中建独占 schema，并在清理前重验数据库身份。"""
    try:
        test_database_url, run_id, expected_database_user = _validated_test_database_url()
    except ValueError as error:
        pytest.fail(str(error))
    engine = create_engine(test_database_url, pool_pre_ping=True)
    schema_name = f"progress_test_{run_id}"
    try:
        with engine.connect() as connection:
            database_name, actual_database_user, data_directory = connection.execute(
                text(
                    "SELECT current_database(), current_user, current_setting('data_directory')"
                )
            ).one()
    except Exception as error:
        engine.dispose()
        pytest.fail(f"无法只读验证 PostgreSQL 测试实例身份：{type(error).__name__}")
    if (
        database_name != f"crm_lead_test_{run_id}"
        or actual_database_user != expected_database_user
        or not str(data_directory).startswith("/var/lib/postgresql/data/")
    ):
        engine.dispose()
        pytest.fail("PostgreSQL 实例身份或 tmpfs PGDATA 路径未通过安全校验")
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS t15_progress_test_run_guard "
                    "(run_id varchar(32) PRIMARY KEY)"
                )
            )
            claimed_id = connection.execute(
                text(
                    "INSERT INTO t15_progress_test_run_guard (run_id) VALUES (:run_id) "
                    "ON CONFLICT DO NOTHING RETURNING run_id"
                ),
                {"run_id": run_id},
            ).scalar_one_or_none()
        if claimed_id != run_id:
            engine.dispose()
            pytest.fail("该 TEST_DATABASE_ID 已使用过，拒绝重复执行数据库集成测试")
    except Exception:
        engine.dispose()
        raise
    try:
        # 同一个 TEST_DATABASE_ID 第二次运行会因 schema 已存在而失败，不会覆盖或清理旧对象。
        with engine.begin() as connection:
            connection.execute(CreateSchema(schema_name))
    except Exception:
        engine.dispose()
        raise
    schema_engine = engine.execution_options(schema_translate_map={None: schema_name})
    try:
        Base.metadata.create_all(schema_engine)
        yield sessionmaker(schema_engine)
    finally:
        schema_engine.dispose()
        try:
            with engine.begin() as connection:
                database_name, actual_database_user, data_directory = connection.execute(
                    text(
                        "SELECT current_database(), current_user, current_setting('data_directory')"
                    )
                ).one()
                if (
                    database_name != f"crm_lead_test_{run_id}"
                    or actual_database_user != expected_database_user
                    or not str(data_directory).startswith("/var/lib/postgresql/data/")
                ):
                    raise RuntimeError("拒绝清理：PostgreSQL 实例身份与创建时不一致")
                connection.execute(DropSchema(schema_name, cascade=True))
        finally:
            engine.dispose()


def test_test_database_guard_rejects_default_and_reused_database_names() -> None:
    """验证 PostgreSQL fixture 不回退到 DATABASE_URL 且限定随机一次性库名。"""
    with pytest.raises(ValueError):
        _validated_test_database_url({"DATABASE_URL": "postgresql+psycopg://bad"})
    with pytest.raises(ValueError):
        _validated_test_database_url(
            {
                "TEST_DATABASE_ID": "0" * 32,
                "TEST_DATABASE_URL": "postgresql+psycopg://t15_migration:test@postgres/crm_lead",
            }
        )
    run_id = "1" * 32
    test_url = (
        f"postgresql+psycopg://t15_migration:t15_{run_id}@postgres:5432/"
        f"crm_lead_test_{run_id}"
    )
    url, identity, database_user = _validated_test_database_url(
        {
            "TEST_DATABASE_ID": run_id,
            "TEST_DATABASE_USER": "t15_migration",
            "TEST_DATABASE_URL": test_url,
            "DATABASE_URL": test_url,
        }
    )
    assert identity == run_id
    assert database_user == "t15_migration"
    assert url.endswith(f"crm_lead_test_{run_id}")
    with pytest.raises(ValueError):
        _validated_test_database_url(
            {
                "TEST_DATABASE_ID": run_id,
                "TEST_DATABASE_USER": "t15_migration",
                "TEST_DATABASE_URL": test_url,
                "DATABASE_URL": "postgresql+psycopg://unexpected",
            }
        )


def test_concurrent_schedulers_create_one_logical_notification(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """两个并发 Scheduler 竞争同一 due_at 时只持久化一条逻辑通知。"""
    started_at = datetime(2026, 10, 8, 1, 0, tzinfo=UTC)
    with postgres_session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-concurrent",
                is_authorized=True,
                is_active=True,
            )
        )
        session.flush()
        session.add(
            IncomingMessage(
                message_id="concurrent-message",
                sales_user_id="sales-concurrent",
                sequence=1,
                raw_payload={"text": "需求"},
                normalized_text="需求",
                received_at=started_at,
            )
        )
        session.flush()
        session.add(
            OutboxEvent(
                message_id="concurrent-message",
                sales_user_id="sales-concurrent",
                sequence=1,
                status="succeeded",
            )
        )
        session.flush()
        session.add(
            Lead(
                id="concurrent-lead",
                source_message_id="concurrent-message",
                source_segment_index=0,
                original_capturing_sales_user_id="sales-concurrent",
                smart_table_owner_user_id="sales-concurrent",
                smart_table_record_id="concurrent-record",
                field_values={"线索名称": "测试公司"},
            )
        )
        session.flush()
        session.add(
            SmartTableSync(
                lead_id="concurrent-lead",
                source_message_id="concurrent-message",
                source_segment_index=0,
                smart_table_record_id="concurrent-record",
                status="succeeded",
            )
        )
        session.add(
            LeadMessageResolution(
                message_id="concurrent-message",
                segment_index=0,
                lead_id="concurrent-lead",
                status="assigned",
            )
        )

    adapter = ConcurrentSnapshotAdapter()
    service = LeadProgressService(postgres_session_factory, adapter)
    assert service.record_resolved_message("concurrent-message", now=started_at)
    due_at = started_at + timedelta(minutes=15)
    barrier = Barrier(2)

    def schedule() -> int:
        """同步启动一次并发排程调用。"""
        barrier.wait()
        return LeadProgressService(postgres_session_factory, adapter).schedule_due_reports(
            now=due_at
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        created = list(pool.map(lambda _: schedule(), range(2)))

    with postgres_session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_progress_summary"
            )
        ).all()
        progress = session.scalar(select(LeadProgressSession))
    assert sum(created) == 1
    assert len(notices) == 1
    assert progress is not None
    assert progress.next_report_at == due_at + timedelta(minutes=15)
