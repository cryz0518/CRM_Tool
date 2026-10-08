"""需求进度调度器的隔离 PostgreSQL 行锁与唯一通知集成测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import get_settings
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
    """在配置的隔离 PostgreSQL 中创建随机 schema，不访问任何 Compose 卷。"""
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    schema_name = f"progress_{uuid4().hex}"
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        engine.dispose()
        pytest.skip("需要显式配置隔离 PostgreSQL DATABASE_URL 执行并发测试")
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
