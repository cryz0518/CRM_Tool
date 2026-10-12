"""验证 CRM 重试迁移在已有同步记录上的前向与回退行为。"""

from __future__ import annotations

import runpy
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text


def test_crm_retry_migration_backfills_only_retryable_creates() -> None:
    """历史 transient create 被排程；其它记录不变，downgrade 移除新增结构。"""
    migration_path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "0037_crm_submission_retry_schedule.py"
    )
    migration = runpy.run_path(str(migration_path))
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE crm_sync_records ("
                    "id INTEGER PRIMARY KEY, status VARCHAR(32), "
                    "operation VARCHAR(32), failure_category VARCHAR(32))"
                )
            )
            connection.execute(
                text("INSERT INTO crm_sync_records VALUES (:id, :status, :operation, :category)"),
                [
                    {
                        "id": 1,
                        "status": "retrying",
                        "operation": "create",
                        "category": "transient",
                    },
                    {
                        "id": 2,
                        "status": "retrying",
                        "operation": "update",
                        "category": "transient",
                    },
                    {
                        "id": 3,
                        "status": "failed_pending_review",
                        "operation": "create",
                        "category": "permanent",
                    },
                ],
            )
            with Operations.context(MigrationContext.configure(connection)):
                migration["upgrade"]()
            rows = connection.execute(
                text("SELECT id, next_retry_at FROM crm_sync_records ORDER BY id")
            ).all()
            assert rows[0].next_retry_at is not None
            assert rows[1].next_retry_at is None and rows[2].next_retry_at is None
            assert any(
                index["name"] == "ix_crm_sync_records_next_retry_at"
                and index["column_names"] == ["next_retry_at"]
                for index in inspect(connection).get_indexes("crm_sync_records")
            )
            with Operations.context(MigrationContext.configure(connection)):
                migration["downgrade"]()
            assert "next_retry_at" not in {
                column["name"] for column in inspect(connection).get_columns("crm_sync_records")
            }
    finally:
        engine.dispose()
