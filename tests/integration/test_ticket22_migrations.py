"""T22 0023 在真实 PostgreSQL 上的 schema 与 downgrade 防丢失验证。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session, sessionmaker

from alembic import command
from app.core.config import get_settings
from app.messaging.models import IncomingMessage, SalesAuthorization


@pytest.fixture(scope="module")
def migration_session_factory() -> sessionmaker[Session]:
    """连接独立 Compose PostgreSQL，验证迁移不以 SQLite 替代。"""
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.connect() as connection:
        connection.execute(select(1))
    factory = sessionmaker(engine)
    try:
        yield factory
    finally:
        engine.dispose()


def _alembic_config() -> Config:
    """构造指向当前真实 PostgreSQL 的 Alembic 配置。"""
    config = Config(str(Path("alembic.ini")))
    config.set_main_option("sqlalchemy.url", get_settings().database_url)
    return config


def test_ticket22_schema_and_downgrade_guard(
    migration_session_factory: sessionmaker[Session],
) -> None:
    """验证 0023 新列存在，且 retention scrub fact 会拒绝回退到 0022。"""
    engine = migration_session_factory.kw["bind"]
    columns = {column["name"] for column in inspect(engine).get_columns("incoming_messages")}
    assert {"scrubbed_at", "retention_policy_version"} <= columns
    attachment_columns = {
        column["name"] for column in inspect(engine).get_columns("message_attachments")
    }
    assert {"retention_expires_at", "ingest_operation_id"} <= attachment_columns

    with migration_session_factory.begin() as session:
        sales_user_id = "t22-migration-sales"
        message_id = "t22-migration-scrub-fact"
        session.merge(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        session.merge(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=900001,
                raw_payload={"_retention": "scrubbed"},
                scrubbed_at=datetime.now(UTC),
                retention_policy_version="migration-test",
            )
        )

    with pytest.raises(RuntimeError, match="不可逆事实|禁止回退"):
        command.downgrade(_alembic_config(), "0022_ticket18_wecom_actions")
