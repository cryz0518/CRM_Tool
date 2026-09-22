"""T22 0023 在真实 PostgreSQL 上的 schema 与 downgrade 防丢失验证。"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session, sessionmaker

from alembic import command
from app.core.config import get_settings
from app.messaging.models import IncomingMessage, SalesAuthorization


@contextmanager
def _preserve_pytest_logging() -> Iterator[None]:
    """隔离 Alembic fileConfig 对 pytest 全局 logging handler 的副作用。

    参数：无。
    返回值：上下文管理器，退出时恢复 root 和已存在命名 logger 的状态。
    异常：上下文中的迁移异常原样向上传递。
    副作用：仅保存并恢复进程内 logging 配置，不改变业务日志实现。
    """
    root_logger = logging.getLogger()
    root_handlers = list(root_logger.handlers)
    root_level = root_logger.level
    manager_disable = logging.Logger.manager.disable
    logger_states = {
        name: (
            logger,
            list(logger.handlers),
            logger.level,
            logger.disabled,
            logger.propagate,
        )
        for name, logger in logging.Logger.manager.loggerDict.items()
        if isinstance(logger, logging.Logger)
    }
    try:
        yield
    finally:
        # Alembic fileConfig 可能清空 pytest capture handler；先移除并关闭迁移新建的 handler。
        for handler in list(root_logger.handlers):
            if handler not in root_handlers:
                root_logger.removeHandler(handler)
                handler.close()
        for handler in root_handlers:
            if handler not in root_logger.handlers:
                root_logger.addHandler(handler)
        root_logger.setLevel(root_level)
        for logger, handlers, level, disabled, propagate in logger_states.values():
            for handler in list(logger.handlers):
                if handler not in handlers:
                    logger.removeHandler(handler)
                    handler.close()
            for handler in handlers:
                if handler not in logger.handlers:
                    logger.addHandler(handler)
            logger.setLevel(level)
            logger.disabled = disabled
            logger.propagate = propagate
        logging.disable(manager_disable)


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
        with _preserve_pytest_logging():
            command.downgrade(_alembic_config(), "0022_ticket18_wecom_actions")
