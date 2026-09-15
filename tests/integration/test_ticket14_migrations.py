"""T14 migration chain 的真实 PostgreSQL 建库与升级验证。"""

from __future__ import annotations

import os
import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def test_ticket14_migration_chain_upgrades_fresh_database() -> None:
    """在显式提供的独立 PostgreSQL 管理连接上，从空数据库执行完整 Alembic 链。"""
    admin_url_text = os.environ.get("T14_MIGRATION_ADMIN_URL")
    if not admin_url_text:
        pytest.skip("设置 T14_MIGRATION_ADMIN_URL 后执行真实空库 migration 验证")

    database_name = f"t14_migration_{uuid4().hex}"
    admin_engine = create_engine(admin_url_text, isolation_level="AUTOCOMMIT")
    target_url = make_url(admin_url_text).set(database=database_name)
    command_environment = {
        **os.environ,
        "DATABASE_URL": target_url.render_as_string(hide_password=False),
    }
    try:
        with admin_engine.connect() as connection:
            # 数据库名由 UUID 生成，不含用户可控 SQL；仅在测试专用 PostgreSQL 中创建临时库。
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))

        for arguments in (("upgrade", "head"), ("heads",), ("current",)):
            completed = subprocess.run(
                ["alembic", *arguments],
                check=False,
                capture_output=True,
                text=True,
                env=command_environment,
            )
            assert completed.returncode == 0, completed.stdout + completed.stderr
        heads = subprocess.run(
            ["alembic", "heads"],
            check=False,
            capture_output=True,
            text=True,
            env=command_environment,
        )
        assert "0018_message_retry_lease_segment" in heads.stdout
    finally:
        # 只删除本测试刚创建的临时数据库，不触碰 Compose volume 或其他数据库。
        with admin_engine.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()
