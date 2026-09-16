"""T15 migration 链升级、head 和 current 验证。"""

from __future__ import annotations

import os
import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def test_ticket15_migration_chain_reaches_single_current_head() -> None:
    """在显式 PostgreSQL 管理连接上验证 0019 成为唯一 head。"""
    admin_url_text = os.environ.get("T15_MIGRATION_ADMIN_URL")
    if not admin_url_text:
        pytest.skip("设置 T15_MIGRATION_ADMIN_URL 后执行真实空库 migration 验证")

    database_name = f"t15_migration_{uuid4().hex}"
    admin_engine = create_engine(admin_url_text, isolation_level="AUTOCOMMIT")
    target_url = make_url(admin_url_text).set(database=database_name)
    command_environment = {
        **os.environ,
        "DATABASE_URL": target_url.render_as_string(hide_password=False),
    }
    try:
        with admin_engine.connect() as connection:
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
        assert heads.stdout.count("0019_ticket15_console_observability") == 1
    finally:
        with admin_engine.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()
