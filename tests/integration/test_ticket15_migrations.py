"""历史 migration 链升级、head 和 current 验证。"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from uuid import uuid4

import pytest


def _run_compose(
    project_name: str,
    environment: dict[str, str],
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """在独立 Compose 项目中执行一条 Docker Compose 命令。

    参数：project_name 为本测试专用项目名；environment 为固定测试凭据和运行配置；
    arguments 为 Compose 参数。
    返回值：包含退出码、标准输出和标准错误的命令结果。
    异常：宿主找不到 docker 命令时由测试入口转换为失败。
    副作用：可能创建或操作本测试专用的容器、网络和 Volume。
    """
    return subprocess.run(
        # 显式排除本地开发 override，避免迁移专项复用默认开发数据库卷。
        ["docker", "compose", "-p", project_name, "-f", "docker-compose.yml", *arguments],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
    )


def _assert_compose_success(
    completed: subprocess.CompletedProcess[str], arguments: tuple[str, ...]
) -> None:
    """断言 Docker Compose 命令成功，并在失败时保留可排查的命令摘要。

    参数：completed 为 Compose 命令结果；arguments 为不含凭据的命令参数。
    返回值：无。
    异常：命令非零退出时抛出 pytest 断言失败。
    副作用：无。
    """
    assert completed.returncode == 0, (
        f"docker compose {' '.join(arguments)} failed\n"
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )


def test_current_migration_chain_reaches_single_head() -> None:
    """在独立临时 PostgreSQL 容器中执行真实 Alembic upgrade、heads 和 current。"""
    if shutil.which("docker") is None:
        pytest.fail("T15 migration 验证需要可用的 docker 命令，禁止静默跳过真实验证")

    project_name = f"t17-migration-{uuid4().hex[:12]}"
    database_user = "t17_migration"
    database_name = "crm_lead"
    database_password = "t17_migration_password"
    environment = {
        **os.environ,
        # Compose 项目名和数据库 Volume 均为本测试独立生成，不触碰默认 crm-lead 环境。
        "POSTGRES_DB": database_name,
        "POSTGRES_USER": database_user,
        "POSTGRES_PASSWORD": database_password,
        "APP_ENV": "test",
        "SMART_TABLE_ADAPTER": "mock",
        "CONSOLE_DEV_ADMIN_TOKEN": "t17-migration-test-token",
    }
    compose_started = False
    try:
        # 先构建当前代码镜像，确保容器内执行的 Alembic 与被测提交一致。
        # Compose 的 BuildKit raw 输出可能填满 subprocess pipe；wrapper 只需验证构建成功。
        build = _run_compose(project_name, environment, "build", "--quiet", "migrate")
        _assert_compose_success(build, ("build", "--quiet", "migrate"))

        # 只启动本测试专用 PostgreSQL 服务；不复用损坏的默认 Compose Volume。
        start = _run_compose(project_name, environment, "up", "-d", "postgres")
        _assert_compose_success(start, ("up", "-d", "postgres"))
        compose_started = True

        # 等待临时数据库接受连接，再由 migrate 容器执行迁移命令。
        for _ in range(30):
            readiness = _run_compose(
                project_name,
                environment,
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                database_user,
                "-d",
                database_name,
            )
            if readiness.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("独立临时 PostgreSQL 未在 30 秒内 ready")

        for arguments in (
            ("run", "--rm", "--no-deps", "migrate", "alembic", "upgrade", "head"),
            ("run", "--rm", "--no-deps", "migrate", "alembic", "heads"),
            ("run", "--rm", "--no-deps", "migrate", "alembic", "current"),
        ):
            completed = _run_compose(project_name, environment, *arguments)
            _assert_compose_success(completed, arguments)

        # 已写入的目录操作人属于 T17 审计事实，回退不能静默删除该字段数据。
        audit_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO sales_authorizations "
                "(wecom_user_id, is_authorized, is_active, is_administrator, "
                "next_message_sequence, created_by, updated_by, created_at, updated_at) "
                "VALUES ('audit-sales', true, true, false, 0, 'test-operator', "
                "'test-operator', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(audit_seed, ("run", "migrate", "python", "-c", "<audit-seed>"))
        downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0019_ticket15_console_observability",
        )
        assert downgrade.returncode != 0
        assert "存在 T17 审计数据" in (downgrade.stdout + downgrade.stderr)

        heads = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "heads",
        )
        _assert_compose_success(
            heads, ("run", "--rm", "--no-deps", "migrate", "alembic", "heads")
        )
        # 当前最新迁移继续保持单一 migration head。
        assert heads.stdout.count("0027_repair_notification_claim_schema") == 1

        # 只有 notification payload 的 T18 事实也必须阻止 downgrade，不能因没有 action 行而丢列。
        payload_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO notification_records "
                "(notification_key, sales_user_id, source_message_id, notification_type, "
                "status, attempts, payload, created_at) VALUES ('payload-only', 'audit-sales', "
                "'payload-source', 'wecom_action_card', 'pending', 0, "
                "'{\\\"msgtype\\\":\\\"template_card\\\"}', CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(payload_seed, ("run", "migrate", "python", "-c", "<payload-seed>"))
        payload_downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0021_ticket16_admin_maintenance",
        )
        assert payload_downgrade.returncode != 0
        assert "notification payload" in (payload_downgrade.stdout + payload_downgrade.stderr)
        payload_check = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "assert connection.execute(text(\"SELECT payload FROM notification_records "
                "WHERE notification_key='payload-only'\")).scalar() is not None; "
                "connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(
            payload_check, ("run", "migrate", "python", "-c", "<payload-check>")
        )
        payload_cleanup = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"DELETE FROM notification_records WHERE "
                "notification_key='payload-only'\")); connection.commit(); "
                "connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(
            payload_cleanup, ("run", "migrate", "python", "-c", "<payload-cleanup>")
        )

        # T18 action/callback 事实一旦产生，同样禁止通过 downgrade 静默丢失不可变审计。
        t18_seed = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "python",
            "-c",
            (
                "from sqlalchemy import create_engine, text; "
                "from app.core.config import get_settings; "
                "engine = create_engine(get_settings().database_url); "
                "connection = engine.connect(); "
                "connection.execute(text(\"INSERT INTO wecom_actions "
                "(id, task_id, action_type, bound_actor_wecom_user_id, target_type, target_id, "
                "expected_action_key, status, expires_at, context, created_at, updated_at) "
                "VALUES ('t18-action-1', 't18-task-1', 'lead.discard.confirmation', 'audit-sales', "
                "'lead', 'lead-1', 'lead.discard.confirm', 'pending', CURRENT_TIMESTAMP, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)\")); "
                "connection.execute(text(\"INSERT INTO wecom_callback_deliveries "
                "(action_id, provider_msgid, req_id, actor_user_id, event_key, task_id, "
                "processing_status, received_at) VALUES ('t18-action-1', 'provider-1', 'req-1', "
                "'audit-sales', 'lead.discard.confirm', 't18-task-1', 'claimed', "
                "CURRENT_TIMESTAMP)\")); "
                "connection.commit(); connection.close(); engine.dispose()"
            ),
        )
        _assert_compose_success(t18_seed, ("run", "migrate", "python", "-c", "<t18-seed>"))
        t18_downgrade = _run_compose(
            project_name,
            environment,
            "run",
            "--rm",
            "--no-deps",
            "migrate",
            "alembic",
            "downgrade",
            "0021_ticket16_admin_maintenance",
        )
        assert t18_downgrade.returncode != 0
        assert "存在 T18 企业微信动作或 callback 事实" in (
            t18_downgrade.stdout + t18_downgrade.stderr
        )
    finally:
        if compose_started:
            # 只清理本测试生成的项目、网络和 Volume，不触碰默认 Compose 资源。
            _run_compose(project_name, environment, "down", "-v", "--remove-orphans")
