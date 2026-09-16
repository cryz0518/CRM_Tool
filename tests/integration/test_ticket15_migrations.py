"""T15 migration 链升级、head 和 current 验证。"""

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
        ["docker", "compose", "-p", project_name, *arguments],
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


def test_ticket15_migration_chain_reaches_single_current_head() -> None:
    """在独立临时 PostgreSQL 容器中执行真实 Alembic upgrade、heads 和 current。"""
    if shutil.which("docker") is None:
        pytest.fail("T15 migration 验证需要可用的 docker 命令，禁止静默跳过真实验证")

    project_name = f"t15-migration-{uuid4().hex[:12]}"
    database_user = "t15_migration"
    database_name = "crm_lead"
    database_password = "t15_migration_password"
    environment = {
        **os.environ,
        # Compose 项目名和数据库 Volume 均为本测试独立生成，不触碰默认 crm-lead 环境。
        "POSTGRES_DB": database_name,
        "POSTGRES_USER": database_user,
        "POSTGRES_PASSWORD": database_password,
        "APP_ENV": "test",
        "SMART_TABLE_ADAPTER": "mock",
        "CONSOLE_DEV_ADMIN_TOKEN": "t15-migration-test-token",
    }
    compose_started = False
    try:
        # 先构建当前代码镜像，确保容器内执行的 Alembic 与被测提交一致。
        build = _run_compose(project_name, environment, "build", "migrate")
        _assert_compose_success(build, ("build", "migrate"))

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
        # T15 链必须继续存在，并由当前 0019 revision 作为唯一 head 收束。
        assert heads.stdout.count("0019_ticket15_console_observability") == 1
    finally:
        if compose_started:
            # 只清理本测试生成的项目、网络和 Volume，不触碰默认 Compose 资源。
            _run_compose(project_name, environment, "down", "-v", "--remove-orphans")
