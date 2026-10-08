"""在已验证的一次性测试容器内运行 T15 迁移与排程并发检查。"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text

from tests.integration.test_lead_progress_postgres import _validated_test_database_url


def _validate_worker_environment(environment: dict[str, str]) -> tuple[str, str, str]:
    """验证 worker 仅使用本次随机 PostgreSQL 测试身份。

    参数：environment 为显式进程环境映射。
    返回值：TEST_DATABASE_URL、运行标识及随机数据库用户名。
    异常：URL、用户、运行标识、应用数据库 URL 或 test 环境不一致时抛出 ValueError。
    副作用：无，不建立数据库连接或运行迁移。
    """
    database_url, run_id, database_user = _validated_test_database_url(environment)
    if (
        environment.get("APP_ENV") != "test"
        or environment.get("DATABASE_URL") != database_url
        or database_user != f"t15_{run_id}"
        or environment.get("TEST_DATABASE_USER") != database_user
    ):
        raise ValueError("worker 数据库身份未通过一次性测试校验")
    return database_url, run_id, database_user


def _run() -> tuple[str, str, str | None]:
    """先只读核验随机测试数据库，再运行 0033 迁移和单个并发用例。

    参数：无，所有身份从容器的显式测试环境变量读取。
    返回值：status、stage 和非敏感异常类型。
    异常：业务错误转换为固定阶段码；不返回原始 URL、SQL 或子测试输出。
    副作用：在身份验证通过后执行 Alembic 迁移和指定 PostgreSQL 集成测试。
    """
    engine = None
    try:
        database_url, run_id, database_user = _validate_worker_environment(dict(os.environ))
        # .env 即使意外进入镜像也会阻止运行，配置只允许由本次 run 的随机变量提供。
        if Path(".env").exists():
            return "failed", "environment", "LocalEnvFilePresent"
        from app.core.config import Settings, get_settings

        Settings.model_config["env_file"] = None
        get_settings.cache_clear()
        if get_settings().database_url != database_url:
            return "failed", "environment", "ApplicationDatabaseUrlMismatch"

        # 仅执行身份只读查询；所有 CREATE、迁移和种子动作都在该检查之后。
        engine = create_engine(database_url, pool_pre_ping=True)
        with engine.connect() as connection:
            actual = connection.execute(
                text(
                    "SELECT current_database(), current_user, "
                    "current_setting('data_directory')"
                )
            ).one()
        if (
            actual[0] != f"crm_lead_test_{run_id}"
            or actual[1] != database_user
            or not str(actual[2]).startswith("/var/lib/postgresql/data/")
        ):
            return "failed", "database_identity", "DatabaseIdentityMismatch"

        # 屏蔽迁移与 pytest 的可变输出，外层只得到固定 JSON 状态。
        from alembic.config import Config

        from alembic import command

        captured_output = io.StringIO()
        previous_disable = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            with contextlib.redirect_stdout(captured_output), contextlib.redirect_stderr(
                captured_output
            ):
                config = Config("alembic.ini")
                command.upgrade(config, "head")
                with engine.connect() as connection:
                    versions = connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).scalars().all()
                if versions != ["0033_lead_progress_sessions"]:
                    return "failed", "migration_version", "ExpectedMigrationVersionMissing"

                import pytest

                exit_code = pytest.main(
                    [
                        "-q",
                        "-p",
                        "no:cacheprovider",
                        "tests/integration/test_lead_progress_postgres.py",
                        "-k",
                        "test_concurrent_schedulers_create_one_logical_notification",
                    ]
                )
        finally:
            logging.disable(previous_disable)
        if exit_code != pytest.ExitCode.OK:
            return "failed", "scheduler_concurrency", "PostgresTestFailed"
        return "passed", "migration_0033_and_scheduler_concurrency", None
    except Exception as error:
        return "failed", "worker", type(error).__name__
    finally:
        if engine is not None:
            engine.dispose()


def main() -> int:
    """输出脱敏 worker 状态并以状态码表示迁移或测试结果。

    参数：无。
    返回值：成功为 0，安全校验、迁移或集成测试失败为 1。
    异常：内部异常已转换为安全诊断字段。
    副作用：调用 _run 执行一次性 worker 检查，并向 stdout 写单行 JSON。
    """
    status, stage, exception_type = _run()
    print(
        json.dumps(
            {
                "status": status,
                "stage": stage,
                "error_code": "OK" if status == "passed" else "POSTGRES_WORKER_FAILED",
                "exception_type": exception_type,
            },
            ensure_ascii=False,
        )
    )
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
