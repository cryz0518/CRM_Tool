"""验证统一 Alembic readiness 在应用和生产检查入口中的行为。"""

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Iterator

import pytest

from app.core.config import Settings
from app.core.readiness import ReadinessComponent


class FakeResult:
    """提供 readiness 只读查询所需的最小 SQLAlchemy Result 接口。"""

    def __init__(self, rows: list[str]) -> None:
        """保存模拟的 revision 行。"""
        self.rows = rows

    def scalar(self) -> str | None:
        """兼容旧 readiness 代码的单行读取接口。"""
        return self.rows[0] if self.rows else None

    def scalars(self) -> FakeResult:
        """兼容新 readiness 代码的多行读取接口。"""
        return self

    def all(self) -> list[str]:
        """返回模拟查询的全部 revision。"""
        return self.rows


class FakeConnection:
    """记录 SQL 并返回模拟的 Alembic revision。"""

    def __init__(self, rows: list[str], error: Exception | None = None) -> None:
        """初始化 revision 行和 SQL 记录。"""
        self.rows = rows
        self.error = error
        self.statements: list[str] = []

    def execute(self, statement: object) -> FakeResult:
        """只模拟 SELECT 探针并记录语句。"""
        sql = str(statement)
        self.statements.append(sql)
        if self.error is not None:
            raise self.error
        return FakeResult(self.rows if "alembic_version" in sql else ["1"])


class FakeEngine:
    """提供可控连接的轻量 SQLAlchemy Engine 替身。"""

    def __init__(self, connection: FakeConnection, connect_error: Exception | None = None) -> None:
        """保存供应用和 production_verify 共用的连接替身。"""
        self.connection = connection
        self.connect_error = connect_error

    @contextmanager
    def connect(self) -> Iterator[FakeConnection]:
        """返回模拟数据库连接。"""
        if self.connect_error is not None:
            raise self.connect_error
        yield self.connection

    def dispose(self) -> None:
        """模拟释放数据库连接池。"""


class FakeRedis:
    """提供成功的 Redis readiness 探针。"""

    @classmethod
    def from_url(cls, *_: object, **__: object) -> FakeRedis:
        """返回模拟 Redis 客户端。"""
        return cls()

    def ping(self) -> bool:
        """模拟 Redis PING 成功。"""
        return True


def _migration_component(components: tuple[object, ...]) -> ReadinessComponent:
    """从组件结果中取出 migration 状态。"""
    return next(item for item in components if getattr(item, "component") == "migration")


def _install_runtime_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[str],
    connect_error: Exception | None = None,
) -> FakeConnection:
    """将应用和 production_verify 的运行时依赖指向同一隔离替身。"""
    import app.main as main_module
    import app.production_verify as verify_module

    connection = FakeConnection(rows)
    engine = FakeEngine(connection, connect_error)
    monkeypatch.setattr(main_module, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(verify_module, "create_engine", lambda *_args, **_kwargs: engine)
    monkeypatch.setattr(main_module, "Redis", FakeRedis)
    monkeypatch.setattr(verify_module, "Redis", FakeRedis)
    monkeypatch.setattr(
        main_module,
        "check_heartbeat",
        lambda *_args, **_kwargs: ReadinessComponent("worker", "ok", "heartbeat_current"),
    )
    monkeypatch.setattr(
        verify_module,
        "check_heartbeat",
        lambda *_args, **_kwargs: ReadinessComponent("worker", "ok", "heartbeat_current"),
    )
    return connection


def _check_migration(rows: list[str]) -> ReadinessComponent:
    """通过公共 migration readiness 检查模拟数据库 revision 集合。"""
    from app.core.migration_readiness import check_migration_readiness

    return check_migration_readiness(FakeConnection(rows))


def test_app_runtime_readiness_accepts_database_at_current_alembic_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """代码 head 为 0032 且数据库为 0032 时，应用 migration 必须就绪。"""
    import app.main as main_module

    _install_runtime_dependencies(monkeypatch, ["0032_wecom_quote_routing"])

    component = _migration_component(main_module._runtime_readiness_components())

    assert (component.status, component.reason_code) == ("ok", "migration_current")


def test_production_verify_runtime_accepts_database_at_current_alembic_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """production_verify runtime 使用与应用相同的动态 migration head 判断。"""
    import app.production_verify as verify_module

    _install_runtime_dependencies(monkeypatch, ["0032_wecom_quote_routing"])
    settings = Settings(_env_file=None, app_env="production")

    component = _migration_component(verify_module.verify("runtime", settings).components)

    assert (component.status, component.reason_code) == ("ok", "migration_current")


def test_database_revision_older_than_code_head_is_pending() -> None:
    """数据库仍停留在 0023 时必须 fail closed。"""
    component = _check_migration(["0023_ticket22_storage_retention"])

    assert (component.status, component.reason_code) == ("not_ready", "migration_pending")


def test_missing_alembic_version_table_is_unavailable() -> None:
    """缺少 alembic_version 时必须返回脱敏的 not_ready。"""
    connection = FakeConnection([], RuntimeError("missing alembic_version; password=secret"))
    from app.core.migration_readiness import check_migration_readiness

    component = check_migration_readiness(connection)

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")
    assert "secret" not in str(component)


def test_empty_database_revision_is_unavailable() -> None:
    """数据库没有 current revision 时不能报告就绪。"""
    component = _check_migration([])

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")


def test_multiple_database_revisions_are_unavailable() -> None:
    """数据库同时处于多个 revision 时必须 fail closed。"""
    component = _check_migration(["0032_wecom_quote_routing", "other_revision"])

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")


def test_multiple_code_heads_are_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """代码 revision graph 存在多个 head 时不能选择其中一个。"""
    from app.core import migration_readiness

    monkeypatch.setattr(
        migration_readiness,
        "_get_code_heads",
        lambda: ("head_a", "head_b"),
    )

    component = migration_readiness.check_migration_readiness(FakeConnection(["head_a"]))

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")


def test_missing_alembic_files_are_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """配置或迁移脚本缺失时不能将数据库版本视为当前。"""
    from app.core import migration_readiness

    monkeypatch.setattr(migration_readiness, "_ALEMBIC_CONFIG_PATH", tmp_path / "missing.ini")

    component = migration_readiness.check_migration_readiness(
        FakeConnection(["0032_wecom_quote_routing"])
    )

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")


def test_unreadable_alembic_graph_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """配置存在但脚本目录不可读时不能报告 migration 就绪。"""
    from app.core import migration_readiness

    config_path = tmp_path / "alembic.ini"
    config_path.write_text("[alembic]\nscript_location = alembic\n", encoding="utf-8")
    monkeypatch.setattr(migration_readiness, "_ALEMBIC_CONFIG_PATH", config_path)

    component = migration_readiness.check_migration_readiness(
        FakeConnection(["0032_wecom_quote_routing"])
    )

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")


def test_database_query_failure_is_unavailable_and_redacted() -> None:
    """数据库查询异常只产生稳定原因码，不输出 DSN 或异常正文。"""
    from app.core.migration_readiness import check_migration_readiness

    connection = FakeConnection([], RuntimeError("postgresql://user:password@db/customer"))
    component = check_migration_readiness(connection)

    assert (component.status, component.reason_code) == ("not_ready", "migration_unavailable")
    assert "password" not in str(component)
    assert "customer" not in str(component)


def test_unknown_database_revision_is_not_current() -> None:
    """数据库中的未知 revision 不能被当作成功状态。"""
    component = _check_migration(["unknown_revision"])

    assert (component.status, component.reason_code) == ("not_ready", "migration_pending")


def test_current_graph_is_single_head_and_database_check_only_executes_select(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """当前仓库以 0032 为唯一 head，readiness 只查询 alembic_version。"""
    from app.core import migration_readiness

    connection = FakeConnection(["0032_wecom_quote_routing"])
    monkeypatch.chdir(tmp_path)
    component = migration_readiness.check_migration_readiness(connection)

    assert migration_readiness._get_code_heads() == ("0032_wecom_quote_routing",)
    assert (component.status, component.reason_code) == ("ok", "migration_current")
    assert len(connection.statements) == 1
    assert connection.statements[0].lstrip().upper().startswith("SELECT")
    assert "alembic_version" in connection.statements[0]


def test_app_and_production_verify_runtime_and_all_share_migration_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """生产 /health/ready 与 production_verify runtime/all 使用相同只读判定。"""
    import asyncio

    import app.main as main_module
    import app.production_verify as verify_module

    connection = _install_runtime_dependencies(monkeypatch, ["0032_wecom_quote_routing"])
    production_settings = Settings(_env_file=None, app_env="production")
    monkeypatch.setattr(main_module, "settings", production_settings)
    monkeypatch.setattr(
        main_module,
        "get_provider_policy",
        lambda _settings: SimpleNamespace(
            is_production=True,
            evaluate_settings=lambda _selected: (),
        ),
    )
    monkeypatch.setattr(
        main_module,
        "_smart_table_component",
        lambda _adapter: (
            ReadinessComponent("smart_table", "ok", "checked"),
            (),
        ),
    )
    monkeypatch.setattr(
        main_module.ProductionMediaReadinessChecker,
        "check",
        lambda *_args: SimpleNamespace(ready=True, issues=()),
    )

    response = asyncio.run(main_module.readiness(object()))
    app_payload = json.loads(response.body)
    app_migration = next(
        item for item in app_payload["components"] if item["component"] == "migration"
    )
    runtime = verify_module.verify("runtime", production_settings)
    all_modes = verify_module.verify("all", production_settings)
    migration_status = [
        (item.status, item.reason_code)
        for report in (runtime, all_modes)
        for item in report.components
        if item.component == "migration"
    ]

    assert response.status_code == 200
    assert (app_migration["status"], app_migration["reason_code"]) == (
        "ok",
        "migration_current",
    )
    assert migration_status == [
        ("ok", "migration_current"),
        ("ok", "migration_current"),
    ]
    assert any("alembic_version" in sql for sql in connection.statements)


def test_runtime_entrypoints_report_connection_failure_without_sensitive_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """数据库连接失败时 app 与 production_verify 均返回安全的 not_ready。"""
    import app.main as main_module
    import app.production_verify as verify_module

    _install_runtime_dependencies(
        monkeypatch,
        ["0032_wecom_quote_routing"],
        RuntimeError("postgresql://user:password@private-host/customer"),
    )
    settings = Settings(_env_file=None, app_env="production")

    app_component = _migration_component(main_module._runtime_readiness_components())
    verify_component = _migration_component(verify_module.verify("runtime", settings).components)

    assert (app_component.status, app_component.reason_code) == (
        "not_ready",
        "migration_unavailable",
    )
    assert (verify_component.status, verify_component.reason_code) == (
        "not_ready",
        "migration_unavailable",
    )
    assert "private-host" not in str(app_component)
    assert "password" not in str(verify_component)
