"""T15 Console 健康和 readiness 查询测试。"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.console.health import DefaultConsoleHealthProvider
from app.messaging.models import Base
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


class SuccessfulRedis:
    """模拟可用 Redis 客户端。"""

    def ping(self) -> bool:
        """返回 Redis ping 成功。"""
        return True


def test_default_health_snapshot_names_all_runtime_dependencies(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """验证 Console 健康快照覆盖应用、数据库、缓存、Worker 和 WeCom。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        from app.console import health

        monkeypatch.setattr(health.Redis, "from_url", lambda *args, **kwargs: SuccessfulRedis())
        provider = DefaultConsoleHealthProvider(
            sessionmaker(engine),
            MockSmartTableAdapter(schema=build_required_smart_table_schema()),
        )

        statuses = provider.snapshot()

        assert {status.name for status in statuses} == {
            "app",
            "postgres",
            "redis",
            "smart_table",
            "worker",
            "scheduler",
            "wecom",
        }
        assert next(item for item in statuses if item.name == "postgres").status == "ok"
        assert next(item for item in statuses if item.name == "redis").status == "ok"
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
