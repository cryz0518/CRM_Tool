"""本地与测试环境销售授权管理命令测试。"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.manage_sales import authorize_salesperson
from app.messaging.models import Base, SalesAuthorization


def test_authorize_salesperson_is_repeatable_in_test_environment(tmp_path: Path) -> None:
    """验证受控授权命令可重复启用同一测试销售。

    参数：tmp_path 为 pytest 提供的隔离临时目录。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：创建隔离 SQLite 文件并写入一条销售授权目录记录。
    """
    database_path = tmp_path / "sales.sqlite"
    database_url = f"sqlite+pysqlite:///{database_path}"
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    engine.dispose()
    settings = Settings(app_env="test", database_url=database_url)

    authorize_salesperson(settings, "sales-test-1")
    authorize_salesperson(settings, "sales-test-1")

    engine = create_engine(database_url)
    try:
        with Session(engine) as session:
            authorization = session.get(SalesAuthorization, "sales-test-1")
            assert authorization is not None
            assert authorization.is_authorized is True
            assert authorization.is_active is True
            assert len(session.query(SalesAuthorization).all()) == 1
    finally:
        engine.dispose()


def test_authorize_salesperson_rejects_production_environment(tmp_path: Path) -> None:
    """验证生产环境不能通过本地测试命令改变销售授权。

    参数：tmp_path 为 pytest 提供的隔离临时目录。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：不创建数据库或销售授权记录。
    """
    settings = Settings(app_env="production", database_url=f"sqlite+pysqlite:///{tmp_path}/sales.sqlite")

    with pytest.raises(RuntimeError, match="仅允许"):
        authorize_salesperson(settings, "sales-test-1")
