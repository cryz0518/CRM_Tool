"""本地与测试环境的销售授权目录管理命令。"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.messaging.models import SalesAuthorization

logger = logging.getLogger(__name__)


def authorize_salesperson(settings: Settings, wecom_user_id: str) -> None:
    """在本地或测试数据库中幂等启用一名销售授权记录。

    参数：settings 为运行配置，wecom_user_id 为操作人明确提供的企业微信用户标识。
    返回值：无。
    异常：生产环境、空用户标识或数据库写入失败时抛出 RuntimeError 或数据库异常。
    副作用：新增或启用 sales_authorizations 中的对应记录。
    """
    # 该命令只为本地验收和自动化测试提供受控入口，生产目录由正式管理流程维护。
    if settings.app_env.lower() not in {"development", "test"}:
        raise RuntimeError("销售授权命令仅允许在 development 或 test 环境执行")

    normalized_user_id = wecom_user_id.strip()
    if not normalized_user_id:
        raise RuntimeError("企业微信用户标识不能为空")

    engine = create_engine(settings.database_url)
    try:
        session_factory = sessionmaker(engine)
        with session_factory.begin() as session:
            # 已存在成员仅恢复授权与启用状态，重复运行不会创建第二条目录记录。
            authorization = session.get(SalesAuthorization, normalized_user_id)
            if authorization is None:
                session.add(
                    SalesAuthorization(
                        wecom_user_id=normalized_user_id,
                        is_authorized=True,
                        is_active=True,
                    )
                )
            else:
                authorization.is_authorized = True
                authorization.is_active = True
    finally:
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """解析受控授权命令并记录不含用户标识的成功事件。

    参数：argv 为可选命令行参数列表，省略时读取当前进程参数。
    返回值：成功时返回 0。
    异常：参数、环境或数据库错误向调用方传播并返回非零进程状态。
    副作用：可能修改本地或测试销售授权目录并写入结构化日志。
    """
    parser = argparse.ArgumentParser(description="本地/测试销售授权目录管理")
    parser.add_argument("action", choices=("authorize",))
    parser.add_argument("--wecom-user-id", required=True)
    arguments = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, environment=settings.app_env, service="sales_admin")
    authorize_salesperson(settings, arguments.wecom_user_id)
    # 用户标识属于权限数据，不写入普通容器日志。
    logger.info("sales_authorization_updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
