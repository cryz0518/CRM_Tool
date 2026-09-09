"""Docker 容器健康检查命令。"""

from __future__ import annotations

import argparse
import logging
import sys
from urllib.request import urlopen

from redis import Redis

from app.core.config import get_settings
from app.core.logging import configure_logging


def check_app() -> None:
    """调用本容器的健康端点，并在非成功响应时抛出异常。"""
    with urlopen("http://127.0.0.1:8000/health", timeout=2) as response:  # noqa: S310
        if response.status != 200:
            raise RuntimeError(f"应用健康检查返回异常状态：{response.status}")


def check_broker() -> None:
    """验证 Worker 或 Scheduler 容器仍可连接 Redis Broker。"""
    Redis.from_url(get_settings().redis_url, socket_connect_timeout=2).ping()


def main() -> int:
    """执行指定健康检查，失败时输出错误并返回非零退出码。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("app", "broker"))
    target = parser.parse_args().target
    settings = get_settings()
    configure_logging(settings.log_level, environment=settings.app_env, service="healthcheck")

    try:
        if target == "app":
            check_app()
        else:
            check_broker()
    except Exception:
        logging.getLogger(__name__).exception(
            "container_healthcheck_failed",
            extra={"target": target},
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
