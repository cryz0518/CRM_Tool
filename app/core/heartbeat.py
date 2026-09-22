"""Worker 与 Scheduler 的 Redis heartbeat 发布和新鲜度检查。"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from redis import Redis

from app.core.readiness import ReadinessComponent, not_ready_component, ready_component

logger = logging.getLogger(__name__)
HEARTBEAT_TTL_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 10


def heartbeat_key(component: str) -> str:
    """返回组件专用 Redis heartbeat key。"""
    return f"crm:readiness:heartbeat:{component}"


def publish_heartbeat(
    client: Redis,
    component: str,
    instance_id: str,
    *,
    ttl_seconds: int = HEARTBEAT_TTL_SECONDS,
) -> None:
    """写入带时间戳、实例标识和 Redis TTL 的 heartbeat。

    参数：client 为 Redis 客户端；component 为 worker 或 scheduler；instance_id 为进程实例标识；
    ttl_seconds 为 heartbeat 有效期。
    返回值：无。
    异常：Redis 写入异常向调用方传播。
    副作用：覆盖同组件旧 heartbeat，并刷新 TTL。
    """
    payload = {
        "heartbeat_at": datetime.now(UTC).isoformat(),
        "instance_id": instance_id,
    }
    client.set(heartbeat_key(component), json.dumps(payload), ex=ttl_seconds)


def check_heartbeat(
    client: Redis,
    component: str,
    *,
    now: float | None = None,
    ttl_seconds: int = HEARTBEAT_TTL_SECONDS,
) -> ReadinessComponent:
    """读取 heartbeat 并验证存在、实例标识、时间戳和 TTL。

    参数：client 为 Redis 客户端；component 为待检查组件；now 和 ttl_seconds 供测试注入。
    返回值：带稳定 reason_code 的 readiness 组件结果。
    异常：Redis 或解析异常向 ReadinessRegistry 暴露，由 registry 转换为安全失败。
    副作用：只读取 Redis，不写入业务数据。
    """
    raw_payload = cast(str | bytes | None, client.get(heartbeat_key(component)))
    if raw_payload is None:
        return not_ready_component(component, "heartbeat_missing")
    remaining_ttl = cast(int, client.ttl(heartbeat_key(component)))
    if remaining_ttl <= 0:
        return not_ready_component(component, "heartbeat_expired")
    try:
        payload = json.loads(raw_payload)
        heartbeat_at = datetime.fromisoformat(str(payload["heartbeat_at"])).timestamp()
        instance_id = str(payload["instance_id"]).strip()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return not_ready_component(component, "heartbeat_invalid")
    if not instance_id:
        return not_ready_component(component, "heartbeat_invalid")
    current_time = time.time() if now is None else now
    if current_time - heartbeat_at > ttl_seconds:
        return not_ready_component(component, "heartbeat_expired")
    return ready_component(component, "heartbeat_fresh")


class HeartbeatPublisher:
    """在独立 daemon 线程中持续发布单个进程 heartbeat。"""

    def __init__(
        self,
        client: Redis,
        component: str,
        *,
        ttl_seconds: int = HEARTBEAT_TTL_SECONDS,
        interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """初始化 heartbeat 发布器。

        参数：client 为 Redis 客户端；component 为进程组件；TTL 和 interval 控制新鲜度窗口。
        返回值：无。
        异常：TTL 或 interval 非正数时抛出 ValueError。
        副作用：生成本进程唯一 instance_id，但尚未连接 Redis。
        """
        if ttl_seconds <= 0 or interval_seconds <= 0 or interval_seconds >= ttl_seconds:
            raise ValueError("heartbeat interval 必须小于正数 TTL")
        self._client = client
        self._component = component
        self._ttl_seconds = ttl_seconds
        self._interval_seconds = interval_seconds
        self._instance_id = f"{component}-{uuid4()}"
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """立即发布一次并启动后台刷新线程。"""
        publish_heartbeat(
            self._client,
            self._component,
            self._instance_id,
            ttl_seconds=self._ttl_seconds,
        )
        self._thread = threading.Thread(
            target=self._run,
            name=f"{self._component}-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        """按 interval 刷新 heartbeat，失败时只记录异常类型并继续重试。"""
        while True:
            time.sleep(self._interval_seconds)
            try:
                publish_heartbeat(
                    self._client,
                    self._component,
                    self._instance_id,
                    ttl_seconds=self._ttl_seconds,
                )
            except Exception as error:
                logger.error(
                    "runtime_heartbeat_publish_failed",
                    extra={"component": self._component, "error_type": type(error).__name__},
                )
