"""企业微信 AI 机器人长连接进程入口。"""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any

# 官方 SDK 未提供 py.typed；运行时接口已按其官方源码与发布包核验。
from aibot import WSClient, WSClientOptions  # type: ignore[import-untyped]
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.messaging.service import MessageIntakeService
from app.wecom_bot.adapter import WecomTextMessageAdapter

logger = logging.getLogger(__name__)


class WecomSdkLogger:
    """阻止 SDK 将原始企业微信帧直接写入应用日志。"""

    def debug(self, message: str, *args: Any) -> None:
        """丢弃 SDK 调试信息，避免原始消息和临时响应地址泄露。

        参数：message 为 SDK 调试文本，args 为 SDK 附加参数。
        返回值：无。
        异常：无。
        副作用：不写入任何日志。
        """
        # SDK 的 DEBUG 日志会包含完整入站帧；业务层另行记录已脱敏的状态事件。
        del message, args

    def info(self, message: str, *args: Any) -> None:
        """记录不携带 SDK 原始文本的通用信息事件。

        参数：message 为 SDK 信息文本，args 为 SDK 附加参数。
        返回值：无。
        异常：无。
        副作用：向结构化应用日志写入固定事件名。
        """
        del message, args
        logger.info("wecom_sdk_info")

    def warn(self, message: str, *args: Any) -> None:
        """记录不携带 SDK 原始文本的通用警告事件。

        参数：message 为 SDK 警告文本，args 为 SDK 附加参数。
        返回值：无。
        异常：无。
        副作用：向结构化应用日志写入固定事件名。
        """
        del message, args
        logger.warning("wecom_sdk_warning")

    def error(self, message: str, *args: Any) -> None:
        """记录不携带 SDK 原始文本的通用错误事件。

        参数：message 为 SDK 错误文本，args 为 SDK 附加参数。
        返回值：无。
        异常：无。
        副作用：向结构化应用日志写入固定事件名。
        """
        del message, args
        logger.error("wecom_sdk_error")


class WecomBotRuntime:
    """管理 SDK 长连接生命周期，并将文本消息异步交给接入适配器。"""

    def __init__(
        self,
        engine: Engine,
        message_intake_service: MessageIntakeService,
        bot_id: str,
        bot_secret: str,
    ) -> None:
        """创建使用官方 SDK 的机器人运行时。

        参数：engine 为当前进程持有的数据库引擎，message_intake_service 为 T02 边界，
        bot_id 和 bot_secret 为企业微信机器人凭据。
        返回值：无。
        异常：SDK 构造异常向上抛出。
        副作用：注册 SDK 连接与文本事件回调，但尚未建立网络连接。
        """
        self._engine = engine
        self._text_adapter = WecomTextMessageAdapter(message_intake_service)
        self._ready_file = Path("/tmp/wecom-bot-ready")
        self._shutdown_event: asyncio.Event | None = None
        self._fatal_intake_error: Exception | None = None
        # SDK 负责心跳与重连；-1 让长连接进程在临时网络故障后持续重试。
        self._client = WSClient(
            WSClientOptions(
                bot_id=bot_id,
                secret=bot_secret,
                max_reconnect_attempts=-1,
                logger=WecomSdkLogger(),
            )
        )
        self._register_sdk_handlers()

    def _register_sdk_handlers(self) -> None:
        """注册官方 SDK 暴露的连接状态、错误和文本消息事件。

        参数：无。
        返回值：无。
        异常：SDK 事件注册错误向上抛出。
        副作用：后续 SDK 事件会写入结构化日志或触发消息持久化。
        """
        # 连接事件由 SDK 提供，日志不记录凭据、原始消息或客户文本。
        self._client.on("connected", self._handle_connected)
        self._client.on("authenticated", self._handle_authenticated)
        self._client.on("disconnected", self._handle_disconnected)
        self._client.on("reconnecting", self._handle_reconnecting)
        self._client.on("error", self._handle_sdk_error)
        self._client.on("message.text", self._receive_text_frame)

    def _handle_connected(self) -> None:
        """记录 WebSocket 已建立但尚未完成认证的状态。

        参数：无。
        返回值：无。
        异常：无。
        副作用：移除旧就绪标记并写入连接日志。
        """
        # 底层 socket 打开不等于认证成功，必须等待 SDK 的 authenticated 事件。
        self._mark_not_ready()
        logger.info("wecom_bot_socket_connected")

    def _handle_authenticated(self) -> None:
        """记录 SDK 认证成功，使容器健康检查可确认机器人可接收消息。

        参数：无。
        返回值：无。
        异常：就绪文件写入失败会向 SDK 事件处理器传播。
        副作用：创建进程内健康检查使用的就绪文件并写入日志。
        """
        # SDK 没有 is_authenticated 属性，因此以认证事件维护独立的就绪状态。
        self._ready_file.write_text("authenticated\n", encoding="utf-8")
        logger.info("wecom_bot_authenticated")

    def _handle_disconnected(self, reason: str) -> None:
        """在 SDK 报告断线时撤销就绪状态。

        参数：reason 为 SDK 提供的断线原因。
        返回值：无。
        异常：无。
        副作用：删除健康检查就绪文件并记录断线日志。
        """
        # SDK 随后会按自身策略重连；认证成功前不能把机器人标为可用。
        self._mark_not_ready()
        # 断线原因可能由外部服务提供，不能透传；只记录其是否存在以保留安全状态摘要。
        logger.warning("wecom_bot_disconnected", extra={"has_reason": bool(reason)})

    def _handle_reconnecting(self, attempt: int) -> None:
        """记录 SDK 发起的自动重连次数。

        参数：attempt 为 SDK 当前重连序号。
        返回值：无。
        异常：无。
        副作用：写入不含凭据的重连日志。
        """
        logger.info("wecom_bot_reconnecting", extra={"attempt": attempt})

    def _handle_sdk_error(self, error: Exception) -> None:
        """记录 SDK 异常并确保失败连接不再通过健康检查。

        参数：error 为 SDK 回调提供的异常。
        返回值：无。
        异常：无。
        副作用：删除健康检查就绪文件并写入异常日志。
        """
        self._mark_not_ready()
        logger.error("wecom_bot_sdk_error", exc_info=error)

    def _mark_not_ready(self) -> None:
        """删除认证就绪文件，令容器健康检查反映当前不可接收状态。

        参数：无。
        返回值：无。
        异常：文件系统删除错误向上抛出。
        副作用：撤销 Docker 健康检查的认证成功标记。
        """
        self._ready_file.unlink(missing_ok=True)

    async def _receive_text_frame(self, frame: dict[str, Any]) -> None:
        """在线程中执行同步事务接收，避免数据库 I/O 阻塞 SDK 的事件循环。

        参数：frame 为官方 SDK 的完整文本消息帧。
        返回值：无。
        异常：接收失败只记录异常；SDK 不公开入站确认或重投接口，不能伪造确认。
        副作用：成功时通过 T02 持久化原始消息和 Outbox 事件。
        """
        try:
            # T02 是同步 SQLAlchemy 边界，移至工作线程保持 SDK 心跳与重连任务可运行。
            result = await asyncio.to_thread(self._text_adapter.receive_text_frame, frame)
            if result is not None:
                logger.info(
                    "wecom_bot_text_forwarded",
                    extra={"accepted": result.accepted, "duplicate": result.duplicate},
                )
        except Exception:
            # 事务失败不能被静默当作已接收，终止进程让 Docker 重新建立连接。
            logger.exception("wecom_bot_text_intake_failed")
            self._fatal_intake_error = RuntimeError("企业微信消息未能持久化到 T02")
            if self._shutdown_event is not None:
                self._shutdown_event.set()

    async def run(self) -> None:
        """建立 SDK 长连接并等待进程终止信号。

        参数：无。
        返回值：无。
        异常：事件循环或 SDK 未处理异常向上抛出。
        副作用：建立企业微信长连接；退出时主动断开并释放数据库连接池。
        """
        shutdown_event = asyncio.Event()
        self._shutdown_event = shutdown_event
        loop = asyncio.get_running_loop()
        self._register_shutdown_signals(loop, shutdown_event)
        try:
            # 清理异常退出遗留的文件，避免新连接认证前错误报告为就绪。
            self._mark_not_ready()
            # connect 只启动连接/认证流程，认证结果由 authenticated 或 error 事件体现。
            await self._client.connect()
            await shutdown_event.wait()
            if self._fatal_intake_error is not None:
                raise self._fatal_intake_error
        finally:
            # 主动关闭会让 SDK 停止自动重连，再释放本进程数据库资源。
            self._client.disconnect()
            self._mark_not_ready()
            self._engine.dispose()
            logger.info("wecom_bot_stopped")

    @staticmethod
    def _register_shutdown_signals(
        loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event
    ) -> None:
        """在支持的运行平台上绑定容器终止信号。

        参数：loop 为当前 asyncio 事件循环，shutdown_event 用于通知主协程退出。
        返回值：无。
        异常：不支持信号处理的平台会被安全跳过。
        副作用：SIGINT 或 SIGTERM 到达时触发优雅关闭。
        """
        # Docker/Linux 支持信号处理；Windows 事件循环可能不支持，因此仅记录降级。
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(shutdown_signal, shutdown_event.set)
            except NotImplementedError:
                logger.warning("wecom_bot_signal_handler_unavailable")
                return


def create_runtime(settings: Settings) -> WecomBotRuntime:
    """根据项目配置创建机器人运行时及其数据库接收边界。

    参数：settings 为已解析的运行配置。
    返回值：可启动的企业微信机器人运行时。
    异常：缺少机器人凭据时抛出 RuntimeError，数据库引擎创建错误向上抛出。
    副作用：创建数据库连接池和 SDK 客户端，但不建立企业微信连接。
    """
    # 凭据只用于 SDK 构造，禁止写入日志或回传到健康接口。
    if not settings.wecom_bot_id or not settings.wecom_bot_secret:
        raise RuntimeError("WECOM_BOT_ID 和 WECOM_BOT_SECRET 必须同时配置")

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    session_factory = sessionmaker(engine)
    return WecomBotRuntime(
        engine=engine,
        message_intake_service=MessageIntakeService(session_factory),
        bot_id=settings.wecom_bot_id,
        bot_secret=settings.wecom_bot_secret,
    )


def main() -> None:
    """配置日志并启动企业微信机器人长连接进程。

    参数：无。
    返回值：无。
    异常：配置缺失或运行时启动失败会向进程外传播并使容器重启。
    副作用：启动企业微信 WebSocket 连接和数据库连接池。
    """
    settings = get_settings()
    configure_logging(settings.log_level, environment=settings.app_env, service="wecom_bot")
    asyncio.run(create_runtime(settings).run())


if __name__ == "__main__":
    main()
