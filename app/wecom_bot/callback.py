"""企业微信 template card callback 的 5 秒内响应编排。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.wecom_bot.actions import CallbackParseError, WecomActionService

logger = logging.getLogger(__name__)


class WecomTemplateCardCallbackHandler:
    """把 callback 快速认领与 card update response 隔离于完整业务 Worker。"""

    def __init__(
        self, action_service: WecomActionService, response_timeout_seconds: float = 4.0
    ) -> None:
        """保存动作服务和 callback 总响应预算。

        参数：action_service 为数据库动作认领边界；response_timeout_seconds 为小于五秒的总响应预算。
        返回值：无。
        异常：预算不在安全范围内时抛出 ValueError。
        副作用：仅保存依赖，不执行数据库或网络调用。
        """

        if response_timeout_seconds <= 0 or response_timeout_seconds >= 5:
            raise ValueError("callback 响应预算必须在 0 和 5 秒之间")

        self._action_service = action_service
        self._response_timeout_seconds = response_timeout_seconds

    async def handle(
        self,
        frame: Mapping[str, object],
        update_template_card: Callable[
            [Mapping[str, object], dict[str, object]], Awaitable[Any]
        ],
    ) -> None:
        """解析并认领 callback，最多调用一次 update_template_card。

        参数：frame 为真实 callback 帧；update_template_card 为 SDK 的单次响应函数。
        返回值：无；响应失败只记录传输事实，不重新执行业务动作。
        异常：数据库异常向上抛出；协议错误 fail closed 且不回复。
        副作用：可能创建 action execution Outbox，并在 5 秒窗口内更新卡片状态。
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._response_timeout_seconds
        try:
            # SQLAlchemy 同步事务移出 SDK 事件循环；其中只做解析、鉴权、claim 和 Outbox 写入。
            result = await asyncio.wait_for(
                asyncio.to_thread(self._action_service.claim_callback, frame),
                timeout=self._response_timeout_seconds,
            )
        except CallbackParseError:
            # malformed callback 没有可信 task_id，禁止猜测 response frame 或写业务事实。
            logger.warning("wecom_template_card_callback_rejected")
            return
        except asyncio.TimeoutError:
            # 线程中的数据库调用会由自身事务收敛；当前 callback 不再冒险使用即将过期的 frame。
            logger.error("wecom_template_card_callback_claim_timeout")
            return

        if not result.should_update_card:
            return
        remaining = deadline - loop.time()
        if remaining <= 0:
            logger.error("wecom_template_card_callback_response_deadline_exceeded")
            return
        try:
            # 一个 callback 只允许一个 response；失败不在此处重试，action claim 已经持久化。
            await asyncio.wait_for(
                update_template_card(frame, result.response_card()), timeout=remaining
            )
        except asyncio.TimeoutError:
            logger.error("wecom_template_card_callback_response_timeout")
        except Exception:
            logger.exception("wecom_template_card_callback_response_failed")
