"""企业微信 template card callback 的 5 秒内响应编排。"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.wecom_bot.actions import CallbackParseError, TemplateCardCallbackParser, WecomActionService

logger = logging.getLogger(__name__)


class CallbackUpdateAckError(RuntimeError):
    """表示 SDK 返回缺失或非零的模板卡片更新 ACK。"""

    def __init__(self, errcode: int | None) -> None:
        """保存可安全展示的 ACK 数字码，不保留 errmsg。

        参数：errcode 为 SDK ACK 中的整数错误码；缺失或类型非法时为 None。
        返回值：无。
        异常：无。
        副作用：无。
        """
        self.errcode = errcode
        super().__init__("SDK card update acknowledgement rejected")


def _safe_update_failure(error: BaseException) -> tuple[str, str]:
    """将 SDK/网络异常转换为白名单失败码与固定摘要。

    参数：error 为模板卡片更新调用抛出的异常。
    返回值：不含异常正文、errmsg、req_id 或凭据的受控失败码与摘要。
    异常：无；无法识别时只返回异常类型名。
    副作用：只读取异常文本以提取 SDK 的数字 errcode，不记录原始文本。
    """
    if isinstance(error, CallbackUpdateAckError):
        if error.errcode is None:
            return "sdk_ack_missing", "企业微信 SDK 未返回有效卡片更新回执"
        return (
            f"sdk_ack_errcode_{error.errcode}"[:64],
            f"企业微信 SDK 拒绝卡片更新（错误码 {error.errcode}）",
        )
    # SDK 1.0.2 将 ACK 错误码放在 RuntimeError 中；只抽取整数，不保留 errmsg。
    message = str(error)
    match = re.search(
        r"Reply ack error:\s*(?:reqId=[^,\s]+,\s*)?errcode=(-?\d+)\b", message
    )
    if match is not None:
        errcode = int(match.group(1))
        return (
            f"sdk_ack_errcode_{errcode}"[:64],
            f"企业微信 SDK 拒绝卡片更新（错误码 {errcode}）",
        )
    if "Reply ack timeout" in message:
        return "sdk_ack_timeout", "企业微信 SDK 卡片更新回执超时"
    if "Reply queue for reqId" in message and "exceeds max size" in message:
        return "sdk_reply_queue_full", "企业微信 SDK 卡片更新队列已满"
    if isinstance(error, TimeoutError):
        return "callback_response_timeout", "企业微信 callback 卡片更新超时"
    module = type(error).__module__.casefold()
    if isinstance(error, (ConnectionError, OSError)) or module.startswith(
        ("websocket", "websockets", "aiohttp")
    ):
        return "websocket_transport_error", "企业微信 WebSocket 卡片更新传输异常"
    return type(error).__name__[:64], "企业微信 callback card update 传输失败"


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
        update_template_card: Callable[[Mapping[str, object], dict[str, object]], Awaitable[Any]],
    ) -> None:
        """解析并认领 callback，最多调用一次 update_template_card。

        参数：frame 为真实 callback 帧；update_template_card 为 SDK 的单次响应函数。
        返回值：无；响应失败只记录传输事实，不重新执行业务动作。
        异常：数据库异常向上抛出；协议错误 fail closed 且不回复。
        副作用：可能创建 action execution Outbox，并在 5 秒窗口内更新卡片状态。
        """
        loop = asyncio.get_running_loop()
        # 以 handler 收到原始 frame 的单调时钟起点核算 provider 五秒窗口。
        callback_started = loop.time()
        deadline = callback_started + self._response_timeout_seconds
        callback_claim_duration_ms = 0
        card_update_duration_ms = 0
        remaining_deadline_ms = 0
        try:
            # SQLAlchemy 同步事务移出 SDK 事件循环；其中只做解析、鉴权、claim 和 Outbox 写入。
            claim_started = loop.time()
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(self._action_service.claim_callback, frame),
                    timeout=self._response_timeout_seconds,
                )
            finally:
                callback_claim_duration_ms = max(
                    0, round((loop.time() - claim_started) * 1000)
                )
            remaining = deadline - loop.time()
            remaining_deadline_ms = max(0, round(remaining * 1000))
            if not result.should_update_card:
                return
            if remaining <= 0:
                logger.error("wecom_template_card_callback_response_deadline_exceeded")
                return
            update_started = loop.time()
            try:
                try:
                    # SDK 必须收到原 frame；其 req_id 与返回卡片的原 task_id 均保持不变。
                    ack = await asyncio.wait_for(
                        update_template_card(frame, result.response_card()), timeout=remaining
                    )
                finally:
                    # 该时长只覆盖 update_template_card 到 ACK，不含后续 evidence 数据库写入。
                    card_update_duration_ms = max(
                        0, round((loop.time() - update_started) * 1000)
                    )
                # 仅 ACK errcode 为整数 0 时记录传输成功；缺失 ACK 不能冒充成功。
                errcode = ack.get("errcode") if isinstance(ack, Mapping) else None
                if type(errcode) is not int or errcode != 0:
                    raise CallbackUpdateAckError(
                        errcode if type(errcode) is int else None
                    )
            except asyncio.TimeoutError:
                logger.error("wecom_template_card_callback_response_timeout")
                self._record_update_failure(frame, TimeoutError("callback response timeout"))
            except Exception as error:
                failure_code, _ = _safe_update_failure(error)
                logger.warning(
                    "wecom_template_card_callback_response_failed",
                    extra={
                        "event": "callback_card_update_failed",
                        "error_type": type(error).__name__,
                        "failure_code": failure_code,
                    },
                )
                self._record_update_failure(frame, error)
            else:
                try:
                    # ACK=0 只证明传输回执成功，不等价于已验收客户端界面替换。
                    callback = TemplateCardCallbackParser.parse(frame)
                    self._action_service.record_callback_transport_success(
                        callback.provider_msgid
                    )
                except Exception:
                    # 卡片已成功更新时，evidence 写入故障不能伪装为传输失败或重做业务动作。
                    logger.error("wecom_template_card_update_evidence_failed")
        except CallbackParseError:
            # malformed callback 没有可信 task_id，禁止猜测 response frame 或写业务事实。
            logger.warning("wecom_template_card_callback_rejected")
            return
        except asyncio.TimeoutError:
            # 数据库线程会由自身事务收敛；当前 callback 不再使用即将过期的 frame。
            logger.error("wecom_template_card_callback_claim_timeout")
            return
        finally:
            # 只记录毫秒数，不记录 callback frame、用户字段或 SDK 原始异常正文。
            logger.info(
                "wecom_template_card_callback_timing",
                extra={
                    "callback_claim_duration_ms": callback_claim_duration_ms,
                    "card_update_duration_ms": card_update_duration_ms,
                    "remaining_deadline_ms": remaining_deadline_ms,
                    "callback_total_duration_ms": max(
                        0, round((loop.time() - callback_started) * 1000)
                    ),
                },
            )

    def _record_update_failure(self, frame: Mapping[str, object], error: BaseException) -> None:
        """按白名单 msgid 保存 card update transport failure，不重做业务 action。"""

        try:
            callback = TemplateCardCallbackParser.parse(frame)
        except CallbackParseError:
            return
        failure_code, failure_summary = _safe_update_failure(error)
        self._action_service.record_callback_transport_failure(
            callback.provider_msgid,
            error,
            failure_code=failure_code,
            failure_summary=failure_summary,
        )
