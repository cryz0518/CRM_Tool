"""企业微信 SDK 日志脱敏测试。"""

from __future__ import annotations

import logging

from _pytest.logging import LogCaptureFixture

from app.wecom_bot.runner import WecomSdkLogger


def test_sdk_debug_payload_is_not_written_to_application_logs(caplog: LogCaptureFixture) -> None:
    """验证 SDK 原始帧调试日志不会泄露到应用日志。

    参数：caplog 为 pytest 提供的日志捕获器。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：临时捕获 Python 日志记录，不写入外部日志系统。
    """
    with caplog.at_level(logging.DEBUG):
        # 模拟 SDK 的原始帧日志；客户正文或响应 URL 不应进入 Docker 日志。
        WecomSdkLogger().debug("Received push message: sensitive frame")

    assert caplog.records == []


def test_sdk_warning_uses_a_fixed_message_without_sdk_payload(caplog: LogCaptureFixture) -> None:
    """验证 SDK 警告日志不会转发其原始文本。

    参数：caplog 为 pytest 提供的日志捕获器。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：临时捕获 Python 日志记录，不写入外部日志系统。
    """
    with caplog.at_level(logging.WARNING):
        # 即使 SDK 错误包含帧内容，应用日志仍只保留固定、可检索的事件名。
        WecomSdkLogger().warn("Received push message: sensitive frame")

    assert caplog.messages == ["wecom_sdk_warning"]
