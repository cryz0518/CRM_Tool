"""结构化日志测试。"""

import json
import logging

from app.core.logging import ContextFilter, JsonFormatter, bind_log_context, reset_log_context


def test_json_log_includes_bound_trace_fields() -> None:
    """验证结构化日志包含已绑定的请求与消息链路字段。"""
    token = bind_log_context(request_id="request-1", message_id="message-1")
    try:
        record = logging.LogRecord("test", logging.INFO, __file__, 1, "测试日志", (), None)
        ContextFilter().filter(record)
        payload = json.loads(JsonFormatter().format(record))
    finally:
        reset_log_context(token)

    assert payload["request_id"] == "request-1"
    assert payload["message_id"] == "message-1"
    assert payload["lead_id"] is None
    assert payload["environment"] == "development"
    assert payload["service"] == "app"


def test_json_log_includes_only_allowlisted_safe_status_fields() -> None:
    """验证日志仅输出安全状态字段，不透传任意外部额外数据。

    参数：无。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：仅在内存中构造日志记录，不写入外部日志系统。
    """
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "安全状态", (), None)
    # 模拟接入层的状态与 SDK 原始载荷；格式化器只能保留白名单字段。
    record.accepted = True
    record.raw_payload = {"body": "sensitive frame"}

    payload = json.loads(JsonFormatter().format(record))

    assert payload["accepted"] is True
    assert "raw_payload" not in payload
