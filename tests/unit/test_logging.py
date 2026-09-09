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
