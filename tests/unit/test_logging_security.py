"""统一结构化日志的敏感信息隔离测试。"""

from __future__ import annotations

import logging
import sys

from app.core.logging import JsonFormatter


def test_json_formatter_keeps_traceback_shape_but_hides_exception_details() -> None:
    """验证日志保留异常类型和调用栈位置，但不输出异常中的敏感原值。"""
    try:
        raise RuntimeError(
            "phone 13812345678 email alice@example.com token=top-secret-token "
            "raw message 原始客户描述-should-not-appear"
        )
    except RuntimeError:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=15,
            msg="operation_failed",
            args=(),
            exc_info=sys.exc_info(),
        )

    output = JsonFormatter().format(record)

    assert "Traceback" in output
    assert "RuntimeError" in output
    assert "13812345678" not in output
    assert "alice@example.com" not in output
    assert "top-secret-token" not in output
    assert "原始客户描述-should-not-appear" not in output
