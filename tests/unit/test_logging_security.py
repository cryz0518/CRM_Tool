"""统一结构化日志的敏感信息隔离测试。"""

from __future__ import annotations

import json
import logging
import sys

from app.core.logging import JsonFormatter


def test_json_formatter_keeps_traceback_shape_but_hides_exception_details() -> None:
    """验证日志保留异常类型和调用栈位置，但不输出异常中的敏感原值。"""
    try:
        # 用异常正文模拟外部消息和认证凭据，确保 formatter 不回显异常详情。
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

    # 栈形状和异常类型可用于定位问题，但所有正文中的敏感值都必须消失。
    assert "Traceback" in output
    assert "RuntimeError" in output
    assert "13812345678" not in output
    assert "alice@example.com" not in output
    assert "top-secret-token" not in output
    assert "原始客户描述-should-not-appear" not in output


def test_json_formatter_redacts_traceback_extra_fields() -> None:
    """验证 readiness 使用的 traceback 字段不会携带源码行或敏感原值。"""
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=40,
        msg="smart_table_readiness_failed",
        args=(),
        exc_info=None,
    )
    # readiness 的 traceback 字段包含源码行，测试必须确认源码和其中的原值均不会出日志。
    record.error_traceback = (
        '  File "readiness.py", line 47, in check\n'
        "    raise RuntimeError('phone 13812345678 email alice@example.com "
        "token=top-secret-token secret=top-secret-secret cookie=session-cookie "
        "raw message 原始客户描述-should-not-appear')\n"
    )

    output = JsonFormatter().format(record)
    traceback_summary = json.loads(output)["error_traceback"]

    # 只允许定位栈帧保留，不能把源码行或任何敏感原值带入序列化结果。
    assert 'File "readiness.py", line 47, in check' in traceback_summary
    assert "raise RuntimeError" not in traceback_summary
    assert "13812345678" not in traceback_summary
    assert "alice@example.com" not in traceback_summary
    assert "top-secret-token" not in traceback_summary
    assert "top-secret-secret" not in traceback_summary
    assert "session-cookie" not in traceback_summary
    assert "原始客户描述-should-not-appear" not in traceback_summary
