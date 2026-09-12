"""Qwen LLM Provider 的结构化输出请求测试。"""

from __future__ import annotations

import httpx

from app.ai.models import LLMRequest
from app.ai.provider import QwenLLMProvider


def test_qwen_provider_forwards_gateway_schema_as_strict_response_format(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """验证 Provider 将 Gateway schema 原样作为 Qwen 严格结构化输出约束发送。"""
    observed: dict[str, object] = {}
    schema: dict[str, object] = {
        "type": "object",
        "properties": {"crm_fields": {"type": "object", "additionalProperties": False}},
    }

    def fake_post(url: str, **kwargs: object) -> httpx.Response:
        """捕获供应商请求并返回最小兼容响应。"""
        observed["url"] = url
        observed["payload"] = kwargs["json"]
        return httpx.Response(
            200,
            request=httpx.Request("POST", url),
            json={"choices": [{"message": {"content": "{}"}}]},
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    QwenLLMProvider(api_key="key", base_url="https://qwen.example/v1").complete(
        LLMRequest(messages=({"role": "user", "content": "JSON"},), json_schema=schema),
        timeout_seconds=1,
    )

    assert observed["url"] == "https://qwen.example/v1/chat/completions"
    assert observed["payload"] == {
        "model": "qwen3.7-flash",
        "messages": [{"role": "user", "content": "JSON"}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "lead_analysis", "schema": schema, "strict": True},
        },
    }
