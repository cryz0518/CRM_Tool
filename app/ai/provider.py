"""LLM Provider 抽象及 Qwen、Mock 实现。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import httpx

from app.ai.models import LLMRequest, LLMResponse


class LLMProviderError(RuntimeError):
    """表示可由 AI Gateway 统一归一化的模型传输或供应商错误。"""


class LLMProvider(Protocol):
    """定义业务层唯一可依赖的同步 LLM 调用边界。"""

    def complete(self, request: LLMRequest, *, timeout_seconds: float) -> LLMResponse:
        """执行一次模型调用并返回文本结果。

        参数：request 为最小化后的消息与结构约束；timeout_seconds 为单次调用超时。
        返回：模型文本和可用调用量。
        异常：传输、超时或供应商响应异常时抛出 LLMProviderError。
        副作用：可能发起一次外部模型请求。
        """


class QwenLLMProvider:
    """通过 Qwen OpenAI 兼容 Chat Completions 接口调用指定模型。"""

    def __init__(self, *, api_key: str, base_url: str, model: str = "qwen3.7-flash") -> None:
        """保存 Qwen 连接配置，禁止记录 API Key。

        参数：api_key 为运行时注入密钥；base_url 为兼容 API 地址；model 为模型名称。
        返回：无。
        异常：无。
        副作用：仅保存配置，不发起网络请求。
        """
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model

    def complete(self, request: LLMRequest, *, timeout_seconds: float) -> LLMResponse:
        """调用 Qwen 兼容接口并提取首个 assistant 文本。

        参数：request 为网关已最小化的结构化请求；timeout_seconds 为 HTTP 超时。
        返回：模型返回文本和 usage 中的 token 计数。
        异常：网络、非成功响应或响应层级不完整时抛出 LLMProviderError。
        副作用：向 Qwen 发起一次 HTTPS POST 请求。
        """
        messages = list(request.messages)
        if request.repair_source is not None:
            # 结构修复必须只看原输出，避免模型在修复时重新读取或扩展业务事实。
            messages.append({"role": "user", "content": request.repair_source})
        payload: dict[str, object] = {
            "model": self._model,
            "messages": messages,
            "response_format": {"type": "json_object"},
        }
        try:
            response = httpx.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=payload,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            body = response.json()
            content = body["choices"][0]["message"]["content"]
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as error:
            raise LLMProviderError(f"qwen_request_failed:{type(error).__name__}") from error
        if not isinstance(content, str):
            raise LLMProviderError("qwen_response_content_invalid")
        usage = body.get("usage", {})
        return LLMResponse(
            content=content,
            input_tokens=usage.get("prompt_tokens"),
            output_tokens=usage.get("completion_tokens"),
        )


class MockLLMProvider:
    """按预设响应顺序返回结果的测试 Provider，不访问外部服务。"""

    def __init__(self, responses: Sequence[str]) -> None:
        """保存待消费响应。

        参数：responses 为每次 complete 依次返回的 JSON 或异常文本。
        返回：无。
        异常：无。
        副作用：初始化可供测试检查的请求记录。
        """
        self._responses = list(responses)
        self.requests: list[LLMRequest] = []

    def complete(self, request: LLMRequest, *, timeout_seconds: float) -> LLMResponse:
        """消费下一个预设响应，模拟一次成功模型调用。

        参数：request 为网关请求；timeout_seconds 仅保持契约一致。
        返回：预设模型文本。
        异常：响应耗尽时抛出 LLMProviderError。
        副作用：记录请求并移除一个预设响应。
        """
        del timeout_seconds
        self.requests.append(request)
        if not self._responses:
            raise LLMProviderError("mock_responses_exhausted")
        return LLMResponse(content=self._responses.pop(0))
