"""OCR 与 ASR 的提供器接口及 Qwen、测试实现。"""

from __future__ import annotations

import base64
from collections.abc import Sequence
from typing import Protocol

import httpx


class MediaProviderError(RuntimeError):
    """表示 OCR 或 ASR 的可归一化外部调用失败。"""


class OCRProvider(Protocol):
    """定义图片到文本的供应商无关边界。"""

    def recognize(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """识别一张已校验图片中的文本。"""


class ASRProvider(Protocol):
    """定义音频到文本的供应商无关边界。"""

    def transcribe(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """转写一段未超过配置时长的已校验音频。"""


class FileScanProvider(Protocol):
    """定义媒体恶意内容扫描边界，供未来安全产品替换。"""

    def scan(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """返回 clean、infected 或 not_required 等受控扫描结论。"""


class NoopFileScanProvider:
    """首期不接入扫描引擎时返回明确的 not_required 状态。"""

    def scan(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """不读取媒体内容，保留首期扫描豁免这一审计状态。"""
        del content, mime_type, timeout_seconds
        return "not_required"


class QwenOCRProvider:
    """通过 Qwen OpenAI 兼容 Chat Completions 接口执行 OCR。"""

    def __init__(self, *, api_key: str, base_url: str, model: str = "qwen3.5-ocr") -> None:
        """保存运行时注入的连接参数，不记录密钥。"""
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model

    def recognize(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """向 Qwen 提交单张图片并返回文本，不记录图片内容。"""
        image_url = f"data:{mime_type};base64,{base64.b64encode(content).decode('ascii')}"
        content_parts = [
            {"type": "image_url", "image_url": {"url": image_url}},
            {"type": "text", "text": "仅输出图片中可辨认的文字，不要补充或猜测。"},
        ]
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": content_parts}],
        }
        try:
            response = httpx.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json=payload,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            text = response.json()["choices"][0]["message"]["content"]
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as error:
            raise MediaProviderError(f"qwen_ocr_failed:{type(error).__name__}") from error
        if not isinstance(text, str):
            raise MediaProviderError("qwen_ocr_response_invalid")
        return text


class QwenASRProvider:
    """通过 Qwen 兼容音频转写接口执行 ASR。"""

    def __init__(self, *, api_key: str, base_url: str, model: str = "qwen3-asr-flash") -> None:
        """保存运行时注入的连接参数，不记录密钥。"""
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model

    def transcribe(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """提交完整音频到 Qwen 转写端点，首期不执行自动切片。"""
        try:
            response = httpx.post(
                f"{self._base_url}/audio/transcriptions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                data={"model": self._model},
                files={"file": ("audio", content, mime_type)},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            text = response.json()["text"]
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            raise MediaProviderError(f"qwen_asr_failed:{type(error).__name__}") from error
        if not isinstance(text, str):
            raise MediaProviderError("qwen_asr_response_invalid")
        return text


class MockOCRProvider:
    """按预设顺序返回 OCR 文本的测试实现。"""

    def __init__(self, responses: Sequence[str | MediaProviderError]) -> None:
        """保存待返回的识别结果。"""
        self._responses = list(responses)

    def recognize(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """消费一个预设 OCR 结果，不访问外部服务。"""
        del content, mime_type, timeout_seconds
        return self._next()

    def _next(self) -> str:
        """取得下一个预设成功文本或抛出预设失败。"""
        if not self._responses:
            raise MediaProviderError("mock_ocr_responses_exhausted")
        result = self._responses.pop(0)
        if isinstance(result, MediaProviderError):
            raise result
        return result


class MockASRProvider(MockOCRProvider):
    """复用预设响应机制的 ASR 测试实现。"""

    def transcribe(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
        """消费一个预设 ASR 结果，不访问外部服务。"""
        del content, mime_type, timeout_seconds
        return self._next()
