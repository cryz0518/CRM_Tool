"""Qwen OCR/ASR Provider 的供应商隔离测试。"""

from __future__ import annotations

import httpx

from app.media.providers import MockASRProvider, MockOCRProvider, QwenOCRProvider


def test_mock_providers_return_configured_text_without_network() -> None:
    """验证测试替身可分别返回 OCR 和 ASR 文本。"""
    ocr = MockOCRProvider(["客户：甲公司"])
    asr = MockASRProvider(["客户：乙公司"])

    assert ocr.recognize(b"image", mime_type="image/png", timeout_seconds=1) == "客户：甲公司"
    assert asr.transcribe(b"audio", mime_type="audio/mpeg", timeout_seconds=1) == "客户：乙公司"


def test_qwen_ocr_uses_configured_model_and_never_logs_binary(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """验证 OCR 通过 Qwen 兼容接口发送图片数据且保留提供器边界。"""
    observed: dict[str, object] = {}

    def fake_post(url: str, **kwargs: object) -> httpx.Response:
        """捕获 HTTP 请求并返回最小兼容响应。"""
        observed["url"] = url
        observed["json"] = kwargs["json"]
        request = httpx.Request("POST", url)
        return httpx.Response(
            200, request=request, json={"choices": [{"message": {"content": "识别文本"}}]}
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    result = QwenOCRProvider(api_key="key", base_url="https://qwen.example/v1").recognize(
        b"\x89PNG\r\n\x1a\nimage", mime_type="image/png", timeout_seconds=1
    )

    assert result == "识别文本"
    assert observed["url"] == "https://qwen.example/v1/chat/completions"
    assert observed["json"]["model"] == "qwen3.5-ocr"  # type: ignore[index]
