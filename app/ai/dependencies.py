"""为 Worker 组装可替换的 T08 AI Gateway 运行时依赖。"""

from __future__ import annotations

from functools import lru_cache

from app.ai.gateway import AIGateway
from app.ai.provider import MockLLMProvider, QwenLLMProvider
from app.core.config import get_settings


@lru_cache
def get_ai_gateway() -> AIGateway:
    """依据运行配置返回 Qwen 或测试 Mock 网关。

    返回值：使用统一超时、重试和置信度阈值的可替换 AI Gateway。
    异常：配置阈值非法时由 AIGateway 抛出 ValueError。
    副作用：首次调用缓存 Provider 与 Gateway，不记录密钥或发起模型请求。
    """
    settings = get_settings()
    # Mock 只供测试或显式本地开发；Worker 的默认 qwen 配置始终走真实供应商边界。
    provider = (
        MockLLMProvider([])
        if settings.llm_provider == "mock"
        else QwenLLMProvider(
            api_key=settings.qwen_api_key or "",
            base_url=settings.qwen_base_url,
            model=settings.qwen_model,
        )
    )
    return AIGateway(
        provider,
        timeout_seconds=settings.ai_gateway_timeout_seconds,
        retry_count=settings.ai_gateway_retry_count,
        high_confidence_threshold=settings.ai_high_confidence_threshold,
        medium_confidence_threshold=settings.ai_medium_confidence_threshold,
    )
