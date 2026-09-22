"""为 Worker 组装可替换的 T08 AI Gateway 运行时依赖。"""

from __future__ import annotations

from app.ai.gateway import AIGateway
from app.ai.persistence import AIExecutionRecorder
from app.ai.provider import MockLLMProvider, QwenLLMProvider
from app.core.config import get_settings
from app.core.provider_policy import get_provider_policy


def get_ai_gateway(*, execution_recorder: AIExecutionRecorder | None = None) -> AIGateway:
    """依据运行配置返回 Qwen 或测试 Mock 网关。

    参数：execution_recorder 为可选的脱敏执行记录器。
    返回值：使用统一超时、重试和置信度阈值的可替换 AI Gateway。
    异常：配置阈值非法时由 AIGateway 抛出 ValueError。
    副作用：构造 Provider 与 Gateway，不记录密钥或发起模型请求。
    """
    settings = get_settings()
    # LLM 的测试 Provider 只能在非生产环境显式使用。
    policy = get_provider_policy(settings)
    provider_result = policy.require("llm", settings.llm_provider, settings=settings)
    # Mock 只供测试或显式本地开发；Worker 的默认 qwen 配置始终走真实供应商边界。
    provider = (
        MockLLMProvider([])
        if provider_result.provider == "mock"
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
        execution_recorder=execution_recorder,
    )
