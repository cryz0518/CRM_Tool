"""应用配置默认值与环境变量覆盖测试。"""

from __future__ import annotations

from app.core.config import Settings


def test_ai_gateway_defaults_use_sixty_second_timeout_and_one_retry(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """验证未配置环境变量时 AI Gateway 使用 60 秒超时和一次重试。

    参数：monkeypatch 用于清除可能污染测试的进程环境变量。
    返回值：无。
    异常：断言失败时由 pytest 报告。
    副作用：临时移除两个 AI Gateway 配置环境变量。
    """
    monkeypatch.delenv("AI_GATEWAY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("AI_GATEWAY_RETRY_COUNT", raising=False)

    settings = Settings(_env_file=None)

    assert settings.ai_gateway_timeout_seconds == 60.0
    assert settings.ai_gateway_retry_count == 1


def test_ai_gateway_timeout_environment_variable_overrides_default(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """验证环境变量可覆盖 AI Gateway 的默认超时。

    参数：monkeypatch 用于设置隔离的进程环境变量。
    返回值：无。
    异常：断言失败时由 pytest 报告。
    副作用：临时设置 AI Gateway 超时环境变量。
    """
    monkeypatch.setenv("AI_GATEWAY_TIMEOUT_SECONDS", "30")

    settings = Settings(_env_file=None)

    assert settings.ai_gateway_timeout_seconds == 30.0
