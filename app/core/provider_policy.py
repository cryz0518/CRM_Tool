"""统一管理各类 Provider 的环境策略和 fail-closed 行为。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.config import Settings


PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod"})
"""生产环境名称集合，避免各工厂重复实现环境判断。"""

FORBIDDEN_PROVIDER_NAMES = frozenset({
    "mock",
    "fake",
    "local",
    "noop",
    "unconfigured",
    "development",
    "dev",
})
"""生产环境不得使用的测试、开发或空 Provider 名称。"""

PROVIDER_IMPLEMENTATIONS = {
    "admin_identity_provider": frozenset({"development"}),
    "smart_table": frozenset({"mock", "wecom_cli"}),
    "crm": frozenset({"mock"}),
    "llm": frozenset({"mock", "qwen"}),
    "ocr": frozenset({"mock", "qwen"}),
    "asr": frozenset({"mock", "qwen"}),
    "storage": frozenset({"local", "fake"}),
    "scanner": frozenset({"noop", "fake"}),
}
"""声明当前代码实际存在的 Provider 实现，避免名称与 factory 行为漂移。"""


@dataclass(frozen=True)
class ProviderPolicyResult:
    """保存单个 Provider 的脱敏策略判定结果。"""

    component: str
    provider: str
    status: str
    reason_code: str

    def as_dict(self) -> dict[str, str]:
        """将策略结果转换为不含凭据的机器可读字典。

        返回值：只包含组件、状态、Provider 名称和稳定原因码的字典。
        异常：无。
        副作用：无。
        """
        return {
            "component": self.component,
            "status": self.status,
            "reason_code": self.reason_code,
        }


class ProviderPolicyError(RuntimeError):
    """表示 Provider 选择违反统一环境策略。"""

    def __init__(self, result: ProviderPolicyResult) -> None:
        """用脱敏判定结果构造错误，禁止携带配置值或外部响应。

        参数：result 为已完成的策略判定。
        返回值：无。
        异常：无。
        副作用：仅构造受控错误文本。
        """
        self.result = result
        super().__init__(f"{result.component} provider policy denied: {result.reason_code}")


class ProviderPolicy:
    """集中执行 production 与非生产环境的 Provider 选择策略。"""

    def __init__(self, app_env: str) -> None:
        """保存规范化环境名，后续工厂不再自行读取 APP_ENV。

        参数：app_env 为部署环境名称。
        返回值：无。
        异常：无。
        副作用：无外部调用。
        """
        self._app_env = app_env.strip().lower()

    @property
    def is_production(self) -> bool:
        """返回当前环境是否属于生产环境。"""
        return self._app_env in PRODUCTION_ENVIRONMENTS

    def evaluate(
        self,
        component: str,
        provider: str | None,
        *,
        settings: Settings | None = None,
    ) -> ProviderPolicyResult:
        """判断一个 Provider 是否可以在当前环境使用。

        参数：component 为能力组件名；provider 为配置中的 Provider 名称；settings 为可选配置。
        返回值：脱敏的 ready/not_ready 判定结果。
        异常：无。
        副作用：无。
        """
        normalized = (provider or "").strip().lower()
        if not normalized or normalized == "unconfigured":
            return ProviderPolicyResult(component, normalized, "not_ready", "provider_missing")
        if self.is_production and normalized in FORBIDDEN_PROVIDER_NAMES:
            return ProviderPolicyResult(
                component,
                normalized,
                "not_ready",
                "production_test_provider_forbidden",
            )
        if normalized not in PROVIDER_IMPLEMENTATIONS.get(component, frozenset()):
            return ProviderPolicyResult(component, normalized, "not_ready", "provider_unavailable")
        missing = self._missing_configuration(settings, component, normalized)
        if missing:
            return ProviderPolicyResult(
                component, normalized, "not_ready", "provider_configuration_missing"
            )
        return ProviderPolicyResult(component, normalized, "ok", "provider_allowed")

    def require(
        self,
        component: str,
        provider: str | None,
        *,
        settings: Settings | None = None,
    ) -> ProviderPolicyResult:
        """校验 Provider，失败时抛出安全的策略错误。

        参数：component 为能力组件名；provider 为配置中的 Provider 名称。
        返回值：通过策略的判定结果。
        异常：Provider 缺失或生产环境使用测试 Provider 时抛出 ProviderPolicyError。
        副作用：无外部调用，不会创建 Provider。
        """
        result = self.evaluate(component, provider, settings=settings)
        if result.status != "ok":
            raise ProviderPolicyError(result)
        return result

    def evaluate_settings(self, settings: Settings) -> tuple[ProviderPolicyResult, ...]:
        """检查项目首期所有外部 Provider 选择，不连接外部系统。

        参数：settings 为已解析的应用配置。
        返回值：管理员身份、智能表格、CRM、AI、OCR、ASR、存储和扫描器结果。
        异常：无。
        副作用：无网络、数据库或文件写操作。
        """
        selections = (
            ("admin_identity_provider", getattr(settings, "admin_identity_provider", None)),
            ("smart_table", settings.smart_table_adapter),
            ("crm", settings.crm_adapter),
            ("llm", settings.llm_provider),
            ("ocr", settings.ocr_provider),
            ("asr", settings.asr_provider),
            ("storage", settings.media_storage_provider),
            ("scanner", settings.media_scanner_provider),
        )
        return tuple(
            self.evaluate(component, provider, settings=settings)
            for component, provider in selections
        )

    def _missing_configuration(
        self, settings: Settings | None, component: str, provider: str
    ) -> tuple[str, ...]:
        """返回当前 Provider 缺失的必要配置字段名。

        参数：settings 为可选应用配置；component 和 provider 标识 Provider。
        返回值：缺失字段名集合；不会返回字段值或密钥内容。
        异常：无。
        副作用：无。
        """
        if settings is None:
            return ()
        required: tuple[str, ...] = ()
        if component in {"llm", "ocr", "asr"} and provider == "qwen":
            required = (
                ("qwen_api_key",)
                if self.is_production
                else ()
            )
        elif component == "smart_table" and provider == "wecom_cli":
            required = ("wecom_smart_table_doc_id", "wecom_smart_table_sheet_id")
        return tuple(
            field
            for field in required
            if not str(getattr(settings, field, "") or "").strip()
        )


def get_provider_policy(settings: Settings) -> ProviderPolicy:
    """根据配置创建统一 Provider policy。

    参数：settings 为应用配置。
    返回值：供所有 Provider 工厂复用的策略对象。
    异常：无。
    副作用：无外部调用。
    """
    return ProviderPolicy(settings.app_env)
