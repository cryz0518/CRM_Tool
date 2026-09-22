"""统一的只读生产 readiness 组件注册和安全输出框架。"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReadinessComponent:
    """表示单个依赖的脱敏 readiness 状态。"""

    component: str
    status: str
    reason_code: str

    def as_dict(self) -> dict[str, str]:
        """转换为只含稳定字段的机器可读结果。

        返回值：组件、状态和原因码字典。
        异常：无。
        副作用：无。
        """
        return {
            "component": self.component,
            "status": self.status,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True)
class ReadinessReport:
    """汇总所有组件的 readiness 结果和稳定问题列表。"""

    components: tuple[ReadinessComponent, ...]

    @property
    def ready(self) -> bool:
        """返回是否所有注册组件均已就绪。"""
        return all(item.status == "ok" for item in self.components)

    @property
    def issues(self) -> tuple[str, ...]:
        """返回不含异常正文、凭据和令牌的稳定问题摘要。"""
        return tuple(
            f"{item.component}:{item.reason_code}"
            for item in self.components
            if item.status != "ok"
        )

    def as_dict(self) -> dict[str, object]:
        """转换为 readiness HTTP/CLI 共用的安全字典。

        返回值：包含 overall status、组件列表和稳定问题码的字典。
        异常：无。
        副作用：无。
        """
        return {
            "status": "ok" if self.ready else "error",
            "components": [item.as_dict() for item in self.components],
            "issues": list(self.issues),
        }


class ReadinessRegistry:
    """按组件名称执行只读检查，并将意外异常转换为安全原因码。"""

    def __init__(self, checks: Mapping[str, Callable[[], ReadinessComponent]]) -> None:
        """保存组件检查函数，不在构造阶段触发任何探针。

        参数：checks 为组件名到只读检查函数的映射。
        返回值：无。
        异常：无。
        副作用：无。
        """
        self._checks = dict(checks)

    def check(self) -> ReadinessReport:
        """执行全部组件检查并生成统一报告。

        返回值：按注册顺序排列的 readiness 报告。
        异常：单个检查的未预期异常不会向外泄露，转换为 dependency_unavailable。
        副作用：调用注册的只读探针；不会执行写操作。
        """
        results: list[ReadinessComponent] = []
        for component, check in self._checks.items():
            try:
                result = check()
                if result.component != component:
                    # 检查函数不能伪造组件归属，统一以注册键为准。
                    result = ReadinessComponent(component, result.status, result.reason_code)
            except Exception as error:
                # readiness 输出不能泄露 traceback、token 或 endpoint，只保留异常类型日志。
                logger.error(
                    "readiness_component_probe_failed",
                    extra={"component": component, "error_type": type(error).__name__},
                )
                result = ReadinessComponent(component, "not_ready", "dependency_unavailable")
            results.append(result)
        return ReadinessReport(tuple(results))


def ready_component(component: str, reason_code: str = "checked") -> ReadinessComponent:
    """创建一个成功的 readiness 结果。

    参数：component 为组件名；reason_code 为非敏感成功原因码。
    返回值：status 为 ok 的组件结果。
    异常：无。
    副作用：无。
    """
    return ReadinessComponent(component, "ok", reason_code)


def not_ready_component(component: str, reason_code: str) -> ReadinessComponent:
    """创建一个失败的 readiness 结果。

    参数：component 为组件名；reason_code 为稳定、非敏感失败原因码。
    返回值：status 为 not_ready 的组件结果。
    异常：无。
    副作用：无。
    """
    return ReadinessComponent(component, "not_ready", reason_code)
