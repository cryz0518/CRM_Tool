"""在生产环境尚未注入真实智能表格适配器时显式阻止服务就绪。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import NoReturn

from app.smart_table.adapter import (
    SmartTableActor,
    SmartTableAdapterConfigurationError,
)
from app.smart_table.models import (
    SmartTablePermissions,
    SmartTableRecord,
    SmartTableSchema,
)


class UnconfiguredSmartTableAdapter:
    """表示尚未配置真实 CLI/API 适配器的安全占位实现。"""

    @staticmethod
    def _raise_not_configured() -> NoReturn:
        """统一抛出配置错误，防止业务流量误用占位适配器。

        异常：始终抛出 SmartTableAdapterConfigurationError。
        副作用：阻止调用方读取或修改智能表格。
        """
        raise SmartTableAdapterConfigurationError(
            "需要部署真实 CLI/API 适配器或显式启用 Mock"
        )

    def get_schema(self) -> SmartTableSchema:
        """尝试读取表结构，但未配置时明确失败。

        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def get_permissions(self) -> SmartTablePermissions:
        """尝试读取权限配置，但未配置时明确失败。

        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def get_record(self, record_id: str) -> SmartTableRecord | None:
        """尝试读取单条记录，但未配置时明确失败。

        参数：record_id 为目标记录标识。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def find_records(self, filters: Mapping[str, object]) -> list[SmartTableRecord]:
        """尝试查询记录，但未配置时明确失败。

        参数：filters 为字段匹配条件。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def get_records(self) -> list[SmartTableRecord]:
        """尝试列出记录，但未配置时明确失败。

        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def create_record(
        self,
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
    ) -> SmartTableRecord:
        """尝试创建记录，但未配置时明确失败。

        参数：fields 为待写字段；actor 为调用主体。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()

    def update_record(self, record_id: str, fields: Mapping[str, object]) -> SmartTableRecord:
        """尝试增量更新记录，但未配置时明确失败。

        参数：record_id 为目标记录标识；fields 为字段补丁。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()
