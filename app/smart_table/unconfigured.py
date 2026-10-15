"""在生产环境尚未注入真实智能表格适配器时显式阻止服务就绪。"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from datetime import date
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

    def find_records_by_field_values(
        self, field_name: str, values: Collection[str]
    ) -> list[SmartTableRecord]:
        """尝试按一个字段的多个文本值查找记录，但未配置时明确失败。

        参数：field_name 为字段名；values 为待匹配文本值集合。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        副作用：阻止调用方误以为幂等查重已成功。
        """
        self._raise_not_configured()

    def get_records(
        self,
        record_ids: Collection[str] | None = None,
        *,
        owner_user_ids: Collection[str] | None = None,
        owner_display_names: Mapping[str, str] | None = None,
        submission_status: str | None = None,
        capture_date: date | None = None,
    ) -> list[SmartTableRecord]:
        """尝试列出全部、指定记录或负责人范围记录，但未配置时明确失败。

        参数：record_ids 和负责人身份/显示名为可选过滤范围；提交状态和日期仅作范围过滤。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        副作用：阻止调用方读取或修改智能表格。
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

    def create_records(
        self, records: Sequence[Mapping[str, object]], *, actor: SmartTableActor
    ) -> list[SmartTableRecord]:
        """尝试批量创建记录，但未配置时明确失败。

        参数：records 为批量字段映射；actor 为调用主体。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        副作用：阻止调用方写入未配置的目标表。
        """
        self._raise_not_configured()

    def update_record(
        self,
        record_id: str,
        fields: Mapping[str, object],
        *,
        skip_preflight: bool = False,
    ) -> SmartTableRecord:
        """尝试增量更新记录，但未配置时明确失败。

        参数：record_id 为目标记录标识；fields 为字段补丁；skip_preflight 为已持有创建快照标记。
        异常：始终抛出 SmartTableAdapterConfigurationError。
        """
        self._raise_not_configured()
