"""按适配器缓存管理员枚举，并为一次线索处理固定同一结构版本。"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Hashable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache, wraps
from threading import RLock
from time import monotonic
from types import MappingProxyType
from typing import Any, ParamSpec, TypeVar, cast

from app.core.config import get_settings
from app.smart_table.adapter import SmartTableAdapter, SmartTableAdapterConfigurationError
from app.smart_table.models import SmartTableFieldType, SmartTableSchema
from app.smart_table.registry import (
    DEFAULT_LEAD_BUSINESS_VALUES,
    INTERNATIONAL_CUSTOMER_OPTIONS,
    REQUIRED_SMART_TABLE_FIELDS,
)

BUSINESS_ENUM_FIELDS = frozenset({"业务线", "线索来源", "沟通方式", "客户行业", "客户级别", "工艺"})
logger = logging.getLogger(__name__)
_P = ParamSpec("_P")
_R = TypeVar("_R")


class EnumConfigurationError(SmartTableAdapterConfigurationError):
    """携带可展示的字段配置问题，禁止业务回退到旧枚举或第一个选项。"""

    def __init__(self, issues: tuple[str, ...]) -> None:
        """保存中文配置问题；返回无，副作用仅初始化异常，不包含外部响应或凭据。"""
        self.issues = issues
        super().__init__("；".join(issues))


def schema_configuration_issues(schema: SmartTableSchema) -> tuple[str, ...]:
    """校验固定字段类型、动态选项、默认值与系统枚举；返回问题且不修改表结构。"""
    issues: list[str] = []
    names = [field.name.removeprefix("*") for field in schema.fields]
    for requirement in REQUIRED_SMART_TABLE_FIELDS:
        # 规范名称冲突必须拒绝，不能按字段顺序绑定其中一个。
        if names.count(requirement.name) > 1:
            issues.append(f"字段同名冲突：{requirement.name}")
            continue
        field = schema.get_field(requirement.name)
        if field is None:
            issues.append(f"缺少必需字段：{requirement.name}")
            continue
        if field.field_type is not requirement.field_type:
            issues.append(
                f"字段类型不匹配：{requirement.name}，期望 {requirement.field_type.value}，"
                f"实际 {field.field_type.value}"
            )
            continue
        if field.field_type not in {
            SmartTableFieldType.SINGLE_SELECT,
            SmartTableFieldType.MULTI_SELECT,
        }:
            continue
        options = [option.name.removeprefix("*") for option in field.options]
        if not options or any(not option.strip() for option in options):
            issues.append(f"字段枚举选项为空：{requirement.name}")
        if len(set(options)) != len(options):
            issues.append(f"字段枚举同名冲突：{requirement.name}")
        # 六个业务字段允许增删改名；系统语义及字段名白名单仍要求固定选项。
        required = (
            (
                (DEFAULT_LEAD_BUSINESS_VALUES[requirement.name],)
                if requirement.name in DEFAULT_LEAD_BUSINESS_VALUES
                else ()
            )
            if requirement.name in BUSINESS_ENUM_FIELDS
            else requirement.required_options
        )
        for option in required:
            if option not in options:
                issues.append(f"字段枚举选项缺失：{requirement.name}，缺少 {option}")
    return tuple(issues)


@dataclass(frozen=True)
class EnumSnapshot:
    """承载同一版本的真实表结构和只读合法选项，不包含 CRM 数字映射。"""

    schema: SmartTableSchema
    options: Mapping[str, tuple[str, ...]]
    version: str


class EnumSnapshotService:
    """用单调时钟和进程锁刷新结构，处理范围内延迟绑定且禁止使用过期失败缓存。"""

    def __init__(self, loader: Callable[[], SmartTableSchema], ttl_seconds: int = 300) -> None:
        """注入只读加载器与正数 TTL；非法值抛 ValueError，仅创建缓存和上下文。"""
        if ttl_seconds <= 0:
            raise ValueError("SMART_TABLE_ENUM_CACHE_SECONDS 必须大于 0")
        self._loader = loader
        self._ttl_seconds = ttl_seconds
        self._schema: SmartTableSchema | None = None
        self._deadline = 0.0
        self._lock = RLock()
        self._scope: ContextVar[dict[str, SmartTableSchema] | None] = ContextVar(
            "smart_table_enum_scope", default=None
        )

    def get_schema(self) -> SmartTableSchema:
        """返回范围内固定或 TTL 有效的结构；加载失败传播异常，不回退旧结构或改历史数据。"""
        scope = self._scope.get()
        if scope is not None and "schema" in scope:
            return scope["schema"]
        with self._lock:
            # 只有成功加载才延长有效期；失败后的下一次调用继续尝试刷新。
            if self._schema is None or monotonic() >= self._deadline:
                try:
                    schema = self._loader()
                except Exception as error:
                    logger.warning(
                        "smart_table_enum_refresh_failed",
                        extra={"error_type": type(error).__name__},
                    )
                    raise
                self._schema = schema
                self._deadline = monotonic() + self._ttl_seconds
                logger.info("smart_table_enum_refreshed")
            if scope is not None:
                scope["schema"] = self._schema
            return self._schema

    def get_snapshot(self) -> EnumSnapshot:
        """构造已校验的只读枚举快照；配置异常抛 EnumConfigurationError，无写表副作用。"""
        schema = self.get_schema()
        issues = schema_configuration_issues(schema)
        if issues:
            raise EnumConfigurationError(issues)
        # 保留类型和 option ID 在版本摘要中，AI 与写入只读取同一 schema。
        options = {
            field.name.removeprefix("*"): tuple(
                option.name.removeprefix("*") for option in field.options
            )
            for field in schema.fields
            if field.name.removeprefix("*") in BUSINESS_ENUM_FIELDS | {"是否为国际客户"}
        }
        # 国内/国外是固定系统语义；管理员额外新增选项不能扩展模型的地域判定。
        options["是否为国际客户"] = INTERNATIONAL_CUSTOMER_OPTIONS
        version = hashlib.sha256(repr(schema).encode()).hexdigest()[:16]
        return EnumSnapshot(schema, MappingProxyType(options), version)

    def source_defaults(self) -> dict[str, str]:
        """返回仍合法的展会场景默认来源；删除或改名后留空，不猜新默认且不修改历史记录。"""
        return {"线索来源": "展会"} if "展会" in self.get_snapshot().options["线索来源"] else {}

    @contextmanager
    def scope(self, *, isolate: bool = False) -> Iterator[None]:
        """延迟固定当前调用的结构；isolate 为真时开新快照上下文，退出恢复父上下文。"""
        if self._scope.get() is not None and not isolate:
            yield
            return
        token = self._scope.set({})
        try:
            yield
        finally:
            self._scope.reset(token)


def get_enum_snapshot_service(adapter: SmartTableAdapter) -> EnumSnapshotService:
    """复用适配器的快照服务；返回进程缓存，CLI 使用原始加载器避免双重 TTL 或递归。"""
    existing = getattr(adapter, "_enum_snapshots", None)
    if isinstance(existing, EnumSnapshotService):
        return existing
    # 稳定适配器实例按身份复用；Protocol 不声明 __hash__，仅在缓存边界显式收窄类型。
    return _cached_enum_snapshot_service(cast(Hashable, adapter))


@lru_cache(maxsize=128)
def _cached_enum_snapshot_service(adapter: SmartTableAdapter) -> EnumSnapshotService:
    """缓存非 CLI 适配器的只读加载器；返回服务，首次实际读取才访问适配器接口。"""
    return EnumSnapshotService(
        lambda: adapter.get_schema(), get_settings().smart_table_enum_cache_seconds
    )


def enum_snapshot_operation(method: Callable[_P, _R]) -> Callable[_P, _R]:
    """为持有快照服务的方法固定一次处理版本；返回包装方法，异常原样传播。"""
    return _enum_snapshot_operation(method, isolate=False)


def isolated_enum_snapshot_operation(method: Callable[_P, _R]) -> Callable[_P, _R]:
    """为独立消息固定独立快照上下文；返回包装方法，异常原样传播。"""
    return _enum_snapshot_operation(method, isolate=True)


def _enum_snapshot_operation(
    method: Callable[_P, _R], *, isolate: bool
) -> Callable[_P, _R]:
    """执行快照作用域包装；isolate 控制嵌套操作是否开启独立消息版本。"""

    @wraps(method)
    def scoped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        """在调用对象的枚举范围内执行方法，返回原结果并保证异常后释放上下文。"""
        owner: Any = args[0]
        with owner._enum_snapshots.scope(isolate=isolate):
            return method(*args, **kwargs)

    return scoped
