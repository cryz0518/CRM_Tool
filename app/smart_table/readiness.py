"""智能表格管理员预配置的就绪检查。"""

from __future__ import annotations

import logging
from time import perf_counter

from app.smart_table.adapter import SmartTableAdapter, SmartTableAdapterConfigurationError
from app.smart_table.models import SmartTableReadinessReport
from app.smart_table.registry import REQUIRED_SMART_TABLE_FIELDS

logger = logging.getLogger(__name__)


class SmartTableReadinessChecker:
    """确定性校验核心字段、字段类型、枚举和普通销售权限。"""

    def check(self, adapter: SmartTableAdapter) -> SmartTableReadinessReport:
        """检查适配器读取的表格配置，并返回可直接展示的中文问题列表。

        参数：adapter 为业务层注入的稳定智能表格适配器。
        返回：包含是否就绪和全部中文配置问题的报告。
        异常：所有适配器异常均转换为稳定 not-ready 报告，不向 API 泄露异常正文。
        副作用：记录结构化的适配器、耗时、结果和问题数量日志。
        """
        adapter_name = type(adapter).__name__
        started_at = perf_counter()
        try:
            # 先读取真实表配置；未配置时必须阻止服务被误判为可接收业务流量。
            schema = adapter.get_schema()
            permissions = adapter.get_permissions()
        except SmartTableAdapterConfigurationError:
            # 保留既有中文问题契约，但不插入异常正文，避免携带 endpoint 或 token。
            issue = "智能表格适配器未配置：需要部署真实 CLI/API 适配器或显式启用 Mock"
            report = SmartTableReadinessReport(ready=False, issues=(issue,))
            self._log_report(adapter_name, started_at, report)
            return report
        except Exception as error:
            # 只记录异常类型；traceback 和外部响应可能携带 secret/token，禁止进入日志或 API。
            logger.error(
                "smart_table_readiness_failed",
                extra={
                    "smart_table_adapter": adapter_name,
                    "readiness_status": "error",
                    "duration_ms": round((perf_counter() - started_at) * 1000, 2),
                    "error_type": type(error).__name__,
                },
            )
            report = SmartTableReadinessReport(
                ready=False,
                issues=("智能表格依赖不可用",),
            )
            self._log_report(adapter_name, started_at, report)
            return report

        issues: list[str] = []

        for requirement in REQUIRED_SMART_TABLE_FIELDS:
            # 按名称绑定管理员维护的字段，缺失或类型偏差均不允许静默降级。
            field = schema.get_field(requirement.name)
            if field is None:
                issues.append(f"缺少必需字段：{requirement.name}")
                continue
            if field.field_type is not requirement.field_type:
                issues.append(
                    "字段类型不匹配："
                    f"{requirement.name}，期望 {requirement.field_type.value}，"
                    f"实际 {field.field_type.value}"
                )
                continue

            # 仅要求项目依赖的选项存在，允许管理员保留不影响业务的额外选项。
            # AI待确认的选项复用管理员字段展示名；必填字段的 `*` 前缀不参与匹配。
            configured_option_names = {option.name.removeprefix("*") for option in field.options}
            for option in requirement.required_options:
                if option not in configured_option_names:
                    issues.append(f"字段枚举选项缺失：{requirement.name}，缺少 {option}")

        # 普通销售只能经机器人创建或废弃记录，权限偏差会破坏审计和记录级隔离。
        if permissions.sales_can_create_records:
            issues.append("销售权限配置错误：sales_can_create_records 必须为 false")
        if permissions.sales_can_delete_records:
            issues.append("销售权限配置错误：sales_can_delete_records 必须为 false")
        report = SmartTableReadinessReport(ready=not issues, issues=tuple(issues))
        self._log_report(adapter_name, started_at, report)
        return report

    @staticmethod
    def _log_report(
        adapter_name: str,
        started_at: float,
        report: SmartTableReadinessReport,
    ) -> None:
        """输出不含表格敏感数据的就绪检查结构化日志。

        参数：adapter_name 为适配器类型；started_at 为单调时钟起点；report 为检查结果。
        副作用：向应用结构化日志写入适配器、耗时、状态和问题数量。
        """
        logger_method = logger.info if report.ready else logger.warning
        logger_method(
            "smart_table_readiness_checked",
            extra={
                "smart_table_adapter": adapter_name,
                "readiness_status": "ready" if report.ready else "not_ready",
                "readiness_issue_count": len(report.issues),
                "duration_ms": round((perf_counter() - started_at) * 1000, 2),
            },
        )
