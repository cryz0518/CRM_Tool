"""统一执行脱敏、追踪、重试与确定性校验的 AI Gateway。"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import uuid4

from pydantic import ValidationError

from app.ai.models import ExtractedLeadPatch, LeadAnalysis, LeadFieldValue, LLMRequest, LLMResponse
from app.ai.persistence import AIExecutionRecorder, AIExecutionRecorderEvent
from app.ai.provider import LLMProvider, LLMProviderError
from app.core.failures import PermanentTaskFailure, RetryableTaskFailure
from app.smart_table.registry import (
    BUSINESS_LINE_OPTIONS,
    COMMUNICATION_METHOD_OPTIONS,
    CRM_BUSINESS_FIELD_NAMES,
    CUSTOMER_INDUSTRY_OPTIONS,
    CUSTOMER_LEVEL_OPTIONS,
    ENUM_FIELDS_WITH_OTHER,
    INTERNATIONAL_CUSTOMER_OPTIONS,
    LEAD_SOURCE_OPTIONS,
    PROCESS_OPTIONS,
)

logger = logging.getLogger(__name__)

_ENUM_OPTIONS = {
    "业务线": BUSINESS_LINE_OPTIONS,
    "线索来源": LEAD_SOURCE_OPTIONS,
    "沟通方式": COMMUNICATION_METHOD_OPTIONS,
    "客户行业": CUSTOMER_INDUSTRY_OPTIONS,
    "客户级别": CUSTOMER_LEVEL_OPTIONS,
    "工艺": PROCESS_OPTIONS,
    "是否为国际客户": INTERNATIONAL_CUSTOMER_OPTIONS,
}
_PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9 -]{5,24}$")
_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_PHONE_SEARCH_PATTERN = re.compile(r"(?<!\d)(?:\+?86[ -]?)?(1[3-9]\d{9})(?!\d)")
_EMAIL_SEARCH_PATTERN = re.compile(r"(?<![\w.+-])[\w.+-]+@[\w-]+(?:\.[\w-]+)+(?![\w.-])")
_COMMUNICATION_EVIDENCE = (
    ("线上会议", "线上会议"),
    ("电话", "打电话"),
    ("邮件", "发邮件"),
    ("微信", "微信"),
    ("拜访", "见面拜访"),
    ("现场", "见面拜访"),
)
_COMMUNICATION_ALIASES = frozenset(
    {
        "电话",
        "电话沟通",
        "电话咨询",
        "邮件",
        "邮件沟通",
        "邮件咨询",
        "微信沟通",
        "微信联系",
        "拜访",
        "现场",
        "现场拜访",
    }
)
_SENSITIVE_PATTERN = re.compile(
    r"(?i)(?:(?<!\d)(?:\d{17}[\dXx]|\d{16,19}|\d{15})(?!\d)|"
    r"(?:密码|口令|验证码|password|token)\s*[:：]?\s*[^\s；;，,]+)"
)
_ENRICHMENT_ONLY_FIELD_NAMES = frozenset(
    {"城市/地区", "主营产品", "年销售额", "客户需求/痛点", "预算", "特殊要求"}
)
_ENRICHMENT_FIELD_NAMES = _ENRICHMENT_ONLY_FIELD_NAMES | ENUM_FIELDS_WITH_OTHER
_FORBIDDEN_AI_CRM_FIELD_NAMES = frozenset({"备注"})
_NON_QUARANTINABLE_AI_FIELD_NAMES = frozenset(
    {"备注", "AI待确认", "创建人", "负责人", "智能表格负责人", "CRM线索负责人"}
)
_CUSTOMER_REFERENCE_FIELD_ALIASES = {
    "company": "线索名称",
    "company_name": "线索名称",
    "企业": "线索名称",
    "企业名称": "线索名称",
    "公司": "线索名称",
    "公司名称": "线索名称",
    "线索名称": "线索名称",
    "contact": "联系人",
    "contact_name": "联系人",
    "联系人": "联系人",
    "联系人姓名": "联系人",
    "title": "职务",
    "job_title": "职务",
    "position": "职务",
    "role": "职务",
    "职务": "职务",
    "职位": "职务",
    "岗位": "职务",
    "mobile": "手机",
    "mobile_phone": "手机",
    "phone": "手机",
    "手机号": "手机",
    "手机": "手机",
    "telephone": "电话",
    "tel": "电话",
    "电话": "电话",
    "email": "邮箱",
    "email_address": "邮箱",
    "邮箱": "邮箱",
}


class AIGatewayError(RuntimeError):
    """表示网关已经完成重试和错误归一化后的失败结论。"""


class AIGatewayTransportError(AIGatewayError, RetryableTaskFailure):
    """表示模型传输重试耗尽但仍可由人工任务重试的暂态故障。"""


class BusinessValidationError(AIGatewayError, PermanentTaskFailure):
    """表示结构合法但不满足 CRM 字段业务规则的候选。"""


class FailedStructuredOutputError(AIGatewayError, PermanentTaskFailure):
    """承载待人工审查所需的两次模型输出和结构校验摘要。"""

    def __init__(self, original_output: str, repaired_output: str, error_summary: str) -> None:
        """保存失败事实，供任务层以 failed_pending_review 状态持久化审计。

        参数：original_output 与 repaired_output 为两次原始结果；error_summary 为校验摘要。
        返回：无。
        异常：无。
        副作用：异常对象保留待持久化的失败事实，日志不输出原文。
        """
        super().__init__("ai_structured_output_failed_pending_review")
        self.original_output = original_output
        self.repaired_output = repaired_output
        self.error_summary = error_summary
        self.task_status = "failed_pending_review"


class AIGateway:
    """封装 LLM 调用，使业务层不接触供应商、原始输出或敏感日志。"""

    def __init__(
        self,
        provider: LLMProvider,
        *,
        timeout_seconds: float = 60.0,
        retry_count: int = 1,
        high_confidence_threshold: float = 0.85,
        medium_confidence_threshold: float = 0.60,
        execution_recorder: AIExecutionRecorder | None = None,
    ) -> None:
        """注入 Provider 与冻结为配置的调用、置信度参数。

        参数：provider 为可替换模型边界；其余参数控制传输重试和候选分级。
        返回：无。
        异常：阈值或重试数非法时抛出 ValueError。
        副作用：仅保存依赖和配置。
        """
        valid_thresholds = 0 <= medium_confidence_threshold <= high_confidence_threshold <= 1
        if retry_count < 0 or not valid_thresholds:
            raise ValueError("AI Gateway 配置非法")
        self._provider = provider
        self._timeout_seconds = timeout_seconds
        self._retry_count = retry_count
        self._high_confidence_threshold = high_confidence_threshold
        self._medium_confidence_threshold = medium_confidence_threshold
        self._execution_recorder = execution_recorder

    def extract_fields(
        self,
        text: str,
        *,
        source_message_id: str | None = None,
        lead_id: str | None = None,
        context_fields: Mapping[str, str] | None = None,
    ) -> ExtractedLeadPatch:
        """提取一条文本的字段补丁并完成 Parse、Schema 与 Business Validation。

        参数：text 为已持久化消息中本次字段提取必要的文本；context_fields 为当前销售最近
        线索的非敏感字段，仅供模型判断新客户或补充消息。
        返回：正式字段、中置信度待确认字段及低置信度后台候选。
        异常：传输/结构二次失败时抛出 AIGatewayError；业务校验失败抛出 BusinessValidationError。
        副作用：调用 Provider，输出不含原文和联系方式的结构化日志。
        """
        trace_id = str(uuid4())
        # 只将字段提取所需文本交给模型，并在发送前移除无关的高敏感信息。
        safe_text = _SENSITIVE_PATTERN.sub("[已遮蔽敏感号码]", text)
        safe_context_fields = {
            field_name: _SENSITIVE_PATTERN.sub("[已遮蔽敏感号码]", value)
            for field_name, value in (context_fields or {}).items()
            if value
        }
        json_schema = self._output_json_schema()
        request = LLMRequest(
            messages=self._messages(safe_text, json_schema, safe_context_fields),
            json_schema=json_schema,
        )
        started_at = time.monotonic()
        try:
            # 先取得可观测响应，确保 repair 与传输重试也计入调用量。
            response, attempts = self._call_with_transport_retry(request, trace_id)
            analysis, repair_response, repair_attempts = self._parse_schema_or_repair(
                request, response, trace_id
            )
            # 只把原文可逐字核验的身份引用提升为正式字段，避免自然表达因模型字段放错位置而待归属。
            analysis = self._recover_verified_identity_fields(analysis, safe_text)
            # 先将少量可确定映射的沟通自然表达归一化，再执行枚举校验。
            analysis = self._normalize_communication_candidate(analysis, safe_text)
            # 兼容模型把预算、痛点等备注素材误放入 CRM 字段的结果，先归位再做业务白名单校验。
            analysis = self._normalize_enrichment_field_placement(analysis, safe_text)
            # 兼容 Qwen 兼容模式偶发漏返回置信度键的结果；无置信度字段不参与写入，但不阻塞其他字段。
            analysis = self._drop_fields_missing_confidence(analysis)
            self._validate_business(analysis)
            validated_enrichment = self._validate_enrichment_evidence(analysis, safe_text)
            # 补充信息只是备注素材；证据不足时丢弃该字段，不能阻塞已通过校验的 CRM 主字段。
            analysis = analysis.model_copy(update={"enrichment": validated_enrichment})
            fields, pending, low_candidates = self._apply_confidence(analysis, safe_text)
        except AIGatewayError as error:
            self._log(trace_id, started_at, "failed", error_type=type(error).__name__)
            self._record_execution(
                trace_id,
                status="failed",
                source_message_id=source_message_id,
                lead_id=lead_id,
                error_type=type(error).__name__,
                duration_ms=round((time.monotonic() - started_at) * 1000),
            )
            raise
        # 只写统计量，既可定位成本和重试，又不会把客户文本写入日志。
        all_responses = (response,) + ((repair_response,) if repair_response is not None else ())
        self._log(
            trace_id,
            started_at,
            "succeeded",
            call_count=attempts + repair_attempts,
            input_tokens=sum(item.input_tokens or 0 for item in all_responses),
            output_tokens=sum(item.output_tokens or 0 for item in all_responses),
        )
        self._record_execution(
            trace_id,
            status="succeeded",
            source_message_id=source_message_id,
            lead_id=lead_id,
            call_count=attempts + repair_attempts,
            input_tokens=sum(item.input_tokens or 0 for item in all_responses),
            output_tokens=sum(item.output_tokens or 0 for item in all_responses),
            duration_ms=round((time.monotonic() - started_at) * 1000),
        )
        return ExtractedLeadPatch(
            trace_id, analysis, fields, pending, low_candidates, validated_enrichment
        )

    @staticmethod
    def _recover_verified_identity_fields(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """恢复可由原文确定性核验的公司、联系人和联系方式字段。

        参数：analysis 为模型输出的结构化分析；source_text 为脱敏后的当前消息文本。
        返回值：补入原文证据充分的身份字段，或纠正明确的身份拼接错误后的分析结果。
        异常：无；不合格引用被忽略，普通模型字段不被覆盖。
        副作用：无；不调用外部服务，仅修正模型已拆分身份被拼回单字段的格式错误。
        """
        recovered = AIGateway._extract_identity_from_source(source_text)
        # 模型按语义拆开的引用优先于本地标签兜底，但仍必须逐字存在于原文。
        reference_fields = AIGateway._verified_customer_reference_fields(
            analysis.customer_reference, source_text
        )
        recovered.update(reference_fields)
        # 联系人与职位在自然语言中经常连写；仅在原文明确出现职位并紧邻模型核验联系人时补回职务。
        contact = recovered.get("联系人")
        # 若模型把联系人放在正式字段而不是 customer_reference，仅借用该已输出值判断相邻职位，
        # 不提升其原有置信度，也不改变人工/低置信度分级结果。
        contact_candidate = analysis.crm_fields.get("联系人")
        if not contact and contact_candidate and contact_candidate.strip() in source_text:
            contact = contact_candidate.strip()
        if contact:
            recovered_title = AIGateway._extract_verified_title(source_text, contact)
            if recovered_title:
                recovered["职务"] = recovered_title

        if not recovered:
            return analysis
        fields = dict(analysis.crm_fields)
        confidence_by_field = dict(analysis.confidence_by_field)
        identity_pair = (recovered.get("线索名称"), recovered.get("联系人"))
        changed = False
        for field_name, value in recovered.items():
            # 普通模型候选可能包含上下文信息，确定性兜底只补空位或修正明确的身份拼接错误。
            if field_name in fields:
                current_confidence = confidence_by_field.get(field_name)
                if (
                    fields[field_name] == value
                    and (current_confidence is None or current_confidence < 1.0)
                ):
                    # 原文直接证实模型候选时，确定性证据可以补齐缺失或过低置信度造成的误过滤。
                    confidence_by_field[field_name] = 1.0
                    changed = True
                elif (
                    identity_pair[0]
                    and identity_pair[1]
                    and field_name in {"线索名称", "联系人"}
                    and AIGateway._is_combined_identity_value(
                        fields[field_name], identity_pair[0], identity_pair[1]
                    )
                ):
                    # 仅纠正模型已语义拆分、但把公司和联系人拼回单字段的格式错误；不依赖任何分隔符。
                    fields[field_name] = value
                    confidence_by_field[field_name] = 1.0
                    changed = True
                continue
            fields[field_name] = value
            confidence_by_field[field_name] = 1.0
            changed = True
        if not changed:
            return analysis
        return analysis.model_copy(
            update={"crm_fields": fields, "confidence_by_field": confidence_by_field}
        )

    @staticmethod
    def _verified_customer_reference_fields(
        customer_reference: dict[str, str], source_text: str
    ) -> dict[str, str]:
        """筛选模型按语义识别且能在当前原文中逐字核验的身份引用。

        参数：customer_reference 为模型输出的身份引用；source_text 为脱敏后的当前消息文本。
        返回值：映射到 CRM 标准字段的可信公司、联系人和联系方式。
        异常：无；未知键、无原文证据或格式非法的引用被忽略。
        副作用：无；不调用外部服务。
        """
        fields: dict[str, str] = {}
        for raw_name, raw_value in customer_reference.items():
            field_name = _CUSTOMER_REFERENCE_FIELD_ALIASES.get(raw_name.strip().lower())
            value = raw_value.strip()
            if field_name is None or not value or value not in source_text:
                continue
            if field_name in {"手机", "电话"} and not _PHONE_PATTERN.fullmatch(value):
                continue
            if field_name == "邮箱" and not _EMAIL_PATTERN.fullmatch(value):
                continue
            fields[field_name] = value
        return fields

    @staticmethod
    def _extract_verified_title(source_text: str, contact: str) -> str | None:
        """从联系人附近的明确职位表达中恢复职务字段。

        参数：source_text 为脱敏后的当前消息文本；contact 为已由模型并经原文核验的联系人。
        返回值：唯一且有明确职位语义的原文职位；无法确认或出现多个冲突职位时返回 None。
        异常：无。
        副作用：无；仅执行本地文本匹配，不覆盖模型已经给出的其他字段。
        """
        # 先处理带字段标签的写法，避免把后续联系人、需求等内容误认为职位。
        labeled_match = re.search(
            r"(?:职务|职位|岗位|职称|身份)\s*[:：]\s*([^，,；;。\n]+)",
            source_text,
        )
        candidates: set[str] = set()
        if labeled_match:
            labeled_title = labeled_match.group(1).strip()
            if labeled_title:
                candidates.add(labeled_title)

        # 只识别紧邻已核验联系人之前的常见职位词，支持“总经理张总”“采购负责人李工”等连写形式。
        title_terms = (
            "副总经理",
            "总经理",
            "执行董事",
            "董事长",
            "副总裁",
            "总裁",
            "采购负责人",
            "技术负责人",
            "项目负责人",
            "销售负责人",
            "负责人",
            "副总监",
            "总监",
            "工程师",
            "采购经理",
            "项目经理",
            "产品经理",
            "经理",
            "主管",
            "主任",
            "厂长",
            "老板",
        )
        title_pattern = "|".join(re.escape(term) for term in title_terms)
        contact_pattern = re.escape(contact.strip())
        for match in re.finditer(rf"({title_pattern})\s*{contact_pattern}", source_text):
            candidates.add(match.group(1))

        # 同一联系人若对应多个不同职位，保守留空，交由模型/销售后续确认。
        return next(iter(candidates)) if len(candidates) == 1 else None

    @staticmethod
    def _is_combined_identity_value(value: str, company: str, contact: str) -> bool:
        """判断单字段值是否只是公司与联系人被符号或空白拼接后的组合。

        参数：value 为模型填入单个 CRM 字段的值；company 与 contact 为模型已拆分、
        经原文核验的身份值。
        返回值：去除标点和空白后恰好等于公司加联系人的组合时返回 True。
        异常：无。
        副作用：无；不按具体分隔符分支处理。
        """
        normalized_value = AIGateway._normalize_identity_text(value)
        normalized_expected = AIGateway._normalize_identity_text(company + contact)
        return normalized_value == normalized_expected and value.strip() != company.strip()

    @staticmethod
    def _extract_identity_from_source(source_text: str) -> dict[str, str]:
        """从明确标签、手机号和邮箱提取可直接证明的身份字段。

        参数：source_text 为脱敏后的当前消息文本。
        返回值：可由本地确定性规则直接证明的 CRM 身份字段；公司与联系人语义拆分交给模型。
        异常：无；普通自然语言无法满足标签或格式边界时返回部分或空结果。
        副作用：无；仅执行本地文本匹配。
        """
        fields: dict[str, str] = {}
        phone_match = _PHONE_SEARCH_PATTERN.search(source_text)
        if phone_match:
            fields["手机"] = phone_match.group(1)
        email_match = _EMAIL_SEARCH_PATTERN.search(source_text)
        if email_match:
            fields["邮箱"] = email_match.group(0)

        labeled_patterns = (
            ("线索名称", r"(?:客户|公司|企业)\s*[:：]\s*([^；;，,、\n]+)"),
            ("联系人", r"(?:联系人|联系人姓名)\s*[:：]\s*([^；;，,、\n]+)"),
        )
        for field_name, pattern in labeled_patterns:
            match = re.search(pattern, source_text)
            if match and match.group(1).strip():
                fields[field_name] = match.group(1).strip().rstrip("\\")
        return fields

    @staticmethod
    def _normalize_identity_text(value: str) -> str:
        """规范化身份文本中的空白和标点，供字段误拼接比较使用。

        参数：value 为待比较的公司、联系人或组合文本。
        返回值：仅保留字母、数字、下划线和中文等词字符的文本。
        异常：无。
        副作用：无。
        """
        return re.sub(r"[^\w]+", "", value, flags=re.UNICODE)

    def _record_execution(
        self,
        trace_id: str,
        *,
        status: str,
        source_message_id: str | None,
        lead_id: str | None,
        call_count: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        duration_ms: int | None = None,
        error_type: str | None = None,
    ) -> None:
        """尽力持久化不含原文的 AI 执行元数据，不让观测故障伪造业务结果。

        参数：trace_id 为调用追踪标识；其余参数为状态、来源引用和统计信息。
        返回值：无。
        异常：记录器异常被吞并写入脱敏日志，原始 AI 结果不受影响。
        副作用：可能新增一条 AI execution record，但绝不保存 Prompt 或响应。
        """
        if self._execution_recorder is None:
            return
        try:
            self._execution_recorder.record(
                AIExecutionRecorderEvent(
                    trace_id=trace_id,
                    operation="extract_fields",
                    provider=type(self._provider).__name__,
                    model=self._provider.model_name,
                    status=status,
                    message_id=source_message_id,
                    lead_id=lead_id,
                    call_count=call_count,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    duration_ms=duration_ms,
                    error_type=error_type,
                    error_summary=(f"ai_gateway:{error_type}" if error_type else None),
                    created_at=datetime.fromtimestamp(time.time(), tz=UTC),
                    completed_at=datetime.fromtimestamp(time.time(), tz=UTC),
                )
            )
        except Exception:
            # 观测表故障不能改变已完成的 AI 业务结论，日志同样不带模型内容。
            logger.exception("ai_execution_record_failed")

    def _call_with_transport_retry(
        self, request: LLMRequest, trace_id: str
    ) -> tuple[LLMResponse, int]:
        """仅针对 Provider 传输故障执行配置次数的重试。

        参数：request 为单次模型请求。
        返回：原始模型文本。
        异常：重试耗尽时抛出 AIGatewayError。
        副作用：最多调用 Provider `retry_count + 1` 次。
        """
        for attempt in range(1, self._retry_count + 2):
            try:
                # Provider 是唯一外部调用点，超时由 Provider 使用网关传入的统一配置。
                return (
                    self._provider.complete(request, timeout_seconds=self._timeout_seconds),
                    attempt,
                )
            except LLMProviderError as error:
                # 记录每次失败的白名单元数据，避免重试过程成为不可观测黑盒。
                logger.warning(
                    "ai_gateway_retry",
                    extra={
                        "ai_trace_id": trace_id,
                        "target": type(self._provider).__name__,
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                    },
                )
                if attempt > self._retry_count:
                    raise AIGatewayTransportError("ai_transport_failed") from error
        raise AssertionError("不可达：循环在成功或耗尽时结束")

    @staticmethod
    def _output_json_schema() -> dict[str, object]:
        """构造仅供模型输出使用的受限 schema，不改变领域模型或业务校验。

        参数：无。
        返回：禁止系统字段、未知 CRM 字段和未知补充信息键的 JSON Schema 副本。
        异常：无。
        副作用：无；仅改变发送给 Provider 的结构化输出约束。
        """
        schema = LeadAnalysis.model_json_schema()
        properties = schema["properties"]
        allowed_crm_fields = tuple(
            field_name
            for field_name in CRM_BUSINESS_FIELD_NAMES
            if field_name not in _FORBIDDEN_AI_CRM_FIELD_NAMES
        )
        # 结构化输出只能生成明确列出的键，避免通用 dict schema 放行备注或自然语言别名。
        for field_name, allowed_names in (
            ("crm_fields", allowed_crm_fields),
            ("confidence_by_field", allowed_crm_fields),
            ("enrichment", tuple(_ENRICHMENT_FIELD_NAMES)),
        ):
            value_type = "number" if field_name == "confidence_by_field" else "string"
            properties[field_name] = {
                "type": "object",
                "properties": {
                    name: {
                        "type": (
                            ["string", "array"]
                            if field_name == "crm_fields" and name == "工艺"
                            else value_type
                        ),
                        **(
                            {"items": {"type": "string", "enum": list(_ENUM_OPTIONS[name])}}
                            if field_name == "crm_fields" and name == "工艺"
                            else {}
                        ),
                        # 仅 CRM 枚举字段附加当前注册表选项，其他字段维持字符串契约。
                        **(
                            {"enum": list(_ENUM_OPTIONS[name])}
                            if field_name == "crm_fields"
                            and name in _ENUM_OPTIONS
                            and name != "工艺"
                            else {}
                        ),
                    }
                    for name in allowed_names
                },
                "additionalProperties": False,
            }
        # JSON Schema 不能用一个通用动态规则比较两个对象的键集合；CRM 白名单有限，
        # 因此为每个字段生成“建议字段出现时必须携带同名置信度”的静态条件。
        schema["allOf"] = [
            {
                "if": {
                    "required": ["crm_fields"],
                    "properties": {"crm_fields": {"required": [field_name]}},
                },
                "then": {
                    "required": ["confidence_by_field"],
                    "properties": {"confidence_by_field": {"required": [field_name]}},
                },
            }
            for field_name in allowed_crm_fields
        ]
        return schema

    def _parse_schema_or_repair(
        self, request: LLMRequest, response: LLMResponse, trace_id: str
    ) -> tuple[LeadAnalysis, LLMResponse | None, int]:
        """解析并验证模型结构，且仅为 JSON/Schema 失败进行一次受约束修复。

        参数：request 为原请求；response 为初始模型响应。
        返回：经 Pydantic 验证的结构化分析。
        异常：第二次结构失败时抛出 AIGatewayError。
        副作用：结构失败时额外调用 Provider 一次，业务校验永不在此发生。
        """
        try:
            return LeadAnalysis.model_validate_json(response.content), None, 0
        except ValidationError:
            # 修复请求仅保留脱敏原输入、明确 Schema 与受限修复规则。
            repair_messages = (
                {
                    "role": "system",
                    "content": (
                        "只修复随后的输出 JSON 结构，不得新增、删除或改写任何事实值。"
                        "必须符合此 JSON Schema："
                        f"{json.dumps(request.json_schema, ensure_ascii=False)}"
                        f"{self._field_contract_instructions()}"
                    ),
                },
                request.messages[-1],
            )
            repair_request = LLMRequest(
                messages=repair_messages,
                json_schema=request.json_schema,
                repair_source=response.content,
            )
            try:
                # 结构修复只携带模型原输出和 Schema，不重新向模型索取业务事实。
                repaired_response, attempts = self._call_with_transport_retry(
                    repair_request, trace_id
                )
                return (
                    LeadAnalysis.model_validate_json(repaired_response.content),
                    repaired_response,
                    attempts,
                )
            except ValidationError as error:
                raise FailedStructuredOutputError(
                    response.content, repaired_response.content, str(error)
                ) from error

    def _validate_business(self, analysis: LeadAnalysis) -> None:
        """以确定性规则校验字段名、枚举、格式及置信度完整性。

        参数：analysis 为已通过 Pydantic Schema 的模型建议。
        返回：无。
        异常：发现任何候选不合法时抛出 BusinessValidationError。
        副作用：无；禁止在业务校验失败后重试模型。
        """
        for field_name, value in analysis.crm_fields.items():
            # CRM 注册表是字段白名单，禁止模型引入权限或提交等业务控制字段。
            if field_name not in CRM_BUSINESS_FIELD_NAMES:
                raise BusinessValidationError(f"未知 CRM 字段：{field_name}")
            # 备注只能由 T09 后的确定性生成器写入，模型不得直接提供或绕过人工保护。
            if field_name in _FORBIDDEN_AI_CRM_FIELD_NAMES:
                raise BusinessValidationError(f"AI 禁止输出字段：{field_name}")
            if isinstance(value, list) and field_name != "工艺":
                raise BusinessValidationError(f"字段不支持多值：{field_name}")
            # 枚举字段必须使用管理员配置的合法选项，失败后不再调用模型修正。
            if field_name in _ENUM_OPTIONS:
                values = value if isinstance(value, list) else [value]
                if not values or not all(
                    isinstance(item, str) and item in _ENUM_OPTIONS[field_name] for item in values
                ):
                    raise BusinessValidationError(f"枚举值不合法：{field_name}")
            # 联系方式格式由确定性规则校验，避免模型用猜测值绕过约束。
            if field_name in {"手机", "电话"} and (
                not isinstance(value, str) or not _PHONE_PATTERN.fullmatch(value)
            ):
                raise BusinessValidationError(f"联系方式格式不合法：{field_name}")
            if field_name == "邮箱" and (
                not isinstance(value, str) or not _EMAIL_PATTERN.fullmatch(value)
            ):
                raise BusinessValidationError("邮箱格式不合法")
            # 每个建议字段都必须提供区间内置信度，才能进入后续分级。
            confidence = analysis.confidence_by_field.get(field_name)
            if confidence is None or not 0 <= confidence <= 1:
                raise BusinessValidationError(f"置信度缺失或不合法：{field_name}")
        for field_name in analysis.enrichment:
            # 补充信息只能是冻结备注模板可消费、且要求模型保留原文证据的键。
            if field_name not in _ENRICHMENT_FIELD_NAMES:
                raise BusinessValidationError(f"未知补充信息字段：{field_name}")

    @staticmethod
    def _normalize_communication_candidate(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """在枚举校验前归一化或丢弃不可靠的沟通方式候选。

        参数：analysis 为模型输出分析；source_text 为当前消息原文。
        返回值：明确匹配时替换为注册枚举；无唯一证据时移除已知别名候选；未知表达保持原样交由业务校验拒绝。
        异常：无。
        副作用：记录被丢弃候选的字段类型，不记录客户原文或候选值。
        """
        candidate = analysis.crm_fields.get("沟通方式")
        if candidate is None or candidate in COMMUNICATION_METHOD_OPTIONS:
            return analysis
        if candidate not in _COMMUNICATION_ALIASES:
            # 未注册且不属于有限别名集合的值仍由统一业务校验拦截，避免静默猜测。
            return analysis

        matched_values = {
            expected for keyword, expected in _COMMUNICATION_EVIDENCE if keyword in source_text
        }
        fields = dict(analysis.crm_fields)
        confidences = dict(analysis.confidence_by_field)
        if len(matched_values) == 1:
            # 只有原文唯一支持一个沟通方式时，才允许把模型自然表达映射成注册值。
            fields["沟通方式"] = next(iter(matched_values))
        else:
            # 没有或同时存在多个证据时，不让不确定的单字段阻塞其他可靠字段。
            fields.pop("沟通方式", None)
            confidences.pop("沟通方式", None)
            logger.warning(
                "ai_communication_candidate_dropped_invalid_evidence",
                extra={"error_type": "communication_method_without_unique_evidence"},
            )
        return analysis.model_copy(
            update={"crm_fields": fields, "confidence_by_field": confidences}
        )

    @staticmethod
    def _drop_fields_missing_confidence(analysis: LeadAnalysis) -> LeadAnalysis:
        """丢弃模型未提供置信度的 CRM 字段候选，避免单字段阻塞整条消息。

        参数：analysis 为已完成身份恢复和沟通方式归一化的模型分析结果。
        返回值：移除置信度缺失字段后的分析结果；其余字段及置信度保持不变。
        异常：无；置信度越界等非缺失错误仍交由业务校验抛出。
        副作用：对被丢弃字段记录不含客户原文的结构化告警。
        """
        missing_fields = tuple(
            field_name
            for field_name in analysis.crm_fields
            if field_name not in analysis.confidence_by_field
        )
        if not missing_fields:
            return analysis
        fields = {
            field_name: value
            for field_name, value in analysis.crm_fields.items()
            if field_name not in missing_fields
        }
        logger.warning(
            "ai_fields_dropped_missing_confidence",
            extra={"field_names": list(missing_fields)},
        )
        return analysis.model_copy(update={"crm_fields": fields})

    @staticmethod
    def _normalize_enrichment_field_placement(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """把模型误放入 crm_fields 的备注素材归回 enrichment。

        参数：analysis 为模型结构化分析结果；source_text 为当前脱敏原文。
        返回值：仅调整补充信息字段位置、同步移除其无关置信度后的分析结果。
        异常：无；真正未知的 CRM 字段仍交由业务白名单校验抛出异常。
        副作用：记录字段归位或冲突告警，不记录客户原文和候选值。
        """
        fields = dict(analysis.crm_fields)
        enrichment = dict(analysis.enrichment)
        confidences = dict(analysis.confidence_by_field)
        moved_fields: list[str] = []
        quarantined_fields: list[str] = []
        dropped_unknown_fields: list[str] = []
        conflicted_fields: list[str] = []
        for field_name in tuple(fields):
            if field_name in _ENRICHMENT_ONLY_FIELD_NAMES:
                value = fields.pop(field_name)
                confidences.pop(field_name, None)
                existing_value = enrichment.get(field_name)
                if existing_value is None or existing_value == value:
                    enrichment[field_name] = value
                    moved_fields.append(field_name)
                    continue

                # 两个位置的值冲突时，只保留有唯一原文证据的候选；两者都有证据则宁缺毋滥。
                value_has_evidence = bool(value and value in source_text)
                existing_has_evidence = bool(existing_value and existing_value in source_text)
                if value_has_evidence and not existing_has_evidence:
                    enrichment[field_name] = value
                    moved_fields.append(field_name)
                elif value_has_evidence == existing_has_evidence:
                    enrichment.pop(field_name, None)
                    conflicted_fields.append(field_name)
                continue

            # 注册 CRM 字段交给后续枚举、格式和置信度校验；只有未知业务字段进入保守隔离。
            if (
                field_name in CRM_BUSINESS_FIELD_NAMES
                or field_name in _NON_QUARANTINABLE_AI_FIELD_NAMES
            ):
                continue

            value = fields.pop(field_name)
            confidences.pop(field_name, None)
            if not value or value not in source_text:
                # 未知字段没有原文证据时只丢弃候选，避免模型幻觉阻塞整条线索。
                dropped_unknown_fields.append(field_name)
                continue

            # 未知业务字段不能新增智能表格列，统一带原字段名保留到备注的“特殊要求”素材。
            detail = f"{field_name}：{value}"
            existing_requirement = enrichment.get("特殊要求")
            if not existing_requirement or not AIGateway._has_enrichment_source_evidence(
                "特殊要求", existing_requirement, source_text
            ):
                enrichment["特殊要求"] = detail
            elif detail not in existing_requirement:
                enrichment["特殊要求"] = f"{existing_requirement}；{detail}"
            quarantined_fields.append(field_name)

        if moved_fields:
            logger.warning(
                "ai_enrichment_fields_relocated",
                extra={"field_names": moved_fields},
            )
        if conflicted_fields:
            logger.warning(
                "ai_enrichment_field_conflict_dropped",
                extra={"field_names": conflicted_fields},
            )
        if quarantined_fields:
            logger.warning(
                "ai_unknown_fields_quarantined_to_enrichment",
                extra={"field_names": quarantined_fields},
            )
        if dropped_unknown_fields:
            logger.warning(
                "ai_unknown_fields_dropped_without_evidence",
                extra={"field_names": dropped_unknown_fields},
            )
        if not (
            moved_fields or quarantined_fields or dropped_unknown_fields or conflicted_fields
        ):
            return analysis
        return analysis.model_copy(
            update={
                "crm_fields": fields,
                "enrichment": enrichment,
                "confidence_by_field": confidences,
            }
        )

    def _apply_confidence(
        self, analysis: LeadAnalysis, source_text: str
    ) -> tuple[dict[str, LeadFieldValue], tuple[str, ...], dict[str, LeadFieldValue]]:
        """按阈值将合法候选分为正式字段、待确认或后台低置信度候选。

        参数：analysis 为已完成业务校验的分析结果；source_text 为当前脱敏原文。
        返回：正式字段、稳定排序待确认字段与低置信度候选。
        异常：无。
        副作用：无；T08 只返回待确认元数据，不实现 T09 人工确认流程。
        """
        fields: dict[str, LeadFieldValue] = {}
        pending: list[str] = []
        low_candidates: dict[str, LeadFieldValue] = {}
        for field_name, value in analysis.crm_fields.items():
            if field_name == "沟通方式" and not self._has_explicit_communication_evidence(
                source_text, value
            ):
                # “后续沟通”等弱描述不足以选择枚举，必须保留正式字段为空。
                continue
            confidence = analysis.confidence_by_field[field_name]
            # 高置信度可直接预填；中置信度保留待确认元数据；低置信度只后台留存。
            if confidence >= self._high_confidence_threshold:
                fields[field_name] = value
            elif confidence >= self._medium_confidence_threshold:
                fields[field_name] = value
                pending.append(field_name)
            else:
                low_candidates[field_name] = value
        return fields, tuple(pending), low_candidates

    @staticmethod
    def _validate_enrichment_evidence(
        analysis: LeadAnalysis, source_text: str
    ) -> dict[str, str]:
        """确认并筛选可由当前原文逐字定位的补充信息事实。

        参数：analysis 为已通过字段业务校验的模型建议；source_text 为当前脱敏原文。
        返回值：仅包含有原文证据且通过分类校验的补充信息。
        异常：无；不合格的可选补充信息被丢弃，不阻塞核心 CRM 字段。
        副作用：对被丢弃的补充信息记录脱敏告警；禁止模型摘要、扩写或猜测进入备注。
        """
        valid_enrichment: dict[str, str] = {}
        for field_name, value in analysis.enrichment.items():
            if (
                field_name in ENUM_FIELDS_WITH_OTHER
                and "其他"
                not in (
                    analysis.crm_fields.get(field_name)
                    if isinstance(analysis.crm_fields.get(field_name), list)
                    else [analysis.crm_fields.get(field_name)]
                )
            ):
                # 枚举实际说明只能附着在同名“其他”字段上，避免无关文本进入备注。
                logger.warning(
                    "ai_enrichment_dropped_invalid_evidence",
                    extra={"error_type": f"enum_other_without_other_value:{field_name}"},
                )
                continue
            if not AIGateway._has_enrichment_source_evidence(field_name, value, source_text):
                logger.warning(
                    "ai_enrichment_dropped_invalid_evidence",
                    extra={"error_type": f"missing_source_evidence:{field_name}"},
                )
                continue
            if field_name == "年销售额" and not AIGateway._has_enrichment_category_evidence(
                source_text, value, ("收入", "营收", "销售额")
            ):
                # 金额本身不表示经营规模，避免预算被错误写入年销售额事实。
                logger.warning(
                    "ai_enrichment_dropped_invalid_evidence",
                    extra={"error_type": "missing_revenue_category_evidence"},
                )
                continue
            if field_name == "预算" and not AIGateway._has_enrichment_category_evidence(
                source_text, value, ("预算", "投入金额", "项目金额")
            ):
                # 金额本身不表示项目预算，避免经营规模被错误写入备注事实。
                logger.warning(
                    "ai_enrichment_dropped_invalid_evidence",
                    extra={"error_type": "missing_budget_category_evidence"},
                )
                continue
            valid_enrichment[field_name] = value
        return valid_enrichment

    @staticmethod
    def _has_enrichment_source_evidence(
        field_name: str, value: str, source_text: str
    ) -> bool:
        """判断补充信息值或系统包装后的未知字段内容是否来自当前原文。

        参数：field_name 为补充信息字段名；value 为候选内容；source_text 为当前脱敏原文。
        返回值：候选整体或其每个“字段名：内容”片段均有原文证据时返回 True。
        异常：无。
        副作用：无；不调用外部服务。
        """
        if value in source_text:
            return True
        if field_name != "特殊要求":
            return False
        parts = [part.strip() for part in re.split(r"[；;]", value) if part.strip()]
        return bool(parts) and all(
            "：" in part and part.split("：", 1)[1].strip() in source_text
            for part in parts
        )

    @staticmethod
    def _has_enrichment_category_evidence(
        source_text: str, value: str, keywords: tuple[str, ...]
    ) -> bool:
        """判断补充信息值是否与其类别词构成同一条明确原文表达。

        参数：source_text 为当前原文；value 为模型保留的连续事实片段；keywords 为字段类别关键词。
        返回值：类别词紧邻值并通过明确连接词关联时返回 True。
        异常：无。
        副作用：无；金额大小或金额本身不会作为分类依据。
        """
        if any(keyword in value for keyword in keywords):
            # 模型保留完整“预算50万元”等原文片段时，字段类别已在值内明确出现。
            return True
        category_pattern = "|".join(re.escape(keyword) for keyword in keywords)
        connector_pattern = r"(?:为|是|约|大约|预计(?:为|达到)?|可达|达到|在|:|：)?"
        # 类别词与金额之间只允许明确连接词或空白，不能跨越另一条金额事实进行匹配。
        return re.search(
            rf"(?:{category_pattern})\s*{connector_pattern}\s*{re.escape(value)}",
            source_text,
        ) is not None

    @staticmethod
    def _has_explicit_communication_evidence(text: str, value: str) -> bool:
        """判断沟通方式枚举是否由当前文本中的明确词语直接支持。

        参数：text 为当前消息文本；value 为模型建议的冻结枚举值。
        返回值：文本含对应明确证据且枚举映射一致时返回 True。
        异常：无。
        副作用：无。
        """
        return any(
            keyword in text and value == expected
            for keyword, expected in _COMMUNICATION_EVIDENCE
        )

    def _messages(
        self,
        safe_text: str,
        json_schema: dict[str, object],
        context_fields: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, str], ...]:
        """构造包含当前文本、可选当前客户上下文和受控规则的提取提示。

        参数：safe_text 为已移除无关高敏感信息的当前消息文本；json_schema 为输出约束；
        context_fields 为已脱敏的当前销售最近线索字段，仅用于判断消息关系。
        返回：供应商无关的聊天消息序列。
        异常：无。
        副作用：无。
        """
        context_instruction = self._context_instructions(context_fields)
        return (
            {
                "role": "system",
                "content": (
                    "仅提取线索建议 JSON；不得决定提交、删除、负责人、"
                    "CRM 合并或覆盖人工值。"
                    "intent 只用于判断当前消息与销售会话的关系：明确介绍另一个客户时返回 "
                    "NEW_LEAD；名片、OCR、语音转写或碎片补充属于当前客户时返回 "
                    "UPDATE_LEAD；无法可靠判断时不要伪造公司名。"
                    f"{context_instruction}"
                    f"必须符合此 JSON Schema：{json.dumps(json_schema, ensure_ascii=False)}"
                    f"{self._field_contract_instructions()}"
                ),
            },
            {"role": "user", "content": safe_text},
        )

    @staticmethod
    def _context_instructions(context_fields: Mapping[str, str] | None) -> str:
        """生成当前销售线索上下文的受控提示，防止模型把历史值当作新事实。

        参数：context_fields 为已脱敏的后台上下文字段。
        返回值：无上下文时返回空字符串，否则返回带边界说明的 JSON 文本。
        异常：无；调用方已保证字段值为字符串。
        副作用：无；只生成模型提示文本。
        """
        if not context_fields:
            return ""
        return (
            "当前销售最近一条线索上下文如下（仅用于判断 UPDATE_LEAD/NEW_LEAD，"
            "不是本条消息事实；本条消息没有证据的字段不得从上下文复制）："
            f"{json.dumps(dict(context_fields), ensure_ascii=False)}。"
        )

    @staticmethod
    def _field_contract_instructions() -> str:
        """返回初始提取与结构修复共用的冻结 CRM 字段输出约束。

        参数：无。
        返回：要求模型遵循字段名、值类型、置信度与枚举注册表的提示文本。
        异常：无。
        副作用：无；仅基于冻结注册表拼装提示，不改变任何确定性校验。
        """
        allowed_names = "、".join(CRM_BUSINESS_FIELD_NAMES)
        enum_options = "；".join(
            f"{field_name}：{'、'.join(options)}" for field_name, options in _ENUM_OPTIONS.items()
        )
        return (
            "crm_fields 的 key 只能是以下 CRM 注册表中的中文原名："
            f"{allowed_names}。"
            "禁止使用英文或其他别名（例如 phone），不得映射或自造字段名。"
            "禁止输出 JSON key：客户名称、公司名称、企业名称、联系人姓名、手机号；"
            "它们仅是输入标签，不是 CRM 字段。"
            "公司名称/企业名称 -> 线索名称；"
            "客户名称（个人语境）-> 联系人；"
            "联系人姓名 -> 联系人；手机号 -> 手机。"
            "公司或个人语义不明确时省略字段，不得猜测或输出未注册字段。"
            "必须先按语义区分公司主体与联系人个人，不依赖波浪线、短横线、空格、斜线等特定分隔符；"
            "即使公司名和联系人连写，也只能在语义足够明确时分别输出。"
            "线索名称只能填写公司主体，联系人只能填写个人姓名；禁止把公司和联系人拼接后写入任一单字段。"
            "职务是与联系人对应的职位或身份称谓，必须独立写入职务字段；"
            "例如‘总经理张总’应输出职务=总经理、联系人=张总，‘老板李总’应输出职务=老板、联系人=李总；"
            "仅出现‘张总’等称呼而没有明确职位关系时，不得仅凭‘总’猜测职务。"
            "customer_reference 可使用 company、contact、title、phone、email 五类身份引用，"
            "分别填写已识别的公司、联系人、职务、手机号和邮箱；"
            "这些引用也必须来自当前原文，不确定时留空。"
            "对所有 CRM 字段都必须先理解整条消息的语义，再独立提取字段；"
            "不得按换行、逗号、短横线、波浪线或其他符号机械切段。"
            "每个 crm_fields value 只能填写该字段自身的单个事实值；"
            "工艺因 CRM 接口支持多选，可填写合法工艺字符串数组，"
            "不能带字段标签、解释文字、其他字段值或整句原文。"
            "业务线、客户行业、工艺、客户级别和沟通方式必须根据上下文映射到注册表合法选项；"
            "语义不足以区分候选时省略，不得用相邻字段或关键词强行推断。"
            "手机号、电话、邮箱、日期和地点分别按各自格式识别，不能把姓名、职位、公司名或说明文字混入。"
            "主营产品、客户需求/痛点、预算、年销售额和特殊要求属于 enrichment，"
            "按语义归类但 value 必须保留当前原文中的连续事实片段；模型摘要或扩写不得写入。"
            "一条消息包含多个不同候选时，必须先判断是否为多个客户；"
            "无法可靠拆分时返回 MULTI_LEAD 或省略不确定字段。"
            "允许输出的只有 CRM 注册业务字段（不含备注）及 enrichment 冻结字段。"
            "禁止输出 JSON key：备注、comment、remark、notes、description；"
            "也禁止输出 AI待确认、缺失字段、审核状态、provenance 或其他系统计算字段。"
            "备注由已审核正式字段和 enrichment 在 T09 后通过 RemarksBuilder 确定性生成。"
            "除工艺外，crm_fields 的每个 value 必须是单个字符串；"
            "其他多值信息不得使用数组塞入 CRM 字段。"
            "没有可靠信息的字段必须直接省略，不得返回 null、空字符串或空数组。"
            "confidence_by_field 的 key 必须与 crm_fields 的 key 完全一一对应，"
            "并使用相同中文字段名。"
            f"枚举字段只能使用以下注册表选项：{enum_options}。"
            "enrichment 的 key 只能是城市/地区、主营产品、年销售额、客户需求/痛点、预算、特殊要求，"
            f"以及取值为“其他”的枚举字段（{'、'.join(sorted(ENUM_FIELDS_WITH_OTHER))}）；"
            "当枚举字段取值为“其他”且原文存在明确实际内容时，必须使用同名 key 保存原文连续片段；"
            "实际内容无法确定时省略该 enrichment，由备注生成器写入“字段名：其他（请补充）”；"
            "每个 value 必须是当前原文中连续出现的单个字符串片段，没有证据时省略。"
            "年销售额仅提取含收入、营收或销售额等明确经营规模表达的原文片段；"
            "预算仅提取含预算、投入金额或项目金额等明确预算表达的原文片段；"
            "不得仅根据金额大小或金额本身猜测分类。"
        )

    def _log(
        self,
        trace_id: str,
        started_at: float,
        status: str,
        *,
        error_type: str | None = None,
        call_count: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
    ) -> None:
        """写入不含输入、输出或密钥的调用观测日志。

        参数：trace_id 为调用追踪标识；started_at 为单调起点；status 为受控结果；
        error_type 为可选错误分类；call_count 为本追踪中的实际模型调用次数。
        返回：无。
        异常：无。
        副作用：向结构化日志写入脱敏运行元数据。
        """
        logger.info(
            "ai_gateway_call",
            extra={
                "ai_trace_id": trace_id,
                "ai_status": status,
                "target": type(self._provider).__name__,
                "ai_call_count": call_count,
                "ai_input_tokens": input_tokens,
                "ai_output_tokens": output_tokens,
                "duration_ms": round((time.monotonic() - started_at) * 1000),
                "error_type": error_type,
            },
        )
