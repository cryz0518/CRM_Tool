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

from app.ai.models import (
    ExtractedLeadPatch,
    LeadAnalysis,
    LeadFieldValue,
    LLMRequest,
    LLMResponse,
    SubmissionIntent,
)
from app.ai.persistence import AIExecutionRecorder, AIExecutionRecorderEvent
from app.ai.provider import LLMProvider, LLMProviderError
from app.core.failures import PermanentTaskFailure, RetryableTaskFailure
from app.smart_table.models import SmartTableFieldType
from app.smart_table.registry import (
    AI_FIELD_ALIASES,
    BUSINESS_LINE_OPTIONS,
    COMMUNICATION_METHOD_OPTIONS,
    CRM_BUSINESS_FIELD_NAMES,
    CUSTOMER_INDUSTRY_OPTIONS,
    CUSTOMER_LEVEL_OPTIONS,
    ENUM_FIELDS_WITH_OTHER,
    INTERNATIONAL_CUSTOMER_OPTIONS,
    LEAD_SOURCE_OPTIONS,
    PROCESS_OPTIONS,
    REQUIRED_SMART_TABLE_FIELDS,
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
_FIELD_TYPES = {field.name: field.field_type for field in REQUIRED_SMART_TABLE_FIELDS}
_PURCHASE_INTENT_PATTERN = re.compile(r"(?:采购|购买|买|想要|需要|计划采购|准备采购)")
_PRODUCT_CATEGORY_TERMS = ("主营产品", "主要产品", "产品为", "产品是", "产品包括", "主营为")


def _enum_option_evidence_terms(field_name: str, option: str) -> tuple[str, ...]:
    """生成枚举选项的受控原文证据词，不改变最终枚举值。

    参数：field_name 为注册表字段名；option 为合法枚举选项。
    返回值：用于逐字证据判断的完整词和受控别名，最长词优先。
    异常：无。
    副作用：无；不进行模糊匹配或外部查询。
    """
    if field_name != "工艺":
        return (option,)
    terms = {option}
    # 仅维护明确业务别名；禁止把任意两个汉字拆成枚举候选。
    terms.update(
        {
            "自助充电",
            "自助加油",
        }
        if option == "自助加油/充电"
        else set()
    )
    return tuple(sorted(terms, key=len, reverse=True))
_PHONE_PATTERN = re.compile(r"^\+?[0-9][0-9 -]{5,24}$")
_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_PHONE_SEARCH_PATTERN = re.compile(r"(?<!\d)(?:\+?86[ -]?)?(1[3-9]\d{9})(?!\d)")
_EMAIL_SEARCH_PATTERN = re.compile(r"(?<![\w.+-])[\w.+-]+@[\w-]+(?:\.[\w-]+)+(?![\w.-])")
_NATURAL_COMPANY_CONTACT_PATTERN = re.compile(
    r"(?P<company>[^，,；;。\n]{2,80}?)的"
    r"(?P<contact>[\u4e00-\u9fffA-Za-z·]{1,6}"
    r"(?:总|经理|工|先生|女士|老师|主任|老板|主管))"
    r"(?=\s*(?:[，,；;。]|手机号|手机|电话|邮箱|是|做|想|主要|他们|找我|沟通|联系|对接|交流|事项|预计|计划|实施))"
)
_NATURAL_ROLE_PREFIXES = (
    "副总经理",
    "总经理",
    "采购负责人",
    "技术负责人",
    "项目负责人",
    "销售负责人",
    "负责人",
    "副总监",
    "总监",
    "项目经理",
    "产品经理",
)
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
_MODEL_FIELD_ALIASES = AI_FIELD_ALIASES
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
            # 模型偶发漏掉原文中明确的合法枚举；仅恢复唯一且有字段语义提示的候选，不做猜测。
            analysis = self._recover_explicit_enum_fields(analysis, safe_text)
            # 兼容模型把预算、痛点等备注素材误放入 CRM 字段的结果，先归位再做业务白名单校验。
            analysis = self._normalize_enrichment_field_placement(analysis, safe_text)
            # 枚举候选必须有当前消息的明确证据，禁止模型凭经验补填客户级别等字段。
            analysis = self._drop_enum_candidates_without_evidence(analysis, safe_text)
            # 对“是做某产品”“想了解某工艺”等逐字事实补充备注素材，避免模型漏返回导致备注为空。
            analysis = self._recover_explicit_enrichment_fields(analysis, safe_text)
            # 低置信度枚举按字段类型做受控预填，文本字段仍保留在后台候选。
            analysis = self._normalize_low_confidence_enum_candidates(analysis, safe_text)
            # 单个无法映射的正式枚举只留作待确认候选，不阻断同消息其它可靠字段。
            analysis, rejected_enum_candidates = self._drop_invalid_enum_candidates(analysis)
            # 兼容 Qwen 兼容模式偶发漏返回置信度键；非法枚举已先保留为待确认候选。
            analysis = self._drop_fields_missing_confidence(analysis)
            self._validate_business(analysis)
            validated_enrichment = self._validate_enrichment_evidence(analysis, safe_text)
            # 补充信息只是备注素材；证据不足时丢弃该字段，不能阻塞已通过校验的 CRM 主字段。
            analysis = analysis.model_copy(update={"enrichment": validated_enrichment})
            fields, pending, low_candidates, pending_prefill_allowed = self._apply_confidence(
                analysis, safe_text
            )
            pending = tuple(dict.fromkeys((*pending, *rejected_enum_candidates)))
            low_candidates = {**low_candidates, **rejected_enum_candidates}
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
            trace_id,
            analysis,
            fields,
            pending,
            low_candidates,
            validated_enrichment,
            pending_prefill_allowed,
        )

    def classify_submission_intent(self, text: str) -> SubmissionIntent:
        """用结构化模型识别消息工作流意图，不生成任何 CRM 业务参数。

        参数：text 为已持久化的销售消息，可为普通线索文本或提交请求。
        返回值：受限的结构化意图；结构不合法时返回 UNKNOWN。
        异常：模型传输失败抛出 AIGatewayTransportError。
        副作用：调用一次 LLM，不执行 CRM、数据库或智能表格写操作。
        """
        safe_text = _SENSITIVE_PATTERN.sub("[已遮蔽敏感号码]", text)
        schema = SubmissionIntent.model_json_schema()
        request = LLMRequest(
            messages=(
                {
                    "role": "system",
                    "content": (
                        "只做销售消息意图分类，不提取线索字段，不调用任何工具。"
                        "只能返回 JSON：intent 必须是 LEAD_CAPTURE、SUBMIT_TODAY、"
                        "SUBMIT_ALL、SUBMIT_SINGLE、SUBMIT_ABANDONED、SUBMIT_UPDATES、"
                        "SUBMIT_RETRY_INCOMPLETE 或 UNKNOWN。"
                        "‘提交今天/今天的线索’归类 SUBMIT_TODAY；‘提交所有/全部/我的所有线索’归类 "
                        "SUBMIT_ALL；‘提交放弃提交/放弃的线索’归类 SUBMIT_ABANDONED；"
                        "‘提交更新/我的更新’归类 SUBMIT_UPDATES；明确提交某一家公司归类 "
                        "SUBMIT_SINGLE 并只填写 company_name。‘重新提交’、‘重新提交待完善的线索’、"
                        "‘上次没完善的再提交’归类 SUBMIT_RETRY_INCOMPLETE；"
                        "放弃提交专用表达优先归类 "
                        "SUBMIT_ABANDONED，CRM 更新表达优先归类 SUBMIT_UPDATES。"
                        "录入客户资料属于 LEAD_CAPTURE；"
                        "普通客户信息即使包含‘今天’或‘提交方案’也归类 LEAD_CAPTURE。"
                        "无法确定时返回 UNKNOWN。必须符合 JSON Schema："
                        f"{json.dumps(schema, ensure_ascii=False)}"
                    ),
                },
                {"role": "user", "content": safe_text},
            ),
            json_schema=schema,
        )
        response, _ = self._call_with_transport_retry(request, str(uuid4()))
        try:
            intent = SubmissionIntent.model_validate_json(response.content)
        except (ValidationError, ValueError, TypeError):
            logger.warning("submission_intent_invalid_structure")
            return SubmissionIntent(intent="UNKNOWN")
        if intent.intent != "SUBMIT_SINGLE":
            if intent.company_name is not None:
                return SubmissionIntent(intent="UNKNOWN")
            return intent
        if intent.company_name is None:
            return SubmissionIntent(intent="UNKNOWN")
        company_name = intent.company_name.strip()
        if (
            not company_name
            or len(company_name) > 128
            or any(char in company_name for char in "\r\n。")
        ):
            return SubmissionIntent(intent="UNKNOWN")
        return intent.model_copy(update={"company_name": company_name})

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
        # 标签或明确“公司-联系人”句式是比模型引用更强的原文证据；模型误把两者
        # 拼接后放进 company 时，不能让 customer_reference 再次覆盖确定性拆分结果。
        for field_name, value in reference_fields.items():
            recovered.setdefault(field_name, value)
        # 联系人与职位在自然语言中经常连写；仅在原文明确出现职位并紧邻模型核验联系人时补回职务。
        contact = recovered.get("联系人")
        # 若模型把联系人放在正式字段而不是 customer_reference，仅借用该已输出值判断相邻职位，
        # 不提升其原有置信度，也不改变人工/低置信度分级结果。
        contact_candidate = analysis.crm_fields.get("联系人")
        if (
            not contact
            and isinstance(contact_candidate, str)
            and contact_candidate.strip() in source_text
        ):
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
                field_value = fields[field_name]
                if (
                    field_value == value
                    and (current_confidence is None or current_confidence < 1.0)
                ):
                    # 原文直接证实模型候选时，确定性证据可以补齐缺失或过低置信度造成的误过滤。
                    confidence_by_field[field_name] = 1.0
                    changed = True
                elif (
                    identity_pair[0]
                    and identity_pair[1]
                    and field_name in {"线索名称", "联系人"}
                    and isinstance(field_value, str)
                    and (
                        AIGateway._is_combined_identity_value(
                            field_value, identity_pair[0], identity_pair[1]
                        )
                        or AIGateway._is_expanded_identity_value(
                            field_value, identity_pair[0], identity_pair[1]
                        )
                    )
                ):
                    # 纠正模型把公司、联系人及后续活动描述拼回单字段的错误；不依赖具体业务词。
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
        # 中文自然表达常用“公司名的联系人”连接身份；只在公司和联系人均已被
        # 原文核验时移除该连接词，避免把普通公司名误判成拼接值。
        normalized_relation_value = AIGateway._normalize_identity_text(value.replace("的", ""))
        return (
            (
                normalized_value == normalized_expected
                or normalized_relation_value == normalized_expected
            )
            and value.strip() != company.strip()
        )

    @staticmethod
    def _is_expanded_identity_value(value: str, company: str, contact: str) -> bool:
        """判断模型是否把身份与额外叙述拼在同一个 CRM 身份字段中。

        参数：value 为模型候选；company 与 contact 为原文核验后的公司和联系人。
        返回值：候选同时包含两类身份且明显长于身份组合时返回 True。
        异常：无。
        副作用：无；仅执行本地字符串规范化。
        """
        normalized_value = AIGateway._normalize_identity_text(value)
        normalized_company = AIGateway._normalize_identity_text(company)
        normalized_contact = AIGateway._normalize_identity_text(contact)
        return (
            normalized_company in normalized_value
            and normalized_contact in normalized_value
            and len(normalized_value) > len(normalized_company) + len(normalized_contact)
        )

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
        natural_match = _NATURAL_COMPANY_CONTACT_PATTERN.search(source_text)
        if natural_match:
            company = AIGateway._clean_natural_company_candidate(
                natural_match.group("company")
            )
            contact = natural_match.group("contact").strip()
            # “总经理张总”属于职位+联系人连写，模型已有专门的职务恢复规则；
            # 此处只接受没有职位前缀的“某公司/某客户的张总”句式。
            if (
                company
                and contact
                and not contact.startswith(_NATURAL_ROLE_PREFIXES)
                and "的" not in company
            ):
                fields.setdefault("线索名称", company)
                fields.setdefault("联系人", contact)
        return fields

    @staticmethod
    def _clean_natural_company_candidate(value: str) -> str:
        """清除公司前的有限叙述连接词，不改变公司主体文本。

        参数：value 为“公司名的联系人”句式中“的”之前的原文片段。
        返回值：去掉句首时间或连接关系后的候选公司名；无法形成主体时返回空串。
        异常：无。
        副作用：无；不调用模型或外部服务。
        """
        candidate = value.strip(" \t，,、")
        candidate = re.sub(r"^(?:(?:今天|刚才|刚刚)\s*)?(?:和|与|跟)\s*", "", candidate)
        if not candidate or any(token in candidate for token in ("他们", "我们", "客户的", "主要")):
            return ""
        return candidate

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

    @staticmethod
    def _drop_invalid_enum_candidates(
        analysis: LeadAnalysis,
    ) -> tuple[LeadAnalysis, dict[str, LeadFieldValue]]:
        """将不属于受控选项的枚举候选移出正式字段并标记待确认。

        参数：analysis 为已完成受控别名归一化的结构化结果。
        返回值：移除非法枚举后的分析结果及可后台留存的原始候选。
        异常：无；字段名、权限字段和非枚举业务校验仍由后续严格校验处理。
        副作用：只记录字段名称，不记录客户候选值。
        """
        fields = dict(analysis.crm_fields)
        confidences = dict(analysis.confidence_by_field)
        candidates: dict[str, LeadFieldValue] = {}
        for field_name, value in tuple(fields.items()):
            options = _ENUM_OPTIONS.get(field_name)
            if options is None:
                continue
            values = value if isinstance(value, list) else [value]
            if not all(isinstance(item, str) for item in values):
                continue
            if values and all(item in options for item in values):
                continue
            fields.pop(field_name)
            confidences.pop(field_name, None)
            if any(item.strip() for item in values):
                candidates[field_name] = value
        if candidates or fields != analysis.crm_fields:
            logger.warning(
                "ai_enum_candidates_pending_confirmation",
                extra={"field_names": list(candidates)},
            )
            analysis = analysis.model_copy(
                update={"crm_fields": fields, "confidence_by_field": confidences}
            )
        return analysis, candidates

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

    def _recover_explicit_enum_fields(
        self, analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """从原文唯一且带字段语义的合法选项恢复模型漏返回的枚举字段。

        参数：analysis 为模型结构化结果；source_text 为当前消息原文。
        返回值：仅在字段缺失、选项唯一且原文有明确语义提示时补入字段的结果。
        异常：无；多候选、无提示或已有模型候选均保持原样。
        副作用：无，不调用外部服务，不记录客户原文。
        """
        fields = dict(analysis.crm_fields)
        confidences = dict(analysis.confidence_by_field)
        recovered_fields: list[str] = []
        for field_name, options in _ENUM_OPTIONS.items():
            matched = [
                option
                for option in options
                if any(
                    evidence in source_text
                    for evidence in _enum_option_evidence_terms(field_name, option)
                )
            ]
            has_unique_evidence = len(matched) == 1 and AIGateway._has_explicit_enum_evidence(
                field_name, matched[0], source_text
            )
            # 空字符串或空数组只是模型的占位结果，不能阻止原文中的唯一枚举证据恢复。
            existing = fields.get(field_name)
            if field_name in fields and not (
                existing is None
                or existing == ""
                or existing == []
                or (isinstance(existing, str) and not existing.strip())
            ):
                # 原文唯一且明确的枚举证据优先于模型候选，避免模型把产品或行业
                # 语义错放到相邻字段；多候选时仍保留原结果交给人工确认。
                existing_values = existing if isinstance(existing, list) else [existing]
                if has_unique_evidence and matched[0] not in existing_values:
                    fields[field_name] = (
                        [matched[0]] if isinstance(existing, list) else matched[0]
                    )
                    confidences[field_name] = 1.0
                    recovered_fields.append(field_name)
                elif (
                    field_name == "工艺"
                    and has_unique_evidence
                    and confidences.get(field_name, 0.0) < self._medium_confidence_threshold
                ):
                    # 原文与模型候选一致时只修复过低置信度，不抬高中置信度字段。
                    confidences[field_name] = 1.0
                    recovered_fields.append(field_name)
                continue
            fields.pop(field_name, None)
            confidences.pop(field_name, None)
            if not has_unique_evidence:
                continue
            fields[field_name] = matched[0]
            # 原文逐字命中合法选项且语义提示明确，可视为高置信度确定性事实。
            confidences[field_name] = 1.0
            recovered_fields.append(field_name)
        if recovered_fields:
            logger.info(
                "ai_enum_fields_recovered_from_source",
                extra={"field_names": recovered_fields},
            )
        return analysis.model_copy(
            update={"crm_fields": fields, "confidence_by_field": confidences}
        )

    @staticmethod
    def _drop_enum_candidates_without_evidence(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """删除没有原文语义证据的枚举候选，阻止模型臆造业务属性。

        参数：analysis 为模型结构化结果；source_text 为当前消息原文。
        返回值：仅保留能由原文明确字段语义核验的枚举候选。
        异常：无；不合法或无法核验的候选由后续业务校验继续处理。
        副作用：记录被丢弃的字段名，不记录客户原文或候选值。
        """
        fields = dict(analysis.crm_fields)
        confidences = dict(analysis.confidence_by_field)
        dropped_fields: list[str] = []
        for field_name in _ENUM_OPTIONS:
            # 非法枚举只有在原文明确带字段语义时才交给业务校验；无语义证据的
            # 模型臆造直接丢弃，避免一个错误枚举阻断同一消息的可靠身份字段。
            if field_name not in analysis.crm_fields:
                continue
            value = fields.get(field_name)
            if value is None:
                continue
            values = value if isinstance(value, list) else [value]
            if not values or not all(isinstance(item, str) for item in values):
                continue
            if field_name in {"客户行业", "客户级别"}:
                # 行业和级别即使是合法选项，也不能从普通叙述或产品常识反推。
                evidence_ok = all(
                    AIGateway._has_explicit_enum_evidence(field_name, item, source_text)
                    or (field_name == "客户行业" and item in source_text)
                    for item in values
                )
            elif all(item in _ENUM_OPTIONS[field_name] for item in values):
                continue
            else:
                evidence_ok = all(
                    AIGateway._has_explicit_enum_evidence(field_name, item, source_text)
                    for item in values
                )
            if not evidence_ok and "其他" in values:
                enrichment_value = analysis.enrichment.get(field_name)
                evidence_ok = isinstance(enrichment_value, str) and bool(
                    enrichment_value.strip() and enrichment_value in source_text
                )
            if evidence_ok:
                continue
            fields.pop(field_name, None)
            confidences.pop(field_name, None)
            dropped_fields.append(field_name)
        if dropped_fields:
            logger.warning(
                "ai_enum_candidates_dropped_without_evidence",
                extra={"field_names": dropped_fields},
            )
            return analysis.model_copy(
                update={"crm_fields": fields, "confidence_by_field": confidences}
            )
        return analysis

    @staticmethod
    def _has_explicit_enum_evidence(field_name: str, option: str, source_text: str) -> bool:
        """判断枚举选项是否出现在对应字段的明确语义上下文中。

        参数：field_name 为注册表字段名；option 为唯一合法选项；source_text 为原文。
        返回值：有明确标签或受控自然表达时返回 True。
        异常：无。
        副作用：无。
        """
        evidence_terms = _enum_option_evidence_terms(field_name, option)
        escaped = "(?:" + "|".join(re.escape(term) for term in evidence_terms) + ")"
        patterns: tuple[str, ...]
        if field_name == "工艺":
            # “想做装配相关”“工艺：装配”“用于装配”等表达均明确指向工艺。
            patterns = (
                rf"(?:工艺|应用工艺)\s*[:：是为]?[^。；，,\n]{{0,16}}{escaped}",
                rf"(?:主要)?想(?:做)?[^。；，,\n]{{0,16}}{escaped}",
                rf"用于[^。；，,\n]{{0,16}}{escaped}(?:相关|方面|项目)?",
            )
        elif field_name == "业务线":
            # 业务线只接受字段标签或明确采用意图，避免把供应商主营产品当成客户需求。
            patterns = (
                rf"业务线\s*[:：是为]?\s*{escaped}",
                rf"(?:想|计划|准备|考虑|希望|打算)(?:要)?(?:上|用|采用|引入|部署|采购|购买)?"
                rf"[^。；，,\n]{{0,16}}{escaped}",
            )
        else:
            # 其他枚举只接受明确字段标签，避免普通叙述被擅自提升为业务字段。
            labels = {
                "业务线": ("业务线",),
                "线索来源": ("线索来源",),
                "沟通方式": ("沟通方式",),
                "客户行业": ("客户行业", "所属行业"),
                "客户级别": ("客户级别", "客户等级"),
                "是否为国际客户": ("是否为国际客户",),
            }.get(field_name, ())
            patterns = tuple(
                rf"(?:{re.escape(label)})\s*[:：是为]?\s*{escaped}"
                for label in labels
            )
        return any(re.search(pattern, source_text) is not None for pattern in patterns)

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
    def _model_field_alias(raw_name: str) -> str:
        """将模型字段别名解析为注册表字段名，不执行模糊匹配。

        参数：raw_name 为模型返回的字段名。
        返回值：注册表规范字段名，或原字段名（表示无法归一化）。
        异常：无。
        副作用：无；仅查询有限别名表。
        """
        normalized = raw_name.strip().lower()
        return _MODEL_FIELD_ALIASES.get(normalized, raw_name)

    @staticmethod
    def _model_value_has_source_evidence(value: LeadFieldValue, source_text: str) -> bool:
        """判断模型字段值是否能在当前原文中逐字核验。

        参数：value 为 CRM 字段候选；source_text 为脱敏后的当前消息文本。
        返回值：候选为非空字符串或字符串数组且全部有原文证据时返回 True。
        异常：无。
        副作用：无；不调用外部服务。
        """
        values = value if isinstance(value, list) else [value]
        return bool(values) and all(
            isinstance(item, str) and item.strip() and item in source_text for item in values
        )

    @staticmethod
    def _normalize_enum_candidate(
        field_name: str, value: LeadFieldValue, source_text: str
    ) -> tuple[LeadFieldValue | None, str | None]:
        """将枚举候选归一化为合法选项，必要时落到“其他”并保留原文。

        参数：field_name 为规范枚举字段；value 为模型候选；source_text 为当前原文。
        返回值：规范枚举值及可选的“其他”补充原文；无法确认时返回 None。
        异常：无；不合法候选不会抛出业务异常。
        副作用：无；不修改传入对象。
        """
        options = _ENUM_OPTIONS.get(field_name)
        if options is None:
            return value, None
        raw_values = value if isinstance(value, list) else [value]
        if not raw_values or not all(isinstance(item, str) for item in raw_values):
            return None, None
        known_values = [item for item in raw_values if item in options]
        unknown_values = [
            item for item in raw_values if item not in options and item in source_text
        ]
        if len(known_values) != len(raw_values) and field_name not in ENUM_FIELDS_WITH_OTHER:
            return None, None
        if unknown_values:
            if "其他" not in options:
                return None, None
            if "其他" not in known_values:
                known_values.append("其他")
            normalized: LeadFieldValue = (
                known_values if isinstance(value, list) else "其他"
            )
            return normalized, "、".join(unknown_values)
        if not known_values:
            return None, None
        return (
            known_values if isinstance(value, list) else known_values[0],
            None,
        )

    @staticmethod
    def _normalize_enrichment_field_placement(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """把模型误放入错误位置的备注素材安全归入 enrichment。

        参数：analysis 为模型结构化分析结果；source_text 为当前脱敏原文。
        返回值：仅调整补充信息字段位置、同步移除其无关置信度后的分析结果。
        异常：无；可识别别名按注册表归一化，无法归一化字段不会进入业务结构。
        副作用：记录字段归位、别名归一化或丢弃告警，不记录客户原文和候选值。
        """
        fields = dict(analysis.crm_fields)
        enrichment = dict(analysis.enrichment)
        confidences = dict(analysis.confidence_by_field)
        moved_fields: list[str] = []
        aliased_fields: list[str] = []
        quarantined_fields: list[str] = []
        dropped_unknown_fields: list[str] = []
        conflicted_fields: list[str] = []

        # 先把模型返回的有限别名归一化为注册表字段；未知键绝不动态创建字段。
        for alias in tuple(fields):
            if alias == "备注":
                # 备注是智能表格中的文本列，但最终内容仍由 T09 备注生成器统一编排。
                # 模型偶尔把原文备注放在 crm_fields 时，只把有原文证据的文本转为素材，
                # 不能让这个位置错误阻塞整条消息，也不能绕过人工编辑保护直接写表格。
                value = fields.pop(alias)
                confidences.pop(alias, None)
                if not AIGateway._model_value_has_source_evidence(value, source_text):
                    dropped_unknown_fields.append(alias)
                    continue
                text_value = value if isinstance(value, str) else ""
                if not text_value:
                    dropped_unknown_fields.append(alias)
                    continue
                existing_value = enrichment.get("特殊要求")
                if existing_value is None:
                    enrichment["特殊要求"] = text_value
                    moved_fields.append(alias)
                elif existing_value != text_value:
                    conflicted_fields.append(alias)
                continue
            target_field = AIGateway._model_field_alias(alias)
            if target_field == alias:
                continue
            value = fields.pop(alias)
            confidence = confidences.pop(alias, None)
            if not AIGateway._model_value_has_source_evidence(value, source_text):
                dropped_unknown_fields.append(alias)
                continue
            if target_field in _ENRICHMENT_ONLY_FIELD_NAMES:
                text_value = value if isinstance(value, str) else ""
                if not text_value:
                    dropped_unknown_fields.append(alias)
                    continue
                existing_value = enrichment.get(target_field)
                if existing_value is None:
                    enrichment[target_field] = text_value
                    aliased_fields.append(alias)
                elif existing_value != text_value:
                    conflicted_fields.append(alias)
                continue
            normalized_value, detail = AIGateway._normalize_enum_candidate(
                target_field, value, source_text
            )
            if normalized_value is None:
                dropped_unknown_fields.append(alias)
                continue
            current_value = fields.get(target_field)
            if current_value is not None and current_value != normalized_value:
                conflicted_fields.append(alias)
                continue
            fields[target_field] = normalized_value
            confidences[target_field] = confidence if confidence is not None else 1.0
            if detail:
                enrichment[target_field] = detail
            aliased_fields.append(alias)

        for alias in tuple(enrichment):
            target_field = AIGateway._model_field_alias(alias)
            if target_field == alias:
                continue
            value = enrichment.pop(alias)
            if not value or value not in source_text:
                dropped_unknown_fields.append(alias)
                continue
            if target_field in _ENRICHMENT_ONLY_FIELD_NAMES:
                existing_value = enrichment.get(target_field)
                if existing_value is None:
                    enrichment[target_field] = value
                    aliased_fields.append(alias)
                elif existing_value != value:
                    conflicted_fields.append(alias)
                continue
            normalized_value, detail = AIGateway._normalize_enum_candidate(
                target_field, value, source_text
            )
            if normalized_value is None:
                dropped_unknown_fields.append(alias)
                continue
            current_value = fields.get(target_field)
            if current_value is not None and current_value != normalized_value:
                conflicted_fields.append(alias)
                continue
            fields[target_field] = normalized_value
            confidences[target_field] = 1.0
            if detail:
                enrichment[target_field] = detail
            aliased_fields.append(alias)
        for field_name in tuple(fields):
            if field_name in _ENRICHMENT_ONLY_FIELD_NAMES:
                value = fields.pop(field_name)
                confidences.pop(field_name, None)
                if not isinstance(value, str):
                    # 补充信息是文本素材，多值候选不能绕过字段类型校验进入备注。
                    dropped_unknown_fields.append(field_name)
                    continue
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
            if not isinstance(value, str) or not value or value not in source_text:
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

        # Provider 可能绕过输出 schema，将未知键直接放入 enrichment；未知键不能
        # 进入任何业务字段，也不能被当作新的字段契约，只能安全丢弃。
        for field_name in tuple(enrichment):
            if field_name in _ENRICHMENT_FIELD_NAMES:
                continue

            value = enrichment.pop(field_name)
            del value
            dropped_unknown_fields.append(field_name)

        # “想采购20台C12L”描述的是客户需求，不是客户主营产品；
        # 只有原文明确出现产品类别表达时，才允许保留“主营产品”。
        product_value = enrichment.get("主营产品")
        if (
            isinstance(product_value, str)
            and product_value in source_text
            and _PURCHASE_INTENT_PATTERN.search(source_text) is not None
            and not AIGateway._has_enrichment_category_evidence(
                source_text, product_value, _PRODUCT_CATEGORY_TERMS
            )
        ):
            requirement = AIGateway._extract_purchase_requirement(source_text, product_value)
            if requirement:
                enrichment.pop("主营产品", None)
                existing_requirement = enrichment.get("客户需求/痛点")
                if existing_requirement and existing_requirement != requirement:
                    enrichment["客户需求/痛点"] = f"{existing_requirement}；{requirement}"
                else:
                    enrichment["客户需求/痛点"] = requirement
                moved_fields.append("主营产品→客户需求/痛点")

        if aliased_fields:
            logger.warning(
                "ai_model_field_alias_normalized",
                extra={"field_names": aliased_fields},
            )
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
            moved_fields
            or aliased_fields
            or quarantined_fields
            or dropped_unknown_fields
            or conflicted_fields
        ):
            return analysis
        return analysis.model_copy(
            update={
                "crm_fields": fields,
                "enrichment": enrichment,
                "confidence_by_field": confidences,
            }
        )

    @staticmethod
    def _recover_explicit_enrichment_fields(
        analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """从明确自然语言短语恢复主营产品与客户需求备注素材。

        参数：analysis 为已完成字段归位的模型结果；source_text 为当前消息原文。
        返回值：仅新增逐字出现在原文中的补充信息，不覆盖模型已有事实。
        异常：无；无法定位连续原文时保持原结果。
        副作用：无外部调用，不记录客户原文。
        """
        enrichment = dict(analysis.enrichment)
        recovered_fields: list[str] = []
        if not enrichment.get("主营产品"):
            product_match = re.search(
                r"是做\s*([^，,；;。\n]{1,30}?)(?:的)?(?=\s*[，,；;。]|$)",
                source_text,
            )
            if product_match:
                product = product_match.group(1).strip()
                if product:
                    enrichment["主营产品"] = product
                    recovered_fields.append("主营产品")
        if not enrichment.get("客户需求/痛点"):
            demand_match = re.search(
                r"(?:想了解|主要想|想做|希望|需要|打算|计划(?:采购)?)[^。；;\n]*",
                source_text,
            )
            if demand_match:
                demand = demand_match.group(0).strip(" ，,；;")
                # 预算属于独立备注段落，不能被拼进工艺/需求素材。
                demand = re.split(r"[，,；;](?=预算|投入金额|项目金额)", demand, maxsplit=1)[0]
                demand = demand.strip(" ，,；;")
                has_specific_intent = _PURCHASE_INTENT_PATTERN.search(demand) is not None or any(
                    option in demand for option in PROCESS_OPTIONS
                )
                if demand and has_specific_intent:
                    enrichment["客户需求/痛点"] = demand
                    recovered_fields.append("客户需求/痛点")
        if not recovered_fields:
            return analysis
        logger.info(
            "ai_enrichment_fields_recovered_from_source",
            extra={"field_names": recovered_fields},
        )
        return analysis.model_copy(update={"enrichment": enrichment})

    @staticmethod
    def _extract_purchase_requirement(source_text: str, product_value: str) -> str | None:
        """提取包含采购动作和产品型号的连续原文片段。

        参数：source_text 为当前消息原文；product_value 为模型提取的产品片段。
        返回值：可作为客户需求素材的连续片段；无法定位时返回 None。
        异常：无。
        副作用：无，不改写原文。
        """
        pattern = re.compile(
            rf"(?:采购|购买|买|想要|需要|计划采购|准备采购)[^。；，,\n]{{0,24}}{re.escape(product_value)}"
        )
        match = pattern.search(source_text)
        return match.group(0) if match is not None else None

    def _normalize_low_confidence_enum_candidates(
        self, analysis: LeadAnalysis, source_text: str
    ) -> LeadAnalysis:
        """把低置信度枚举候选安全归一化为可写的注册表选项。

        参数：analysis 为已完成字段归位的模型结果；source_text 为脱敏原文。
        返回值：单选唯一候选或多选一至两项候选被归一化后的分析结果。
        异常：无；没有“其他”选项的非法候选仍交由业务校验拒绝。
        副作用：仅在候选有原文证据时把未知值保存为备注补充，不记录原文日志。
        """
        fields = dict(analysis.crm_fields)
        enrichment = dict(analysis.enrichment)
        changed = False
        for field_name, value in tuple(fields.items()):
            confidence = analysis.confidence_by_field.get(field_name)
            field_type = _FIELD_TYPES.get(field_name)
            options = _ENUM_OPTIONS.get(field_name)
            if (
                confidence is None
                or confidence >= self._medium_confidence_threshold
                or field_type
                not in {SmartTableFieldType.SINGLE_SELECT, SmartTableFieldType.MULTI_SELECT}
                or options is None
            ):
                continue
            raw_values = value if isinstance(value, list) else [value]
            if not raw_values or not all(
                isinstance(item, str) and item.strip() for item in raw_values
            ):
                continue
            if field_type is SmartTableFieldType.MULTI_SELECT and len(raw_values) > 2:
                # 多选候选过多无法安全预填，保持低置信度后台候选。
                continue
            unknown_values = [item for item in raw_values if item not in options]
            if not unknown_values:
                continue
            if "其他" not in options:
                continue
            known_values = [item for item in raw_values if item in options and item != "其他"]
            evidence_values = [
                item
                for item in unknown_values
                if self._has_explicit_enum_evidence(field_name, item, source_text)
            ]
            if len(evidence_values) != len(unknown_values):
                continue
            normalized: LeadFieldValue
            if isinstance(value, list):
                normalized = [*known_values, "其他"]
            else:
                normalized = "其他"
            fields[field_name] = normalized
            enrichment[field_name] = "、".join(evidence_values)
            changed = True
        if not changed:
            return analysis
        return analysis.model_copy(update={"crm_fields": fields, "enrichment": enrichment})

    @staticmethod
    def _can_prefill_low_confidence_enum(
        field_name: str, value: LeadFieldValue
    ) -> bool:
        """判断低置信度枚举候选是否满足单选或多选预填数量限制。

        参数：field_name 为注册表字段名；value 为已完成合法化的候选值。
        返回值：候选满足字段类型及枚举选项约束时返回 True。
        异常：无。
        副作用：无。
        """
        field_type = _FIELD_TYPES.get(field_name)
        options = _ENUM_OPTIONS.get(field_name)
        if field_type is SmartTableFieldType.SINGLE_SELECT:
            return isinstance(value, str) and value in (options or ())
        if field_type is SmartTableFieldType.MULTI_SELECT:
            values = value if isinstance(value, list) else [value]
            return 1 <= len(values) <= 2 and all(
                isinstance(item, str) and item in (options or ()) for item in values
            )
        return False

    def _low_confidence_enum_has_evidence(
        self, analysis: LeadAnalysis, field_name: str, value: LeadFieldValue, source_text: str
    ) -> bool:
        """确认低置信度枚举候选逐项具有当前原文的字段语义证据。

        参数：analysis 为已归一化分析结果；field_name、value 为当前枚举候选；
        source_text 为脱敏后的原始消息文本。
        返回值：每个候选均有受控字段证据时返回 True。
        异常：无。
        副作用：无，不调用外部服务。
        """
        values = value if isinstance(value, list) else [value]
        for item in values:
            if item == "其他":
                raw = analysis.enrichment.get(field_name)
                if not raw or not self._has_explicit_enum_evidence(field_name, raw, source_text):
                    return False
            elif not isinstance(item, str) or not self._has_explicit_enum_evidence(
                field_name, item, source_text
            ):
                return False
        return True

    def _apply_confidence(
        self, analysis: LeadAnalysis, source_text: str
    ) -> tuple[
        dict[str, LeadFieldValue],
        tuple[str, ...],
        dict[str, LeadFieldValue],
        tuple[str, ...],
    ]:
        """按阈值将候选分为正式字段、待确认、后台候选和允许预填字段。

        参数：analysis 为已完成业务校验的分析结果；source_text 为当前脱敏原文。
        返回：正式字段、稳定排序待确认字段、低置信度候选和无卡片可预填字段。
        异常：无。
        副作用：无；T08 只返回待确认元数据，不实现 T09 人工确认流程。
        """
        fields: dict[str, LeadFieldValue] = {}
        pending: list[str] = []
        low_candidates: dict[str, LeadFieldValue] = {}
        pending_prefill_allowed: list[str] = []
        for field_name, value in analysis.crm_fields.items():
            if field_name == "沟通方式" and (
                not isinstance(value, str)
                or not self._has_explicit_communication_evidence(source_text, value)
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
            elif self._can_prefill_low_confidence_enum(
                field_name, value
            ) and self._low_confidence_enum_has_evidence(analysis, field_name, value, source_text):
                # 枚举候选数量受控且逐项有原文证据，写入正式字段并保留建议标记。
                fields[field_name] = value
                pending.append(field_name)
                pending_prefill_allowed.append(field_name)
            else:
                low_candidates[field_name] = value
        return fields, tuple(pending), low_candidates, tuple(pending_prefill_allowed)

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
            candidate_value = analysis.crm_fields.get(field_name)
            candidate_values: list[str | None]
            if isinstance(candidate_value, list):
                candidate_values = list(candidate_value)
            else:
                candidate_values = [candidate_value]
            if (
                field_name in ENUM_FIELDS_WITH_OTHER
                and "其他" not in candidate_values
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
            "例如‘做电气自动化’属于客户行业；若不在行业枚举中，客户行业填写‘其他’，"
            "并将连续原文‘电气自动化’放入 enrichment 的‘客户行业’，禁止输出‘业务领域’字段。"
            "例如‘主要想做喷涂方面’属于工艺，工艺填写合法选项‘喷涂’，不得创建新的工艺字段。"
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
            "字段归属必须逐句判断：‘是做/主要做某产品’可归入主营产品；‘想了解/想做/用于某工艺场景’"
            "同时归入工艺和客户需求/痛点，工艺写合法枚举、需求保留完整原文片段；"
            "同一句中的产品、工艺、需求不能互相替代，也不能因为已返回其中一个就省略另一个。"
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
