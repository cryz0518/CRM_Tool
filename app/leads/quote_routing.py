"""企业微信引用消息的确定性匹配与恢复输入。"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.messaging.models import (
    IncomingMessage,
    MessageQuoteResolution,
    MessageQuoteResolutionStatus,
    utc_now,
)


@dataclass(frozen=True)
class QuotePayload:
    """保存从 raw_payload 读取的最小引用协议视图，不写入数据库新字段。"""

    exists: bool
    message_type: str | None
    text_content: str | None


@dataclass(frozen=True)
class QuoteResolutionResult:
    """返回一条消息的引用解析行及其唯一历史来源（若存在）。"""

    resolution: MessageQuoteResolution
    source_message: IncomingMessage | None


@dataclass(frozen=True)
class QuoteRecoverySegment:
    """保存 quote recovery 的一个独立来源片段边界。"""

    message_id: str
    normalized_text: str


@dataclass(frozen=True)
class QuoteRecoveryExtractionRequest:
    """描述由服务端 quote relationship 固定为同一线索的双片段提取请求。"""

    quoted_source: QuoteRecoverySegment
    current_reply: QuoteRecoverySegment


@dataclass(frozen=True)
class QuoteRecoverySegmentResult:
    """保存一个 recovery segment 的字段与补充信息及其真实消息来源。"""

    message_id: str
    fields: dict[str, object]
    enrichment: dict[str, str]


def extract_quote_payload(raw_payload: dict[str, Any]) -> QuotePayload:
    """从完整企业微信 callback 中读取引用消息的协议字段。

    参数：raw_payload 为已持久化的完整 callback。
    返回值：不存在 quote 时返回 exists=False；当前只解释 text quote 的正文。
    异常：输入形状不符合预期时安全返回空引用视图。
    副作用：不修改输入，不访问数据库。
    """
    body = raw_payload.get("body")
    if not isinstance(body, dict) or "quote" not in body:
        return QuotePayload(False, None, None)
    quote = body.get("quote")
    if not isinstance(quote, dict):
        return QuotePayload(True, None, None)
    message_type = quote.get("msgtype")
    text = quote.get("text")
    content = text.get("content") if isinstance(text, dict) else None
    return QuotePayload(
        True,
        message_type if isinstance(message_type, str) else None,
        content if isinstance(content, str) else None,
    )


def normalize_quote_text(value: str) -> str:
    """把 quote 正文和历史 normalized_text 统一为可精确比较的文本。

    参数：value 为待比较的文本。
    返回值：Unicode NFKC 规范化、首尾空白去除并合并连续空白后的文本。
    异常：无；调用方保证 value 为字符串。
    副作用：无。
    """
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()


def resolve_message_quote(
    session: Session, current_message: IncomingMessage
) -> QuoteResolutionResult | None:
    """为当前消息创建或读取唯一的 MessageQuoteResolution。

    参数：session 为当前消费事务；current_message 为待处理消息。
    返回值：无 quote 时返回 None；有 quote 时返回幂等解析结果。
    异常：数据库写入或查询错误向调用方传播；并发唯一键冲突会读取已存在结果。
    副作用：仅在当前事务中新增引用解析事实，不执行 AI 或外部接口调用。
    """
    quote = extract_quote_payload(current_message.raw_payload)
    if not quote.exists:
        return None

    existing = session.scalar(
        select(MessageQuoteResolution)
        .where(MessageQuoteResolution.current_message_id == current_message.message_id)
        .with_for_update()
    )
    if existing is not None:
        source = (
            session.get(IncomingMessage, existing.quoted_source_message_id)
            if existing.quoted_source_message_id is not None
            else None
        )
        return QuoteResolutionResult(existing, source)

    candidates = _find_exact_quote_candidates(session, current_message, quote)
    source_id = candidates[0].message_id if len(candidates) == 1 else None
    status = (
        MessageQuoteResolutionStatus.RESOLVED
        if len(candidates) == 1
        else (
            MessageQuoteResolutionStatus.AMBIGUOUS
            if len(candidates) > 1
            else MessageQuoteResolutionStatus.NOT_FOUND
        )
    )
    resolution = MessageQuoteResolution(
        current_message_id=current_message.message_id,
        quoted_source_message_id=source_id,
        resolution_status=status.value,
        candidate_count=len(candidates),
        matched_by="exact_text" if quote.message_type == "text" else None,
        resolved_at=utc_now(),
    )
    try:
        # current_message_id 的唯一约束是跨 Worker 重试的最终幂等边界。
        with session.begin_nested():
            session.add(resolution)
            session.flush()
    except IntegrityError:
        existing = session.scalar(
            select(MessageQuoteResolution).where(
                MessageQuoteResolution.current_message_id == current_message.message_id
            )
        )
        if existing is None:
            raise
        resolution = existing
        source_id = existing.quoted_source_message_id
    source = session.get(IncomingMessage, source_id) if source_id is not None else None
    return QuoteResolutionResult(resolution, source)


def _find_exact_quote_candidates(
    session: Session, current_message: IncomingMessage, quote: QuotePayload
) -> list[IncomingMessage]:
    """按销售、顺序、会话范围和标准化正文查找所有精确候选。

    参数：session 为当前事务；current_message 为当前引用消息；quote 为协议视图。
    返回值：按数据库结果返回全部精确候选，不对多个候选排序择一。
    异常：数据库查询错误向调用方传播。
    副作用：仅读取 IncomingMessage，不写入任何状态。
    """
    if quote.message_type != "text" or quote.text_content is None:
        return []
    normalized_quote = normalize_quote_text(quote.text_content)
    candidates = session.scalars(
        select(IncomingMessage).where(
            IncomingMessage.sales_user_id == current_message.sales_user_id,
            IncomingMessage.sequence < current_message.sequence,
        )
    ).all()
    matched: list[IncomingMessage] = []
    for candidate in candidates:
        # chat_type 是强 scope；current chat_id 缺失时按真实协议退化到销售+类型 scope。
        if candidate.chat_type != current_message.chat_type:
            continue
        if current_message.chat_id is not None and candidate.chat_id != current_message.chat_id:
            continue
        if candidate.normalized_text is not None and normalize_quote_text(
            candidate.normalized_text
        ) == normalized_quote:
            matched.append(candidate)
    return matched
