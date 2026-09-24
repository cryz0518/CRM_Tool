"""企业微信确定性卡片动作的解析、持久化认领与执行边界。"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.crm.adapter import CRMAdapter
from app.messaging.models import (
    NotificationRecord,
    SalesAuthorization,
    WecomAction,
    WecomActionOutbox,
    WecomActionOutboxStatus,
    WecomActionStatus,
    WecomCallbackDelivery,
    WecomCallbackProcessingStatus,
    utc_now,
)
from app.smart_table.adapter import SmartTableAdapter

logger = logging.getLogger(__name__)

CARD_EVENT_KEY_CRM_FIELD_CONFIRM = "crm.field_confirmation.confirm"
CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE = "crm.duplicate_confirmation.continue"
CARD_EVENT_KEY_CRM_DUPLICATE_STOP = "crm.duplicate_confirmation.stop"
CARD_EVENT_KEY_DISCARD_CONFIRM = "lead.discard.confirm"
CARD_EVENT_KEY_REASSIGN_CONFIRM = "lead.reassignment.confirm"
CARD_TYPE_BUTTON_INTERACTION = "button_interaction"
CARD_TYPE_VOTE_INTERACTION = "vote_interaction"
ALLOWED_CARD_EVENT_KEYS = frozenset(
    {
        CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
        CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
        CARD_EVENT_KEY_CRM_DUPLICATE_STOP,
        CARD_EVENT_KEY_DISCARD_CONFIRM,
        CARD_EVENT_KEY_REASSIGN_CONFIRM,
    }
)

ACTION_TYPE_CRM_FIELD_CONFIRMATION = "crm_field_confirmation"
ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION = "crm_duplicate_confirmation"
ACTION_TYPE_DISCARD_CONFIRMATION = "lead_discard_confirmation"
ACTION_TYPE_REASSIGN_CONFIRMATION = "lead_reassignment_confirmation"
ALLOWED_ACTION_TYPES = frozenset(
    {
        ACTION_TYPE_CRM_FIELD_CONFIRMATION,
        ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION,
        ACTION_TYPE_DISCARD_CONFIRMATION,
        ACTION_TYPE_REASSIGN_CONFIRMATION,
    }
)
ACTION_EXPECTED_EVENT_KEYS = {
    ACTION_TYPE_CRM_FIELD_CONFIRMATION: CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION: CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
    ACTION_TYPE_DISCARD_CONFIRMATION: CARD_EVENT_KEY_DISCARD_CONFIRM,
    ACTION_TYPE_REASSIGN_CONFIRMATION: CARD_EVENT_KEY_REASSIGN_CONFIRM,
}

_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_ACTION_LEASE = timedelta(minutes=5)
_ACTION_DEFAULT_EXPIRY = timedelta(minutes=10)
_MAX_CARD_PAYLOAD_BYTES = 8192
_PII_EMAIL = re.compile(r"\b[^\s@]+@[^\s@]+\.[^\s@]+\b")
_PII_PHONE = re.compile(r"(?<!\d)\+?\d[\d ()-]{6,}\d(?!\d)")

_ACTION_TRANSITIONS: dict[str, frozenset[str]] = {
    WecomActionStatus.PENDING.value: frozenset(
        {
            WecomActionStatus.PROCESSING.value,
            WecomActionStatus.DENIED.value,
            WecomActionStatus.EXPIRED.value,
        }
    ),
    WecomActionStatus.PROCESSING.value: frozenset(
        {
            WecomActionStatus.SUCCEEDED.value,
            WecomActionStatus.DENIED.value,
            WecomActionStatus.FAILED.value,
            WecomActionStatus.PENDING_RECOVERY.value,
        }
    ),
    WecomActionStatus.PENDING_RECOVERY.value: frozenset(
        {WecomActionStatus.SUCCEEDED.value, WecomActionStatus.FAILED.value}
    ),
    WecomActionStatus.FAILED.value: frozenset(),
    WecomActionStatus.SUCCEEDED.value: frozenset(),
    WecomActionStatus.DENIED.value: frozenset(),
    WecomActionStatus.EXPIRED.value: frozenset(),
}


class CallbackParseError(ValueError):
    """表示 callback 不符合已经冻结的企业微信事件契约。"""


class CardCapabilityUnavailable(RuntimeError):
    """表示当前进程没有经过 readiness 验证的卡片/回调能力。"""


class StaleActionClaim(RuntimeError):
    """表示 Worker 的 claim token 已被 takeover 或 lease 已过期。"""


class InvalidActionTransition(RuntimeError):
    """表示试图绕过集中定义的 action 状态转移。"""


@dataclass(frozen=True)
class TemplateCardCallback:
    """承载 callback 仅允许使用的白名单传输字段。"""

    actor_user_id: str
    event_key: str
    task_id: str
    provider_msgid: str
    req_id: str
    card_type: str
    selected_option_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class CallbackClaimResult:
    """描述一次 callback delivery 的认领结果及可安全返回的摘要。"""

    code: str
    task_id: str
    action_id: str | None
    summary: str
    should_update_card: bool

    def response_card(self) -> dict[str, object]:
        """构造一次 callback response 使用的最小状态卡片。

        参数：无。
        返回值：不含业务目标的卡片响应字典。
        异常：无。
        副作用：无。
        """

        # task_id 必须原样回显服务端已解析的 opaque correlation，不读取客户端其他业务字段。
        return {
            "card_type": "text_notice",
            "task_id": self.task_id,
            "main_title": {"title": "操作状态", "desc": self.summary},
        }


@dataclass(frozen=True)
class ActionSnapshot:
    """提供给 Worker 的动作只读快照，避免把长事务 ORM 对象带出会话。"""

    id: str
    action_type: str
    bound_actor_wecom_user_id: str
    target_type: str
    target_id: str
    context: dict[str, object]
    claim_token: str


@dataclass(frozen=True)
class ActionExecutionResult:
    """描述一次 domain action 执行或已完成幂等重放。"""

    code: str
    summary: str
    executed: bool


@dataclass(frozen=True)
class DeterministicActionCommand:
    """描述消息接入层已严格解析的 T18 machine command。"""

    action_type: str
    target_id: str
    message_id: str | None = None
    segment_index: int | None = None


def parse_deterministic_action_command(text: str) -> DeterministicActionCommand | None:
    """只解析 T18 固定 machine command，不做自然语言或 LLM 意图推断。

    参数：text 为销售消息原文。
    返回值：严格匹配时返回服务端动作发行参数，否则返回 None。
    异常：无；格式错误按普通消息处理。
    副作用：无。
    """

    command_id = r"[A-Za-z0-9._-]{1,128}"
    discard = re.fullmatch(rf"t18\.discard:({command_id})", text)
    if discard is not None:
        return DeterministicActionCommand(ACTION_TYPE_DISCARD_CONFIRMATION, discard.group(1))
    reassign = re.fullmatch(
        rf"t18\.reassign:({command_id}):(\d{{1,6}}):({command_id})",
        text,
    )
    if reassign is not None:
        return DeterministicActionCommand(
            ACTION_TYPE_REASSIGN_CONFIRMATION,
            reassign.group(3),
            message_id=reassign.group(1),
            segment_index=int(reassign.group(2)),
        )
    return None


class TemplateCardCallbackParser:
    """严格解析已经真实验证的 template_card_event callback 字段。"""

    @classmethod
    def parse(cls, frame: Mapping[str, object]) -> TemplateCardCallback:
        """从官方 callback 帧读取 actor、event key、task、msgid 和 req_id。

        参数：frame 为企业微信 SDK 传入的原始 WebSocket 帧。
        返回值：仅含白名单字段的不可变 callback 结构。
        异常：字段缺失、类型错误、长度或字符集非法时抛出 CallbackParseError。
        副作用：无，不保存或记录原始帧。
        """
        # cmd、body.msgtype 和 event.eventtype 是已冻结的入口契约，缺一项即 fail closed。
        if frame.get("cmd") != "aibot_event_callback":
            raise CallbackParseError("callback cmd 不符合真实契约")
        body = cls._mapping(frame.get("body"), "body")
        if body.get("msgtype") != "event":
            raise CallbackParseError("callback body.msgtype 不符合真实契约")
        headers = cls._mapping(frame.get("headers"), "headers")
        event = cls._mapping(body.get("event"), "event")
        if event.get("eventtype") != "template_card_event":
            raise CallbackParseError("callback event.eventtype 不符合真实契约")
        card_event = cls._mapping(event.get("template_card_event"), "template_card_event")

        # 只读取真实契约明确存在的嵌套位置，不兼容猜测性的扁平字段。
        actor = cls._identifier(cls._mapping(body.get("from"), "from").get("userid"), "actor")
        event_key = cls._identifier(card_event.get("event_key"), "event_key")
        task_id = cls._identifier(card_event.get("task_id"), "task_id")
        card_type = cls._identifier(card_event.get("card_type"), "card_type")
        if card_type not in {CARD_TYPE_BUTTON_INTERACTION, CARD_TYPE_VOTE_INTERACTION}:
            raise CallbackParseError("callback card_type 不符合真实契约")
        provider_msgid = cls._identifier(body.get("msgid"), "msgid")
        req_id = cls._identifier(headers.get("req_id"), "req_id")
        return TemplateCardCallback(
            actor,
            event_key,
            task_id,
            provider_msgid,
            req_id,
            card_type,
            cls._selected_option_ids(card_event.get("selected_items")),
        )

    @classmethod
    def _selected_option_ids(cls, value: object) -> tuple[str, ...]:
        """解析卡片多选回调中的 option id，并限制数量与标识格式。

        参数：value 为企业微信 callback 的 selected_items 原始对象。
        返回值：去重后的 option id 元组；未选择时返回空元组。
        异常：结构错误、标识非法或选择数量超限时抛出 CallbackParseError。
        副作用：无。
        """
        if value is None:
            return ()
        selected = cls._mapping(value, "selected_items")
        items = selected.get("selected_item")
        if not isinstance(items, list) or len(items) > 20:
            raise CallbackParseError("callback selected_items 数量非法")
        option_ids: list[str] = []
        for item in items:
            item_mapping = cls._mapping(item, "selected_item")
            option_mapping = cls._mapping(item_mapping.get("option_ids"), "option_ids")
            values = option_mapping.get("option_id")
            if not isinstance(values, list) or len(values) > 20:
                raise CallbackParseError("callback option_ids 数量非法")
            for option_id in values:
                option_ids.append(cls._identifier(option_id, "option_id"))
        if len(option_ids) > 20 or len(set(option_ids)) != len(option_ids):
            raise CallbackParseError("callback option_id 重复或数量非法")
        return tuple(option_ids)

    @staticmethod
    def _mapping(value: object, name: str) -> dict[str, object]:
        """将协议对象限制为字符串键字典。

        参数：value 为待校验对象，name 为错误信息中的字段名。
        返回值：类型安全的字典。
        异常：类型不正确时抛出 CallbackParseError。
        副作用：无。
        """

        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            raise CallbackParseError(f"callback {name} 类型非法")
        return value

    @staticmethod
    def _identifier(value: object, name: str) -> str:
        """校验 callback 标识的长度、字符集和换行安全性。

        参数：value 为外部标识，name 为错误信息中的字段名。
        返回值：通过校验的标识字符串。
        异常：类型、长度或字符集不合法时抛出 CallbackParseError。
        副作用：无。
        """

        if not isinstance(value, str) or _ID_PATTERN.fullmatch(value) is None:
            raise CallbackParseError(f"callback {name} 非法")
        return value


class WecomActionService:
    """管理服务端动作实例、callback 认领和一次性执行 Outbox。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        card_callback_ready: bool = False,
        action_expiry: timedelta = _ACTION_DEFAULT_EXPIRY,
    ) -> None:
        """保存 T18 持久化边界和已验证卡片能力。

        参数：session_factory 为数据库会话工厂；card_callback_ready 表示卡片 transport/readiness；
        action_expiry 为新动作默认有效期。
        返回值：无。
        异常：无；实际数据库错误在公开方法中传播。
        副作用：无，仅保存依赖。
        """
        if action_expiry <= timedelta(0):
            raise ValueError("卡片动作有效期必须大于零")
        self._session_factory = session_factory
        self._card_callback_ready = card_callback_ready
        self._action_expiry = action_expiry

    def issue_action(
        self,
        *,
        actor_user_id: str,
        action_type: str,
        target_type: str,
        target_id: str,
        expected_action_key: str,
        context: Mapping[str, object],
        title: str,
        description: str,
        source_message_id: str | None = None,
        expires_at: datetime | None = None,
    ) -> WecomAction:
        """持久化业务动作并在同一事务中登记待发送卡片通知。

        参数：actor_user_id、动作类型、服务端目标、预期 action key 和白名单 context
        共同冻结业务事实；
        title/description 为卡片展示摘要；source_message_id 为可选的已持久化来源消息。
        返回值：已持久化的动作副本。
        异常：能力未就绪、动作参数非法或销售未授权时抛出 ValueError/PermissionError。
        副作用：新增 WecomAction 与 NotificationRecord，但不直接调用企业微信。
        """
        if not self._card_callback_ready:
            # 没有真实卡片 provider 时不创建会永远无法完成的业务动作，交给既有表格 fallback。
            raise CardCapabilityUnavailable("企业微信卡片回调能力未就绪")
        self._validate_action_definition(action_type, expected_action_key, target_type, target_id)
        if (
            not isinstance(title, str)
            or not 0 < len(title) <= 128
            or "\n" in title
            or "\r" in title
            or not isinstance(description, str)
            or not 0 < len(description) <= 256
            or "\n" in description
            or "\r" in description
        ):
            raise ValueError("动作卡片展示文本非法")
        safe_context = _safe_context(context)
        normalized_expiry = expires_at or (utc_now() + self._action_expiry)
        if normalized_expiry.tzinfo is None:
            normalized_expiry = normalized_expiry.replace(tzinfo=UTC)
        issuance_key = _issuance_key(
            actor_user_id, action_type, target_type, target_id, safe_context
        )
        task_id = f"t18_{uuid4().hex}"
        action_id = str(uuid4())

        with self._session_factory.begin() as session:
            # 每次发卡片前重新读取授权目录，禁止用旧会话或 UI 可见范围代替后端授权。
            authorization = session.get(SalesAuthorization, actor_user_id)
            if (
                authorization is None
                or not authorization.is_active
                or not authorization.is_authorized
            ):
                raise PermissionError("卡片动作操作者未授权")
            # 发行幂等键由服务端业务事实生成；并发发行由数据库唯一约束收敛。
            existing = session.scalar(
                select(WecomAction).where(WecomAction.issuance_key == issuance_key)
            )
            if existing is not None:
                session.expunge(existing)
                return existing
            action = WecomAction(
                id=action_id,
                task_id=task_id,
                issuance_key=issuance_key,
                action_type=action_type,
                bound_actor_wecom_user_id=actor_user_id,
                target_type=target_type,
                target_id=target_id,
                expected_action_key=expected_action_key,
                status=WecomActionStatus.PENDING.value,
                expires_at=normalized_expiry,
                context=safe_context,
            )
            try:
                with session.begin_nested():
                    session.add(action)
                    session.flush()
            except IntegrityError:
                # 另一个发行者已提交唯一键；重新读取同一个服务端 action，不生成第二张卡。
                existing = session.scalar(
                    select(WecomAction).where(WecomAction.issuance_key == issuance_key)
                )
                if existing is None:
                    raise
                session.expunge(existing)
                return existing
            # 事务提交前先把完整卡片 body 固化；通知重试只重发卡片，不重建 action。
            notification_key = hashlib.sha256(f"wecom_action_card:{action_id}".encode()).hexdigest()
            session.add(
                NotificationRecord(
                    notification_key=notification_key,
                    sales_user_id=actor_user_id,
                    source_message_id=source_message_id or action_id,
                    notification_type="wecom_action_card",
                    content=description,
                    payload={
                        "msgtype": "template_card",
                        "template_card": build_action_card(
                            task_id=task_id,
                            event_key=expected_action_key,
                            title=title,
                            description=description,
                            duplicate_leads=safe_context.get("duplicate_leads"),
                        ),
                    },
                )
            )
            logger.info(
                "wecom_action_issued",
                extra={"action_id": action.id, "event": "action_issued"},
            )
            # 调用方只需要把服务端 task_id 写入测试/日志上下文；脱离事务前显式分离以保留已生成字段。
            session.expunge(action)
            return action

    def claim_callback(self, frame: Mapping[str, object]) -> CallbackClaimResult:
        """在 5 秒 callback 窗口内解析、鉴权、原子认领动作并写入执行 Outbox。

        参数：frame 为 SDK callback 原始帧；仅使用真实契约白名单字段。
        返回值：确定性的 ack/update 摘要；不执行完整业务工作流。
        异常：协议错误抛出 CallbackParseError；数据库异常向上抛出以便连接层记录。
        副作用：写入 callback evidence；首次合法点击将动作置为 processing 并新增 Outbox。
        """
        callback = TemplateCardCallbackParser.parse(frame)
        if not self._card_callback_ready:
            # 未通过部署 readiness 的进程即使收到旧 callback 也不能创建执行 Outbox。
            return CallbackClaimResult(
                "card_callback_unavailable",
                callback.task_id,
                None,
                "卡片操作能力当前不可用",
                False,
            )
        with self._session_factory.begin() as session:
            # provider_msgid 只做传输投递去重，绝不参与业务 action 身份计算。
            previous = session.scalar(
                select(WecomCallbackDelivery).where(
                    WecomCallbackDelivery.provider_msgid == callback.provider_msgid
                )
            )
            if previous is not None:
                logger.info(
                    "wecom_callback_duplicate",
                    extra={"action_id": previous.action_id, "event": "callback_duplicate"},
                )
                return CallbackClaimResult(
                    code="duplicate_delivery",
                    task_id=callback.task_id,
                    action_id=previous.action_id,
                    summary="该回调已接收，业务动作不会重复执行",
                    should_update_card=False,
                )

            action = session.scalar(
                select(WecomAction).where(WecomAction.task_id == callback.task_id).with_for_update()
            )
            delivery = WecomCallbackDelivery(
                action_id=action.id if action is not None else None,
                provider_msgid=callback.provider_msgid,
                req_id=callback.req_id,
                actor_user_id=callback.actor_user_id,
                event_key=callback.event_key,
                task_id=callback.task_id,
            )
            try:
                # 唯一约束吸收并发同 msgid delivery；不同 msgid 继续由 action 行锁拦截。
                with session.begin_nested():
                    session.add(delivery)
                    session.flush()
            except IntegrityError:
                previous = session.scalar(
                    select(WecomCallbackDelivery).where(
                        WecomCallbackDelivery.provider_msgid == callback.provider_msgid
                    )
                )
                return CallbackClaimResult(
                    code="duplicate_delivery",
                    task_id=callback.task_id,
                    action_id=previous.action_id if previous is not None else None,
                    summary="该回调已接收，业务动作不会重复执行",
                    should_update_card=False,
                )

            if action is None:
                return self._reject_delivery(delivery, "unknown_task", "卡片动作不存在或已失效")
            # 授权和 actor 绑定在每次 callback 重新读取，不能相信卡片发行时的旧权限。
            authorization = session.get(SalesAuthorization, callback.actor_user_id)
            if (
                authorization is None
                or not authorization.is_active
                or not authorization.is_authorized
            ):
                return self._deny_action(
                    action, delivery, "actor_unauthorized", "当前账号无操作权限"
                )
            if callback.actor_user_id != action.bound_actor_wecom_user_id:
                return self._deny_action(action, delivery, "actor_mismatch", "该卡片不属于当前账号")

            if callback.event_key not in ALLOWED_CARD_EVENT_KEYS:
                # 未知 key 不改变服务端 action，防止错误 key 造成合法卡片永久失效。
                return self._reject_delivery(delivery, "unknown_event_key", "不支持的卡片操作")
            duplicate_keys = {
                CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
                CARD_EVENT_KEY_CRM_DUPLICATE_STOP,
            }
            event_key_matches = (
                callback.event_key in duplicate_keys
                if action.action_type == ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION
                else callback.event_key == action.expected_action_key
            )
            if not event_key_matches:
                return self._deny_action(
                    action, delivery, "event_key_mismatch", "卡片操作与服务端动作不匹配"
                )

            # 过期检查使用服务端数据库时钟快照；客户端不能延长 expires_at。
            now = utc_now()
            if action.status != WecomActionStatus.PENDING.value:
                # processing/succeeded/denied/failed 均不重新进入 domain service。
                delivery.processing_status = WecomCallbackProcessingStatus.DUPLICATED.value
                delivery.result_code = f"action_{action.status}"
                delivery.processed_at = now
                return CallbackClaimResult(
                    f"action_{action.status}",
                    action.task_id,
                    action.id,
                    action.result_summary or "该操作已受理，业务动作不会重复执行",
                    True,
                )

            if action.action_type == ACTION_TYPE_CRM_FIELD_CONFIRMATION:
                # 字段确认在 callback claim 前就锁定并核对当前表格负责人，避免先创建 outbox 再拒绝。
                from app.leads.models import Lead

                lead = session.scalar(
                    select(Lead).where(Lead.id == action.target_id).with_for_update()
                )
                if lead is None or lead.smart_table_owner_user_id != callback.actor_user_id:
                    return self._deny_action(
                        action,
                        delivery,
                        "owner_mismatch",
                        "当前账号已不是智能表格负责人",
                    )

            if action.action_type == ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION:
                # 重复确认卡携带的 option id 只能映射到发行时冻结的 Lead 集合。
                from app.leads.models import Lead

                duplicate_leads = action.context.get("duplicate_leads")
                lead_ids = _context_lead_ids(duplicate_leads)
                owned_ids = set(
                    session.scalars(
                        select(Lead.id).where(
                            Lead.id.in_(lead_ids),
                            Lead.smart_table_owner_user_id == callback.actor_user_id,
                        )
                    ).all()
                )
                if not lead_ids or owned_ids != set(lead_ids):
                    return self._deny_action(
                        action,
                        delivery,
                        "owner_mismatch",
                        "重复线索中存在当前账号无权操作的记录",
                    )
                invalid_selected = set(callback.selected_option_ids) - set(lead_ids)
                if invalid_selected:
                    return self._deny_action(
                        action,
                        delivery,
                        "selection_mismatch",
                        "卡片选择项已失效，请重新提交",
                    )
                action.context = {
                    **action.context,
                    "decision": (
                        "continue"
                        if callback.event_key == CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE
                        else "stop"
                    ),
                    "selected_lead_ids": list(callback.selected_option_ids),
                }

            if _as_utc(now) >= _as_utc(action.expires_at):
                _transition_action(action, WecomActionStatus.EXPIRED.value)
                action.processed_at = now
                action.result_code = "expired"
                action.result_summary = "卡片已过期，请重新发起操作"
                delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
                delivery.result_code = "expired"
                delivery.processed_at = now
                logger.info(
                    "wecom_action_expired",
                    extra={"action_id": action.id, "event": "action_expired"},
                )
                return CallbackClaimResult(
                    "expired", action.task_id, action.id, action.result_summary, True
                )

            # 行锁保护下仅 pending 可以变为 processing；Outbox 与 claim 在同一事务中提交。
            _transition_action(action, WecomActionStatus.PROCESSING.value)
            action.claimed_at = now
            action.processing_lease_expires_at = now + _ACTION_LEASE
            session.add(
                WecomActionOutbox(
                    action_id=action.id,
                    status=WecomActionOutboxStatus.PENDING.value,
                )
            )
            delivery.processing_status = WecomCallbackProcessingStatus.CLAIMED.value
            delivery.result_code = "claimed"
            delivery.processed_at = now
            logger.info(
                "wecom_callback_received",
                extra={"action_id": action.id, "event": "callback_received"},
            )
            return CallbackClaimResult(
                "claimed", action.task_id, action.id, "已受理，后台正在处理", True
            )

    def execute_action(
        self,
        action_id: str,
        domain_executor: Callable[[ActionSnapshot], tuple[str, str]],
    ) -> ActionExecutionResult:
        """认领一次动作 Outbox 并调用注入的确定性 domain service。

        参数：action_id 为服务端内部动作 ID；domain_executor 只能调用确定性业务服务。
        返回值：执行结果；已完成动作返回 executed=False。
        异常：数据库异常传播；domain 异常会转为 pending_recovery，避免通知重试重做业务。
        副作用：可能调用一次 domain service，并独立登记最终通知。
        """
        with self._session_factory.begin() as session:
            outbox = session.scalar(
                select(WecomActionOutbox)
                .where(WecomActionOutbox.action_id == action_id)
                .with_for_update()
            )
            action = session.scalar(
                select(WecomAction).where(WecomAction.id == action_id).with_for_update()
            )
            if outbox is None or action is None:
                raise ValueError("T18 动作执行事实不存在")
            outbox.dispatch_claimed_at = None
            outbox.dispatch_lease_expires_at = None
            if outbox.status == WecomActionOutboxStatus.SUCCEEDED.value or action.status == (
                WecomActionStatus.SUCCEEDED.value
            ):
                return ActionExecutionResult(
                    "already_succeeded", action.result_summary or "已处理", False
                )
            if action.status != WecomActionStatus.PROCESSING.value:
                return ActionExecutionResult(
                    f"action_{action.status}", action.result_summary or "动作不可执行", False
                )

            now = utc_now()
            lease_expired = outbox.processing_lease_expires_at is not None and _as_utc(
                outbox.processing_lease_expires_at
            ) <= _as_utc(now)
            if outbox.status == WecomActionOutboxStatus.PROCESSING.value and not lease_expired:
                return ActionExecutionResult("already_processing", "已在处理中", False)
            if lease_expired and outbox.domain_started_at is not None:
                # 领域调用已经取得不可逆 operation 事实，不能自动 takeover 再调用一次。
                _transition_action(action, WecomActionStatus.PENDING_RECOVERY.value)
                action.processed_at = now
                action.result_code = "domain_operation_recovery_required"
                action.result_summary = "领域操作已开始但 Worker 失联，需要恢复核对"
                outbox.status = WecomActionOutboxStatus.FAILED.value
                outbox.processing_started_at = None
                outbox.processing_lease_expires_at = None
                self._add_result_notification(session, action, action.result_summary)
                logger.warning(
                    "wecom_action_recovery_required",
                    extra={"action_id": action.id, "event": "action_recovery"},
                )
                return ActionExecutionResult("recovery_required", action.result_summary, False)

            # Worker 可能在 callback claim 后延迟执行；执行前再次读取授权，避免撤销后仍产生副作用。
            authorization = session.get(SalesAuthorization, action.bound_actor_wecom_user_id)
            if (
                authorization is None
                or not authorization.is_active
                or not authorization.is_authorized
            ):
                _transition_action(action, WecomActionStatus.DENIED.value)
                action.processed_at = now
                action.result_code = "actor_unauthorized"
                action.result_summary = "操作人授权已撤销，未执行该动作"
                outbox.status = WecomActionOutboxStatus.FAILED.value
                outbox.processing_started_at = None
                outbox.processing_lease_expires_at = None
                self._add_result_notification(session, action, action.result_summary)
                return ActionExecutionResult("actor_unauthorized", action.result_summary, False)

            # 每次初次执行或 takeover 都生成新 token；旧 Worker 的 token 随即失效。
            claim_token = uuid4().hex
            outbox.status = WecomActionOutboxStatus.PROCESSING.value
            outbox.claim_token = claim_token
            outbox.attempts += 1
            outbox.processing_started_at = now
            outbox.processing_lease_expires_at = now + _ACTION_LEASE
            action.processing_lease_expires_at = outbox.processing_lease_expires_at
            snapshot = ActionSnapshot(
                id=action.id,
                action_type=action.action_type,
                bound_actor_wecom_user_id=action.bound_actor_wecom_user_id,
                target_type=action.target_type,
                target_id=action.target_id,
                context=dict(action.context),
                claim_token=claim_token,
            )
            logger.info(
                "wecom_action_claimed",
                extra={"action_id": action.id, "event": "action_claimed"},
            )

        try:
            # 外部业务调用在事务之外执行；业务动作状态已锁定，通知重试不会再次到这里。
            result_code, result_summary = domain_executor(snapshot)
        except StaleActionClaim:
            logger.info(
                "wecom_action_stale_worker_ignored",
                extra={"action_id": action_id, "event": "action_recovery"},
            )
            return ActionExecutionResult("stale_claim", "Worker claim 已失效", False)
        except Exception as error:
            logger.warning(
                "wecom_action_domain_execution_failed",
                extra={
                    "action_id": action_id,
                    "event": "action_recovery",
                    "error_type": type(error).__name__,
                },
            )
            return self._finish_action(
                action_id,
                claim_token=claim_token,
                status=WecomActionStatus.PENDING_RECOVERY.value,
                outbox_status=WecomActionOutboxStatus.FAILED.value,
                result_code="domain_failed",
                result_summary=f"业务动作失败，需要人工处理（{type(error).__name__}）",
            )
        return self._finish_action(
            action_id,
            claim_token=claim_token,
            status=WecomActionStatus.SUCCEEDED.value,
            outbox_status=WecomActionOutboxStatus.SUCCEEDED.value,
            result_code=result_code,
            result_summary=result_summary,
        )

    def begin_domain_operation(
        self,
        action_id: str,
        claim_token: str,
        payload: Mapping[str, object] | None = None,
    ) -> None:
        """以 claim token 原子登记领域副作用开始事实。

        参数：action_id 为内部动作标识；claim_token 为当前 Worker token；payload 为白名单操作快照。
        返回值：无。
        异常：claim 失效、lease 过期或领域操作已开始时抛出 StaleActionClaim；数据库错误传播。
        副作用：持久化不可重复的 operation key，阻止 takeover 再次进入领域服务。
        """

        with self._session_factory.begin() as session:
            action, outbox = self._lock_action_and_outbox(session, action_id)
            self._assert_locked_claim(action, outbox, claim_token)
            if outbox.domain_started_at is not None:
                raise StaleActionClaim("领域 operation 已由其他 Worker 开始")
            outbox.domain_started_at = utc_now()
            outbox.domain_operation_key = action.id
            outbox.domain_operation_payload = (
                _safe_operation_payload(payload) if payload is not None else None
            )
            outbox.remote_effect_status = "unknown"
            logger.info(
                "wecom_action_domain_started",
                extra={"action_id": action.id, "event": "action_claimed"},
            )

    def mark_remote_effect_succeeded(
        self,
        action_id: str,
        claim_token: str,
        payload: Mapping[str, object] | None = None,
    ) -> None:
        """保存外部 Smart Table 写入成功事实，供字段确认恢复使用。

        参数：action_id 与 claim_token 定位当前合法领域操作；payload 为已脱敏的远端结果事实。
        返回值：无。
        异常：Worker 已失去 fencing 时抛出 StaleActionClaim；数据库错误传播。
        副作用：只更新 operation recovery metadata，不改变业务 action 状态。
        """

        with self._session_factory.begin() as session:
            action, outbox = self._lock_action_and_outbox(session, action_id)
            self._assert_locked_claim(action, outbox, claim_token)
            outbox.remote_effect_status = "succeeded"
            outbox.remote_effect_at = utc_now()
            if payload is not None:
                outbox.domain_operation_payload = _safe_operation_payload(payload)

    def record_callback_transport_failure(
        self, provider_msgid: str, failure: BaseException
    ) -> None:
        """持久化 callback card update 失败，不改变已认领的业务动作。

        参数：provider_msgid 为已保存的 callback transport id；failure 为 SDK/网络异常。
        返回值：无。
        异常：数据库错误传播；异常摘要只保存类型，不保存原始 payload 或 secret。
        副作用：更新 delivery transport evidence，供 Console/审计查询。
        """

        with self._session_factory.begin() as session:
            delivery = session.scalar(
                select(WecomCallbackDelivery)
                .where(WecomCallbackDelivery.provider_msgid == provider_msgid)
                .with_for_update()
            )
            if delivery is None:
                return
            delivery.transport_stage = "callback_card_update"
            delivery.transport_status = "failed"
            delivery.transport_failure_code = type(failure).__name__[:64]
            delivery.transport_failure_summary = "企业微信 callback card update 传输失败"
            delivery.transport_failed_at = utc_now()
            logger.warning(
                "wecom_callback_card_update_failed",
                extra={
                    "action_id": delivery.action_id,
                    "event": "callback_card_update_failed",
                },
            )

    def reconcile_field_confirmation(
        self,
        action_id: str,
        smart_table_adapter: SmartTableAdapter,
    ) -> ActionExecutionResult:
        """核对远端字段确认已成功但本地 finalize 失败的 pending recovery 动作。

        参数：action_id 为待恢复动作；smart_table_adapter 用于读取当前远端事实。
        返回值：恢复成功或仍需人工处理的确定性结果。
        异常：远端状态不匹配时保留 pending_recovery 并返回 recovery_required。
        副作用：成功时只补写本地 provenance/confirmation event，不再次 update Smart Table。
        """

        from app.leads.review import LeadReviewService

        with self._session_factory() as session:
            action = session.get(WecomAction, action_id)
            outbox = session.scalar(
                select(WecomActionOutbox).where(WecomActionOutbox.action_id == action_id)
            )
            if action is None or outbox is None:
                raise ValueError("字段确认恢复事实不存在")
            if (
                action.action_type != ACTION_TYPE_CRM_FIELD_CONFIRMATION
                or action.status != WecomActionStatus.PENDING_RECOVERY.value
                or outbox.remote_effect_status not in {None, "unknown", "succeeded"}
            ):
                return ActionExecutionResult(
                    "recovery_not_ready", "当前动作没有可核对的远端事实", False
                )
            payload = dict(outbox.domain_operation_payload or {})
            field_values = payload.get("field_values")
            actor = action.bound_actor_wecom_user_id
            lead_id = action.target_id
        if not isinstance(field_values, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in field_values.items()
        ):
            return ActionExecutionResult(
                "recovery_required", "确认值快照不完整，需要人工处理", False
            )
        review = LeadReviewService(self._session_factory, smart_table_adapter)
        try:
            review.finalize_submission_confirmation(
                lead_id, actor, field_values, operation_id=action_id
            )
        except ValueError:
            return ActionExecutionResult("recovery_required", "远端确认事实与当前表格不一致", False)
        with self._session_factory.begin() as session:
            action, outbox = self._lock_action_and_outbox(session, action_id)
            if action.status != WecomActionStatus.PENDING_RECOVERY.value:
                return ActionExecutionResult(
                    "action_not_recoverable", "动作已被其他恢复流程处理", False
                )
            _transition_action(action, WecomActionStatus.SUCCEEDED.value)
            action.processed_at = utc_now()
            action.result_code = "confirmation_recovered"
            action.result_summary = "字段确认事实已从智能表格恢复"
            outbox.status = WecomActionOutboxStatus.SUCCEEDED.value
            outbox.remote_effect_status = "succeeded"
            outbox.remote_effect_at = utc_now()
            outbox.processing_started_at = None
            outbox.processing_lease_expires_at = None
            self._add_result_notification(session, action, action.result_summary)
        return ActionExecutionResult("confirmation_recovered", "字段确认事实已从智能表格恢复", True)

    @staticmethod
    def _lock_action_and_outbox(
        session: Session, action_id: str
    ) -> tuple[WecomAction, WecomActionOutbox]:
        """在同一事务内锁定动作和其唯一 execution outbox。"""

        action = session.scalar(
            select(WecomAction).where(WecomAction.id == action_id).with_for_update()
        )
        outbox = session.scalar(
            select(WecomActionOutbox)
            .where(WecomActionOutbox.action_id == action_id)
            .with_for_update()
        )
        if action is None or outbox is None:
            raise ValueError("T18 动作 execution 事实不存在")
        return action, outbox

    @staticmethod
    def _assert_locked_claim(
        action: WecomAction, outbox: WecomActionOutbox, claim_token: str
    ) -> None:
        """校验锁内 Worker token、状态与 processing lease 仍然有效。"""

        if (
            action.status != WecomActionStatus.PROCESSING.value
            or outbox.status != WecomActionOutboxStatus.PROCESSING.value
            or outbox.claim_token != claim_token
            or outbox.processing_lease_expires_at is None
            or _as_utc(outbox.processing_lease_expires_at) <= _as_utc(utc_now())
        ):
            raise StaleActionClaim("当前 Worker 已失去 action fencing")

    def issue_field_confirmation_action(
        self,
        *,
        actor_user_id: str,
        lead_id: str,
        field_names: tuple[str, ...],
        command_text: str,
        request_message_id: str,
        field_values: Mapping[str, str] | None = None,
    ) -> WecomAction:
        """发行 CRM 提交前字段确认动作卡。

        参数：包含销售、线索、待确认字段和原始命令的受控上下文。
        返回值：已持久化的服务端动作实例。
        异常：权限、参数或数据库错误向调用方传播。
        副作用：写入 action、模板卡通知及事务性 outbox。
        """

        return self.issue_action(
            actor_user_id=actor_user_id,
            action_type=ACTION_TYPE_CRM_FIELD_CONFIRMATION,
            target_type="lead",
            target_id=lead_id,
            expected_action_key=CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
            context={
                "field_names": list(field_names),
                "command_text": command_text,
                "request_message_id": request_message_id,
                **({"field_values": dict(field_values)} if field_values is not None else {}),
            },
            title="提交前确认字段",
            description=f"请确认：{'、'.join(field_names)}",
            source_message_id=request_message_id,
        )

    def issue_duplicate_confirmation_action(
        self,
        *,
        actor_user_id: str,
        request_message_id: str,
        duplicates: tuple[object, ...],
    ) -> WecomAction:
        """发行可批量选择重复线索并继续或停止提交的确认卡。

        参数：actor_user_id 为销售身份；request_message_id 为原提交消息；
        duplicates 为服务端冻结命中集合。
        返回值：已持久化的重复确认动作。
        异常：重复事实不完整、销售未授权或卡片能力未就绪时抛出异常。
        副作用：写入动作、卡片通知和可靠发送 outbox，不直接执行 CRM 写操作。
        """
        duplicate_context = []
        for item in duplicates:
            lead_id = getattr(item, "lead_id", None)
            company_name = getattr(item, "company_name", None)
            if not all(isinstance(value, str) for value in (lead_id, company_name)):
                raise ValueError("重复线索卡片数据不完整")
            duplicate_context.append(
                {
                    "lead_id": lead_id,
                    "company_name": company_name,
                }
            )
        names = "、".join(str(item["company_name"]) for item in duplicate_context)
        names = names[:180]
        return self.issue_action(
            actor_user_id=actor_user_id,
            action_type=ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION,
            target_type="crm_submission",
            target_id=hashlib.sha256(request_message_id.encode()).hexdigest(),
            expected_action_key=CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
            context={
                "duplicate_leads": duplicate_context,
                "request_message_id": request_message_id,
            },
            title="CRM 重复线索确认",
            description=f"当前系统有{names}的信息，请问是进行覆盖还是停止提交",
            source_message_id=request_message_id,
        )

    def issue_discard_action(
        self,
        *,
        actor_user_id: str,
        lead_id: str,
        reason: str,
        source_message_id: str | None = None,
    ) -> WecomAction:
        """发行当前销售明确请求的线索废弃二次确认卡。

        参数：actor_user_id、lead_id、reason 及可选来源消息标识。
        返回值：已持久化的废弃确认动作。
        异常：权限、参数或数据库错误向调用方传播。
        副作用：写入确认卡通知，不执行废弃业务副作用。
        """

        return self.issue_action(
            actor_user_id=actor_user_id,
            action_type=ACTION_TYPE_DISCARD_CONFIRMATION,
            target_type="lead",
            target_id=lead_id,
            expected_action_key=CARD_EVENT_KEY_DISCARD_CONFIRM,
            context={"reason": reason, "request_message_id": source_message_id}
            if source_message_id is not None
            else {"reason": reason},
            title="确认废弃线索",
            description="请确认废弃当前线索",
            source_message_id=source_message_id,
        )

    def issue_reassignment_action(
        self,
        *,
        actor_user_id: str,
        message_id: str,
        segment_index: int,
        target_lead_id: str,
        reason: str,
        source_message_id: str | None = None,
    ) -> WecomAction:
        """发行服务端冻结目标线索的重新归属二次确认卡。

        参数：消息分段和服务端已选择的目标线索信息。
        返回值：已持久化的重归属确认动作。
        异常：权限、参数或数据库错误向调用方传播。
        副作用：写入确认卡通知，不执行重归属业务副作用。
        """

        return self.issue_action(
            actor_user_id=actor_user_id,
            action_type=ACTION_TYPE_REASSIGN_CONFIRMATION,
            target_type="lead",
            target_id=target_lead_id,
            expected_action_key=CARD_EVENT_KEY_REASSIGN_CONFIRM,
            context={
                "message_id": message_id,
                "segment_index": segment_index,
                "reason": reason,
                **(
                    {"request_message_id": source_message_id}
                    if source_message_id is not None
                    else {}
                ),
            },
            title="确认重新归属",
            description="请确认将消息归属到已选择的线索",
            source_message_id=source_message_id or message_id,
        )

    def _finish_action(
        self,
        action_id: str,
        *,
        claim_token: str,
        status: str,
        outbox_status: str,
        result_code: str,
        result_summary: str,
    ) -> ActionExecutionResult:
        """提交动作终态并登记独立的最终文本通知。

        参数：action_id、业务状态、outbox 状态和脱敏结果摘要。
        返回值：动作执行结果快照。
        异常：动作或 outbox 事实不存在时抛出 ValueError，数据库错误传播。
        副作用：更新动作、outbox、callback evidence，并创建最终通知记录。
        """

        with self._session_factory.begin() as session:
            action = session.scalar(
                select(WecomAction).where(WecomAction.id == action_id).with_for_update()
            )
            outbox = session.scalar(
                select(WecomActionOutbox)
                .where(WecomActionOutbox.action_id == action_id)
                .with_for_update()
            )
            if action is None or outbox is None:
                raise ValueError("T18 动作终态事实不存在")
            if (
                action.status != WecomActionStatus.PROCESSING.value
                or outbox.status != WecomActionOutboxStatus.PROCESSING.value
                or outbox.claim_token != claim_token
            ):
                # 旧 Worker 只能丢弃自己的晚到结果，不能覆盖 takeover 或 terminal 结果。
                return ActionExecutionResult("stale_claim", "Worker claim 已失效", False)
            _transition_action(action, status)
            action.processed_at = utc_now()
            action.processing_lease_expires_at = None
            action.result_code = result_code
            action.result_summary = result_summary
            outbox.status = outbox_status
            outbox.processing_started_at = None
            outbox.processing_lease_expires_at = None
            # callback transport 已完成进入后台执行；将 delivery evidence 的终态与 action 结果关联，
            # 但不把通知投递状态混入业务 action 状态机。
            for delivery in session.scalars(
                select(WecomCallbackDelivery).where(
                    WecomCallbackDelivery.action_id == action.id,
                    WecomCallbackDelivery.processing_status
                    == WecomCallbackProcessingStatus.CLAIMED.value,
                )
            ):
                delivery.processing_status = WecomCallbackProcessingStatus.COMPLETED.value
                delivery.result_code = result_code
            self._add_result_notification(session, action, result_summary)
            logger.info(
                "wecom_action_completed",
                extra={"action_id": action.id, "event": "action_completed"},
            )
        return ActionExecutionResult(result_code, result_summary, True)

    @staticmethod
    def _add_result_notification(session: Session, action: WecomAction, summary: str) -> None:
        """把最终动作结果写入可靠通知，不与业务动作共用状态。

        参数：session 为当前事务，action 为动作，summary 为脱敏摘要。
        返回值：无。
        异常：数据库写入错误向调用方传播。
        副作用：最多创建一条幂等的最终通知记录。
        """

        key = hashlib.sha256(f"wecom_action_result:{action.id}".encode()).hexdigest()
        if session.get(NotificationRecord, key) is not None:
            return
        source_message_id = action.context.get("request_message_id")
        session.add(
            NotificationRecord(
                notification_key=key,
                sales_user_id=action.bound_actor_wecom_user_id,
                source_message_id=(
                    source_message_id if isinstance(source_message_id, str) else action.id
                ),
                notification_type="wecom_action_result",
                content=summary,
            )
        )

    @staticmethod
    def _validate_action_definition(
        action_type: str, expected_action_key: str, target_type: str, target_id: str
    ) -> None:
        """校验动作定义只使用受控类型、key 和服务端目标。

        参数：动作类型、期望 event key、目标类型和服务端目标标识。
        返回值：无。
        异常：任一值不在 allowlist 或格式非法时抛出 ValueError。
        副作用：无。
        """

        if action_type not in ALLOWED_ACTION_TYPES:
            raise ValueError("不支持的企业微信业务动作类型")
        if expected_action_key not in ALLOWED_CARD_EVENT_KEYS:
            raise ValueError("不支持的企业微信卡片 action key")
        if ACTION_EXPECTED_EVENT_KEYS[action_type] != expected_action_key:
            raise ValueError("动作类型与卡片 action key 不匹配")
        if not target_type or not target_id or _ID_PATTERN.fullmatch(target_id) is None:
            raise ValueError("动作目标标识非法")
        if _ID_PATTERN.fullmatch(target_type) is None:
            raise ValueError("动作目标类型非法")

    @staticmethod
    def _reject_delivery(
        delivery: WecomCallbackDelivery,
        code: str,
        summary: str,
    ) -> CallbackClaimResult:
        """记录无 action 或未知 key 的拒绝证据。

        参数：delivery 为已保存证据，code 和 summary 为确定性拒绝结果。
        返回值：不可执行的 callback claim 结果。
        异常：无；调用方事务负责持久化变更。
        副作用：把 delivery 标为 rejected 并保存结果码。
        """

        now = utc_now()
        delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
        delivery.result_code = code
        delivery.processed_at = now
        return CallbackClaimResult(code, delivery.task_id, delivery.action_id, summary, False)

    @staticmethod
    def _deny_action(
        action: WecomAction,
        delivery: WecomCallbackDelivery,
        code: str,
        summary: str,
    ) -> CallbackClaimResult:
        """冻结越权 callback 的 denied 结果，确保后续 replay 不会复活动作。

        参数：服务端动作、delivery 证据和拒绝结果。
        返回值：不可执行的 denied callback 结果。
        异常：无；调用方事务负责提交状态。
        副作用：动作永久进入 denied，delivery 进入 rejected。
        """

        now = utc_now()
        if action.status != WecomActionStatus.PENDING.value:
            # processing 或 terminal action 不能被另一 callback 改写，只记录本次 delivery 拒绝。
            delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
            delivery.result_code = code
            delivery.processed_at = now
            return CallbackClaimResult(code, action.task_id, action.id, summary, False)
        _transition_action(action, WecomActionStatus.DENIED.value)
        action.processed_at = now
        action.result_code = code
        action.result_summary = summary
        delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
        delivery.result_code = code
        delivery.processed_at = now
        logger.info(
            "wecom_action_denied",
            extra={"action_id": action.id, "event": "action_denied"},
        )
        return CallbackClaimResult(code, action.task_id, action.id, summary, True)


def build_action_card(
    *,
    task_id: str,
    event_key: str,
    title: str,
    description: str,
    duplicate_leads: object = None,
) -> dict[str, object]:
    """构造服务端生成的 template card body，不携带业务目标或客户原文。

    参数：task_id 和 event_key 为服务端生成的关联值，title 和 description 为安全文案。
    返回值：可交给企业微信发送接口的卡片 body。
    异常：无；调用方应先完成动作定义校验。
    副作用：无，不保存任何客户端业务字段。
    """

    # 卡片只携带 opaque task_id 与固定 action key；Lead/owner/message 不从客户端回传。
    payload: dict[str, object] = {
        "card_type": "button_interaction",
        "task_id": task_id,
        "main_title": {"title": title, "desc": description},
        "button_list": [{"text": "确认", "style": 1, "key": event_key}],
    }
    if isinstance(duplicate_leads, list) and duplicate_leads:
        # 企业微信的批量勾选卡片使用 vote_interaction，回调仍携带统一的 selected_items。
        payload["card_type"] = CARD_TYPE_VOTE_INTERACTION
        payload["checkbox"] = {
            "question_key": "crm_duplicate_leads",
            "mode": 1,
            "option_list": [
                {
                    "id": item["lead_id"],
                    "text": str(item["company_name"])[:32],
                    "is_checked": False,
                }
                for item in duplicate_leads
                if isinstance(item, dict)
                and isinstance(item.get("lead_id"), str)
                and isinstance(item.get("company_name"), str)
            ],
        }
        payload.pop("button_list")
        payload["submit_button"] = {
            "text": "继续提交",
            "key": CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
        }
        payload["action_menu"] = {
            "desc": "重复线索处理",
            "action_list": [{"text": "停止提交", "key": CARD_EVENT_KEY_CRM_DUPLICATE_STOP}],
        }
    if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > _MAX_CARD_PAYLOAD_BYTES:
        raise ValueError("动作卡片 payload 超出大小限制")
    return payload


def _safe_context(context: Mapping[str, object]) -> dict[str, object]:
    """限制动作 context 为白名单键和值，拒绝原始 callback 或不可控对象。

    参数：context 为动作发行方提供的受控上下文。
    返回值：可安全持久化的白名单上下文副本。
    异常：包含未知键、换行、超长文本或非法类型时抛出 ValueError。
    副作用：无，不修改调用方传入的对象。
    """

    allowed_keys = {
        "field_names",
        "field_values",
        "command_text",
        "request_message_id",
        "message_id",
        "segment_index",
        "reason",
        "duplicate_leads",
    }
    if any(key not in allowed_keys for key in context):
        raise ValueError("动作 context 含未允许字段")
    safe: dict[str, object] = {}
    for key, value in context.items():
        if isinstance(value, str):
            if len(value) > 512 or "\n" in value or "\r" in value:
                raise ValueError("动作 context 文本非法")
            safe[key] = _redact_text(value, 256 if key == "reason" else 512)
        elif isinstance(value, int) and key in {"segment_index"} and value >= 0:
            safe[key] = value
        elif (
            key == "field_names"
            and isinstance(value, list)
            and all(
                isinstance(item, str)
                and 0 < len(item) <= 64
                and "\n" not in item
                and "\r" not in item
                for item in value
            )
        ):
            safe[key] = list(value)
        elif (
            key == "field_values"
            and isinstance(value, dict)
            and len(value) <= 16
            and all(
                isinstance(field_name, str)
                and 0 < len(field_name) <= 64
                and isinstance(field_value, str)
                and 0 < len(field_value) <= 512
                and "\n" not in field_value
                and "\r" not in field_value
                for field_name, field_value in value.items()
            )
        ):
            safe[key] = {key: _safe_snapshot_value(value) for key, value in value.items()}
        elif key == "duplicate_leads" and isinstance(value, list) and 0 < len(value) <= 20:
            safe_duplicates: list[dict[str, str]] = []
            for item in value:
                if (
                    not isinstance(item, dict)
                    or set(item) != {"lead_id", "company_name"}
                    or not all(isinstance(item[name], str) for name in item)
                    or _ID_PATTERN.fullmatch(item["lead_id"]) is None
                    or not 0 < len(item["company_name"]) <= 512
                    or "\n" in item["company_name"]
                    or "\r" in item["company_name"]
                ):
                    raise ValueError("重复线索 context 非法")
                safe_duplicates.append(
                    {
                        "lead_id": item["lead_id"],
                        "company_name": _redact_text(item["company_name"], 128),
                    }
                )
            safe[key] = safe_duplicates
        else:
            raise ValueError("动作 context 值类型非法")
    return safe


def _context_lead_ids(value: object) -> tuple[str, ...]:
    """从重复确认动作 context 读取冻结的 Lead 标识集合。

    参数：value 为服务端动作上下文中的重复线索列表。
    返回值：通过格式、数量和唯一性校验的 Lead 标识元组；非法时返回空元组。
    异常：无，非法上下文统一按 fail-closed 返回空元组。
    副作用：无。
    """
    if not isinstance(value, list) or not value or len(value) > 20:
        return ()
    ids: list[str] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("lead_id"), str):
            return ()
        lead_id = item["lead_id"]
        if _ID_PATTERN.fullmatch(lead_id) is None or lead_id in ids:
            return ()
        ids.append(lead_id)
    return tuple(ids)


def _redact_text(value: str, limit: int) -> str:
    """对持久化摘要移除邮箱、电话号码和换行注入并限制长度。"""

    redacted = _PII_EMAIL.sub("[email]", value)
    redacted = _PII_PHONE.sub("[phone]", redacted)
    return redacted[:limit]


def _safe_operation_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """限制领域 operation recovery payload 为小型字段值白名单。"""

    if set(payload) - {"field_values"}:
        raise ValueError("operation payload 含未允许字段")
    field_values = payload.get("field_values")
    if not isinstance(field_values, dict) or len(field_values) > 16:
        raise ValueError("operation payload 字段值非法")
    safe_values: dict[str, str] = {}
    for field_name, field_value in field_values.items():
        if (
            not isinstance(field_name, str)
            or not isinstance(field_value, str)
            or not 0 < len(field_name) <= 64
            or not 0 < len(field_value) <= 512
        ):
            raise ValueError("operation payload 字段值非法")
        safe_values[field_name] = _safe_snapshot_value(field_value)
    return {"field_values": safe_values}


def _safe_snapshot_value(value: str) -> str:
    """保存字段确认快照的不可逆表示，不把客户字段原文写入数据库。"""

    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _issuance_key(
    actor_user_id: str,
    action_type: str,
    target_type: str,
    target_id: str,
    context: Mapping[str, object],
) -> str:
    """生成不依赖 provider msgid 的服务端 action issuance 幂等键。"""

    encoded = json.dumps(dict(context), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    material = "|".join((actor_user_id, action_type, target_type, target_id, encoded))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _transition_action(action: WecomAction, new_status: str) -> None:
    """按集中状态图执行一次 action 状态转移，阻止旧 Worker 覆盖终态。"""

    current = action.status
    if current == new_status:
        return
    if new_status not in _ACTION_TRANSITIONS.get(current, frozenset()):
        raise InvalidActionTransition(f"禁止 action 状态转移：{current} -> {new_status}")
    action.status = new_status


def _as_utc(value: datetime) -> datetime:
    """把数据库可能返回的朴素时间解释为 UTC。

    参数：value 为数据库返回的时间值。
    返回值：带 UTC 时区信息的时间值。
    异常：无。
    副作用：无。
    """

    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class DeterministicWecomActionExecutor:
    """复用既有领域服务执行 T18 动作，不在 callback 或 LLM 中决定业务结果。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        crm_adapter: CRMAdapter,
        action_service: WecomActionService | None = None,
    ) -> None:
        """保存领域服务需要的持久化、智能表格和 CRM 适配器。

        参数：数据库会话工厂、智能表格适配器和 CRM 适配器。
        返回值：无。
        异常：无。
        副作用：无，仅保存依赖引用。
        """

        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._crm_adapter = crm_adapter
        self._action_service = action_service

    def __call__(self, action: ActionSnapshot) -> tuple[str, str]:
        """按已持久化 action_type 调用唯一对应的确定性服务。

        参数：action 为 callback claim 冻结的服务端动作快照。
        返回值：结果码与脱敏摘要，供动作状态和最终通知保存。
        异常：领域服务的校验或外部错误向上抛出，由动作服务转为 pending_recovery。
        副作用：可能确认字段、废弃线索、重新归属消息或复用 CRM 提交服务。
        """
        if action.action_type == ACTION_TYPE_CRM_FIELD_CONFIRMATION:
            return self._confirm_submission_fields(action)
        if action.action_type == ACTION_TYPE_CRM_DUPLICATE_CONFIRMATION:
            return self._confirm_duplicate_submission(action)
        if action.action_type == ACTION_TYPE_DISCARD_CONFIRMATION:
            return self._discard_lead(action)
        if action.action_type == ACTION_TYPE_REASSIGN_CONFIRMATION:
            return self._reassign_message(action)
        raise ValueError("T18 action_type 未注册")

    def _confirm_submission_fields(self, action: ActionSnapshot) -> tuple[str, str]:
        """重读最新表格并确认仍处于阻塞状态的 CRM 必填字段。

        参数：action 为已通过 callback 鉴权的服务端动作快照。
        返回值：结果码和脱敏摘要。
        异常：上下文、领域校验或外部依赖失败时向上抛出。
        副作用：可能写入人工确认事件并调用既有 CRM 提交服务。
        """

        from app.crm.commands import format_submission_reply
        from app.crm.service import CrmSubmissionService, SubmissionCommand
        from app.leads.review import LeadReviewService

        fields = action.context.get("field_names")
        command_text = action.context.get("command_text")
        request_message_id = action.context.get("request_message_id")
        if (
            not isinstance(fields, list)
            or not all(isinstance(item, str) for item in fields)
            or not isinstance(command_text, str)
            or not isinstance(request_message_id, str)
        ):
            raise ValueError("字段确认动作 context 不完整")
        review_service = LeadReviewService(self._session_factory, self._smart_table_adapter)
        # 卡片发行后表格可能已被销售处理；只对最新仍 pending 的字段继续确认，旧字段绝不覆盖。
        reconciled = review_service.reconcile_submission(action.target_id)
        current_fields = tuple(field for field in fields if field in reconciled.blocking_fields)
        if not current_fields:
            return "confirmation_stale", "字段已按最新表格状态处理，未覆盖销售修改"
        field_values = {
            field: value
            for field, value in reconciled.fields.items()
            if field in current_fields and isinstance(value, str)
        }
        if set(field_values) != set(current_fields):
            raise ValueError("字段确认动作缺少服务端冻结值")
        if self._action_service is not None:
            self._action_service.begin_domain_operation(
                action.id,
                action.claim_token,
                {"field_values": field_values},
            )
        state = review_service.confirm_submission_fields(
            action.target_id,
            action.bound_actor_wecom_user_id,
            current_fields,
            on_remote_success=(
                lambda values: (
                    self._action_service.mark_remote_effect_succeeded(
                        action.id, action.claim_token, {"field_values": dict(values)}
                    )
                    if self._action_service is not None
                    else None
                )
            ),
            operation_id=action.id,
        )
        if not state.can_submit:
            return "confirmation_incomplete", "仍有字段待确认，请以最新卡片或智能表格为准"
        result = CrmSubmissionService(
            self._session_factory,
            self._smart_table_adapter,
            self._crm_adapter,
            robot_submission_confirmation_available=True,
        ).submit(
            SubmissionCommand(command_text, action.bound_actor_wecom_user_id, request_message_id)
        )
        return "crm_submission_completed", format_submission_reply(result)

    def _confirm_duplicate_submission(self, action: ActionSnapshot) -> tuple[str, str]:
        """执行重复线索卡片的服务端继续或停止决定。

        参数：action 为已完成 callback 鉴权并冻结选择结果的动作快照。
        返回值：供企业微信回复和动作审计使用的结果码、脱敏摘要。
        异常：动作上下文非法或提交销售未授权时抛出 ValueError。
        副作用：调用 CRM 提交服务，更新重复同步状态和智能表格提交状态。
        """
        from app.crm.service import CrmSubmissionService

        decision = action.context.get("decision")
        selected = action.context.get("selected_lead_ids", [])
        request_message_id = action.context.get("request_message_id")
        if decision not in {"continue", "stop"} or not isinstance(request_message_id, str):
            raise ValueError("重复确认动作 context 不完整")
        if not isinstance(selected, list) or not all(isinstance(item, str) for item in selected):
            raise ValueError("重复确认动作选择项非法")
        if self._action_service is not None:
            self._action_service.begin_domain_operation(action.id, action.claim_token)
        result = CrmSubmissionService(
            self._session_factory,
            self._smart_table_adapter,
            self._crm_adapter,
            robot_submission_confirmation_available=True,
        ).resolve_duplicate_confirmation(
            request_message_id,
            action.bound_actor_wecom_user_id,
            continue_submission=decision == "continue",
            selected_lead_ids=tuple(selected),
        )
        if decision == "stop":
            return (
                "crm_duplicate_submission_abandoned",
                f"已停止提交重复线索，提交状态已更新为放弃提交 {result.abandoned} 条",
            )
        return (
            "crm_duplicate_submission_completed",
            f"重复线索处理完成：覆盖成功 {result.submitted} 条；"
            f"未选择 {result.remaining} 条；失败 {result.failed} 条。",
        )

    def _discard_lead(self, action: ActionSnapshot) -> tuple[str, str]:
        """重新读取当前线索状态后复用既有 LeadDiscardService。

        参数：action 为服务端冻结的线索废弃动作快照。
        返回值：领域状态结果码和脱敏摘要。
        异常：领域校验或数据库错误向上抛出。
        副作用：最多按既有服务规则执行一次线索废弃。
        """

        from app.leads.discard import LeadDiscardService

        reason = action.context.get("reason")
        if not isinstance(reason, str):
            raise ValueError("废弃动作缺少原因")
        if self._action_service is not None:
            self._action_service.begin_domain_operation(action.id, action.claim_token)
        result = LeadDiscardService(self._session_factory).discard(
            action.target_id,
            action.bound_actor_wecom_user_id,
            reason,
            operation_id=action.id,
        )
        return f"lead_discard_{result.status.value}", f"线索废弃处理结果：{result.status.value}"

    def _reassign_message(self, action: ActionSnapshot) -> tuple[str, str]:
        """使用服务端冻结的 target_id 调用既有 LeadReassignmentService。

        参数：action 为含服务端冻结目标线索的重归属快照。
        返回值：结果码和脱敏摘要。
        异常：上下文、权限或领域校验失败时向上抛出。
        副作用：按既有服务规则更新消息归属并记录审计。
        """

        from app.leads.service import LeadReassignmentService

        message_id = action.context.get("message_id")
        segment_index = action.context.get("segment_index")
        reason = action.context.get("reason")
        if (
            not isinstance(message_id, str)
            or not isinstance(segment_index, int)
            or not isinstance(reason, str)
        ):
            raise ValueError("重归属动作 context 不完整")
        if self._action_service is not None:
            self._action_service.begin_domain_operation(action.id, action.claim_token)
        LeadReassignmentService(self._session_factory).reassign(
            message_id,
            segment_index,
            action.target_id,
            action.bound_actor_wecom_user_id,
            reason,
            operation_id=action.id,
        )
        return "lead_reassignment_succeeded", "消息重新归属已完成"
