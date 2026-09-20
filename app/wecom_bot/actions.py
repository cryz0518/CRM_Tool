"""企业微信确定性卡片动作的解析、持久化认领与执行边界。"""

from __future__ import annotations

import hashlib
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
CARD_EVENT_KEY_DISCARD_CONFIRM = "lead.discard.confirm"
CARD_EVENT_KEY_REASSIGN_CONFIRM = "lead.reassignment.confirm"
ALLOWED_CARD_EVENT_KEYS = frozenset(
    {
        CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
        CARD_EVENT_KEY_DISCARD_CONFIRM,
        CARD_EVENT_KEY_REASSIGN_CONFIRM,
    }
)

ACTION_TYPE_CRM_FIELD_CONFIRMATION = "crm_field_confirmation"
ACTION_TYPE_DISCARD_CONFIRMATION = "lead_discard_confirmation"
ACTION_TYPE_REASSIGN_CONFIRMATION = "lead_reassignment_confirmation"
ALLOWED_ACTION_TYPES = frozenset(
    {
        ACTION_TYPE_CRM_FIELD_CONFIRMATION,
        ACTION_TYPE_DISCARD_CONFIRMATION,
        ACTION_TYPE_REASSIGN_CONFIRMATION,
    }
)
ACTION_EXPECTED_EVENT_KEYS = {
    ACTION_TYPE_CRM_FIELD_CONFIRMATION: CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    ACTION_TYPE_DISCARD_CONFIRMATION: CARD_EVENT_KEY_DISCARD_CONFIRM,
    ACTION_TYPE_REASSIGN_CONFIRMATION: CARD_EVENT_KEY_REASSIGN_CONFIRM,
}

_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_ACTION_LEASE = timedelta(minutes=5)


class CallbackParseError(ValueError):
    """表示 callback 不符合已经冻结的企业微信事件契约。"""


class CardCapabilityUnavailable(RuntimeError):
    """表示当前进程没有经过 readiness 验证的卡片/回调能力。"""


@dataclass(frozen=True)
class TemplateCardCallback:
    """承载 callback 仅允许使用的白名单传输字段。"""

    actor_user_id: str
    event_key: str
    task_id: str
    provider_msgid: str
    req_id: str


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


@dataclass(frozen=True)
class ActionExecutionResult:
    """描述一次 domain action 执行或已完成幂等重放。"""

    code: str
    summary: str
    executed: bool


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
        provider_msgid = cls._identifier(body.get("msgid"), "msgid")
        req_id = cls._identifier(headers.get("req_id"), "req_id")
        return TemplateCardCallback(actor, event_key, task_id, provider_msgid, req_id)

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
        action_expiry: timedelta = timedelta(minutes=10),
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
            # 同一确定性命令重试时复用未完成动作；不会因为通知 retry 重新发行第二张业务卡。
            request_message_id = safe_context.get("request_message_id")
            if isinstance(request_message_id, str):
                existing_actions = session.scalars(
                    select(WecomAction).where(
                        WecomAction.action_type == action_type,
                        WecomAction.target_id == target_id,
                        WecomAction.bound_actor_wecom_user_id == actor_user_id,
                        WecomAction.status.in_(
                            (
                                WecomActionStatus.PENDING.value,
                                WecomActionStatus.PROCESSING.value,
                            )
                        ),
                    )
                ).all()
                for existing in existing_actions:
                    if existing.context.get("request_message_id") == request_message_id:
                        session.expunge(existing)
                        return existing
            action = WecomAction(
                id=action_id,
                task_id=task_id,
                action_type=action_type,
                bound_actor_wecom_user_id=actor_user_id,
                target_type=target_type,
                target_id=target_id,
                expected_action_key=expected_action_key,
                status=WecomActionStatus.PENDING.value,
                expires_at=normalized_expiry,
                context=safe_context,
            )
            session.add(action)
            session.flush()
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
                        ),
                    },
                )
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
                return CallbackClaimResult(
                    code="duplicate_delivery",
                    task_id=callback.task_id,
                    action_id=previous.action_id,
                    summary="该回调已接收，业务动作不会重复执行",
                    should_update_card=False,
                )

            action = session.scalar(
                select(WecomAction)
                .where(WecomAction.task_id == callback.task_id)
                .with_for_update()
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
            if callback.event_key != action.expected_action_key:
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

            if _as_utc(now) >= _as_utc(action.expires_at):
                action.status = WecomActionStatus.EXPIRED.value
                action.processed_at = now
                action.result_code = "expired"
                action.result_summary = "卡片已过期，请重新发起操作"
                delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
                delivery.result_code = "expired"
                delivery.processed_at = now
                return CallbackClaimResult(
                    "expired", action.task_id, action.id, action.result_summary, True
                )

            # 行锁保护下仅 pending 可以变为 processing；Outbox 与 claim 在同一事务中提交。
            action.status = WecomActionStatus.PROCESSING.value
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
            # Beat 的调度租约只防止重复投递；真正的业务执行租约仍由 processing 字段控制。
            outbox.dispatch_claimed_at = None
            outbox.dispatch_lease_expires_at = None
            if outbox.status == WecomActionOutboxStatus.SUCCEEDED.value or action.status == (
                WecomActionStatus.SUCCEEDED.value
            ):
                return ActionExecutionResult(
                    "already_succeeded", action.result_summary or "已处理", False
                )
            if outbox.status == WecomActionOutboxStatus.PROCESSING.value:
                lease = outbox.processing_lease_expires_at
                if lease is not None and _as_utc(lease) > _as_utc(utc_now()):
                    return ActionExecutionResult("already_processing", "已在处理中", False)
            if action.status != WecomActionStatus.PROCESSING.value:
                return ActionExecutionResult(
                    f"action_{action.status}", action.result_summary or "动作不可执行", False
                )
            # Worker 可能在 callback claim 后延迟执行；执行前再次读取授权，避免撤销后仍产生副作用。
            authorization = session.get(SalesAuthorization, action.bound_actor_wecom_user_id)
            if (
                authorization is None
                or not authorization.is_active
                or not authorization.is_authorized
            ):
                action.status = WecomActionStatus.DENIED.value
                action.processed_at = utc_now()
                action.result_code = "actor_unauthorized"
                action.result_summary = "操作人授权已撤销，未执行该动作"
                outbox.status = WecomActionOutboxStatus.FAILED.value
                outbox.processing_started_at = None
                outbox.processing_lease_expires_at = None
                self._add_result_notification(session, action, action.result_summary)
                return ActionExecutionResult("actor_unauthorized", action.result_summary, False)
            outbox.status = WecomActionOutboxStatus.PROCESSING.value
            outbox.attempts += 1
            outbox.processing_started_at = utc_now()
            outbox.processing_lease_expires_at = outbox.processing_started_at + _ACTION_LEASE
            snapshot = ActionSnapshot(
                id=action.id,
                action_type=action.action_type,
                bound_actor_wecom_user_id=action.bound_actor_wecom_user_id,
                target_type=action.target_type,
                target_id=action.target_id,
                context=dict(action.context),
            )

        try:
            # 外部业务调用在事务之外执行；业务动作状态已锁定，通知重试不会再次到这里。
            result_code, result_summary = domain_executor(snapshot)
        except Exception as error:
            logger.exception("wecom_action_domain_execution_failed")
            return self._finish_action(
                action_id,
                status=WecomActionStatus.PENDING_RECOVERY.value,
                outbox_status=WecomActionOutboxStatus.FAILED.value,
                result_code="domain_failed",
                result_summary=f"业务动作失败，需要人工处理（{type(error).__name__}）",
            )
        return self._finish_action(
            action_id,
            status=WecomActionStatus.SUCCEEDED.value,
            outbox_status=WecomActionOutboxStatus.SUCCEEDED.value,
            result_code=result_code,
            result_summary=result_summary,
        )

    def issue_field_confirmation_action(
        self,
        *,
        actor_user_id: str,
        lead_id: str,
        field_names: tuple[str, ...],
        command_text: str,
        request_message_id: str,
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
            },
            title="提交前确认字段",
            description=f"请确认：{'、'.join(field_names)}",
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
            action.status = status
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
        return ActionExecutionResult(result_code, result_summary, True)

    @staticmethod
    def _add_result_notification(
        session: Session, action: WecomAction, summary: str
    ) -> None:
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
        action.status = WecomActionStatus.DENIED.value
        action.processed_at = now
        action.result_code = code
        action.result_summary = summary
        delivery.processing_status = WecomCallbackProcessingStatus.REJECTED.value
        delivery.result_code = code
        delivery.processed_at = now
        return CallbackClaimResult(code, action.task_id, action.id, summary, True)


def build_action_card(
    *, task_id: str, event_key: str, title: str, description: str
) -> dict[str, object]:
    """构造服务端生成的 template card body，不携带业务目标或客户原文。

    参数：task_id 和 event_key 为服务端生成的关联值，title 和 description 为安全文案。
    返回值：可交给企业微信发送接口的卡片 body。
    异常：无；调用方应先完成动作定义校验。
    副作用：无，不保存任何客户端业务字段。
    """

    # 卡片只携带 opaque task_id 与固定 action key；Lead/owner/message 不从客户端回传。
    return {
        "card_type": "button_interaction",
        "task_id": task_id,
        "main_title": {"title": title, "desc": description},
        "button_list": [{"text": "确认", "style": 1, "key": event_key}],
    }


def _safe_context(context: Mapping[str, object]) -> dict[str, object]:
    """限制动作 context 为白名单键和值，拒绝原始 callback 或不可控对象。

    参数：context 为动作发行方提供的受控上下文。
    返回值：可安全持久化的白名单上下文副本。
    异常：包含未知键、换行、超长文本或非法类型时抛出 ValueError。
    副作用：无，不修改调用方传入的对象。
    """

    allowed_keys = {
        "field_names",
        "command_text",
        "request_message_id",
        "message_id",
        "segment_index",
        "reason",
    }
    if any(key not in allowed_keys for key in context):
        raise ValueError("动作 context 含未允许字段")
    safe: dict[str, object] = {}
    for key, value in context.items():
        if isinstance(value, str):
            if len(value) > 512 or "\n" in value or "\r" in value:
                raise ValueError("动作 context 文本非法")
            safe[key] = value
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
        else:
            raise ValueError("动作 context 值类型非法")
    return safe


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

    def __call__(self, action: ActionSnapshot) -> tuple[str, str]:
        """按已持久化 action_type 调用唯一对应的确定性服务。

        参数：action 为 callback claim 冻结的服务端动作快照。
        返回值：结果码与脱敏摘要，供动作状态和最终通知保存。
        异常：领域服务的校验或外部错误向上抛出，由动作服务转为 pending_recovery。
        副作用：可能确认字段、废弃线索、重新归属消息或复用 CRM 提交服务。
        """
        if action.action_type == ACTION_TYPE_CRM_FIELD_CONFIRMATION:
            return self._confirm_submission_fields(action)
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
        latest_state = review_service.get_submission_confirmation_state(action.target_id)
        current_fields = tuple(field for field in fields if field in latest_state.blocking_fields)
        if not current_fields:
            return "confirmation_stale", "字段已按最新表格状态处理，未覆盖销售修改"
        state = review_service.confirm_submission_fields(
            action.target_id, action.bound_actor_wecom_user_id, current_fields
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
        result = LeadDiscardService(self._session_factory).discard(
            action.target_id, action.bound_actor_wecom_user_id, reason
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
        LeadReassignmentService(self._session_factory).reassign(
            message_id,
            segment_index,
            action.target_id,
            action.bound_actor_wecom_user_id,
            reason,
        )
        return "lead_reassignment_succeeded", "消息重新归属已完成"
