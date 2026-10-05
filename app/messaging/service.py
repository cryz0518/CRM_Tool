"""授权校验后的可靠消息接收应用服务。"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.logging import bind_log_context, reset_log_context
from app.crm.commands import looks_like_submission_intent, parse_crm_submission_command
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.wecom_bot.actions import parse_deterministic_action_command

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IncomingMessageCommand:
    """定义接入层交给消息接收服务的已标准化消息。

    参数：message_id 为企业微信消息幂等键，sales_user_id 为发送人，raw_payload 为原始载荷；
    display_name 为可信企业微信来源可提供的成员展示名。
    返回值：本类仅承载输入；外部副作用由 MessageIntakeService 负责。
    异常：数据库异常由调用方统一处理，避免伪造接收成功结果。
    """

    message_id: str
    sales_user_id: str
    raw_payload: dict[str, Any]
    normalized_text: str | None = None
    requires_media_enrichment: bool = False
    display_name: str | None = None
    chat_id: str | None = None
    chat_type: str | None = None
    quote: dict[str, Any] | None = None


@dataclass(frozen=True)
class MessageIntakeResult:
    """描述一次消息接收的确定性结果。

    参数：accepted 表示消息是否已被系统接收，duplicate 表示同一输入是否已处理。
    返回值：调用方据此决定是否继续后续接入动作。
    异常：不吞没数据库异常，以便上层触发安全重试。
    """

    accepted: bool
    duplicate: bool


class MessageIntakeService:
    """在一个数据库事务内完成 Actor Registry、消息去重和发件箱写入。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存创建数据库事务的工厂。

        参数：session_factory 为 SQLAlchemy 会话工厂。
        返回值：无。
        异常：无额外异常；工厂调用失败时由 receive 透传。
        副作用：不建立连接，也不执行外部调用。
        """
        self._session_factory = session_factory

    def receive(self, command: IncomingMessageCommand) -> MessageIntakeResult:
        """接收一条消息并原子保存 actor 消息及其待处理 Outbox 事件。

        参数：command 为标准化后的企业微信消息。
        返回值：返回授权结果和重复处理标记。
        异常：任何数据库错误都会回滚整个事务并向调用方抛出。
        副作用：有效 actor 会新增原始消息和 Outbox；已停用 actor 只保留幂等通知记录。
        """
        token = bind_log_context(message_id=command.message_id, wecom_user_id=command.sales_user_id)
        try:
            with self._session_factory.begin() as session:
                # 已持久化消息的接收结论不可被后续授权状态改变，重投只能返回原有幂等结果。
                if session.get(IncomingMessage, command.message_id) is not None:
                    self._record_audit_event(session, command, "message_deduplicated")
                    logger.info("重复消息已忽略")
                    return MessageIntakeResult(accepted=True, duplicate=True)

                # 先锁定 Actor Registry 行，既保证启用状态一致，也串行分配该成员的顺序号。
                authorization = session.scalar(
                    select(SalesAuthorization)
                    .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
                    .with_for_update()
                )
                if authorization is None:
                    authorization = self._register_actor(session, command)
                if not authorization.is_active:
                    return self._record_actor_inactive_notification(session, command)

                # 同一销售的并发重投会在授权目录行锁后串行到达，此处再次读取才能稳定返回幂等结果。
                if session.get(IncomingMessage, command.message_id) is not None:
                    self._record_audit_event(session, command, "message_deduplicated")
                    logger.info("重复消息已忽略")
                    return MessageIntakeResult(accepted=True, duplicate=True)

                # 顺序号与消息和 Outbox 一同提交，后续 Worker 可按持久化顺序进行消费。
                authorization.next_message_sequence += 1
                message = IncomingMessage(
                    message_id=command.message_id,
                    sales_user_id=command.sales_user_id,
                    sequence=authorization.next_message_sequence,
                    raw_payload=command.raw_payload,
                    normalized_text=command.normalized_text,
                    chat_id=command.chat_id,
                    chat_type=command.chat_type,
                    requires_media_enrichment=command.requires_media_enrichment,
                )
                session.add(message)
                session.add(
                    OutboxEvent(
                        message_id=command.message_id,
                        sales_user_id=command.sales_user_id,
                        sequence=authorization.next_message_sequence,
                        # 精确命令保留快速路径；普通文本由 Worker 做结构化意图路由。
                        event_type=(
                            "crm_submission_command"
                            if parse_crm_submission_command(command.normalized_text or "")
                            is not None
                            else (
                                "crm_submission_intent"
                                if looks_like_submission_intent(command.normalized_text or "")
                                else (
                                    "wecom_action_command"
                                    if parse_deterministic_action_command(
                                        command.normalized_text or ""
                                    )
                                    is not None
                                    else "message_received"
                                )
                            )
                        ),
                    )
                )
                self._record_audit_event(session, command, "message_received")
                logger.info("有效 actor 消息与发件箱事件已入库")
                return MessageIntakeResult(accepted=True, duplicate=False)
        except Exception:
            # 事务异常必须保留完整堆栈，供 Docker 日志与后续运维界面定位失败原因。
            logger.exception("消息接收事务失败")
            raise
        finally:
            # 无论事务成功、回滚还是提前返回，都清理当前消息的链路上下文。
            reset_log_context(token)

    @staticmethod
    def _register_actor(session: Session, command: IncomingMessageCommand) -> SalesAuthorization:
        """在首次收到合法 WeCom 成员消息时创建最小 Actor Registry 记录。

        参数：session 为当前接收事务；command 携带成员标识和可信展示名。
        返回值：已锁定的 Actor Registry 记录。
        异常：并发创建由唯一键收敛；数据库异常在无法读取 actor 时向上抛出。
        副作用：最多新增一条启用 actor 记录，不设置业务授权门槛。
        """
        try:
            # 嵌套事务只包住首次插入，唯一键冲突不会回滚消息接收外层事务。
            with session.begin_nested():
                created_actor = SalesAuthorization(
                    wecom_user_id=command.sales_user_id,
                    display_name=(
                        command.display_name.strip()
                        if isinstance(command.display_name, str) and command.display_name.strip()
                        else None
                    ),
                    is_authorized=False,
                    is_active=True,
                )
                session.add(created_actor)
                session.flush()
        except IntegrityError:
            # 另一个并发接收已完成注册；随后用行锁取得同一顺序分配主体。
            existing_actor = session.scalar(
                select(SalesAuthorization)
                .where(SalesAuthorization.wecom_user_id == command.sales_user_id)
                .with_for_update()
            )
            if existing_actor is None:
                raise
            return existing_actor
        return created_actor

    def _record_actor_inactive_notification(
        self, session: Session, command: IncomingMessageCommand
    ) -> MessageIntakeResult:
        """为已停用 actor 登记一次幂等通知。

        参数：session 为当前接收事务，command 为来源消息。
        返回值：返回停用 actor 的拒绝结果及通知是否已存在。
        异常：数据库写入异常会使当前事务整体回滚。
        副作用：首次停用 actor 消息新增一条待发送通知，不新增消息或 Outbox 处理任务。
        """
        # 通知键绑定销售、来源消息和通知类型，确保重连不会重复打扰已停用成员。
        notification_type = "sales_actor_inactive"
        notification_key = hashlib.sha256(
            f"{command.sales_user_id}:{command.message_id}:{notification_type}".encode()
        ).hexdigest()
        if session.get(NotificationRecord, notification_key) is not None:
            self._record_audit_event(session, command, "actor_inactive_message_deduplicated")
            logger.info("停用 actor 通知已登记")
            return MessageIntakeResult(accepted=False, duplicate=True)

        try:
            # actor 行已存在但无法继续接收时，以唯一键与嵌套事务吸收并发插入冲突，保持通知幂等返回。
            with session.begin_nested():
                session.add(
                    NotificationRecord(
                        notification_key=notification_key,
                        sales_user_id=command.sales_user_id,
                        source_message_id=command.message_id,
                        notification_type=notification_type,
                    )
                )
                session.flush()
        except IntegrityError:
            self._record_audit_event(session, command, "actor_inactive_message_deduplicated")
            logger.info("停用 actor 通知已登记")
            return MessageIntakeResult(accepted=False, duplicate=True)

        self._record_audit_event(session, command, "actor_inactive_message_rejected")
        logger.warning("停用 actor 消息已拒绝并登记通知")
        return MessageIntakeResult(accepted=False, duplicate=False)

    def _record_audit_event(
        self, session: Session, command: IncomingMessageCommand, event_type: str
    ) -> None:
        """登记当前消息的唯一业务审计事件。

        参数：session 为当前事务，command 为来源消息，event_type 为受控事件类型。
        返回值：无。
        异常：数据库查询或写入失败会回滚整个接收事务。
        副作用：首次事件写入业务审计表，不调用外部系统。
        """
        # 审计与业务写入位于同一事务，确保查询结果不会与实际接收结果脱节。
        existing_event = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == command.message_id,
                BusinessAuditEvent.event_type == event_type,
            )
        )
        if existing_event is None:
            try:
                # 唯一键兜底处理跨请求竞争，重复审计不应破坏消息或通知的幂等结果。
                with session.begin_nested():
                    session.add(
                        BusinessAuditEvent(
                            message_id=command.message_id,
                            sales_user_id=command.sales_user_id,
                            event_type=event_type,
                        )
                    )
                    session.flush()
            except IntegrityError:
                logger.info("重复业务审计已忽略")
