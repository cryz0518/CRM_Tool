"""消费已持久化 CRM 提交命令并生成脱敏销售汇总。"""

from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.crm.adapter import CRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionBatchResult, SubmissionCommand
from app.leads.models import CrmSyncRecord
from app.messaging.models import IncomingMessage, NotificationRecord, OutboxEvent
from app.smart_table.adapter import SmartTableAdapter


def consume_submission_command(
    session_factory: sessionmaker[Session],
    smart_table_adapter: SmartTableAdapter,
    crm_adapter: CRMAdapter,
    outbox_event_id: int,
) -> str:
    """消费一条已认领命令 Outbox，并返回不含敏感数据的销售汇总文本。

    参数：前三项为业务依赖，outbox_event_id 为现有可靠消息事件标识。
    返回值：可安全发送给当前销售的确定性中文汇总。
    异常：消息或事件事实缺失时抛出 ValueError；业务依赖异常被转换为可靠重试状态。
    副作用：调用 T12 submission service，写入脱敏通知，并结束或重试本命令事件。
    """
    with session_factory() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None or event.event_type != "crm_submission_command":
            raise ValueError("不是 CRM 提交命令 Outbox")
        message = session.get(IncomingMessage, event.message_id)
        if message is None:
            raise ValueError("CRM 提交命令缺少来源消息")
        command = SubmissionCommand(
            text=message.normalized_text or "",
            sales_user_id=message.sales_user_id,
            request_message_id=message.message_id,
        )
    try:
        service = CrmSubmissionService(session_factory, smart_table_adapter, crm_adapter)
        result = service.submit(command)
    except Exception:
        # 命令编排失败需要有界结束，不能永久占住同销售的消息顺序检查点。
        return _record_command_failure(session_factory, outbox_event_id, command)
    # 重放时必须将本请求已成功的同步事实重新计入汇总，不能因 Lead 已 synced 漏报成功。
    result = _include_persisted_results(session_factory, command, result)
    reply = format_submission_reply(result)
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is not None:
            retrying_or_processing = session.scalar(
                select(CrmSyncRecord.id)
                .where(
                    CrmSyncRecord.status.in_(("retrying", "processing")),
                    # 仅以本命令冻结的同步记录决定其 Outbox 状态，禁止串到同销售的其他命令。
                    CrmSyncRecord.request_message_id == command.request_message_id,
                )
                .limit(1)
            )
            event.status = "retrying" if retrying_or_processing is not None else "succeeded"
        key = notification_key_for_message(command.request_message_id)
        if session.get(NotificationRecord, key) is None:
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=command.sales_user_id,
                    source_message_id=command.request_message_id,
                    notification_type="crm_submission_summary",
                    content=reply,
                )
            )
    return reply


def notification_key_for_message(message_id: str) -> str:
    """为 CRM 提交通知生成带命名空间的固定长度 SHA-256 键。

    参数：message_id 为企业微信来源消息标识。
    返回值：不超过 notification_key 列限制的 64 位十六进制键。
    异常：无。
    副作用：无。
    """
    return hashlib.sha256(f"crm_submission_notification:{message_id}".encode()).hexdigest()


def terminal_failure_notification_key_for_message(message_id: str) -> str:
    """为命令终态失败通知生成与成功汇总隔离的固定长度键。"""
    return hashlib.sha256(
        f"crm_submission_terminal_failure:{message_id}".encode()
    ).hexdigest()


def _include_persisted_results(
    session_factory: sessionmaker[Session],
    command: SubmissionCommand,
    result: SubmissionBatchResult,
) -> SubmissionBatchResult:
    """将同一请求已冻结的 CRM 同步状态补入重放汇总。

    参数：session_factory 读取持久化事实；command 标识本次提交；result 为本轮处理结果。
    返回值：首次或恢复执行均可使用的确定性汇总。
    异常：数据库读取错误向调用方传播。
    副作用：仅读取 CRM 同步记录。
    """
    with session_factory() as session:
        statuses = session.scalars(
            select(CrmSyncRecord.status).where(
                CrmSyncRecord.request_message_id == command.request_message_id
            )
        ).all()
    # 本轮实际成功已经在 result 中，不应被同一持久化记录再次累计。
    if result.succeeded:
        return result
    return SubmissionBatchResult(
        succeeded=statuses.count("succeeded"),
        incomplete=result.incomplete,
        retrying=max(result.retrying, statuses.count("retrying")),
        processing=max(result.processing, statuses.count("processing")),
        failed_pending_review=max(
            result.failed_pending_review, statuses.count("failed_pending_review")
        ),
        updates_not_implemented=result.updates_not_implemented,
        incomplete_lead_ids=result.incomplete_lead_ids,
    )


def _record_command_failure(
    session_factory: sessionmaker[Session], outbox_event_id: int, command: SubmissionCommand
) -> str:
    """记录一次命令编排失败，并在重试耗尽后释放销售顺序检查点。

    参数：session_factory 提供事务；outbox_event_id 为已认领的命令事件。
    返回值：可安全发送给销售的失败摘要。
    异常：数据库写入错误向 Worker 传播。
    副作用：增加尝试次数并置为 retrying 或 failed_pending_review。
    """
    with session_factory.begin() as session:
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.id == outbox_event_id).with_for_update()
        )
        if event is None:
            raise ValueError("CRM 提交命令 Outbox 不存在")
        event.attempts += 1
        # 配置值表示额外重试次数；耗尽后该命令成为完成检查点，后续消息可继续。
        if event.attempts <= get_settings().lead_message_retry_count:
            event.status = "retrying"
            return "CRM 提交任务暂时失败，系统将自动重试；请勿重复提交。"
        reply = (
            "本次线索提交未能完成，需要人工处理。"
            "已成功提交：0 条；待完善：0 条；需人工处理：1 条。"
        )
        key = terminal_failure_notification_key_for_message(command.request_message_id)
        if session.get(NotificationRecord, key) is None:
            # 通知插入与命令终态在同一事务；插入失败会回滚，命令仍可由 lease 恢复。
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=command.sales_user_id,
                    source_message_id=command.request_message_id,
                    notification_type="crm_submission_summary",
                    content=reply,
                )
            )
        event.status = "failed_pending_review"
        return reply


def format_submission_reply(result: SubmissionBatchResult) -> str:
    """将批次结果格式化为只含计数的销售回复。

    参数：result 为 T12 application service 返回的批次汇总。
    返回值：不包含线索名称、联系方式、payload 或异常堆栈的中文文本。
    异常：无。
    副作用：无。
    """
    if result.updates_not_implemented:
        return "提交我的更新将在 T13 实现；本次未调用 CRM。"
    return (
        f"CRM 提交结果：创建成功 {result.succeeded} 条；待完善或待明确确认 {result.incomplete} 条；"
        f"提交处理中 {result.processing} 条；可重试失败 {result.retrying} 条；"
        f"需人工处理失败 {result.failed_pending_review} 条。"
    )
