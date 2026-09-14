"""消费已持久化 CRM 提交命令并生成脱敏销售汇总。"""

from __future__ import annotations

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
        _record_command_failure(session_factory, outbox_event_id)
        return "CRM 提交任务暂时失败，系统将自动重试；请勿重复提交。"
    reply = format_submission_reply(result)
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is not None:
            retrying = session.scalar(
                select(CrmSyncRecord.id)
                .where(
                    CrmSyncRecord.status == "retrying",
                    # 仅以本命令冻结的同步记录决定其 Outbox 状态，禁止串到同销售的其他命令。
                    CrmSyncRecord.request_message_id == command.request_message_id,
                )
                .limit(1)
            )
            event.status = "retrying" if retrying is not None else "succeeded"
        key = f"crm-submission:{command.request_message_id}"
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


def _record_command_failure(session_factory: sessionmaker[Session], outbox_event_id: int) -> None:
    """记录一次命令编排失败，并在重试耗尽后释放销售顺序检查点。

    参数：session_factory 提供事务；outbox_event_id 为已认领的命令事件。
    返回值：无。
    异常：数据库写入错误向 Worker 传播。
    副作用：增加尝试次数并置为 retrying 或 failed_pending_review。
    """
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is None:
            return
        event.attempts += 1
        # 配置值表示额外重试次数；耗尽后该命令成为完成检查点，后续消息可继续。
        event.status = (
            "failed_pending_review"
            if event.attempts > get_settings().lead_message_retry_count
            else "retrying"
        )


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
