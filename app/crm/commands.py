"""消费已持久化 CRM 提交命令并生成脱敏销售汇总。"""

from __future__ import annotations

from sqlalchemy.orm import Session, sessionmaker

from app.crm.adapter import CRMAdapter
from app.crm.service import CrmSubmissionService, SubmissionBatchResult, SubmissionCommand
from app.messaging.models import IncomingMessage, OutboxEvent
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
    异常：消息或事件事实缺失、提交服务异常时向 Worker 传播。
    副作用：调用 T12 submission service，并将本命令事件置为 succeeded。
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
    result = CrmSubmissionService(session_factory, smart_table_adapter, crm_adapter).submit(command)
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, outbox_event_id)
        if event is not None:
            event.status = "succeeded"
    return format_submission_reply(result)


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
        f"CRM 提交结果：创建成功 {result.succeeded} 条；待完善 {result.incomplete} 条；"
        f"可重试失败 {result.failed} 条。"
    )
