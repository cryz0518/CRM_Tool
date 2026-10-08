"""销售需求进度会话登记、统计和可靠通知排程。"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Mapping

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, get_settings
from app.leads.completeness import LeadCompletenessService
from app.leads.models import (
    Lead,
    LeadMessageResolution,
    LeadProgressMessage,
    LeadProgressSession,
    SmartTableSync,
)
from app.messaging.models import (
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import SmartTableAdapter

logger = logging.getLogger(__name__)

_ACTIVE_OUTBOX_STATUSES = frozenset({"pending", "processing", "retrying"})
_ACTIVE_SYNC_STATUSES = frozenset({"pending", "processing", "retrying"})
_UNASSIGNED_RESOLUTION_STATUSES = frozenset({"unassigned", "quote_unresolved"})
_SCHEDULER_BATCH_SIZE = 100


@dataclass(frozen=True)
class LeadProgressStats:
    """承载单个销售会话的去重计数与当前表格完整度。"""

    received_messages: int
    identified_leads: int
    successful_leads: int
    incomplete_leads: int
    missing_required_leads: int
    ai_pending_leads: int
    sync_failed_leads: int
    processing_items: int
    unassigned_segments: int
    snapshot_unverified_leads: int


class LeadProgressService:
    """按销售隔离需求会话，并只通过既有通知 outbox 汇报统计。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
        settings: Settings | None = None,
    ) -> None:
        """注入数据库、现有智能表格只读适配器和进度配置。

        参数：session_factory 创建短事务；smart_table_adapter 读取当前审核表记录；
        settings 可覆盖进度开关、汇报间隔和空闲关闭时长。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不访问数据库或企业微信。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter
        self._settings = settings or get_settings()
        self._completeness = LeadCompletenessService()

    def record_resolved_message(self, message_id: str, *, now: datetime | None = None) -> bool:
        """将有归属结论的有效需求消息登记到当前销售会话且保持幂等。

        参数：message_id 为已完成一次有效线索处理的来源消息；now 可注入时钟供测试使用。
        返回值：首次登记或已登记时返回 True；不属于有效需求、销售停用或功能关闭时为 False。
        异常：数据库约束或读取错误向调用方传播。
        副作用：可能创建销售会话并新增唯一消息关联，不调用模型、CRM 或消息通道。
        """
        if not self._settings.lead_progress_enabled:
            return False
        current_time = _as_utc(now or utc_now())
        with self._session_factory.begin() as session:
            message = session.get(IncomingMessage, message_id)
            if message is None:
                return False
            # 先锁销售授权行，统一并发建会话与调度器的加锁顺序。
            authorization = session.scalar(
                select(SalesAuthorization)
                .where(SalesAuthorization.wecom_user_id == message.sales_user_id)
                .with_for_update()
            )
            if authorization is None or not authorization.is_active:
                return False
            event = session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.message_id == message_id,
                    OutboxEvent.sales_user_id == message.sales_user_id,
                    OutboxEvent.event_type == "message_received",
                )
            )
            resolution_exists = session.scalar(
                select(LeadMessageResolution.id)
                .where(LeadMessageResolution.message_id == message_id)
                .limit(1)
            )
            if event is None or resolution_exists is None:
                # 命令、确认动作、闲聊和仅收到但未完成识别的消息都不会进入进度会话。
                return False
            if session.get(LeadProgressMessage, message_id) is not None:
                return True

            progress = session.scalar(
                select(LeadProgressSession)
                .where(
                    LeadProgressSession.sales_user_id == message.sales_user_id,
                    LeadProgressSession.status == "active",
                )
                .with_for_update()
            )
            received_at = _as_utc(message.received_at)
            idle_window = timedelta(minutes=self._settings.lead_progress_idle_stop_minutes)
            # 超过一个空闲周期才被恢复处理的旧消息从当前处理时刻起计，避免启动后补发历史窗口。
            activity_at = current_time if received_at < current_time - idle_window else received_at
            if progress is None:
                progress = LeadProgressSession(
                    sales_user_id=message.sales_user_id,
                    status="active",
                    started_at=activity_at,
                    last_activity_at=activity_at,
                    next_report_at=activity_at
                    + timedelta(minutes=self._settings.lead_progress_interval_minutes),
                )
                session.add(progress)
                session.flush()
            elif activity_at > _as_utc(progress.last_activity_at):
                progress.last_activity_at = activity_at

            session.add(
                LeadProgressMessage(
                    message_id=message_id,
                    progress_session_id=progress.id,
                    created_at=current_time,
                )
            )
            logger.info(
                "lead_progress_message_recorded",
                extra={
                    "progress_session_id": progress.id,
                    "event": "lead_progress_message_recorded",
                },
            )
        return True

    def schedule_due_reports(self, *, now: datetime | None = None) -> int:
        """扫描到期会话，创建唯一汇总通知并在空闲完成后发送最终汇总。

        参数：now 可注入调度时钟；省略时使用当前 UTC 时间。
        返回值：本次新建的逻辑通知数量。
        异常：数据库约束和适配器读取异常会被安全记录；数据库事务错误向调用方传播。
        副作用：只更新进度会话和 NotificationRecord，不发送消息或执行线索副作用。
        """
        if not self._settings.lead_progress_enabled:
            return 0
        current_time = _as_utc(now or utc_now())
        idle_before = current_time - timedelta(
            minutes=self._settings.lead_progress_idle_stop_minutes
        )
        with self._session_factory() as session:
            candidates = session.execute(
                select(
                    LeadProgressSession.id,
                    LeadProgressSession.sales_user_id,
                    LeadProgressSession.next_report_at,
                    LeadProgressSession.last_activity_at,
                    SalesAuthorization.is_active,
                )
                .join(
                    SalesAuthorization,
                    SalesAuthorization.wecom_user_id == LeadProgressSession.sales_user_id,
                )
                .where(
                    LeadProgressSession.status == "active",
                    or_(
                        LeadProgressSession.next_report_at <= current_time,
                        LeadProgressSession.last_activity_at <= idle_before,
                        SalesAuthorization.is_active.is_(False),
                    ),
                )
                .order_by(LeadProgressSession.next_report_at, LeadProgressSession.id)
                .limit(_SCHEDULER_BATCH_SIZE)
            ).all()

        created = 0
        for progress_id, sales_user_id, next_report_at, last_activity_at, is_active in candidates:
            due = _as_utc(next_report_at) <= current_time
            idle = _as_utc(last_activity_at) <= idle_before
            if not is_active:
                self._close_disabled_session(progress_id, sales_user_id, current_time)
                continue
            if not due and not idle:
                continue
            if not due and self._processing_item_count(progress_id) > 0:
                # 空闲但仍有任务时继续等处理终态；无需读取共享表格快照。
                continue

            fresh_fields = self._read_successful_record_snapshots(progress_id)
            with self._session_factory.begin() as session:
                # 锁顺序与消息登记相同，避免销售停用与会话收敛互相死锁。
                authorization = session.scalar(
                    select(SalesAuthorization)
                    .where(SalesAuthorization.wecom_user_id == sales_user_id)
                    .with_for_update()
                )
                progress = session.scalar(
                    select(LeadProgressSession)
                    .where(LeadProgressSession.id == progress_id)
                    .with_for_update()
                )
                if progress is None or progress.status != "active":
                    continue
                if authorization is None or not authorization.is_active:
                    progress.status = "disabled"
                    progress.closed_at = current_time
                    progress.close_reason = "sales_disabled"
                    continue

                stats = self._calculate_stats(session, progress, fresh_fields)
                now_idle = _as_utc(progress.last_activity_at) <= idle_before
                is_final = now_idle and stats.processing_items == 0
                is_due = _as_utc(progress.next_report_at) <= current_time
                if not is_final and not is_due:
                    continue

                occurrence = "final" if is_final else _as_utc(progress.next_report_at).isoformat()
                notification_key = hashlib.sha256(
                    f"lead_progress:{progress.id}:{occurrence}".encode()
                ).hexdigest()
                if session.get(NotificationRecord, notification_key) is None:
                    content = _render_summary(stats, final=is_final)
                    session.add(
                        NotificationRecord(
                            notification_key=notification_key,
                            sales_user_id=sales_user_id,
                            source_message_id=progress.id,
                            notification_type="lead_progress_summary",
                            content=content,
                            payload={
                                "progress_session_id": progress.id,
                                "window_started_at": _as_utc(progress.started_at).isoformat(),
                                "window_ended_at": current_time.isoformat(),
                                "scheduled_due_at": _as_utc(progress.next_report_at).isoformat(),
                                "final": is_final,
                                "stats": asdict(stats),
                            },
                        )
                    )
                    created += 1
                    logger.info(
                        "lead_progress_notification_queued",
                        extra={
                            "progress_session_id": progress.id,
                            "notification_key": notification_key,
                            "is_final": is_final,
                            "event": "lead_progress_notification_queued",
                        },
                    )
                if is_final:
                    progress.status = "closed"
                    progress.closed_at = current_time
                    progress.close_reason = "idle_complete"
                else:
                    # 错过多个扫描周期时只排一条通知，并从本次扫描时间继续计时。
                    next_report_at = _as_utc(progress.next_report_at) + timedelta(
                        minutes=self._settings.lead_progress_interval_minutes
                    )
                    progress.next_report_at = (
                        next_report_at
                        if next_report_at > current_time
                        else current_time
                        + timedelta(minutes=self._settings.lead_progress_interval_minutes)
                    )
        return created

    def _close_disabled_session(self, progress_id: str, sales_user_id: str, now: datetime) -> None:
        """关闭已停用销售的活跃会话且不创建主动汇报。

        参数：progress_id 为会话标识；sales_user_id 为会话销售；now 为调度时间。
        返回值：无。
        异常：数据库写入错误向调用方传播。
        副作用：把会话标记为 disabled，不新增通知。
        """
        with self._session_factory.begin() as session:
            authorization = session.scalar(
                select(SalesAuthorization)
                .where(SalesAuthorization.wecom_user_id == sales_user_id)
                .with_for_update()
            )
            progress = session.scalar(
                select(LeadProgressSession)
                .where(LeadProgressSession.id == progress_id)
                .with_for_update()
            )
            if (
                progress is not None
                and progress.status == "active"
                and (authorization is None or not authorization.is_active)
            ):
                progress.status = "disabled"
                progress.closed_at = now
                progress.close_reason = "sales_disabled"

    def _processing_item_count(self, progress_id: str) -> int:
        """返回唯一处理中工作项数量，线索同步与消息处理不重复相加。

        参数：progress_id 为统计会话标识。
        返回值：非终态 SmartTableSync 线索数，加上未被这些同步覆盖的非终态消息数。
        异常：数据库读取错误向调用方传播。
        副作用：仅读取数据库。
        """
        with self._session_factory() as session:
            progress = session.get(LeadProgressSession, progress_id)
            if progress is None:
                return 0
            return self._calculate_stats(session, progress, {}).processing_items

    def _read_successful_record_snapshots(
        self, progress_id: str
    ) -> dict[str, Mapping[str, object]]:
        """只读获取本会话已核实成功线索的当前表格字段快照。

        参数：progress_id 为统计会话标识。
        返回值：记录标识到当前远端字段的映射；缺失或读取失败的记录不使用旧草稿补值。
        异常：逐条适配器读取异常被计入待核实，不阻断通知调度。
        副作用：调用既有 SmartTableAdapter.get_record，不写表格或后台 Lead 字段。
        """
        with self._session_factory() as session:
            progress = session.get(LeadProgressSession, progress_id)
            if progress is None:
                return {}
            records = self._successful_record_ids(session, progress)

        snapshots: dict[str, Mapping[str, object]] = {}
        for record_id in records:
            try:
                record = self._smart_table_adapter.get_record(record_id)
            except Exception as error:
                logger.warning(
                    "lead_progress_snapshot_read_failed",
                    extra={"error_type": type(error).__name__},
                )
                continue
            if record is None or record.record_id != record_id:
                continue
            snapshots[record_id] = record.fields
        return snapshots

    def _successful_record_ids(
        self, session: Session, progress: LeadProgressSession
    ) -> tuple[str, ...]:
        """列出 Lead 与 succeeded SmartTableSync 记录标识完全一致的线索。

        参数：session 为只读数据库会话；progress 为目标销售会话。
        返回值：去重后的当前智能表格 record_id。
        异常：数据库读取错误向调用方传播。
        副作用：仅读取消息归属、线索和同步事实。
        """
        message_ids = select(LeadProgressMessage.message_id).where(
            LeadProgressMessage.progress_session_id == progress.id
        )
        lead_ids = select(LeadMessageResolution.lead_id).where(
            LeadMessageResolution.message_id.in_(message_ids),
            LeadMessageResolution.lead_id.is_not(None),
        )
        rows = session.execute(
            select(Lead.smart_table_record_id)
            .join(SmartTableSync, SmartTableSync.lead_id == Lead.id)
            .where(
                Lead.id.in_(lead_ids),
                Lead.original_capturing_sales_user_id == progress.sales_user_id,
                Lead.smart_table_record_id.is_not(None),
                SmartTableSync.status == "succeeded",
                SmartTableSync.smart_table_record_id == Lead.smart_table_record_id,
            )
        ).all()
        return tuple(dict.fromkeys(row[0] for row in rows if isinstance(row[0], str)))

    def _calculate_stats(
        self,
        session: Session,
        progress: LeadProgressSession,
        fresh_fields: Mapping[str, Mapping[str, object]],
    ) -> LeadProgressStats:
        """按消息关联与当前同步事实计算互不串线的会话统计。

        参数：session 为数据库事务；progress 为统计窗口；fresh_fields 为当前远端字段。
        返回值：所有消息、Lead、同步和归属指标均按各自主键去重。
        异常：数据库读取失败向调用方传播；不完整字段由 LeadCompletenessService 处理。
        副作用：仅读取数据库快照，不修改 Lead 或 SmartTableSync。
        """
        message_ids = tuple(
            session.scalars(
                select(LeadProgressMessage.message_id).where(
                    LeadProgressMessage.progress_session_id == progress.id
                )
            ).all()
        )
        if not message_ids:
            return LeadProgressStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        resolutions = session.scalars(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id.in_(message_ids))
        ).all()
        referenced_lead_ids = {item.lead_id for item in resolutions if item.lead_id is not None}
        leads = (
            session.scalars(
                select(Lead).where(
                    Lead.id.in_(referenced_lead_ids),
                    Lead.original_capturing_sales_user_id == progress.sales_user_id,
                )
            ).all()
            if referenced_lead_ids
            else []
        )
        leads_by_id = {lead.id: lead for lead in leads}
        lead_ids = set(leads_by_id)
        syncs = (
            session.scalars(
                select(SmartTableSync).where(SmartTableSync.lead_id.in_(lead_ids))
            ).all()
            if lead_ids
            else []
        )
        syncs_by_lead = {sync.lead_id: sync for sync in syncs}
        successful_ids = {
            lead_id
            for lead_id, lead in leads_by_id.items()
            if (sync := syncs_by_lead.get(lead_id)) is not None
            and lead.smart_table_record_id is not None
            and sync.status == "succeeded"
            and sync.smart_table_record_id == lead.smart_table_record_id
        }
        failed_ids = {
            lead_id
            for lead_id, sync in syncs_by_lead.items()
            if lead_id in leads_by_id and sync.status == "failed_pending_review"
        }
        processing_lead_ids = {
            lead_id
            for lead_id, sync in syncs_by_lead.items()
            if lead_id in leads_by_id and sync.status in _ACTIVE_SYNC_STATUSES
        }
        active_messages = {
            message_id: status
            for message_id, status in session.execute(
                select(OutboxEvent.message_id, OutboxEvent.status).where(
                    OutboxEvent.message_id.in_(message_ids),
                    OutboxEvent.sales_user_id == progress.sales_user_id,
                    OutboxEvent.event_type == "message_received",
                )
            ).all()
            if status in _ACTIVE_OUTBOX_STATUSES
        }
        covered_messages = {
            item.message_id for item in resolutions if item.lead_id in processing_lead_ids
        }
        processing_items = len(processing_lead_ids) + len(set(active_messages) - covered_messages)
        unassigned_segments = len(
            {
                (item.message_id, item.segment_index)
                for item in resolutions
                if item.status in _UNASSIGNED_RESOLUTION_STATUSES
            }
        )

        missing_ids: set[str] = set()
        pending_ids: set[str] = set()
        incomplete_ids: set[str] = set()
        unverified_ids = 0
        for lead_id in successful_ids:
            lead = leads_by_id[lead_id]
            record_id = lead.smart_table_record_id
            current_fields = fresh_fields.get(record_id or "")
            if current_fields is None:
                unverified_ids += 1
                continue
            completeness = self._completeness.evaluate(current_fields)
            if completeness.missing_required_fields:
                missing_ids.add(lead_id)
            if completeness.pending_confirmation_fields:
                pending_ids.add(lead_id)
            if completeness.missing_required_fields or completeness.pending_confirmation_fields:
                incomplete_ids.add(lead_id)

        return LeadProgressStats(
            received_messages=len(set(message_ids)),
            identified_leads=len(lead_ids),
            successful_leads=len(successful_ids),
            incomplete_leads=len(incomplete_ids),
            missing_required_leads=len(missing_ids),
            ai_pending_leads=len(pending_ids),
            sync_failed_leads=len(failed_ids),
            processing_items=processing_items,
            unassigned_segments=unassigned_segments,
            snapshot_unverified_leads=unverified_ids,
        )


def _render_summary(stats: LeadProgressStats, *, final: bool) -> str:
    """渲染不包含客户名称、联系方式或原始需求的手机端进度通知。

    参数：stats 为同一销售会话的统计快照；final 表示是否为会话最终汇报。
    返回值：按固定字段顺序排列的短消息文本。
    异常：无。
    副作用：无，不访问数据库或消息通道。
    """
    title = "📊 需求录入进度最终汇报" if final else "📊 需求录入进度汇报"
    content = (
        f"{title}\n"
        f"收到消息：{stats.received_messages} 条\n"
        f"识别线索：{stats.identified_leads} 条\n"
        f"录入成功：{stats.successful_leads} 条\n"
        "其中待完善："
        f"{stats.incomplete_leads} 条（缺字段 {stats.missing_required_leads}，"
        f"AI待确认 {stats.ai_pending_leads}，可重叠）\n"
        f"同步失败：{stats.sync_failed_leads} 条\n"
        f"处理中／等待重试：{stats.processing_items} 项\n"
        f"无法可靠归属客户：{stats.unassigned_segments} 段"
    )
    if stats.snapshot_unverified_leads:
        content += f"\n当前表格待核实：{stats.snapshot_unverified_leads} 条"
    return content


def _as_utc(value: datetime) -> datetime:
    """将数据库读出的无时区时间按 UTC 解释，以兼容 SQLite 测试存储。

    参数：value 为 session 数据库返回或调用方提供的时间。
    返回值：带 UTC 时区的 datetime；原本有时区时保留原值。
    异常：无。
    副作用：无，不修改输入对象。
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
