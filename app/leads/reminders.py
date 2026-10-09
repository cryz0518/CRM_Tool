"""每天未提交线索的提醒排程。"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from app.leads.models import CrmSyncRecord, Lead
from app.messaging.models import NotificationRecord, SalesAuthorization, utc_now

logger = logging.getLogger(__name__)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REMINDER_TYPE = "daily_unsubmitted_lead_reminder"
DAILY_UNSUBMITTED_REMINDER_CONTENT = "⏰ 今天仍有未提交线索，请完成审核后提交 CRM。"
_UNSUBMITTED_LEAD_STATES = ("temporary", "pending_create")


class DailyUnsubmittedLeadReminderService:
    """为有当日待创建线索的在职销售登记唯一提醒。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存数据库会话工厂。

        参数：session_factory 为应用数据库会话工厂。
        返回值：无。
        异常：无。
        副作用：无；构造时不访问数据库或消息服务。
        """
        self._session_factory = session_factory

    def schedule_due_reminders(self, *, now: datetime | None = None) -> int:
        """在上海时间 20:00 后为符合条件的销售登记每日提醒。

        参数：now 为可注入的当前时间；省略时使用 UTC 当前时间。
        返回值：本轮新登记的逻辑提醒数。
        异常：数据库错误向 Celery 传播，以便安全重试。
        副作用：只向现有 NotificationRecord Outbox 插入提醒，不发送消息或修改线索。
        """
        current = _as_utc(now or utc_now())
        local_now = current.astimezone(_SHANGHAI)
        # Beat 以 20:00 触发；迟到重扫仍可补发当天提醒，提前扫描则完全不排队。
        if local_now.time().replace(tzinfo=None) < time(20, 0):
            return 0
        day_start, day_end = _business_day_bounds(local_now.date())

        with self._session_factory() as session:
            sales_user_ids = tuple(
                session.scalars(
                    select(SalesAuthorization.wecom_user_id)
                    .join(
                        Lead,
                        Lead.smart_table_owner_user_id == SalesAuthorization.wecom_user_id,
                    )
                    .where(
                        SalesAuthorization.is_authorized.is_(True),
                        SalesAuthorization.is_active.is_(True),
                        SalesAuthorization.is_administrator.is_(False),
                        SalesAuthorization.crm_user_id.is_not(None),
                        func.trim(SalesAuthorization.crm_user_id) != "",
                        Lead.created_at >= day_start,
                        Lead.created_at < day_end,
                        Lead.lifecycle_state.in_(_UNSUBMITTED_LEAD_STATES),
                    )
                    .distinct()
                ).all()
            )

        created = 0
        for sales_user_id in sales_user_ids:
            with self._session_factory.begin() as session:
                # 重新锁定并核验销售目录，避免扫描后停用或转为管理员仍排入提醒。
                authorization = session.scalar(
                    select(SalesAuthorization)
                    .where(SalesAuthorization.wecom_user_id == sales_user_id)
                    .with_for_update()
                )
                if not _eligible_sales(authorization):
                    continue
                lead_count = _unsubmitted_lead_count(
                    session, sales_user_id, day_start, day_end
                )
                if not lead_count:
                    continue

                # 每名销售每天使用固定主键；数据库冲突处理覆盖并发扫描与任务重试。
                notification_key = hashlib.sha256(
                    f"daily_unsubmitted_leads:{sales_user_id}:{local_now.date().isoformat()}".encode()
                ).hexdigest()
                content = DAILY_UNSUBMITTED_REMINDER_CONTENT
                values = {
                    "notification_key": notification_key,
                    "sales_user_id": sales_user_id,
                    "source_message_id": notification_key,
                    "notification_type": _REMINDER_TYPE,
                    "content": content,
                    "payload": {
                        "msgtype": "markdown",
                        "markdown": {"content": content},
                        "business_date": local_now.date().isoformat(),
                        "lead_count": lead_count,
                    },
                    "status": "pending",
                }
                dialect = session.get_bind().dialect.name
                if dialect == "postgresql":
                    result = cast(
                        CursorResult[Any],
                        session.execute(
                            postgresql_insert(NotificationRecord)
                            .values(**values)
                            .on_conflict_do_nothing(
                                index_elements=[NotificationRecord.notification_key]
                            )
                        ),
                    )
                elif dialect == "sqlite":
                    result = cast(
                        CursorResult[Any],
                        session.execute(
                            sqlite_insert(NotificationRecord)
                            .values(**values)
                            .on_conflict_do_nothing(
                                index_elements=[NotificationRecord.notification_key]
                            )
                        ),
                    )
                else:
                    # 生产使用 PostgreSQL，SQLite 用于隔离测试；其他方言沿用锁后插入。
                    if session.get(NotificationRecord, notification_key) is not None:
                        continue
                    session.add(NotificationRecord(**values))
                    created += 1
                    continue
                if result.rowcount == 1:
                    created += 1
                    logger.info(
                        "daily_unsubmitted_lead_reminder_queued",
                        extra={
                            "notification_key": notification_key,
                            "sales_user_id": sales_user_id,
                            "business_date": local_now.date().isoformat(),
                            "lead_count": lead_count,
                            "event": "daily_unsubmitted_lead_reminder_queued",
                        },
                    )
        return created


def daily_reminder_is_current(
    session: Session,
    sales_user_id: str,
    business_date: object,
    *,
    now: datetime | None = None,
) -> bool:
    """发送前确认提醒日期、销售资格和未提交线索仍然有效。

    参数：session 为当前发送事务；sales_user_id 为收件销售；business_date 为载荷日期；
    now 为可注入的当前时间。
    返回值：仍是上海当天且销售当前有效并有未提交线索时返回 True。
    异常：数据库错误向 Outbox 发送器传播，通知可按现有机制重试。
    副作用：仅读取销售目录、线索和 CRM 提交记录。
    """
    # 日期载荷无效或已跨上海业务日时抑制过期提醒。
    try:
        reminder_date = (
            date.fromisoformat(business_date) if isinstance(business_date, str) else None
        )
    except ValueError:
        return False
    current_date = _as_utc(now or utc_now()).astimezone(_SHANGHAI).date()
    if reminder_date != current_date:
        return False
    authorization = session.get(SalesAuthorization, sales_user_id)
    if not _eligible_sales(authorization):
        return False
    day_start, day_end = _business_day_bounds(current_date)
    return _unsubmitted_lead_count(session, sales_user_id, day_start, day_end) > 0


def _eligible_sales(authorization: SalesAuthorization | None) -> bool:
    """判定提醒收件人已授权、启用、非管理员且已有 CRM 映射。

    参数：authorization 为数据库销售目录记录，缺失时为 None。
    返回值：符合提醒资格时返回 True。
    异常：无。
    副作用：无；提醒遵循 AGENTS.md 销售授权目录的显式授权要求。
    """
    return bool(
        authorization is not None
        and authorization.is_authorized
        and authorization.is_active
        and not authorization.is_administrator
        and authorization.crm_user_id
        and authorization.crm_user_id.strip()
    )


def _business_day_bounds(day: date) -> tuple[datetime, datetime]:
    """返回上海业务日对应的 UTC 半开区间。

    参数：day 为 Asia/Shanghai 业务日期。
    返回值：含本日、排除次日的 UTC 起止时间。
    异常：无。
    副作用：无。
    """
    start = datetime.combine(day, time.min, tzinfo=_SHANGHAI).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=_SHANGHAI).astimezone(UTC)
    return start, end


def _unsubmitted_lead_count(
    session: Session,
    sales_user_id: str,
    day_start: datetime,
    day_end: datetime,
) -> int:
    """统计销售在上海当日采集且最新 CRM 创建代次未成功或放弃的线索。

    参数：session 为当前数据库会话；sales_user_id 为当前负责人；day_start/day_end 为 UTC 区间。
    返回值：符合条件的线索数量。
    异常：数据库读取错误向调用方传播。
    副作用：仅读取线索及最新 CRM create generation；仅 succeeded/abandoned 视为已处理。
    """
    # 与“提交今天的线索”复核规则一致，只检查最新 create generation。
    latest_create_status = (
        select(CrmSyncRecord.status)
        .where(
            CrmSyncRecord.lead_id == Lead.id,
            CrmSyncRecord.operation == "create",
        )
        .order_by(CrmSyncRecord.generation.desc().nullslast(), CrmSyncRecord.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return int(
        session.scalar(
            select(func.count(Lead.id)).where(
                Lead.smart_table_owner_user_id == sales_user_id,
                Lead.created_at >= day_start,
                Lead.created_at < day_end,
                Lead.lifecycle_state.in_(_UNSUBMITTED_LEAD_STATES),
                func.coalesce(latest_create_status, "").not_in(("succeeded", "abandoned")),
            )
        )
        or 0
    )


def _as_utc(value: datetime) -> datetime:
    """将无时区数据库或测试时间兼容解释为 UTC。

    参数：value 为数据库时间或调用方传入时间。
    返回值：带 UTC 时区的时间对象。
    异常：无。
    副作用：无，不修改传入对象。
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
