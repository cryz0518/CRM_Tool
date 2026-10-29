"""以经过通讯录核验的成员身份读取跨员工 CRM 提交权限。"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.messaging.models import SalesAuthorization
from app.smart_table.adapter import SmartTableAdapter

logger = logging.getLogger(__name__)


def can_submit_other_sales_leads(
    session: Session,
    actor: SalesAuthorization | None,
    smart_table_adapter: SmartTableAdapter | None = None,
    *,
    lock_member: bool = False,
) -> bool:
    """通过回调身份或其已核验通讯录成员读取专用权限，不复制授权。

    参数：session 为当前业务事务；actor 为原始发送者记录；smart_table_adapter 提供
    唯一成员映射；lock_member 表示冻结提交时同时锁定通讯录授权行。
    返回值：发送者启用，且直接获权或其已确认成员启用并持有专用权限时返回 True。
    异常：数据库读取错误向调用方传播；身份无法确认时拒绝授权并记录安全告警。
    副作用：必要时只读查询通讯录并复用 Adapter 的身份缓存；不缓存权限、不修改记录。
    """
    # 原始回调身份被停用时不能通过另一个启用的成员记录绕过停用。
    if actor is None or not actor.is_active:
        return False
    if actor.can_submit_other_sales_leads:
        return True

    try:
        if smart_table_adapter is None:
            from app.smart_table.dependencies import get_smart_table_adapter

            smart_table_adapter = get_smart_table_adapter()
        # 只按真实回调标识核验映射，绝不以同名或客户端提供的姓名继承权限。
        member_id = smart_table_adapter.resolve_member_user_id(actor.wecom_user_id)
    except Exception as error:
        logger.warning(
            "crm_cross_owner_identity_resolution_failed",
            extra={"error_type": type(error).__name__},
        )
        return False
    if not isinstance(member_id, str) or not member_id or member_id == actor.wecom_user_id:
        return False

    # 每次重新读授权事实，卡片延迟点击、撤权及停用不能被身份缓存掩盖。
    member = session.get(
        SalesAuthorization, member_id, populate_existing=True, with_for_update=lock_member,
    )
    return bool(member is not None and member.is_active and member.can_submit_other_sales_leads)
