"""WeCom actor registry 的最小读取边界。"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session

from app.messaging.models import SalesAuthorization


class SalesIdentityProvider(Protocol):
    """在进入业务流程前判定企业微信成员是否存在且仍启用。"""

    def is_active(self, session: Session, sales_user_id: str) -> bool:
        """判断给定企业微信成员在当前事务中是否存在且仍启用。

        参数：session 为当前业务事务；sales_user_id 为企业微信成员标识。
        返回值：actor 存在且启用时返回 True，否则返回 False。
        异常：Actor Registry 读取失败时由数据库层抛出。
        副作用：仅读取 Actor Registry。
        """
        ...


class DatabaseSalesIdentityProvider:
    """以数据库 Actor Registry 作为运行时权威来源的身份提供器。"""

    def is_active(self, session: Session, sales_user_id: str) -> bool:
        """读取 Actor Registry 并返回成员存在与启用状态。

        参数：session 为当前业务事务；sales_user_id 为企业微信成员标识。
        返回值：成员存在且启用时返回 True。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：无。
        """
        # Bot 可见范围不能替代后端 actor 状态；is_authorized 仅为历史兼容字段。
        actor = session.get(SalesAuthorization, sales_user_id)
        return bool(actor is not None and actor.is_active)
