"""销售授权目录的最小读取边界。"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session

from app.messaging.models import SalesAuthorization


class SalesIdentityProvider(Protocol):
    """在进入任何线索流程前判定企业微信成员是否具备销售录入资格。"""

    def is_authorized(self, session: Session, sales_user_id: str) -> bool:
        """判断给定企业微信成员在当前事务中是否被授权且仍启用。

        参数：session 为当前业务事务；sales_user_id 为企业微信成员标识。
        返回值：成员可录入时返回 True，否则返回 False。
        异常：授权目录读取失败时由数据库层抛出。
        副作用：仅读取销售授权目录。
        """
        ...


class DatabaseSalesIdentityProvider:
    """以数据库销售授权目录作为运行时权威来源的身份提供器。"""

    def is_authorized(self, session: Session, sales_user_id: str) -> bool:
        """读取授权目录并返回成员的显式授权与启用状态。

        参数：session 为当前业务事务；sales_user_id 为企业微信成员标识。
        返回值：成员同时授权和启用时返回 True。
        异常：数据库读取失败时由 SQLAlchemy 抛出。
        副作用：无。
        """
        # 授权目录是销售身份唯一权威来源，不能以机器人可见范围或环境变量替代。
        authorization = session.get(SalesAuthorization, sales_user_id)
        return bool(
            authorization is not None and authorization.is_authorized and authorization.is_active
        )
