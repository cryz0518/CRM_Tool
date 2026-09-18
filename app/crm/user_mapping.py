"""CRM 提交销售身份映射的可替换读取边界。"""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session

from app.messaging.models import SalesAuthorization


class CRMUserMapper(Protocol):
    """在 CRM 提交前解析企业微信销售对应的 CRM 用户身份。"""

    def get_crm_user_id(self, session: Session, sales_user_id: str) -> str | None:
        """返回销售当前可用的 CRM 用户标识，缺失时返回 None。

        参数：session 为当前业务事务；sales_user_id 为企业微信销售用户标识。
        返回值：经确定性校验的 CRM 用户标识，或缺失标记 None。
        异常：目录读取失败时由数据库层抛出。
        副作用：仅读取映射目录，不改变授权或 CRM 数据。
        """
        ...


class DatabaseCRMUserMapper:
    """从销售授权目录读取 CRM 用户映射的数据库实现。"""

    def get_crm_user_id(self, session: Session, sales_user_id: str) -> str | None:
        """规范化读取指定销售的 CRM 用户标识。

        参数：session 为当前业务事务；sales_user_id 为企业微信销售用户标识。
        返回值：去除首尾空白后的 CRM 用户标识，缺失或空白时返回 None。
        异常：目录读取失败时由 SQLAlchemy 抛出。
        副作用：无，不以 CRM 映射反向授予销售录入权限。
        """
        # 映射只能由目录显式提供，查询不到目录成员不允许回退到其他身份。
        authorization = session.get(SalesAuthorization, sales_user_id)
        if authorization is None or authorization.crm_user_id is None:
            return None
        normalized = authorization.crm_user_id.strip()
        return normalized or None
