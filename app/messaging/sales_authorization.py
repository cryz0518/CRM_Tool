"""销售授权目录的受审计写入边界。"""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.messaging.models import SalesAuthorization


class SalesAuthorizationDirectoryService:
    """集中执行销售授权目录的创建和受控更新。"""

    def authorize_salesperson(
        self, session: Session, wecom_user_id: str, operator_subject: str
    ) -> SalesAuthorization:
        """创建或恢复一名销售的授权与启用状态，并记录操作人。

        参数：session 为调用方持有的事务；wecom_user_id 为目标企业微信成员；
        operator_subject 为执行目录操作的受控主体。
        返回值：创建或更新后的销售授权目录记录。
        异常：空目标或操作人抛出 ValueError；数据库错误由 SQLAlchemy 抛出。
        副作用：新增或更新销售授权目录，并写入创建人或修改人。
        """
        normalized_user_id = wecom_user_id.strip()
        normalized_operator = operator_subject.strip()
        if not normalized_user_id or not normalized_operator:
            raise ValueError("销售授权目录目标和操作人不能为空")
        authorization = session.get(SalesAuthorization, normalized_user_id)
        if authorization is None:
            # 新记录的创建人和修改人均是本次受控写入主体。
            authorization = SalesAuthorization(
                wecom_user_id=normalized_user_id,
                is_authorized=True,
                is_active=True,
                created_by=normalized_operator,
                updated_by=normalized_operator,
            )
            session.add(authorization)
            return authorization
        # 重复授权仍形成一次目录更新，必须保留最后修改主体。
        authorization.is_authorized = True
        authorization.is_active = True
        authorization.updated_by = normalized_operator
        return authorization
