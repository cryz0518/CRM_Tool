"""通过命令行授权企微员工代提交其他员工的 CRM 线索。"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.messaging.models import SalesAuthorization

_CLI_OPERATOR = "crm_cross_owner_submission_cli"
logger = logging.getLogger(__name__)


def grant_crm_submission_access(session: Session, employee_name: str) -> str:
    """按唯一企微显示名授予跨员工 CRM 提交权限。

    参数：session 为调用方持有的数据库事务；employee_name 为企微员工显示名。
    返回值：已授权员工的显示名。
    异常：姓名为空、未登记、重名或员工停用时抛出 ValueError。
    副作用：更新该员工的跨员工提交权限和目录修改主体。
    """
    normalized_name = employee_name.strip()
    # 空白姓名不能回退到默认员工，也不能使用模糊查询选择成员。
    if not normalized_name:
        raise ValueError("企微员工姓名不能为空")
    authorizations = session.scalars(
        select(SalesAuthorization)
        .where(SalesAuthorization.display_name == normalized_name)
        .with_for_update()
    ).all()
    # 只接受精确且唯一的员工记录；重名或未登记时拒绝授权，避免授错账号。
    if len(authorizations) != 1:
        raise ValueError("未找到唯一的企微员工显示名，请先确认员工已在系统登记且姓名不重名")
    authorization = authorizations[0]
    # 停用成员不能获得新的提交权限。
    if not authorization.is_active:
        raise ValueError("目标企微员工已停用，不能授予提交权限")
    # 只开启跨员工范围权限，不修改该成员的企微身份或 CRM 映射。
    authorization.can_submit_other_sales_leads = True
    authorization.updated_by = _CLI_OPERATOR
    return normalized_name


def main(argv: list[str] | None = None) -> int:
    """解析授权命令并将变更写入当前环境数据库。

    参数：argv 为可选命令行参数；省略时读取当前进程参数。
    返回值：成功返回 0，参数或数据库失败返回非零状态。
    异常：业务校验失败显示安全错误；数据库异常记录堆栈并返回 1。
    副作用：可能更新 sales_authorizations 中一名员工的权限。
    """
    parser = argparse.ArgumentParser(description="授权企微员工提交其他员工的 CRM 线索")
    subparsers = parser.add_subparsers(dest="action", required=True)
    grant_parser = subparsers.add_parser("grant", help="按企微显示姓名授予跨员工提交权限")
    grant_parser.add_argument("employee_name", help="企微成员在系统登记的显示姓名")
    arguments = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(
        settings.log_level, environment=settings.app_env, service="crm_submission_admin"
    )
    engine = create_engine(settings.database_url)
    try:
        session_factory = sessionmaker(engine)
        with session_factory.begin() as session:
            granted_name = grant_crm_submission_access(session, arguments.employee_name)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    except Exception:
        logger.exception("crm_cross_owner_submission_authorization_failed")
        print("授权失败，请查看应用日志。", file=sys.stderr)
        return 1
    finally:
        engine.dispose()
    logger.info("crm_cross_owner_submission_authorization_granted")
    print(f"已授权企微员工「{granted_name}」提交其他员工的 CRM 线索。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
