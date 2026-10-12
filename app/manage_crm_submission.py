"""通过命令行授权企微员工代提交其他员工的 CRM 线索。"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.messaging.sales_authorization import SalesAuthorizationDirectoryService
from app.messaging.wecom_directory import (
    WecomDirectoryError,
    WecomDirectoryMember,
    resolve_wecom_member,
)

_CLI_OPERATOR = "crm_cross_owner_submission_cli"
logger = logging.getLogger(__name__)


def grant_crm_submission_access(session: Session, member: WecomDirectoryMember) -> str:
    """为已由企微通讯录确认的成员授予跨员工 CRM 提交权限。

    参数：session 为调用方持有的数据库事务；member 为企微通讯录精确解析出的成员。
    返回值：已授权员工的显示名。
    异常：成员信息不完整时抛 ValueError；数据库错误由会话层抛出。
    副作用：按需创建或更新 Actor Registry 行，并授予该成员跨员工提交权限。
    """
    if not member.user_id.strip() or not member.name.strip():
        raise ValueError("企微通讯录成员信息不完整")
    # 企微通讯录已证明成员有效；Actor Registry 可在首次授权时自动创建或恢复。
    authorization = SalesAuthorizationDirectoryService().authorize_salesperson(
        session, member.user_id, _CLI_OPERATOR
    )
    authorization.display_name = member.name.strip()
    # 只开启跨员工范围权限，不修改该成员的企微身份或 CRM 映射。
    authorization.can_submit_other_sales_leads = True
    authorization.updated_by = _CLI_OPERATOR
    return authorization.display_name


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
    grant_parser.add_argument("employee_name", help="企微通讯录中的唯一成员姓名")
    arguments = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(
        settings.log_level, environment=settings.app_env, service="crm_submission_admin"
    )
    engine = create_engine(settings.database_url)
    try:
        # 必须先确认成员来自企微通讯录，再创建或更新本地授权目录行。
        member = resolve_wecom_member(
            settings.wecom_cli_command,
            arguments.employee_name,
            settings.wecom_cli_timeout_seconds,
        )
        session_factory = sessionmaker(engine)
        with session_factory.begin() as session:
            granted_name = grant_crm_submission_access(session, member)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    except WecomDirectoryError as error:
        print(str(error), file=sys.stderr)
        return 1
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
