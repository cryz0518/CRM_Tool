"""为 T17 映射缺失同步事实增加可空 CRM 身份和失败代码。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0020_ticket17_crm_user_mapping"
down_revision = "0019_ticket15_console_observability"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """允许映射缺失验证记录不携带 CRM 用户，并保存机器可读失败代码。

    参数：无。
    返回值：无。
    异常：DDL 失败时由 Alembic 抛出并回滚事务。
    副作用：修改 CRM 同步记录的 CRM 用户可空约束并增加失败代码列。
    """
    # 映射缺失在调用 CRM 前终止，因此该同步事实没有可冻结的 CRM 用户标识。
    op.alter_column(
        "crm_sync_records",
        "submitting_crm_user_id",
        existing_type=sa.String(length=128),
        nullable=True,
    )
    op.add_column("crm_sync_records", sa.Column("failure_kind", sa.String(length=64)))
    op.add_column("crm_sync_records", sa.Column("failure_code", sa.String(length=64)))
    op.add_column("sales_authorizations", sa.Column("created_by", sa.String(length=128)))
    op.add_column("sales_authorizations", sa.Column("updated_by", sa.String(length=128)))


def downgrade() -> None:
    """回退 T17 的同步失败扩展，并拒绝丢失任何 T17 审计数据。

    参数：无。
    返回值：无。
    异常：存在 T17 新增审计数据时抛出 RuntimeError，避免静默篡改审计数据。
    副作用：无 T17 审计数据时恢复旧列约束并删除 T17 新增列。
    """
    connection = op.get_bind()
    audit_data_exists = connection.execute(
        sa.text(
            "SELECT 1 FROM crm_sync_records "
            "WHERE failure_kind IS NOT NULL OR failure_code IS NOT NULL "
            "OR submitting_crm_user_id IS NULL LIMIT 1"
        )
    ).first()
    directory_audit_exists = connection.execute(
        sa.text(
            "SELECT 1 FROM sales_authorizations "
            "WHERE created_by IS NOT NULL OR updated_by IS NOT NULL LIMIT 1"
        )
    ).first()
    if audit_data_exists is not None or directory_audit_exists is not None:
        raise RuntimeError("存在 T17 审计数据，禁止回退 T17 迁移")
    op.drop_column("crm_sync_records", "failure_code")
    op.drop_column("crm_sync_records", "failure_kind")
    op.drop_column("sales_authorizations", "updated_by")
    op.drop_column("sales_authorizations", "created_by")
    op.alter_column(
        "crm_sync_records",
        "submitting_crm_user_id",
        existing_type=sa.String(length=128),
        nullable=False,
    )
