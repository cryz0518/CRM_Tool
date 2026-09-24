"""移除智能表格阶段的同公司唯一约束。"""

from __future__ import annotations

from alembic import op

revision = "0028_remove_smart_table_company_dedup"
down_revision = "0027_repair_notification_claim_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """允许同一销售在智能表格保留多个同名线索。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：目标数据库缺少历史唯一约束时由 Alembic 抛出异常。
    副作用：删除线索表中按销售和标准公司名建立的唯一约束。
    """
    op.drop_constraint("uq_leads_sales_standard_company", "leads", type_="unique")


def downgrade() -> None:
    """恢复历史唯一约束；仅适用于没有重复数据的旧库回滚。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：数据库已有重复销售和公司名时由数据库拒绝回滚。
    副作用：重新限制线索表中同销售同标准公司名只能存在一条记录。
    """
    op.create_unique_constraint(
        "uq_leads_sales_standard_company",
        "leads",
        ["smart_table_owner_user_id", "standard_company_name"],
    )
