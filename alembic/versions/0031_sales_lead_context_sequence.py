"""为销售当前线索上下文增加消息顺序事实。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0031_sales_lead_context_sequence"
down_revision = "0030_audit_mirror_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """新增可空消息序号并按历史接收事实尽力回填。

    参数：无，Alembic 通过当前数据库连接执行 DDL 和回填。
    返回值：无。
    异常：数据库结构错误由 Alembic 或数据库驱动抛出。
    副作用：新增销售上下文序号列；无法可靠匹配的历史上下文保持 NULL。
    """
    op.add_column(
        "sales_lead_contexts",
        sa.Column("last_message_sequence", sa.Integer(), nullable=True),
    )
    connection = op.get_bind()
    contexts = connection.execute(
        sa.text(
            "SELECT sales_user_id, last_message_received_at "
            "FROM sales_lead_contexts"
        )
    ).mappings().all()
    for context in contexts:
        sequence = connection.scalar(
            sa.text(
                "SELECT MAX(sequence) FROM incoming_messages "
                "WHERE sales_user_id = :sales_user_id "
                "AND received_at <= :last_message_received_at"
            ),
            {
                "sales_user_id": context["sales_user_id"],
                "last_message_received_at": context["last_message_received_at"],
            },
        )
        if sequence is not None:
            connection.execute(
                sa.text(
                    "UPDATE sales_lead_contexts "
                    "SET last_message_sequence = :sequence "
                    "WHERE sales_user_id = :sales_user_id"
                ),
                {"sequence": sequence, "sales_user_id": context["sales_user_id"]},
            )


def downgrade() -> None:
    """删除销售上下文消息序号列，保留原有上下文字段。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：数据库结构错误由 Alembic 或数据库驱动抛出。
    副作用：删除本迁移新增的顺序事实列。
    """
    op.drop_column("sales_lead_contexts", "last_message_sequence")
