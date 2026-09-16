"""为 T15 Console 增加 Break-glass 审计和 AI 执行元数据表。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0019_ticket15_console_observability"
down_revision = "0018_message_retry_lease_segment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建 T15 所需的追加式 Break-glass 审计和 AI execution 表。"""
    # Break-glass 审计不对业务对象建外键，确保对象后续清理不会抹掉安全审计事实。
    op.create_table(
        "break_glass_access_audits",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("access_id", sa.String(length=36), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("operator_subject", sa.String(length=128), nullable=False),
        sa.Column("operator_role", sa.String(length=64), nullable=False),
        sa.Column("auth_source", sa.String(length=64), nullable=False),
        sa.Column("object_type", sa.String(length=32), nullable=False),
        sa.Column("object_id", sa.String(length=128), nullable=False),
        sa.Column("access_type", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("phase", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("data_returned", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("request_context", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_break_glass_access_audits_access_id",
        "break_glass_access_audits",
        ["access_id"],
    )
    op.create_index(
        "ix_break_glass_access_audits_request_id",
        "break_glass_access_audits",
        ["request_id"],
    )
    op.create_index(
        "ix_break_glass_access_audits_operator_subject",
        "break_glass_access_audits",
        ["operator_subject"],
    )
    op.create_index(
        "ix_break_glass_access_audits_object_created",
        "break_glass_access_audits",
        ["object_type", "object_id", "created_at"],
    )
    op.create_index(
        "ix_break_glass_access_audits_access_created",
        "break_glass_access_audits",
        ["access_type", "created_at"],
    )

    # AI execution 只保存运行元数据；不设计 prompt、response 或原始文本列。
    op.create_table(
        "ai_execution_records",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("trace_id", sa.String(length=128), nullable=False),
        sa.Column("message_id", sa.String(length=128)),
        sa.Column("lead_id", sa.String(length=36)),
        sa.Column("operation", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=128), nullable=False),
        sa.Column("model", sa.String(length=128)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("call_count", sa.Integer()),
        sa.Column("input_tokens", sa.Integer()),
        sa.Column("output_tokens", sa.Integer()),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("error_type", sa.String(length=128)),
        sa.Column("error_summary", sa.String(length=256)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("trace_id", name="uq_ai_execution_records_trace_id"),
    )
    op.create_index("ix_ai_execution_records_message_id", "ai_execution_records", ["message_id"])
    op.create_index("ix_ai_execution_records_lead_id", "ai_execution_records", ["lead_id"])
    op.create_index(
        "ix_ai_execution_records_status_created",
        "ai_execution_records",
        ["status", "created_at"],
    )


def downgrade() -> None:
    """删除 T15 新增表及索引，显式回退会丢失 T15 观测数据。"""
    op.drop_index("ix_ai_execution_records_status_created", table_name="ai_execution_records")
    op.drop_index("ix_ai_execution_records_lead_id", table_name="ai_execution_records")
    op.drop_index("ix_ai_execution_records_message_id", table_name="ai_execution_records")
    op.drop_table("ai_execution_records")
    op.drop_index(
        "ix_break_glass_access_audits_access_created",
        table_name="break_glass_access_audits",
    )
    op.drop_index(
        "ix_break_glass_access_audits_object_created",
        table_name="break_glass_access_audits",
    )
    op.drop_index(
        "ix_break_glass_access_audits_operator_subject",
        table_name="break_glass_access_audits",
    )
    op.drop_index(
        "ix_break_glass_access_audits_request_id",
        table_name="break_glass_access_audits",
    )
    op.drop_index(
        "ix_break_glass_access_audits_access_id",
        table_name="break_glass_access_audits",
    )
    op.drop_table("break_glass_access_audits")
