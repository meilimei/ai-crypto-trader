"""add paper risk and position policy tables

Revision ID: 0009_add_paper_policy_tables
Revises: 0008_add_admin_actions_dedupe_key
Create Date: 2025-12-26
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0009_add_paper_policy_tables"
down_revision = "0008_add_admin_actions_dedupe_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_risk_policies",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("max_drawdown_pct", sa.Numeric(18, 6), nullable=True),
        sa.Column("max_daily_loss_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("min_equity_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_order_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index(
        "ix_paper_risk_policies_account_id",
        "paper_risk_policies",
        ["account_id"],
        unique=True,
    )

    op.create_table(
        "paper_position_policies",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("min_qty", sa.Numeric(24, 10), nullable=True),
        sa.Column("min_order_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_position_notional_per_symbol_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_total_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index(
        "ix_paper_position_policies_account_id",
        "paper_position_policies",
        ["account_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_paper_position_policies_account_id", table_name="paper_position_policies")
    op.drop_table("paper_position_policies")
    op.drop_index("ix_paper_risk_policies_account_id", table_name="paper_risk_policies")
    op.drop_table("paper_risk_policies")
