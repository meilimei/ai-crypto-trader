"""add strategy policy override tables

Revision ID: 0014_add_strategy_policy_overrides
Revises: 0013_add_reconcile_log_window_seconds
Create Date: 2025-12-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0014_add_strategy_policy_overrides"
down_revision = "0013_add_reconcile_log_window_seconds"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_risk_policy_overrides",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("max_order_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ux_paper_risk_policy_overrides_account_strategy",
        "paper_risk_policy_overrides",
        ["account_id", "strategy_id"],
        unique=True,
    )

    op.create_table(
        "paper_position_policy_overrides",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("min_qty", sa.Numeric(24, 10), nullable=True),
        sa.Column("min_order_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_position_notional_per_symbol_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_total_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ux_paper_position_policy_overrides_account_strategy",
        "paper_position_policy_overrides",
        ["account_id", "strategy_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ux_paper_position_policy_overrides_account_strategy",
        table_name="paper_position_policy_overrides",
    )
    op.drop_table("paper_position_policy_overrides")
    op.drop_index(
        "ux_paper_risk_policy_overrides_account_strategy",
        table_name="paper_risk_policy_overrides",
    )
    op.drop_table("paper_risk_policy_overrides")
