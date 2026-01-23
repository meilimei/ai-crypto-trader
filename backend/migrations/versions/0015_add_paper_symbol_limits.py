"""add paper symbol limits table

Revision ID: 0015_add_paper_symbol_limits
Revises: 0014_add_strategy_policy_overrides
Create Date: 2025-12-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0015_add_paper_symbol_limits"
down_revision = "0014_add_strategy_policy_overrides"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_symbol_limits",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("max_order_qty", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_position_qty", sa.Numeric(24, 10), nullable=True),
        sa.Column("max_position_notional_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ux_paper_symbol_limits_account_strategy_symbol",
        "paper_symbol_limits",
        ["account_id", "strategy_id", "symbol"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_paper_symbol_limits_account_strategy_symbol", table_name="paper_symbol_limits")
    op.drop_table("paper_symbol_limits")
