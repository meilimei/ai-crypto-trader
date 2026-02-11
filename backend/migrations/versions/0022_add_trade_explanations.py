"""add trade_explanations table for explainability

Revision ID: 0022_add_trade_explanations
Revises: 0021_add_policy_v2_tables
Create Date: 2026-02-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0022_add_trade_explanations"
down_revision = "0021_add_policy_v2_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trade_explanations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("strategy_config_id", sa.BigInteger(), nullable=True),
        sa.Column("strategy_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("requested_qty", sa.Numeric(24, 10), nullable=True),
        sa.Column("executed_trade_id", sa.BigInteger(), nullable=True),
        sa.Column("order_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "decision",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("rationale", sa.Text(), nullable=True),
        sa.Column(
            "outcome",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'open'")),
    )
    op.create_index(
        "ix_trade_explanations_created_at",
        "trade_explanations",
        ["created_at"],
    )
    op.create_index(
        "ix_trade_explanations_account_id_created_at",
        "trade_explanations",
        ["account_id", "created_at"],
    )
    op.create_index(
        "ix_trade_explanations_strategy_config_id_created_at",
        "trade_explanations",
        ["strategy_config_id", "created_at"],
    )
    op.create_index(
        "ix_trade_explanations_symbol_created_at",
        "trade_explanations",
        ["symbol", "created_at"],
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_trade_explanations_executed_trade_id "
        "ON public.trade_explanations (executed_trade_id) "
        "WHERE executed_trade_id IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.ux_trade_explanations_executed_trade_id")
    op.drop_index("ix_trade_explanations_symbol_created_at", table_name="trade_explanations")
    op.drop_index("ix_trade_explanations_strategy_config_id_created_at", table_name="trade_explanations")
    op.drop_index("ix_trade_explanations_account_id_created_at", table_name="trade_explanations")
    op.drop_index("ix_trade_explanations_created_at", table_name="trade_explanations")
    op.drop_table("trade_explanations")
