"""add autopilot_states table for live autopilot governor

Revision ID: 0023_add_autopilot_states
Revises: 0022_add_trade_explanations
Create Date: 2026-02-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0023_add_autopilot_states"
down_revision = "0022_add_trade_explanations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "autopilot_states",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "strategy_config_id",
            sa.BigInteger(),
            sa.ForeignKey("strategy_configs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'running'")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "meta",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("paused_until", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint("account_id", "strategy_config_id", name="ux_autopilot_states_account_strategy"),
    )
    op.create_index("ix_autopilot_states_account_id", "autopilot_states", ["account_id"])
    op.create_index("ix_autopilot_states_strategy_config_id", "autopilot_states", ["strategy_config_id"])
    op.create_index("ix_autopilot_states_status", "autopilot_states", ["status"])


def downgrade() -> None:
    op.drop_index("ix_autopilot_states_status", table_name="autopilot_states")
    op.drop_index("ix_autopilot_states_strategy_config_id", table_name="autopilot_states")
    op.drop_index("ix_autopilot_states_account_id", table_name="autopilot_states")
    op.drop_table("autopilot_states")
