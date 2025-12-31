"""add risk and strategy config tables

Revision ID: 0005_add_risk_and_strategy_configs
Revises: 0004_ensure_admin_actions_table
Create Date: 2025-12-23
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0005_add_risk_and_strategy_configs"
down_revision = "0004_ensure_admin_actions_table"
branch_labels = None
depends_on = None


def _ensure_risk_policies(bind) -> None:
    insp = sa.inspect(bind)
    if not insp.has_table("risk_policies"):
        op.create_table(
            "risk_policies",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("max_loss_per_trade_usd", sa.Numeric(18, 6), nullable=False),
            sa.Column("max_loss_per_day_usd", sa.Numeric(18, 6), nullable=False),
            sa.Column("max_position_usd", sa.Numeric(18, 6), nullable=False),
            sa.Column("max_open_positions", sa.Integer(), nullable=False),
            sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("fee_bps", sa.Numeric(10, 4), nullable=False, server_default="0"),
            sa.Column("slippage_bps", sa.Numeric(10, 4), nullable=False, server_default="0"),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column(
                "created_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                server_onupdate=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
        )
        op.create_index("ix_risk_policies_is_active", "risk_policies", ["is_active"])
    else:
        existing_indexes = {ix["name"] for ix in insp.get_indexes("risk_policies")}
        if "ix_risk_policies_is_active" not in existing_indexes:
            op.create_index("ix_risk_policies_is_active", "risk_policies", ["is_active"])


def _ensure_strategy_configs(bind) -> None:
    insp = sa.inspect(bind)
    if not insp.has_table("strategy_configs"):
        op.create_table(
            "strategy_configs",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("symbols", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
            sa.Column("timeframe", sa.String(length=50), nullable=False),
            sa.Column("thresholds", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("order_type", sa.String(length=20), nullable=False, server_default="market"),
            sa.Column("allow_short", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("min_notional_usd", sa.Numeric(18, 6), nullable=False, server_default="0"),
            sa.Column("risk_policy_id", sa.Integer(), sa.ForeignKey("risk_policies.id", ondelete="SET NULL"), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column(
                "created_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                server_onupdate=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
        )
        op.create_index("ix_strategy_configs_is_active", "strategy_configs", ["is_active"])
    else:
        existing_indexes = {ix["name"] for ix in insp.get_indexes("strategy_configs")}
        if "ix_strategy_configs_is_active" not in existing_indexes:
            op.create_index("ix_strategy_configs_is_active", "strategy_configs", ["is_active"])


def upgrade() -> None:
    bind = op.get_bind()
    _ensure_risk_policies(bind)
    _ensure_strategy_configs(bind)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("strategy_configs"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("strategy_configs")}
        if "ix_strategy_configs_is_active" in existing_indexes:
            op.drop_index("ix_strategy_configs_is_active", table_name="strategy_configs")
        op.drop_table("strategy_configs")
    if insp.has_table("risk_policies"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("risk_policies")}
        if "ix_risk_policies_is_active" in existing_indexes:
            op.drop_index("ix_risk_policies_is_active", table_name="risk_policies")
        op.drop_table("risk_policies")
