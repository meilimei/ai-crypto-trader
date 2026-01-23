"""add order rate limits

Revision ID: 0016_add_order_rate_limits
Revises: 0015_add_paper_symbol_limits
Create Date: 2026-01-23
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0016_add_order_rate_limits"
down_revision = "0015_add_paper_symbol_limits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("paper_risk_policies", sa.Column("order_rate_limit_max", sa.Integer(), nullable=True))
    op.add_column(
        "paper_risk_policies", sa.Column("order_rate_limit_window_seconds", sa.Integer(), nullable=True)
    )
    op.add_column(
        "paper_risk_policy_overrides", sa.Column("order_rate_limit_max", sa.Integer(), nullable=True)
    )
    op.add_column(
        "paper_risk_policy_overrides",
        sa.Column("order_rate_limit_window_seconds", sa.Integer(), nullable=True),
    )

    op.create_table(
        "paper_order_request_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_paper_order_request_events_key_created_at",
        "paper_order_request_events",
        ["account_id", "strategy_id", "symbol", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_paper_order_request_events_key_created_at", table_name="paper_order_request_events"
    )
    op.drop_table("paper_order_request_events")
    op.drop_column("paper_risk_policy_overrides", "order_rate_limit_window_seconds")
    op.drop_column("paper_risk_policy_overrides", "order_rate_limit_max")
    op.drop_column("paper_risk_policies", "order_rate_limit_window_seconds")
    op.drop_column("paper_risk_policies", "order_rate_limit_max")
