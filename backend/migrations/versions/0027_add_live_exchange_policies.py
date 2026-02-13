"""add live_exchange_policies table

Revision ID: 0027_add_live_exchange_policies
Revises: 0026_exchange_orders_idempotency_scope_account
Create Date: 2026-02-13
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0027_add_live_exchange_policies"
down_revision = "0026_exchange_orders_idempotency_scope_account"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_exchange_policies",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=True),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("8")),
        sa.Column("base_backoff_seconds", sa.Integer(), nullable=False, server_default=sa.text("5")),
        sa.Column("max_backoff_seconds", sa.Integer(), nullable=False, server_default=sa.text("300")),
        sa.Column("jitter_seconds", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("rate_limit_min_interval_ms", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("pause_on_consecutive_failures", sa.Integer(), nullable=False, server_default=sa.text("5")),
        sa.Column("pause_window_seconds", sa.Integer(), nullable=False, server_default=sa.text("300")),
        sa.Column("pause_on_failures_in_window", sa.Integer(), nullable=False, server_default=sa.text("8")),
        sa.Column("recv_window_ms", sa.Integer(), nullable=False, server_default=sa.text("5000")),
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
        sa.ForeignKeyConstraint(["account_id"], ["paper_accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("exchange", "account_id", name="uq_live_exchange_policies_exchange_account"),
    )
    op.create_index("ix_live_exchange_policies_exchange", "live_exchange_policies", ["exchange"])
    op.create_index("ix_live_exchange_policies_updated_at", "live_exchange_policies", ["updated_at"])
    op.execute(
        "CREATE UNIQUE INDEX ux_live_exchange_policies_exchange_default "
        "ON public.live_exchange_policies (exchange) "
        "WHERE account_id IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.ux_live_exchange_policies_exchange_default")
    op.drop_index("ix_live_exchange_policies_updated_at", table_name="live_exchange_policies")
    op.drop_index("ix_live_exchange_policies_exchange", table_name="live_exchange_policies")
    op.drop_table("live_exchange_policies")
