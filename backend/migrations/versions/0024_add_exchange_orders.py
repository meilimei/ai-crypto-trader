"""add exchange_orders table for live exchange pipeline

Revision ID: 0024_add_exchange_orders
Revises: 0023_add_autopilot_states
Create Date: 2026-02-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0024_add_exchange_orders"
down_revision = "0023_add_autopilot_states"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.create_table(
        "exchange_orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("strategy_config_id", sa.Integer(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("order_type", sa.Text(), nullable=False, server_default=sa.text("'market'")),
        sa.Column("qty", sa.Numeric(24, 10), nullable=False),
        sa.Column("price", sa.Numeric(24, 10), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("client_order_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("exchange_order_id", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_attempt_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("exchange", "idempotency_key", name="uq_exchange_orders_exchange_idempotency"),
        sa.UniqueConstraint("exchange", "client_order_id", name="uq_exchange_orders_exchange_client_order"),
    )
    op.create_index(
        "ix_exchange_orders_exchange_status_next_attempt",
        "exchange_orders",
        ["exchange", "status", "next_attempt_at"],
    )
    op.create_index(
        "ix_exchange_orders_account_created_at",
        "exchange_orders",
        ["account_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_exchange_orders_account_created_at", table_name="exchange_orders")
    op.drop_index("ix_exchange_orders_exchange_status_next_attempt", table_name="exchange_orders")
    op.drop_table("exchange_orders")
