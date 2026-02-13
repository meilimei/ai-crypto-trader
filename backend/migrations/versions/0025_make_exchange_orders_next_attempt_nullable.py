"""make exchange_orders.next_attempt_at nullable for terminal rejections

Revision ID: 0025_make_exchange_orders_next_attempt_nullable
Revises: 0024_add_exchange_orders
Create Date: 2026-02-13
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0025_make_exchange_orders_next_attempt_nullable"
down_revision = "0024_add_exchange_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("exchange_orders", "next_attempt_at", existing_type=sa.TIMESTAMP(timezone=True), nullable=True)


def downgrade() -> None:
    op.execute(
        "UPDATE public.exchange_orders "
        "SET next_attempt_at = COALESCE(next_attempt_at, CURRENT_TIMESTAMP) "
        "WHERE next_attempt_at IS NULL"
    )
    op.alter_column("exchange_orders", "next_attempt_at", existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
