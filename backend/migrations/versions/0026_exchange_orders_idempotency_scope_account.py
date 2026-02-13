"""scope exchange order idempotency by account

Revision ID: 0026_exchange_orders_idempotency_scope_account
Revises: 0025_make_exchange_orders_next_attempt_nullable
Create Date: 2026-02-13
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0026_exchange_orders_idempotency_scope_account"
down_revision = "0025_make_exchange_orders_next_attempt_nullable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_exchange_orders_exchange_idempotency",
        "exchange_orders",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_exchange_orders_exchange_account_idempotency",
        "exchange_orders",
        ["exchange", "account_id", "idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_exchange_orders_exchange_account_idempotency",
        "exchange_orders",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_exchange_orders_exchange_idempotency",
        "exchange_orders",
        ["exchange", "idempotency_key"],
    )
