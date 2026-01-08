"""set default initial_cash_usd for paper_accounts

Revision ID: 0007_set_default_initial_cash_usd
Revises: 0006_add_initial_cash_usd_to_paper_accounts
Create Date: 2025-12-25
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0007_set_default_initial_cash_usd"
down_revision = "0006_add_initial_cash_usd_to_paper_accounts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE paper_accounts SET initial_cash_usd = 10000.00 WHERE initial_cash_usd IS NULL")
    op.alter_column(
        "paper_accounts",
        "initial_cash_usd",
        existing_type=sa.Numeric(18, 6),
        server_default=sa.text("10000.00"),
    )
    op.alter_column(
        "paper_accounts",
        "initial_cash_usd",
        existing_type=sa.Numeric(18, 6),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "paper_accounts",
        "initial_cash_usd",
        existing_type=sa.Numeric(18, 6),
        nullable=True,
    )
    op.alter_column(
        "paper_accounts",
        "initial_cash_usd",
        existing_type=sa.Numeric(18, 6),
        server_default=None,
    )
