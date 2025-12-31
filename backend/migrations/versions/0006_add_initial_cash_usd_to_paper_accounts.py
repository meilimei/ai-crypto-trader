"""add initial_cash_usd to paper_accounts

Revision ID: 0006_add_initial_cash_usd_to_paper_accounts
Revises: 0005_add_risk_and_strategy_configs
Create Date: 2025-12-24
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0006_add_initial_cash_usd_to_paper_accounts"
down_revision = "0005_add_risk_and_strategy_configs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {col["name"] for col in insp.get_columns("paper_accounts")}
    if "initial_cash_usd" not in cols:
        op.add_column(
            "paper_accounts",
            sa.Column("initial_cash_usd", sa.Numeric(18, 6), nullable=True, server_default="10000"),
        )
    op.execute("UPDATE paper_accounts SET initial_cash_usd = COALESCE(initial_cash_usd, 10000)")
    op.alter_column("paper_accounts", "initial_cash_usd", nullable=False, server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {col["name"] for col in insp.get_columns("paper_accounts")}
    if "initial_cash_usd" in cols:
        op.drop_column("paper_accounts", "initial_cash_usd")
