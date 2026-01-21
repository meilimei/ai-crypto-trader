"""add paper reconcile policies table

Revision ID: 0012_add_paper_reconcile_policies
Revises: 0011_widen_admin_actions_status
Create Date: 2025-12-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0012_add_paper_reconcile_policies"
down_revision = "0011_widen_admin_actions_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "paper_reconcile_policies",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("paper_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "mode",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'strict'"),
        ),
        sa.Column("baseline_cash_usdt", sa.Numeric(24, 10), nullable=True),
        sa.Column("ok_usdt_diff", sa.Numeric(24, 10), nullable=True),
        sa.Column("alert_usdt_diff", sa.Numeric(24, 10), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_paper_reconcile_policies_account_id",
        "paper_reconcile_policies",
        ["account_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_paper_reconcile_policies_account_id", table_name="paper_reconcile_policies")
    op.drop_table("paper_reconcile_policies")
