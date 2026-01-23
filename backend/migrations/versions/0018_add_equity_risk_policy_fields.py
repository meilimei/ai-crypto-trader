"""add equity risk policy fields

Revision ID: 0018_add_equity_risk_policy_fields
Revises: 0017_add_equity_snapshot_meta_source
Create Date: 2026-01-23
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0018_add_equity_risk_policy_fields"
down_revision = "0017_add_equity_snapshot_meta_source"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE paper_risk_policies ADD COLUMN IF NOT EXISTS max_daily_loss_usdt numeric")
    op.execute("ALTER TABLE paper_risk_policies ADD COLUMN IF NOT EXISTS max_drawdown_usdt numeric")
    op.execute("ALTER TABLE paper_risk_policies ADD COLUMN IF NOT EXISTS equity_lookback_hours integer")
    op.execute("ALTER TABLE paper_risk_policy_overrides ADD COLUMN IF NOT EXISTS max_daily_loss_usdt numeric")
    op.execute("ALTER TABLE paper_risk_policy_overrides ADD COLUMN IF NOT EXISTS max_drawdown_usdt numeric")
    op.execute("ALTER TABLE paper_risk_policy_overrides ADD COLUMN IF NOT EXISTS equity_lookback_hours integer")


def downgrade() -> None:
    op.execute("ALTER TABLE paper_risk_policy_overrides DROP COLUMN IF EXISTS equity_lookback_hours")
    op.execute("ALTER TABLE paper_risk_policy_overrides DROP COLUMN IF EXISTS max_drawdown_usdt")
    op.execute("ALTER TABLE paper_risk_policy_overrides DROP COLUMN IF EXISTS max_daily_loss_usdt")
    op.execute("ALTER TABLE paper_risk_policies DROP COLUMN IF EXISTS equity_lookback_hours")
    op.execute("ALTER TABLE paper_risk_policies DROP COLUMN IF EXISTS max_drawdown_usdt")
    op.execute("ALTER TABLE paper_risk_policies DROP COLUMN IF EXISTS max_daily_loss_usdt")
