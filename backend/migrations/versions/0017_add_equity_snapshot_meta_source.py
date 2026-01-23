"""add equity snapshot standard columns

Revision ID: 0017_add_equity_snapshot_meta_source
Revises: 0016_add_order_rate_limits
Create Date: 2026-01-23
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0017_add_equity_snapshot_meta_source"
down_revision = "0016_add_order_rate_limits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS equity_usdt numeric")
    op.execute("ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS cash_usdt numeric")
    op.execute("ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS positions_notional_usdt numeric")
    op.execute("ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS source text")
    op.execute("ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS meta jsonb")
    op.execute(
        "ALTER TABLE equity_snapshots ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT CURRENT_TIMESTAMP"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_equity_snapshots_account_created_at "
        "ON equity_snapshots (account_id, created_at DESC)"
    )


def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS idx_equity_snapshots_account_created_at"
    )
    op.execute("ALTER TABLE equity_snapshots DROP COLUMN IF EXISTS meta")
    op.execute("ALTER TABLE equity_snapshots DROP COLUMN IF EXISTS source")
    op.execute("ALTER TABLE equity_snapshots DROP COLUMN IF EXISTS positions_notional_usdt")
    op.execute("ALTER TABLE equity_snapshots DROP COLUMN IF EXISTS cash_usdt")
    op.execute("ALTER TABLE equity_snapshots DROP COLUMN IF EXISTS equity_usdt")
