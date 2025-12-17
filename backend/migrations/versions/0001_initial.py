"""Initial schema for AI Crypto Trader.

Run migrations with (example for SQLite dev):
- export DATABASE_URL=sqlite+aiosqlite:///./dev.db
- poetry run alembic upgrade head

Switch DATABASE_URL to PostgreSQL when ready for production.
"""

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    op.create_table(
        "exchanges",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("slug", sa.String(length=100), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sqlite_autoincrement=True,
    )
    op.create_index("ix_exchanges_name", "exchanges", ["name"], unique=True)
    op.create_index("ix_exchanges_slug", "exchanges", ["slug"], unique=True)

    op.create_table(
        "strategies",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Uuid(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(length=150), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("params", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_strategies_user_id", "strategies", ["user_id"])

    op.create_table(
        "user_exchange_accounts",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Uuid(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False),
        sa.Column("label", sa.String(length=100), nullable=False),
        sa.Column("api_key", sa.Text(), nullable=True),
        sa.Column("api_secret", sa.Text(), nullable=True),
        sa.Column("api_passphrase", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("config", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint("user_id", "exchange_id", "label", name="uq_user_exchange_label"),
    )
    op.create_index("ix_user_exchange_accounts_user_id", "user_exchange_accounts", ["user_id"])

    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False, server_default=sa.text("'running'")),
        sa.Column("metrics", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
    )
    op.create_index("ix_strategy_runs_strategy_id_started_at", "strategy_runs", ["strategy_id", "started_at"])

    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("exchange_id", sa.Integer(), sa.ForeignKey("exchanges.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("timeframe", sa.String(length=20), nullable=False),
        sa.Column("open_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("close_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(24, 10), nullable=False),
        sa.Column("high", sa.Numeric(24, 10), nullable=False),
        sa.Column("low", sa.Numeric(24, 10), nullable=False),
        sa.Column("close", sa.Numeric(24, 10), nullable=False),
        sa.Column("volume", sa.Numeric(24, 10), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint("exchange_id", "symbol", "timeframe", "open_time", name="uq_candles_exchange_symbol_timeframe_open_time"),
        sqlite_autoincrement=True,
    )
    op.create_index(
        "ix_candles_exchange_symbol_timeframe_open_time",
        "candles",
        ["exchange_id", "symbol", "timeframe", "open_time"],
    )

    op.create_table(
        "trades",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Uuid(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("account_id", sa.Uuid(as_uuid=True), sa.ForeignKey("user_exchange_accounts.id", ondelete="SET NULL"), nullable=True),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True),
        sa.Column("strategy_run_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategy_runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("side", sa.String(length=10), nullable=False),
        sa.Column("quantity", sa.Numeric(24, 10), nullable=False),
        sa.Column("price", sa.Numeric(24, 10), nullable=False),
        sa.Column("fee", sa.Numeric(24, 10), nullable=True),
        sa.Column("executed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("context", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
    )
    op.create_index("ix_trades_user_id", "trades", ["user_id"])
    op.create_index("ix_trades_strategy_id", "trades", ["strategy_id"])
    op.create_index("ix_trades_symbol_executed_at", "trades", ["symbol", "executed_at"])

    op.create_table(
        "positions",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Uuid(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("account_id", sa.Uuid(as_uuid=True), sa.ForeignKey("user_exchange_accounts.id", ondelete="SET NULL"), nullable=True),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True),
        sa.Column("trade_id", sa.Uuid(as_uuid=True), sa.ForeignKey("trades.id", ondelete="SET NULL"), nullable=True),
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("size", sa.Numeric(24, 10), nullable=False),
        sa.Column("entry_price", sa.Numeric(24, 10), nullable=False),
        sa.Column("stop_loss", sa.Numeric(24, 10), nullable=True),
        sa.Column("take_profit", sa.Numeric(24, 10), nullable=True),
        sa.Column("is_open", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("opened_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("closed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.UniqueConstraint("trade_id", name="uq_positions_trade_id"),
    )
    op.create_index("ix_positions_user_id", "positions", ["user_id"])
    op.create_index("ix_positions_symbol", "positions", ["symbol"])

    op.create_table(
        "llm_analyses",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True),
        sa.Column("symbol", sa.String(length=50), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("raw_output", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_llm_analyses_strategy_id", "llm_analyses", ["strategy_id"])
    op.create_index("ix_llm_analyses_symbol", "llm_analyses", ["symbol"])

    op.create_table(
        "risk_events",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("user_id", sa.Uuid(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("strategy_id", sa.Uuid(as_uuid=True), sa.ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("severity", sa.String(length=50), nullable=False),
        sa.Column("symbol", sa.String(length=50), nullable=True),
        sa.Column("details", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_risk_events_user_id", "risk_events", ["user_id"])
    op.create_index("ix_risk_events_strategy_id", "risk_events", ["strategy_id"])
    op.create_index("ix_risk_events_symbol", "risk_events", ["symbol"])


def downgrade() -> None:
    op.drop_index("ix_risk_events_symbol", table_name="risk_events")
    op.drop_index("ix_risk_events_strategy_id", table_name="risk_events")
    op.drop_index("ix_risk_events_user_id", table_name="risk_events")
    op.drop_table("risk_events")

    op.drop_index("ix_llm_analyses_symbol", table_name="llm_analyses")
    op.drop_index("ix_llm_analyses_strategy_id", table_name="llm_analyses")
    op.drop_table("llm_analyses")

    op.drop_index("ix_positions_symbol", table_name="positions")
    op.drop_index("ix_positions_user_id", table_name="positions")
    op.drop_table("positions")

    op.drop_index("ix_trades_symbol_executed_at", table_name="trades")
    op.drop_index("ix_trades_strategy_id", table_name="trades")
    op.drop_index("ix_trades_user_id", table_name="trades")
    op.drop_table("trades")

    op.drop_index("ix_candles_exchange_symbol_timeframe_open_time", table_name="candles")
    op.drop_table("candles")

    op.drop_index("ix_strategy_runs_strategy_id_started_at", table_name="strategy_runs")
    op.drop_table("strategy_runs")

    op.drop_index("ix_user_exchange_accounts_user_id", table_name="user_exchange_accounts")
    op.drop_table("user_exchange_accounts")

    op.drop_index("ix_strategies_user_id", table_name="strategies")
    op.drop_table("strategies")

    op.drop_index("ix_exchanges_slug", table_name="exchanges")
    op.drop_index("ix_exchanges_name", table_name="exchanges")
    op.drop_table("exchanges")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
