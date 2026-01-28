import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import Uuid


class Base(DeclarativeBase):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    full_name: Mapped[Optional[str]] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utc_now, onupdate=utc_now
    )

    exchange_accounts: Mapped[List["UserExchangeAccount"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    strategies: Mapped[List["Strategy"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    risk_events: Mapped[List["RiskEvent"]] = relationship(back_populates="user")


class Exchange(Base):
    __tablename__ = "exchanges"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    slug: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utc_now, onupdate=utc_now
    )

    accounts: Mapped[List["UserExchangeAccount"]] = relationship(
        back_populates="exchange", cascade="all, delete-orphan"
    )
    candles: Mapped[List["Candle"]] = relationship(back_populates="exchange")


class UserExchangeAccount(Base):
    __tablename__ = "user_exchange_accounts"
    __table_args__ = (
        UniqueConstraint("user_id", "exchange_id", "label", name="uq_user_exchange_label"),
        Index("ix_user_exchange_accounts_user_id", "user_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id", ondelete="CASCADE"))
    label: Mapped[str] = mapped_column(String(100))
    api_key: Mapped[Optional[str]] = mapped_column(Text)
    api_secret: Mapped[Optional[str]] = mapped_column(Text)
    api_passphrase: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utc_now, onupdate=utc_now
    )

    user: Mapped["User"] = relationship(back_populates="exchange_accounts")
    exchange: Mapped["Exchange"] = relationship(back_populates="accounts")
    trades: Mapped[List["Trade"]] = relationship(back_populates="account")
    positions: Mapped[List["Position"]] = relationship(back_populates="account")

    __mapper_args__ = {"eager_defaults": True}


class Strategy(Base):
    __tablename__ = "strategies"
    __table_args__ = (Index("ix_strategies_user_id", "user_id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(150))
    description: Mapped[Optional[str]] = mapped_column(Text)
    params: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=utc_now, onupdate=utc_now
    )

    user: Mapped["User"] = relationship(back_populates="strategies")
    runs: Mapped[List["StrategyRun"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    trades: Mapped[List["Trade"]] = relationship(back_populates="strategy")
    positions: Mapped[List["Position"]] = relationship(back_populates="strategy")
    llm_analyses: Mapped[List["LLMAnalysis"]] = relationship(back_populates="strategy")


class StrategyRun(Base):
    __tablename__ = "strategy_runs"
    __table_args__ = (
        Index("ix_strategy_runs_strategy_id_started_at", "strategy_id", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("strategies.id", ondelete="CASCADE"))
    started_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    finished_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    status: Mapped[str] = mapped_column(String(50), default="running")
    metrics: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    strategy: Mapped["Strategy"] = relationship(back_populates="runs")
    trades: Mapped[List["Trade"]] = relationship(back_populates="strategy_run")


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        Index("ix_trades_user_id", "user_id"),
        Index("ix_trades_strategy_id", "strategy_id"),
        Index("ix_trades_symbol_executed_at", "symbol", "executed_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("user_exchange_accounts.id", ondelete="SET NULL"), nullable=True
    )
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True
    )
    strategy_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="SET NULL"), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(50), index=True)
    side: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    price: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    executed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), index=True)
    context: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)

    user: Mapped["User"] = relationship()
    account: Mapped[Optional["UserExchangeAccount"]] = relationship(back_populates="trades")
    strategy: Mapped[Optional["Strategy"]] = relationship(back_populates="trades")
    strategy_run: Mapped[Optional["StrategyRun"]] = relationship(back_populates="trades")
    position: Mapped[Optional["Position"]] = relationship(
        back_populates="trade", cascade="all, delete-orphan", uselist=False, single_parent=True
    )


class Position(Base):
    __tablename__ = "positions"
    __table_args__ = (
        Index("ix_positions_user_id", "user_id"),
        Index("ix_positions_symbol", "symbol"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    account_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("user_exchange_accounts.id", ondelete="SET NULL"), nullable=True
    )
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True
    )
    trade_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("trades.id", ondelete="SET NULL"), nullable=True, unique=True
    )
    symbol: Mapped[str] = mapped_column(String(50))
    size: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    stop_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    take_profit: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    is_open: Mapped[bool] = mapped_column(Boolean, default=True)
    opened_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)
    closed_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))

    user: Mapped["User"] = relationship()
    account: Mapped[Optional["UserExchangeAccount"]] = relationship(back_populates="positions")
    strategy: Mapped[Optional["Strategy"]] = relationship(back_populates="positions")
    trade: Mapped[Optional["Trade"]] = relationship(back_populates="position")


class Candle(Base):
    __tablename__ = "candles"
    __table_args__ = (
        Index(
            "ix_candles_exchange_symbol_timeframe_open_time",
            "exchange_id",
            "symbol",
            "timeframe",
            "open_time",
        ),
        UniqueConstraint(
            "exchange_id",
            "symbol",
            "timeframe",
            "open_time",
            name="uq_candles_exchange_symbol_timeframe_open_time",
        ),
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id", ondelete="CASCADE"))
    symbol: Mapped[str] = mapped_column(String(50))
    timeframe: Mapped[str] = mapped_column(String(20))
    open_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    close_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    open: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    high: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    low: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    close: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    volume: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    exchange: Mapped["Exchange"] = relationship(back_populates="candles")


class LLMAnalysis(Base):
    __tablename__ = "llm_analyses"
    __table_args__ = (
        Index("ix_llm_analyses_strategy_id", "strategy_id"),
        Index("ix_llm_analyses_symbol", "symbol"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True
    )
    symbol: Mapped[str] = mapped_column(String(50), nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    raw_output: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    strategy: Mapped[Optional["Strategy"]] = relationship(back_populates="llm_analyses")


class RiskEvent(Base):
    __tablename__ = "risk_events"
    __table_args__ = (
        Index("ix_risk_events_user_id", "user_id"),
        Index("ix_risk_events_strategy_id", "strategy_id"),
        Index("ix_risk_events_symbol", "symbol"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True
    )
    event_type: Mapped[str] = mapped_column(String(100))
    severity: Mapped[str] = mapped_column(String(50))
    symbol: Mapped[Optional[str]] = mapped_column(String(50))
    details: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    user: Mapped["User"] = relationship(back_populates="risk_events")
    strategy: Mapped[Optional["Strategy"]] = relationship()


# Paper trading models
class PaperAccount(Base):
    __tablename__ = "paper_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    base_ccy: Mapped[str] = mapped_column(String(10))
    initial_cash_usd: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=Decimal("10000"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    balances: Mapped[List["PaperBalance"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    positions: Mapped[List["PaperPosition"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    orders: Mapped[List["PaperOrder"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    trades: Mapped[List["PaperTrade"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    snapshots: Mapped[List["EquitySnapshot"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )


class PaperBalance(Base):
    __tablename__ = "paper_balances"
    __table_args__ = (
        UniqueConstraint("account_id", "ccy", name="uq_paper_balances_account_ccy"),
        Index("ix_paper_balances_account_id", "account_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"))
    ccy: Mapped[str] = mapped_column(String(10))
    available: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal("0"))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    account: Mapped["PaperAccount"] = relationship(back_populates="balances")


class PaperPosition(Base):
    __tablename__ = "paper_positions"
    __table_args__ = (
        UniqueConstraint("account_id", "symbol", name="uq_paper_positions_account_symbol"),
        Index("ix_paper_positions_account_id", "account_id"),
        Index("ix_paper_positions_symbol", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"))
    symbol: Mapped[str] = mapped_column(String(50))
    side: Mapped[str] = mapped_column(String(5))  # long/short
    qty: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal("0"))
    avg_entry_price: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal("0"))
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal("0"))
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    account: Mapped["PaperAccount"] = relationship(back_populates="positions")


class PaperOrder(Base):
    __tablename__ = "paper_orders"
    __table_args__ = (
        Index("ix_paper_orders_account_id", "account_id"),
        Index("ix_paper_orders_symbol_created_at", "symbol", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"))
    symbol: Mapped[str] = mapped_column(String(50))
    side: Mapped[str] = mapped_column(String(5))  # buy/sell
    type: Mapped[str] = mapped_column(String(20))  # market
    status: Mapped[str] = mapped_column(String(20))  # new/filled/canceled/rejected
    requested_qty: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    filled_qty: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal("0"))
    avg_fill_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    fee_paid: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    account: Mapped["PaperAccount"] = relationship(back_populates="orders")


class PaperTrade(Base):
    __tablename__ = "paper_trades"
    __table_args__ = (
        Index("ix_paper_trades_account_id", "account_id"),
        Index("ix_paper_trades_symbol_created_at", "symbol", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"))
    symbol: Mapped[str] = mapped_column(String(50))
    side: Mapped[str] = mapped_column(String(5))
    qty: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    price: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    realized_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    account: Mapped["PaperAccount"] = relationship(back_populates="trades")


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"
    __table_args__ = (
        Index("ix_equity_snapshots_account_id_created_at", "account_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"))
    equity: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    balance: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(24, 10))
    equity_usdt: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10), nullable=True)
    cash_usdt: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10), nullable=True)
    positions_notional_usdt: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 10), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    meta: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=utc_now)

    account: Mapped["PaperAccount"] = relationship(back_populates="snapshots")


class RiskPolicy(Base):
    __tablename__ = "risk_policies"
    __table_args__ = (Index("ix_risk_policies_is_active", "is_active"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    max_loss_per_trade_usd: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    max_loss_per_day_usd: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    max_position_usd: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    max_open_positions: Mapped[int] = mapped_column(Integer)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, default=0)
    fee_bps: Mapped[Decimal] = mapped_column(Numeric(10, 4), default=Decimal("0"))
    slippage_bps: Mapped[Decimal] = mapped_column(Numeric(10, 4), default=Decimal("0"))
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now, onupdate=utc_now
    )

    strategies: Mapped[List["StrategyConfig"]] = relationship(
        back_populates="risk_policy", cascade="all, delete-orphan"
    )


class StrategyConfig(Base):
    __tablename__ = "strategy_configs"
    __table_args__ = (
        Index("ix_strategy_configs_is_active", "is_active"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbols: Mapped[List[str]] = mapped_column(JSON, default=list)
    timeframe: Mapped[str] = mapped_column(String(50))
    thresholds: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    order_type: Mapped[str] = mapped_column(String(20), default="market")
    allow_short: Mapped[bool] = mapped_column(Boolean, default=False)
    min_notional_usd: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=Decimal("0"))
    risk_policy_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("risk_policies.id", ondelete="SET NULL"), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now, onupdate=utc_now
    )

    risk_policy: Mapped[Optional["RiskPolicy"]] = relationship(back_populates="strategies")


class AdminAction(Base):
    __tablename__ = "admin_actions"
    __table_args__ = (
        Index("ix_admin_actions_created_at", "created_at"),
        Index("ix_admin_actions_dedupe_key", "dedupe_key"),
        Index("ix_admin_actions_dedupe_key_created_at", "dedupe_key", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dedupe_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    action: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(64))
    message: Mapped[Optional[str]] = mapped_column(Text)
    meta: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now
    )


class NotificationOutbox(Base):
    __tablename__ = "notifications_outbox"
    __table_args__ = (
        UniqueConstraint("admin_action_id", name="uq_notifications_outbox_admin_action_id"),
        Index("ix_notifications_outbox_status_next_attempt", "status", "next_attempt_at"),
        Index("ix_notifications_outbox_dedupe_key", "dedupe_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now, onupdate=utc_now
    )
    status: Mapped[str] = mapped_column(String(32), default="pending", server_default=text("'pending'"))
    channel: Mapped[str] = mapped_column(String(32), default="noop", server_default=text("'noop'"))
    admin_action_id: Mapped[int] = mapped_column(ForeignKey("admin_actions.id", ondelete="CASCADE"))
    dedupe_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP"), default=utc_now
    )
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=text("'{}'::jsonb"))
