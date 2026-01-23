from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import PaperAccount, PaperBalance, PaperPosition
from ai_crypto_trader.services.paper_trader.accounting import MONEY_EXP, PRICE_EXP, QTY_EXP, normalize_symbol
from ai_crypto_trader.services.paper_trader.execution_costs import compute_execution
from ai_crypto_trader.services.paper_trader.rejects import RejectCode, RejectReason


def _quant(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp), rounding=ROUND_DOWN)


@dataclass
class RiskPolicy:
    max_drawdown_pct: Optional[Decimal] = None
    max_daily_loss: Optional[Decimal] = None
    min_equity: Optional[Decimal] = None
    max_daily_loss_usdt: Optional[Decimal] = None
    max_drawdown_usdt: Optional[Decimal] = None
    equity_lookback_hours: Optional[int] = None


@dataclass
class PositionPolicy:
    max_position_notional_per_symbol: Optional[Decimal] = None
    max_total_notional: Optional[Decimal] = None
    min_order_notional: Decimal = Decimal("0.01")
    min_qty: Decimal = Decimal("0.00000001")


DEFAULT_RISK_POLICY = RiskPolicy()
DEFAULT_POSITION_POLICY = PositionPolicy()


async def _available_balance(session: AsyncSession, account_id: int) -> Decimal:
    balance = await session.scalar(
        select(PaperBalance.available).where(PaperBalance.account_id == account_id, PaperBalance.ccy == "USDT").limit(1)
    )
    if balance is not None:
        return Decimal(str(balance))
    initial_cash = await session.scalar(select(PaperAccount.initial_cash_usd).where(PaperAccount.id == account_id).limit(1))
    return Decimal(str(initial_cash or "0"))


async def _current_notional(session: AsyncSession, account_id: int, symbol: str, mark_price: Decimal) -> Decimal:
    symbol_norm = normalize_symbol(symbol)
    pos_qty = await session.scalar(
        select(PaperPosition.qty)
        .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol_norm)
        .limit(1)
    )
    qty = Decimal(str(pos_qty)) if pos_qty is not None else Decimal("0")
    return _quant(abs(qty) * _quant(mark_price, PRICE_EXP), MONEY_EXP)


async def _total_notional(session: AsyncSession, account_id: int, mark_price_by_symbol: dict[str, Decimal]) -> Decimal:
    totals = Decimal("0")
    positions = await session.execute(select(PaperPosition.symbol, PaperPosition.qty).where(PaperPosition.account_id == account_id))
    for sym, qty in positions.all():
        price = mark_price_by_symbol.get(sym) or mark_price_by_symbol.get(normalize_symbol(sym))
        if price is None:
            continue
        totals += _quant(abs(Decimal(str(qty or 0))) * _quant(price, PRICE_EXP), MONEY_EXP)
    return _quant(totals, MONEY_EXP)


async def validate_order(
    session: AsyncSession,
    *,
    account_id: int,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
    risk_policy: RiskPolicy | None = None,
    position_policy: PositionPolicy | None = None,
) -> RejectReason | None:
    risk_policy = risk_policy or DEFAULT_RISK_POLICY
    position_policy = position_policy or DEFAULT_POSITION_POLICY

    try:
        qty_dec = _quant(Decimal(str(qty)), QTY_EXP)
    except Exception:
        return RejectReason(code=RejectCode.INVALID_QTY, reason="Quantity must be a valid decimal")
    if qty_dec <= 0:
        return RejectReason(code=RejectCode.QTY_ZERO, reason="Quantity must be positive")

    price_dec = _quant(Decimal(str(price)), PRICE_EXP)
    if not price_dec.is_finite() or price_dec <= 0:
        return RejectReason(code=RejectCode.INVALID_QTY, reason="Price must be positive")

    if position_policy.min_qty and qty_dec < _quant(position_policy.min_qty, QTY_EXP):
        return RejectReason(code=RejectCode.MIN_QTY, reason="Quantity below minimum", details={"min_qty": str(position_policy.min_qty)})

    exec_meta = compute_execution(price_dec, side, qty_dec, fee_bps, slippage_bps)
    notional = exec_meta["notional"]
    fee = exec_meta["fee"]

    if position_policy.min_order_notional and notional < _quant(position_policy.min_order_notional, MONEY_EXP):
        return RejectReason(
            code=RejectCode.MIN_ORDER_NOTIONAL,
            reason="Order notional below minimum",
            details={
                "min_order_notional_usdt": str(position_policy.min_order_notional),
                "notional": str(notional),
                "qty": str(qty_dec),
                "market_price": str(price_dec),
            },
        )

    available = await _available_balance(session, account_id)
    if side.lower().strip() == "buy":
        required = _quant(notional + fee, MONEY_EXP)
        if available + Decimal("0.0001") < required:
            return RejectReason(
                code=RejectCode.INSUFFICIENT_BALANCE,
                reason="Insufficient balance for order",
                details={"available": str(available), "required": str(required)},
            )

    current_notional = await _current_notional(session, account_id, symbol, price_dec)
    if position_policy.max_position_notional_per_symbol:
        cap = _quant(position_policy.max_position_notional_per_symbol, MONEY_EXP)
        if current_notional + notional > cap:
            return RejectReason(
                code=RejectCode.POSITION_LIMIT,
                reason="Position notional exceeds limit",
                details={"limit": str(cap), "current": str(current_notional), "order": str(notional)},
            )

    if position_policy.max_total_notional:
        price_map = {symbol: price_dec}
        total = await _total_notional(session, account_id, price_map)
        cap_total = _quant(position_policy.max_total_notional, MONEY_EXP)
        if total + notional > cap_total:
            return RejectReason(
                code=RejectCode.POSITION_LIMIT,
                reason="Account notional exceeds limit",
                details={"limit": str(cap_total), "current": str(total), "order": str(notional)},
            )

    if risk_policy.min_equity:
        if available < _quant(risk_policy.min_equity, MONEY_EXP):
            return RejectReason(
                code=RejectCode.RISK_MAX_LOSS,
                reason="Equity below minimum threshold",
                details={"min_equity": str(risk_policy.min_equity), "available": str(available)},
            )

    return None
