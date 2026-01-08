from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import PaperAccount, PaperBalance, PaperPosition
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol, QTY_EXP, PRICE_EXP, MONEY_EXP


def _quantize(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp), rounding=ROUND_DOWN)


class RiskRejected(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class PreparedOrder:
    symbol: str
    qty: Decimal
    price: Decimal


def prepare_order_inputs(symbol: str, qty: object, price: object) -> PreparedOrder:
    if "/" in str(symbol):
        raise ValueError("SYMBOL_CONTAINS_SLASH")
    symbol_norm = normalize_symbol(symbol)
    if not symbol_norm:
        raise ValueError("SYMBOL_EMPTY")
    try:
        qty_dec = _quantize(Decimal(str(qty)), QTY_EXP)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("QTY_INVALID")
    if qty_dec <= 0:
        raise ValueError("QTY_NOT_POSITIVE")
    try:
        price_dec = _quantize(Decimal(str(price)), PRICE_EXP)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("PRICE_INVALID")
    if price_dec <= 0:
        raise ValueError("PRICE_NOT_POSITIVE")
    return PreparedOrder(symbol=symbol_norm, qty=qty_dec, price=price_dec)


async def guard_market_risk(
    session: AsyncSession,
    *,
    account_id: int,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    fee_bps: Decimal = Decimal("0"),
    slippage_bps: Decimal = Decimal("0"),
    allow_short: bool = False,
) -> None:
    side_norm = (side or "").strip().lower()
    if side_norm not in {"buy", "sell"}:
        raise ValueError("SIDE_INVALID")
    symbol_norm = normalize_symbol(symbol)
    if not symbol_norm or "/" in symbol_norm:
        raise ValueError("SYMBOL_CONTAINS_SLASH")

    balance = await session.scalar(
        select(PaperBalance.available)
        .where(PaperBalance.account_id == account_id, PaperBalance.ccy == "USDT")
        .limit(1)
    )
    if balance is None:
        initial_cash = await session.scalar(
            select(PaperAccount.initial_cash_usd)
            .where(PaperAccount.id == account_id)
            .limit(1)
        )
        cash = Decimal(str(initial_cash)) if initial_cash is not None else Decimal("0")
    else:
        cash = Decimal(str(balance))

    pos_qty = await session.scalar(
        select(PaperPosition.qty)
        .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol_norm)
        .limit(1)
    )
    current_qty = Decimal(str(pos_qty)) if pos_qty is not None else Decimal("0")

    notional = _quantize(qty * price, MONEY_EXP)
    fee_est = _quantize(notional * (fee_bps / Decimal("10000")), MONEY_EXP)
    slip_est = _quantize(notional * (slippage_bps / Decimal("10000")), MONEY_EXP)

    if side_norm == "buy":
        needed = notional + fee_est + slip_est
        if cash < needed:
            raise RiskRejected("INSUFFICIENT_CASH")
    else:
        if not allow_short and current_qty <= 0:
            raise RiskRejected("SHORT_NOT_ALLOWED")
        if not allow_short and current_qty < qty:
            raise RiskRejected("INSUFFICIENT_POSITION")
