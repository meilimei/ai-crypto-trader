from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ai_crypto_trader.common.models import PaperAccount, PaperBalance, PaperOrder, PaperPosition, PaperTrade, utc_now
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol, QTY_EXP, MONEY_EXP, PRICE_EXP
from ai_crypto_trader.services.paper_trader.execution_costs import compute_execution
from ai_crypto_trader.services.paper_trader.utils import guard_market_risk, prepare_order_inputs


@dataclass
class ExecutionResult:
    order: PaperOrder
    trade: PaperTrade
    position: PaperPosition
    balance: PaperBalance
    costs: Dict[str, str]


def _quantize(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp), rounding=ROUND_DOWN)


async def _get_balance_for_update(
    session: AsyncSession,
    account_id: int,
    ccy: str,
    initial_available: Decimal | None = None,
) -> PaperBalance:
    balance = await session.scalar(
        select(PaperBalance)
        .where(PaperBalance.account_id == account_id, PaperBalance.ccy == ccy)
        .with_for_update()
    )
    if balance:
        return balance
    initial = Decimal(str(initial_available)) if initial_available is not None else Decimal("0")
    balance = PaperBalance(account_id=account_id, ccy=ccy, available=initial)
    session.add(balance)
    await session.flush()
    return balance


async def _get_position_for_update(session: AsyncSession, account_id: int, symbol: str) -> PaperPosition:
    symbol_norm = normalize_symbol(symbol)
    position = await session.scalar(
        select(PaperPosition)
        .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol_norm)
        .with_for_update()
    )
    if not position and symbol_norm != symbol:
        position = await session.scalar(
            select(PaperPosition)
            .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol)
            .with_for_update()
        )
        if position:
            position.symbol = symbol_norm
    if position:
        return position
    position = PaperPosition(account_id=account_id, symbol=symbol_norm, side="long", qty=Decimal("0"), avg_entry_price=Decimal("0"))
    session.add(position)
    await session.flush()
    return position


def _apply_trade_to_position(
    entry_qty: Decimal,
    entry_avg: Decimal,
    trade_qty: Decimal,
    trade_price: Decimal,
    side: str,
) -> tuple[Decimal, Decimal, str]:
    qty_abs = _quantize(Decimal(str(trade_qty)).copy_abs(), QTY_EXP)
    if qty_abs <= 0:
        side_state = "short" if entry_qty < 0 else "long"
        return _quantize(entry_qty, QTY_EXP), _quantize(entry_avg, PRICE_EXP), side_state

    trade_qty_signed = qty_abs if side == "buy" else -qty_abs
    entry_qty_q = _quantize(Decimal(str(entry_qty)), QTY_EXP)
    entry_avg_q = _quantize(Decimal(str(entry_avg)), PRICE_EXP)
    new_qty = _quantize(entry_qty_q + trade_qty_signed, QTY_EXP)
    qty_tol = Decimal(QTY_EXP)
    if new_qty.copy_abs() <= qty_tol:
        return Decimal("0"), Decimal("0"), "long"

    if new_qty == 0:
        new_avg = Decimal("0")
    elif entry_qty_q == 0 or (entry_qty_q * new_qty) < 0:
        new_avg = _quantize(trade_price, PRICE_EXP)
    elif abs(new_qty) > abs(entry_qty_q) and (entry_qty_q * trade_qty_signed) > 0:
        weighted = (abs(entry_qty_q) * entry_avg_q + abs(trade_qty_signed) * trade_price) / abs(new_qty)
        new_avg = _quantize(weighted, PRICE_EXP)
    else:
        new_avg = _quantize(entry_avg_q, PRICE_EXP)

    side_state = "short" if new_qty < 0 else "long"
    return new_qty, new_avg, side_state


async def apply_trade_to_state(
    session: AsyncSession,
    account_id: int,
    *,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    fee: Decimal,
    initial_cash_usd: Decimal,
) -> tuple[PaperPosition, PaperBalance]:
    if "/" in str(symbol):
        raise ValueError("SYMBOL_CONTAINS_SLASH")
    symbol_norm = normalize_symbol(symbol)
    if not symbol_norm or "/" in symbol_norm:
        raise ValueError("Invalid symbol")
    qty_dec = _quantize(Decimal(str(qty)), QTY_EXP)
    if qty_dec <= 0:
        raise ValueError("Quantity must be positive")
    if side not in {"buy", "sell"}:
        raise ValueError("Side must be buy or sell")

    price_dec = _quantize(Decimal(str(price)), PRICE_EXP)
    if not price_dec.is_finite() or price_dec <= 0:
        raise ValueError("Price must be positive")
    fee_dec = _quantize(Decimal(str(fee)), MONEY_EXP)
    notional = _quantize(qty_dec * price_dec, MONEY_EXP)
    if not notional.is_finite() or not fee_dec.is_finite():
        raise ValueError("Invalid notional or fee")

    position = await _get_position_for_update(session, account_id, symbol_norm)
    entry_qty = Decimal(str(position.qty or 0))
    entry_avg = Decimal(str(position.avg_entry_price or 0))
    new_qty, new_avg, side_state = _apply_trade_to_position(
        entry_qty,
        entry_avg,
        qty_dec,
        price_dec,
        side,
    )
    position.qty = new_qty
    position.avg_entry_price = new_avg
    position.side = side_state
    position.updated_at = utc_now()

    balance = await _get_balance_for_update(session, account_id, "USDT", initial_cash_usd)
    if side == "buy":
        cash_delta = -notional - fee_dec
    else:
        cash_delta = notional - fee_dec
    new_balance = _quantize(Decimal(str(balance.available)) + cash_delta, MONEY_EXP)
    if not new_balance.is_finite():
        raise ValueError("Invalid balance update")
    balance.available = new_balance
    balance.updated_at = utc_now()
    await session.flush()

    return position, balance


async def execute_market_order_with_costs(
    session: AsyncSession,
    account_id: int,
    symbol: str,
    side: str,
    qty: Decimal,
    mid_price: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
    meta: Optional[Dict[str, str]] = None,
) -> ExecutionResult:
    prepared = prepare_order_inputs(symbol, qty, mid_price)
    qty_dec = prepared.qty
    symbol_norm = prepared.symbol

    side_norm = side.lower().strip()
    if side_norm not in {"buy", "sell"}:
        raise ValueError("Side must be buy or sell")
    mid_price_dec = prepared.price
    exec_meta = compute_execution(mid_price_dec, side_norm, qty_dec, fee_bps, slippage_bps)
    fill_price = exec_meta["exec_price"]
    notional = exec_meta["notional"]
    fee_usd = exec_meta["fee"]

    # Use a savepoint if we're already inside a transaction (e.g., request-scoped session).
    if session.in_transaction():
        ctx = session.begin_nested()
    else:
        ctx = session.begin()

    async with ctx:
        account = await session.get(PaperAccount, account_id, with_for_update=True)
        if not account:
            raise ValueError("Paper account not found")

        allow_short = False
        if meta and isinstance(meta, dict):
            allow_short = bool(meta.get("allow_short", False))
        await guard_market_risk(
            session,
            account_id=account_id,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty_dec,
            price=mid_price_dec,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            allow_short=allow_short,
        )

        order = PaperOrder(
            account_id=account_id,
            symbol=symbol_norm,
            side=side_norm,
            type="market",
            status="filled",
            requested_qty=qty_dec,
            filled_qty=qty_dec,
            avg_fill_price=fill_price,
            fee_paid=fee_usd,
        )
        trade = PaperTrade(
            account_id=account_id,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty_dec,
            price=fill_price,
            fee=fee_usd,
            realized_pnl=Decimal("0"),
        )
        session.add_all([order, trade])
        await session.flush()

        position, balance = await apply_trade_to_state(
            session,
            account_id,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty_dec,
            price=fill_price,
            fee=fee_usd,
            initial_cash_usd=Decimal(str(account.initial_cash_usd or "10000.00")),
        )

    return ExecutionResult(
        order=order,
        trade=trade,
        position=position,
        balance=balance,
        costs={
            "fill_price": str(fill_price),
            "notional_usd": str(notional),
            "fee_usd": str(fee_usd),
            "slippage_bps": str(slippage_bps),
            "fee_bps": str(fee_bps),
        },
    )


async def apply_market_order(
    session: AsyncSession,
    account_id: int,
    symbol: str,
    side: str,
    qty: Optional[Decimal] = None,
    price: Optional[Decimal] = None,
    mid_price: Optional[Decimal] = None,
    fee_bps: Decimal = Decimal("0"),
    slippage_bps: Decimal = Decimal("0"),
    meta: Optional[Dict[str, str]] = None,
    **kwargs,
) -> ExecutionResult:
    """
    Backward-compatible wrapper that calls execute_market_order_with_costs.

    Supports legacy shape with qty+price or new shape with mid_price and optional notional_usd.
    """
    side_norm = (side or "").lower().strip()
    mid = mid_price if mid_price is not None else price
    if mid is None:
        raise ValueError("price or mid_price is required")

    qty_dec: Optional[Decimal] = Decimal(str(qty)) if qty is not None else None
    if qty_dec is None:
        notional = kwargs.get("notional_usd")
        if notional is None:
            raise ValueError("qty or notional_usd must be provided")
        qty_dec = _quantize(Decimal(str(notional)) / Decimal(str(mid)), "0.00000001")

    return await execute_market_order_with_costs(
        session=session,
        account_id=account_id,
        symbol=symbol,
        side=side_norm,
        qty=qty_dec,
        mid_price=Decimal(str(mid)),
        fee_bps=Decimal(str(fee_bps)),
        slippage_bps=Decimal(str(slippage_bps)),
        meta=meta,
    )
