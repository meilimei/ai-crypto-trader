from decimal import Decimal
from typing import Any, Dict, Iterable, Tuple

from ai_crypto_trader.common.models import PaperAccount
from ai_crypto_trader.services.paper_trader.accounting import (
    normalize_symbol,
    MONEY_EXP,
    update_position_from_trade,
)
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig


def _q(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp))


def normalize_trade_side(side: str) -> int:
    side_norm = (side or "").strip().lower()
    if side_norm in {"buy", "long", "cover"}:
        return 1
    if side_norm in {"sell", "short"}:
        return -1
    return 1


def signed_qty(side: str, qty: Decimal) -> Decimal:
    qty_dec = Decimal(str(qty))
    if qty_dec < 0:
        return qty_dec
    sign = normalize_trade_side(side)
    return qty_dec.copy_abs() * Decimal(sign)


def simulate_positions_and_cash(
    trades: Iterable[Any],
    initial_usdt: Decimal,
) -> Dict[str, Any]:
    positions: Dict[str, Dict[str, Decimal]] = {}
    cash = {"USDT": {"free": _q(initial_usdt, MONEY_EXP)}}
    stats = {
        "realized_pnl_usd": Decimal("0"),
        "fees_usd": Decimal("0"),
        "slippage_usd": Decimal("0"),
        "sum_buy_notional": Decimal("0"),
        "sum_sell_notional": Decimal("0"),
        "trade_count": 0,
    }

    for trade in trades:
        side = getattr(trade, "side", None)
        qty_raw = getattr(trade, "qty", None)
        price_raw = getattr(trade, "price", None)
        symbol_raw = getattr(trade, "symbol", None)
        if qty_raw is None or price_raw is None or symbol_raw is None:
            continue

        qty_signed = signed_qty(side, Decimal(str(qty_raw)))
        if qty_signed == 0:
            continue
        qty_abs = qty_signed.copy_abs()
        price = Decimal(str(price_raw))
        symbol = normalize_symbol(str(symbol_raw))
        if not symbol:
            continue

        fee_raw = getattr(trade, "fee", None)
        fee = Decimal(str(fee_raw)) if fee_raw is not None else Decimal("0")
        realized_raw = getattr(trade, "realized_pnl", None)
        realized_override = (
            Decimal(str(realized_raw)) if realized_raw is not None else None
        )

        notional = _q(qty_abs * price, MONEY_EXP)
        if qty_signed > 0:
            cash["USDT"]["free"] = _q(cash["USDT"]["free"] - notional - fee, MONEY_EXP)
            stats["sum_buy_notional"] += notional
        else:
            cash["USDT"]["free"] = _q(cash["USDT"]["free"] + notional - fee, MONEY_EXP)
            stats["sum_sell_notional"] += notional

        stats["fees_usd"] += fee
        stats["trade_count"] += 1

        pos = positions.get(symbol, {"qty": Decimal("0"), "avg_entry": Decimal("0")})
        pos_qty = Decimal(str(pos["qty"]))
        pos_avg = Decimal(str(pos["avg_entry"]))
        if pos_qty != 0 and qty_signed != 0 and pos_qty * qty_signed < 0:
            closed_qty = min(abs(pos_qty), qty_abs)
            if realized_override is not None:
                stats["realized_pnl_usd"] += realized_override
            else:
                sign = Decimal("1") if pos_qty > 0 else Decimal("-1")
                stats["realized_pnl_usd"] += (price - pos_avg) * closed_qty * sign

        slippage = Decimal(str(getattr(trade, "slippage_usd", 0) or 0))
        stats["slippage_usd"] += slippage

        updated = update_position_from_trade(pos_qty, pos_avg, price, qty_signed)
        positions[symbol] = {"qty": updated["qty"], "avg_entry": updated["avg"]}

    return {"positions": positions, "cash": cash, "stats": stats}


async def get_initial_usdt(session, account_id: int) -> Tuple[Decimal, str]:
    account = await session.get(PaperAccount, account_id)
    if account and getattr(account, "initial_cash_usd", None) is not None:
        return Decimal(str(account.initial_cash_usd)), "account"
    config = PaperTraderConfig.from_env()
    return Decimal(str(config.initial_balance)), "config"
