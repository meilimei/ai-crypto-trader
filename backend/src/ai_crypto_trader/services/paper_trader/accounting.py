from decimal import Decimal
from typing import Dict, Tuple

QTY_EXP = "0.00000001"
PRICE_EXP = "0.00000001"
MONEY_EXP = "0.01"


def _q(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp))


def normalize_symbol(symbol: str) -> str:
    cleaned = "".join((symbol or "").split())
    return cleaned.replace("/", "").replace("-", "").upper()


def apply_fill_to_state(
    state: Dict[str, object],
    symbol: str,
    side: str,
    qty: Decimal,
    fill_price: Decimal,
    fee_usd: Decimal,
) -> Dict[str, object]:
    """
    Apply a filled trade to in-memory state (positions/balances/fees).
    """
    side_norm = (side or "").lower().strip()
    qty_abs = _q(Decimal(str(qty)).copy_abs(), QTY_EXP)
    price = _q(Decimal(str(fill_price)), PRICE_EXP)
    fee = _q(Decimal(str(fee_usd)), MONEY_EXP)
    if qty_abs <= 0:
        return state

    delta_qty = qty_abs if side_norm == "buy" else -qty_abs
    notional = _q(qty_abs * price, MONEY_EXP)

    positions: Dict[str, Dict[str, Decimal]] = state.setdefault("positions_by_symbol", {})
    balances: Dict[str, Decimal] = state.setdefault("balances_by_asset", {})
    fees_usd: Decimal = state.get("fees_usd", Decimal("0"))

    pos = positions.get(symbol, {"qty": Decimal("0"), "avg": Decimal("0")})
    updated = _update_position_from_trade(pos["qty"], pos["avg"], price, delta_qty)
    positions[symbol] = updated

    # Cash
    cash = balances.get("USDT", Decimal("0"))
    if side_norm == "buy":
        cash -= notional + fee
    else:
        cash += notional - fee
    balances["USDT"] = _q(cash, MONEY_EXP)

    state["fees_usd"] = fees_usd + fee
    return state


def _update_position_from_trade(
    pos_qty: Decimal,
    pos_avg: Decimal,
    price: Decimal,
    delta_qty: Decimal,
) -> Dict[str, Decimal]:
    new_qty = pos_qty + delta_qty
    if new_qty == 0:
        return {"qty": Decimal("0"), "avg": Decimal("0")}

    if pos_qty == 0:
        return {"qty": _q(new_qty, QTY_EXP), "avg": _q(price, PRICE_EXP)}

    # Same direction
    if (pos_qty > 0 and delta_qty > 0) or (pos_qty < 0 and delta_qty < 0):
        if abs(new_qty) > abs(pos_qty):
            weighted = (abs(pos_qty) * pos_avg + abs(delta_qty) * price) / abs(new_qty)
            return {"qty": _q(new_qty, QTY_EXP), "avg": _q(weighted, PRICE_EXP)}
        return {"qty": _q(new_qty, QTY_EXP), "avg": _q(pos_avg, PRICE_EXP)}

    # Flipping direction
    return {"qty": _q(new_qty, QTY_EXP), "avg": _q(price, PRICE_EXP)}
