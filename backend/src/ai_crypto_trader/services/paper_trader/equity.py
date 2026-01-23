from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.jsonable import to_jsonable
from ai_crypto_trader.common.models import Candle, EquitySnapshot, PaperBalance, PaperPosition, PaperTrade
from ai_crypto_trader.services.admin_actions.throttled import write_admin_action_throttled

logger = logging.getLogger(__name__)
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol


@dataclass(frozen=True)
class PositionMark:
    symbol: str
    qty: Decimal
    mark_price: Decimal
    notional_usdt: Decimal
    price_source: Optional[str]


async def _latest_price(session: AsyncSession, symbol: str) -> Optional[Decimal]:
    result = await session.execute(
        select(Candle.close)
        .where(Candle.symbol == symbol)
        .order_by(Candle.open_time.desc())
        .limit(1)
    )
    value = result.scalar_one_or_none()
    if value is None:
        return None
    return Decimal(str(value))


async def _resolve_mark_price(
    session: AsyncSession,
    *,
    symbol_in: str,
    symbol_norm: str,
) -> tuple[Optional[Decimal], Optional[str]]:
    price = await _latest_price(session, symbol_norm)
    if price is not None and price.is_finite() and price > 0:
        return price, "latest"

    symbol_slash = None
    if "/" in symbol_in:
        symbol_slash = symbol_in
    elif symbol_norm.endswith("USDT") and len(symbol_norm) > 4:
        symbol_slash = f"{symbol_norm[:-4]}/USDT"
    symbols = [symbol_norm]
    if symbol_slash and symbol_slash not in symbols:
        symbols.append(symbol_slash)
    last_trade = await session.execute(
        select(PaperTrade.price)
        .where(PaperTrade.symbol.in_(symbols))
        .order_by(desc(PaperTrade.created_at).nullslast(), PaperTrade.id.desc())
        .limit(1)
    )
    row = last_trade.first()
    if row and row[0] is not None:
        candidate = Decimal(str(row[0]))
        if candidate.is_finite() and candidate > 0:
            return candidate, "paper_trades_last"
    return None, None


async def compute_equity(
    session: AsyncSession,
    account_id: int,
    *,
    price_source: str = "engine",
) -> dict:
    cash = await session.scalar(
        select(PaperBalance.available)
        .where(PaperBalance.account_id == account_id, PaperBalance.ccy == "USDT")
        .limit(1)
    )
    cash_usdt = Decimal(str(cash)) if cash is not None else Decimal("0")

    positions = await session.execute(
        select(PaperPosition.symbol, PaperPosition.qty)
        .where(PaperPosition.account_id == account_id)
    )
    marks: list[PositionMark] = []
    warnings: list[dict] = []
    positions_notional = Decimal("0")

    for symbol, qty in positions.all():
        qty_dec = Decimal(str(qty or 0))
        if qty_dec == 0:
            continue
        symbol_in = symbol or ""
        symbol_norm = normalize_symbol(symbol_in)
        if not symbol_norm:
            continue
        mark_price, mark_source = await _resolve_mark_price(
            session,
            symbol_in=symbol_in,
            symbol_norm=symbol_norm,
        )
        if mark_price is None:
            warnings.append({"symbol": symbol_norm, "warning": "missing_price"})
            mark_price = Decimal("0")
        notional = qty_dec * mark_price
        positions_notional += notional
        marks.append(
            PositionMark(
                symbol=symbol_norm,
                qty=qty_dec,
                mark_price=mark_price,
                notional_usdt=notional,
                price_source=mark_source,
            )
        )

    equity_usdt = cash_usdt + positions_notional
    return {
        "cash_usdt": cash_usdt,
        "positions_notional_usdt": positions_notional,
        "equity_usdt": equity_usdt,
        "positions": marks,
        "warnings": warnings,
        "price_source": price_source,
    }


async def write_equity_snapshot(
    session: AsyncSession,
    account_id: int,
    data: dict,
    *,
    source: str,
    min_interval_seconds: int = 60,
) -> bool:
    now = datetime.now(timezone.utc)
    try:
        if min_interval_seconds > 0:
            latest = await session.scalar(
                select(EquitySnapshot.created_at)
                .where(EquitySnapshot.account_id == account_id)
                .order_by(EquitySnapshot.created_at.desc())
                .limit(1)
            )
            if latest is not None:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                if now - latest < timedelta(seconds=min_interval_seconds):
                    return False

        cash_usdt = data.get("cash_usdt", Decimal("0"))
        positions_notional = data.get("positions_notional_usdt", Decimal("0"))
        equity_usdt = data.get("equity_usdt", Decimal("0"))
        meta_payload = {
            "account_id": str(account_id),
            "price_source": data.get("price_source"),
            "positions": data.get("positions", []),
            "warnings": data.get("warnings", []),
            "cash_usdt": cash_usdt,
            "positions_notional_usdt": positions_notional,
            "equity_usdt": equity_usdt,
        }
        snapshot = EquitySnapshot(
            account_id=account_id,
            equity=equity_usdt,
            balance=cash_usdt,
            unrealized_pnl=positions_notional,
            equity_usdt=equity_usdt,
            cash_usdt=cash_usdt,
            positions_notional_usdt=positions_notional,
            source=source,
            meta=to_jsonable(meta_payload),
            created_at=now,
        )
        session.add(snapshot)
        return True
    except Exception as exc:
        await log_equity_snapshot_failed(account_id, exc)
        return False


async def log_equity_snapshot_failed(account_id: int, error: Exception | str) -> None:
    try:
        await write_admin_action_throttled(
            None,
            action_type="EQUITY_SNAPSHOT_FAILED",
            account_id=account_id,
            symbol=None,
            status="error",
            payload={
                "account_id": str(account_id),
                "error": str(error),
            },
            window_seconds=300,
            dedupe_key=f"EQUITY_SNAPSHOT_FAILED:{account_id}",
        )
    except Exception:
        logger.exception("Failed to log equity snapshot error")
