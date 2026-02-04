from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import EquitySnapshot
from ai_crypto_trader.services.paper_trader.rejects import make_reject

logger = logging.getLogger(__name__)


def _to_decimal(value: object | None) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: object | None) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fmt_decimal(value: Decimal | None, *, eps: Decimal = Decimal("1e-9")) -> str | None:
    if value is None:
        return None
    try:
        dec = Decimal(value)
    except Exception:
        return str(value)
    if dec.copy_abs() < eps:
        return "0.00"
    return format(dec, "f")


def _equity_col():
    return func.coalesce(EquitySnapshot.equity_usdt, EquitySnapshot.equity)


async def check_equity_risk(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id: UUID | None,
    policy: object,
    now_utc: datetime,
    return_details: bool = False,
) -> dict | None:
    max_daily_loss = _to_decimal(getattr(policy, "max_daily_loss_usdt", None))
    max_drawdown = _to_decimal(getattr(policy, "max_drawdown_usdt", None))
    max_drawdown_pct = _to_decimal(getattr(policy, "max_drawdown_pct", None))
    lookback_hours = _to_int(getattr(policy, "equity_lookback_hours", None)) or 24

    if max_daily_loss is None and max_drawdown is None and max_drawdown_pct is None:
        return None if not return_details else {"code": None, "reason": None, "details": None}

    try:
        equity_expr = _equity_col()
        latest_row = await session.execute(
            select(equity_expr, EquitySnapshot.created_at)
            .where(EquitySnapshot.account_id == account_id)
            .order_by(EquitySnapshot.created_at.desc())
            .limit(1)
        )
        latest = latest_row.first()
        if not latest or latest[0] is None:
            return None if not return_details else {"code": None, "reason": None, "details": None}
        current_equity = Decimal(str(latest[0]))

        now = now_utc if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc)
        window_start = now - timedelta(hours=lookback_hours)

        peak_equity = None
        if max_drawdown is not None or max_drawdown_pct is not None:
            peak = await session.scalar(
                select(func.max(equity_expr))
                .where(
                    EquitySnapshot.account_id == account_id,
                    EquitySnapshot.created_at >= window_start,
                )
            )
            if peak is not None:
                peak_equity = Decimal(str(peak))

        sod_equity = None
        if max_daily_loss is not None:
            day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            sod = await session.execute(
                select(equity_expr)
                .where(
                    EquitySnapshot.account_id == account_id,
                    EquitySnapshot.created_at >= day_start,
                )
                .order_by(EquitySnapshot.created_at.asc())
                .limit(1)
            )
            row = sod.first()
            if row and row[0] is not None:
                sod_equity = Decimal(str(row[0]))
            else:
                fallback = await session.execute(
                    select(equity_expr)
                    .where(
                        EquitySnapshot.account_id == account_id,
                        EquitySnapshot.created_at >= window_start,
                    )
                    .order_by(EquitySnapshot.created_at.asc())
                    .limit(1)
                )
                fallback_row = fallback.first()
                if fallback_row and fallback_row[0] is not None:
                    sod_equity = Decimal(str(fallback_row[0]))

        details = {
            "account_id": account_id,
            "strategy_id": str(strategy_id) if strategy_id is not None else None,
            "current_equity": fmt_decimal(current_equity),
            "peak_equity": fmt_decimal(peak_equity),
            "drawdown_usdt": None,
            "max_drawdown_usdt": fmt_decimal(max_drawdown),
            "drawdown_pct": None,
            "max_drawdown_pct": fmt_decimal(max_drawdown_pct),
            "sod_equity": fmt_decimal(sod_equity),
            "daily_loss_usdt": None,
            "max_daily_loss_usdt": fmt_decimal(max_daily_loss),
            "window_start": window_start.isoformat(),
            "now": now.isoformat(),
            "equity_lookback_hours": lookback_hours,
        }

        if max_daily_loss is not None and sod_equity is not None:
            daily_loss = sod_equity - current_equity
            details["daily_loss_usdt"] = fmt_decimal(daily_loss)
            if daily_loss > max_daily_loss:
                return make_reject(
                    "MAX_DAILY_LOSS",
                    "Daily loss exceeds maximum",
                    details,
                )

        if max_drawdown is not None and peak_equity is not None:
            drawdown = peak_equity - current_equity
            details["drawdown_usdt"] = fmt_decimal(drawdown)
            if drawdown > max_drawdown:
                return make_reject(
                    "MAX_DRAWDOWN",
                    "Drawdown exceeds maximum",
                    details,
                )

        if max_drawdown_pct is not None and peak_equity is not None and peak_equity > 0:
            pct_limit = max_drawdown_pct / Decimal("100") if max_drawdown_pct > 1 else max_drawdown_pct
            drawdown_pct = (peak_equity - current_equity) / peak_equity
            details["drawdown_pct"] = fmt_decimal(drawdown_pct * Decimal("100"))
            if drawdown_pct > pct_limit:
                return make_reject(
                    "MAX_DRAWDOWN_PCT",
                    "Drawdown exceeds maximum percentage",
                    details,
                )

        if return_details:
            return {"code": None, "reason": None, "details": details}
        return None
    except Exception:
        logger.warning("Equity risk check failed; skipping", exc_info=False)
        return None if not return_details else {"code": None, "reason": None, "details": None}
