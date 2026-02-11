from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, PaperPosition, PaperTrade, TradeExplanation
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _to_utc(value).isoformat()


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def serialize_trade_explanation(row: TradeExplanation) -> dict[str, Any]:
    return {
        "id": row.id,
        "created_at": _to_iso(row.created_at),
        "account_id": row.account_id,
        "strategy_config_id": row.strategy_config_id,
        "strategy_id": row.strategy_id,
        "symbol": row.symbol,
        "side": row.side,
        "requested_qty": str(row.requested_qty) if row.requested_qty is not None else None,
        "executed_trade_id": row.executed_trade_id,
        "order_id": row.order_id,
        "decision": json_safe(row.decision) if row.decision is not None else {},
        "rationale": row.rationale,
        "outcome": json_safe(row.outcome) if row.outcome is not None else {},
        "status": row.status,
    }


async def create_trade_explanation(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int | None,
    strategy_id: str | None,
    symbol: str,
    side: str,
    requested_qty: Decimal | str | None,
    decision: dict[str, Any] | None,
    rationale: str | None,
    status: str,
    executed_trade_id: int | None = None,
    order_id: int | None = None,
    outcome: dict[str, Any] | None = None,
) -> TradeExplanation:
    symbol_norm = normalize_symbol(symbol)
    qty_dec = _to_decimal(requested_qty)
    decision_safe = json_safe(decision or {})
    outcome_safe = json_safe(outcome or {})
    explanation = TradeExplanation(
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        strategy_id=strategy_id,
        symbol=symbol_norm,
        side=(side or "").lower().strip(),
        requested_qty=qty_dec,
        executed_trade_id=executed_trade_id,
        order_id=order_id,
        decision=decision_safe,
        rationale=rationale,
        outcome=outcome_safe,
        status=status,
    )
    session.add(explanation)
    await session.flush()

    session.add(
        AdminAction(
            action="TRADE_EXPLAIN",
            status=status,
            message=rationale or "Trade explanation recorded",
            meta=json_safe(
                {
                    "explanation_id": explanation.id,
                    "status": status,
                    "account_id": account_id,
                    "strategy_config_id": strategy_config_id,
                    "strategy_id": strategy_id,
                    "symbol": symbol_norm,
                    "side": (side or "").lower().strip(),
                    "trade_id": executed_trade_id,
                    "order_id": order_id,
                }
            ),
        )
    )
    return explanation


async def update_open_trade_explanations_outcomes(
    session: AsyncSession,
    *,
    now_utc: datetime,
    min_age_seconds: int = 60,
    limit: int = 200,
) -> dict[str, int]:
    now = _to_utc(now_utc)
    cutoff = now - timedelta(seconds=max(int(min_age_seconds), 0))
    query = (
        select(TradeExplanation)
        .where(
            TradeExplanation.status == "open",
            TradeExplanation.executed_trade_id.isnot(None),
            TradeExplanation.created_at <= cutoff,
        )
        .order_by(TradeExplanation.created_at.asc(), TradeExplanation.id.asc())
        .limit(max(int(limit), 1))
    )
    rows = (await session.execute(query)).scalars().all()
    closed_count = 0

    for row in rows:
        symbol_norm = normalize_symbol(row.symbol)
        position_qty = await session.scalar(
            select(PaperPosition.qty)
            .where(
                PaperPosition.account_id == row.account_id,
                func.upper(PaperPosition.symbol) == symbol_norm,
            )
            .limit(1)
        )
        if position_qty is not None and _to_decimal(position_qty) is not None and abs(_to_decimal(position_qty)) > Decimal("0.0000000001"):
            continue

        entry_trade = await session.get(PaperTrade, int(row.executed_trade_id))
        if entry_trade is None:
            continue

        exit_trade = await session.scalar(
            select(PaperTrade)
            .where(
                and_(
                    PaperTrade.account_id == row.account_id,
                    func.upper(PaperTrade.symbol) == symbol_norm,
                    PaperTrade.created_at >= entry_trade.created_at,
                    PaperTrade.id != entry_trade.id,
                    PaperTrade.side != entry_trade.side,
                )
            )
            .order_by(PaperTrade.created_at.desc(), PaperTrade.id.desc())
            .limit(1)
        )
        if exit_trade is None:
            continue

        qty = _to_decimal(row.requested_qty) or _to_decimal(entry_trade.qty) or Decimal("0")
        entry_price = _to_decimal(entry_trade.price) or Decimal("0")
        exit_price = _to_decimal(exit_trade.price) or Decimal("0")
        if str(entry_trade.side).lower() == "buy":
            pnl = (exit_price - entry_price) * qty
        else:
            pnl = (entry_price - exit_price) * qty
        holding_seconds = max(
            0,
            int((_to_utc(exit_trade.created_at) - _to_utc(entry_trade.created_at)).total_seconds()),
        )

        outcome_payload = row.outcome if isinstance(row.outcome, dict) else {}
        outcome_payload = dict(outcome_payload)
        outcome_payload.update(
            {
                "entry_price": str(entry_price),
                "exit_price": str(exit_price),
                "realized_pnl_usdt": str(pnl),
                "holding_seconds": holding_seconds,
                "max_favorable_excursion": outcome_payload.get("max_favorable_excursion"),
                "max_adverse_excursion": outcome_payload.get("max_adverse_excursion"),
                "win": pnl > 0,
                "closed": True,
                "closed_at": _to_iso(exit_trade.created_at),
                "entry_trade_id": entry_trade.id,
                "exit_trade_id": exit_trade.id,
            }
        )
        row.outcome = json_safe(outcome_payload)
        row.status = "closed"
        closed_count += 1

    return {
        "scanned_count": len(rows),
        "closed_count": closed_count,
        "pending_count": len(rows) - closed_count,
    }


async def list_trade_explanations(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
    account_id: int | None = None,
    strategy_config_id: int | None = None,
    symbol: str | None = None,
    status: str | None = None,
    since_minutes: int = 1440,
) -> tuple[int, list[TradeExplanation]]:
    filters = [TradeExplanation.created_at >= (_to_utc(datetime.now(timezone.utc)) - timedelta(minutes=max(since_minutes, 1)))]
    if account_id is not None:
        filters.append(TradeExplanation.account_id == account_id)
    if strategy_config_id is not None:
        filters.append(TradeExplanation.strategy_config_id == strategy_config_id)
    if symbol:
        filters.append(func.upper(TradeExplanation.symbol) == normalize_symbol(symbol))
    if status:
        filters.append(TradeExplanation.status == status.strip().lower())

    count_stmt = select(func.count()).select_from(TradeExplanation).where(and_(*filters))
    total = int((await session.execute(count_stmt)).scalar_one() or 0)
    stmt = (
        select(TradeExplanation)
        .where(and_(*filters))
        .order_by(TradeExplanation.created_at.desc(), TradeExplanation.id.desc())
        .limit(limit)
        .offset(offset)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return total, list(rows)


async def get_trade_explanation(session: AsyncSession, *, explanation_id: int) -> TradeExplanation | None:
    return await session.get(TradeExplanation, explanation_id)


async def summarize_trade_explanations(
    session: AsyncSession,
    *,
    account_id: int | None = None,
    strategy_config_id: int | None = None,
    symbol: str | None = None,
    window_minutes: int = 1440,
) -> dict[str, Any]:
    window_start = _to_utc(datetime.now(timezone.utc)) - timedelta(minutes=max(window_minutes, 1))
    base_filters = [TradeExplanation.created_at >= window_start]
    if account_id is not None:
        base_filters.append(TradeExplanation.account_id == account_id)
    if strategy_config_id is not None:
        base_filters.append(TradeExplanation.strategy_config_id == strategy_config_id)
    if symbol:
        base_filters.append(func.upper(TradeExplanation.symbol) == normalize_symbol(symbol))

    trades_total = int(
        (await session.execute(select(func.count()).select_from(TradeExplanation).where(and_(*base_filters)))).scalar_one()
        or 0
    )
    closed_rows = (
        await session.execute(
            select(TradeExplanation.outcome)
            .where(and_(*base_filters, TradeExplanation.status == "closed"))
            .order_by(TradeExplanation.created_at.desc(), TradeExplanation.id.desc())
        )
    ).scalars().all()

    closed_total = len(closed_rows)
    pnl_values: list[Decimal] = []
    holding_values: list[int] = []
    win_count = 0
    for outcome in closed_rows:
        payload = outcome if isinstance(outcome, dict) else {}
        pnl = _to_decimal(payload.get("realized_pnl_usdt"))
        if pnl is not None:
            pnl_values.append(pnl)
        holding_values.append(_to_int(payload.get("holding_seconds"), 0))
        if payload.get("win") is not None:
            if _to_bool(payload.get("win")):
                win_count += 1
        elif pnl is not None and pnl > 0:
            win_count += 1

    total_pnl = sum(pnl_values, Decimal("0"))
    avg_pnl = (total_pnl / Decimal(len(pnl_values))) if pnl_values else Decimal("0")
    avg_holding = (sum(holding_values) / len(holding_values)) if holding_values else 0.0
    win_rate = (win_count / closed_total) if closed_total > 0 else 0.0

    return {
        "trades_total": trades_total,
        "closed_total": closed_total,
        "win_rate": win_rate,
        "avg_pnl": str(avg_pnl),
        "total_pnl": str(total_pnl),
        "avg_holding_seconds": avg_holding,
        "window_minutes": window_minutes,
    }

