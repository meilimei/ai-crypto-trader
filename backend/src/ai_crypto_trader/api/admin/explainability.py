from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.admin.pagination import clamp_limit_offset
from ai_crypto_trader.services.explainability.explainability import (
    meta_symbol_upper,
    meta_text,
    run_outcome_tick,
)
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

router = APIRouter(prefix="/admin/explainability", tags=["admin"], dependencies=[Depends(require_admin_token)])


class OutcomeTickRequest(BaseModel):
    limit: int = 200
    min_age_seconds: int = 900
    horizon_seconds: int = 900


def _serialize_action(row: AdminAction) -> dict[str, Any]:
    return {
        "id": row.id,
        "action": row.action,
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "message": row.message,
        "meta": json_safe(row.meta) if row.meta is not None else {},
    }


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _base_filters(
    *,
    action: str,
    since_minutes: int,
    account_id: int | None,
    strategy_config_id: int | None,
    symbol: str | None,
) -> list[Any]:
    filters: list[Any] = [
        AdminAction.action == action,
        AdminAction.created_at >= (datetime.now(timezone.utc) - timedelta(minutes=max(int(since_minutes), 1))),
    ]
    if account_id is not None:
        filters.append(meta_text(AdminAction.meta, "account_id") == str(account_id))
    if strategy_config_id is not None:
        filters.append(meta_text(AdminAction.meta, "strategy_config_id") == str(strategy_config_id))
    if symbol:
        filters.append(meta_symbol_upper(AdminAction.meta) == normalize_symbol(symbol))
    return filters


@router.get("/decisions")
async def list_trade_decisions(
    limit: int = Query(default=50),
    offset: int = Query(default=0, ge=0),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    status: str | None = Query(default=None),
    since_minutes: int = Query(default=1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, offset_value = clamp_limit_offset(limit=limit, offset=offset, default_limit=50, max_limit=200)
    filters = _base_filters(
        action="TRADE_DECISION",
        since_minutes=since_minutes,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
    )
    if status:
        filters.append(meta_text(AdminAction.meta, "status") == status.strip().lower())

    total = int(
        (await session.execute(select(func.count()).select_from(AdminAction).where(and_(*filters)))).scalar_one() or 0
    )
    rows = (
        await session.execute(
            select(AdminAction)
            .where(and_(*filters))
            .order_by(AdminAction.created_at.desc(), AdminAction.id.asc())
            .offset(offset_value)
            .limit(limit_value)
        )
    ).scalars().all()

    decision_keys = []
    for row in rows:
        meta = row.meta if isinstance(row.meta, dict) else {}
        decision_key = str(meta.get("decision_key") or "").strip()
        if decision_key:
            decision_keys.append(decision_key)

    outcomes_by_key: dict[str, dict[str, Any]] = {}
    if decision_keys:
        outcome_rows = (
            await session.execute(
                select(AdminAction)
                .where(
                    and_(
                        AdminAction.action == "TRADE_OUTCOME",
                        meta_text(AdminAction.meta, "decision_key").in_(decision_keys),
                    )
                )
                .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
            )
        ).scalars().all()
        for outcome in outcome_rows:
            outcome_meta = outcome.meta if isinstance(outcome.meta, dict) else {}
            key = str(outcome_meta.get("decision_key") or "").strip()
            if key and key not in outcomes_by_key:
                outcomes_by_key[key] = _serialize_action(outcome)

    items: list[dict[str, Any]] = []
    for row in rows:
        item = _serialize_action(row)
        meta = row.meta if isinstance(row.meta, dict) else {}
        key = str(meta.get("decision_key") or "").strip()
        item["latest_outcome"] = outcomes_by_key.get(key)
        items.append(item)

    return {
        "items": items,
        "total": total,
        "limit": limit_value,
        "offset": offset_value,
    }


@router.get("/outcomes")
async def list_trade_outcomes(
    limit: int = Query(default=50),
    offset: int = Query(default=0, ge=0),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    since_minutes: int = Query(default=1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, offset_value = clamp_limit_offset(limit=limit, offset=offset, default_limit=50, max_limit=200)
    filters = _base_filters(
        action="TRADE_OUTCOME",
        since_minutes=since_minutes,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
    )
    total = int(
        (await session.execute(select(func.count()).select_from(AdminAction).where(and_(*filters)))).scalar_one() or 0
    )
    rows = (
        await session.execute(
            select(AdminAction)
            .where(and_(*filters))
            .order_by(AdminAction.created_at.desc(), AdminAction.id.asc())
            .offset(offset_value)
            .limit(limit_value)
        )
    ).scalars().all()
    return {
        "items": [_serialize_action(row) for row in rows],
        "total": total,
        "limit": limit_value,
        "offset": offset_value,
    }


@router.get("/summary")
async def explainability_summary(
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int = Query(default=1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    filters = _base_filters(
        action="TRADE_OUTCOME",
        since_minutes=window_minutes,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
    )
    rows = (
        await session.execute(
            select(AdminAction.meta)
            .where(and_(*filters))
            .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
        )
    ).all()

    outcomes_total = len(rows)
    if outcomes_total == 0:
        return {
            "outcomes_total": 0,
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "total_pnl_usdt_est": "0",
            "avg_pnl_usdt_est": "0",
            "window_minutes": window_minutes,
        }

    wins = 0
    return_values: list[Decimal] = []
    pnl_values: list[Decimal] = []
    for (meta_raw,) in rows:
        meta = meta_raw if isinstance(meta_raw, dict) else {}
        win = meta.get("win")
        if isinstance(win, bool):
            if win:
                wins += 1
        elif str(win).strip().lower() in {"1", "true", "yes", "on"}:
            wins += 1

        ret = _to_decimal(meta.get("return_pct_signed"))
        pnl = _to_decimal(meta.get("pnl_usdt_est"))
        if ret is not None:
            return_values.append(ret)
        if pnl is not None:
            pnl_values.append(pnl)

    total_pnl = sum(pnl_values, Decimal("0"))
    avg_pnl = total_pnl / Decimal(len(pnl_values)) if pnl_values else Decimal("0")
    avg_return = sum(return_values, Decimal("0")) / Decimal(len(return_values)) if return_values else Decimal("0")
    win_rate = wins / outcomes_total if outcomes_total > 0 else 0.0

    return {
        "outcomes_total": outcomes_total,
        "win_rate": float(win_rate),
        "avg_return_pct": float(avg_return),
        "total_pnl_usdt_est": str(total_pnl),
        "avg_pnl_usdt_est": str(avg_pnl),
        "window_minutes": window_minutes,
    }


@router.post("/outcome-tick")
async def run_explainability_outcome_tick(
    payload: OutcomeTickRequest | None = None,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    body = payload or OutcomeTickRequest()
    try:
        stats = await run_outcome_tick(
            session,
            limit=max(int(body.limit), 1),
            min_age_seconds=max(int(body.min_age_seconds), 0),
            horizon_seconds=max(int(body.horizon_seconds), 0),
        )
        await session.commit()
        return stats
    except Exception as exc:
        await session.rollback()
        raise HTTPException(status_code=500, detail=f"outcome tick failed: {exc}")

