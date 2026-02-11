from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
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
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.services.paper_trader.order_entry import place_order_unified
from ai_crypto_trader.services.paper_trader.rejects import RejectReason
from ai_crypto_trader.utils.json_safe import json_safe

router = APIRouter(prefix="/admin/explainability", tags=["admin"], dependencies=[Depends(require_admin_token)])
logger = logging.getLogger(__name__)


class OutcomeTickRequest(BaseModel):
    limit: int = 200
    min_age_seconds: int = 900
    horizon_seconds: int = 900


class TestExecutedRequest(BaseModel):
    account_id: int
    strategy_config_id: int
    symbol: str
    side: str
    qty: Decimal
    price_override: Decimal | None = None


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
    account_id: int | None,
    strategy_config_id: int | None,
    symbol: str | None,
    since: datetime | None = None,
    until: datetime | None = None,
    since_minutes: int | None = None,
) -> list[Any]:
    filters: list[Any] = [AdminAction.action == action]
    if since_minutes is not None:
        filters.append(
            AdminAction.created_at
            >= (datetime.now(timezone.utc) - timedelta(minutes=max(int(since_minutes), 1)))
        )
    if since is not None:
        since_utc = since if since.tzinfo is not None else since.replace(tzinfo=timezone.utc)
        filters.append(AdminAction.created_at >= since_utc)
    if until is not None:
        until_utc = until if until.tzinfo is not None else until.replace(tzinfo=timezone.utc)
        filters.append(AdminAction.created_at <= until_utc)
    if account_id is not None:
        filters.append(meta_text(AdminAction.meta, "account_id") == str(account_id))
    if strategy_config_id is not None:
        filters.append(meta_text(AdminAction.meta, "strategy_config_id") == str(strategy_config_id))
    if symbol:
        filters.append(meta_symbol_upper(AdminAction.meta) == normalize_symbol(symbol))
    return filters


def _flag_enabled(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


@router.get("/decisions")
async def list_trade_decisions(
    limit: int = Query(default=50),
    cursor: int | None = Query(default=None, ge=1),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    status: str | None = Query(default=None),
    since: datetime | None = Query(default=None),
    until: datetime | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, _ = clamp_limit_offset(limit=limit, offset=0, default_limit=50, max_limit=200)
    base_filters = _base_filters(
        action="TRADE_DECISION",
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        since=since,
        until=until,
    )
    filters = list(base_filters)
    if status:
        filters.append(func.lower(func.coalesce(meta_text(AdminAction.meta, "status"), "")) == status.strip().lower())
    if cursor is not None:
        filters.append(AdminAction.id < cursor)

    total = int(
        (
            await session.execute(
                select(func.count()).select_from(AdminAction).where(and_(*base_filters))
            )
        ).scalar_one()
        or 0
    )
    rows = (
        await session.execute(
            select(AdminAction)
            .where(and_(*filters))
            .order_by(AdminAction.id.desc())
            .limit(limit_value + 1)
        )
    ).scalars().all()
    has_more = len(rows) > limit_value
    rows = rows[:limit_value]
    next_cursor = rows[-1].id if has_more and rows else None

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
                .order_by(AdminAction.id.desc())
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
        "cursor": cursor,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


@router.get("/outcomes")
async def list_trade_outcomes(
    limit: int = Query(default=50),
    cursor: int | None = Query(default=None, ge=1),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    status: str | None = Query(default=None),
    since: datetime | None = Query(default=None),
    until: datetime | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, _ = clamp_limit_offset(limit=limit, offset=0, default_limit=50, max_limit=200)
    base_filters = _base_filters(
        action="TRADE_OUTCOME",
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        since=since,
        until=until,
    )
    filters = list(base_filters)
    if status:
        filters.append(AdminAction.status == status.strip().lower())
    if cursor is not None:
        filters.append(AdminAction.id < cursor)
    total = int(
        (
            await session.execute(
                select(func.count()).select_from(AdminAction).where(and_(*base_filters))
            )
        ).scalar_one()
        or 0
    )
    rows = (
        await session.execute(
            select(AdminAction)
            .where(and_(*filters))
            .order_by(AdminAction.id.desc())
            .limit(limit_value + 1)
        )
    ).scalars().all()
    has_more = len(rows) > limit_value
    rows = rows[:limit_value]
    next_cursor = rows[-1].id if has_more and rows else None
    return {
        "items": [_serialize_action(row) for row in rows],
        "total": total,
        "limit": limit_value,
        "cursor": cursor,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


@router.get("/summary")
async def explainability_summary(
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int = Query(default=1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    decision_filters = _base_filters(
        action="TRADE_DECISION",
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        since_minutes=window_minutes,
    )
    decision_rows = (
        await session.execute(
            select(
                func.lower(func.coalesce(meta_text(AdminAction.meta, "status"), "")).label("decision_status"),
                func.count().label("cnt"),
            )
            .where(and_(*decision_filters))
            .group_by(func.lower(func.coalesce(meta_text(AdminAction.meta, "status"), "")))
        )
    ).all()
    executed_total = 0
    rejected_total = 0
    skipped_total = 0
    decisions_total = 0
    for status_value, count_value in decision_rows:
        cnt = int(count_value or 0)
        decisions_total += cnt
        status_norm = (status_value or "").strip().lower()
        if status_norm == "executed":
            executed_total += cnt
        elif status_norm == "rejected":
            rejected_total += cnt
        elif status_norm == "skipped":
            skipped_total += cnt

    outcome_filters = _base_filters(
        action="TRADE_OUTCOME",
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        since_minutes=window_minutes,
    )
    rows = (
        await session.execute(
            select(AdminAction.meta)
            .where(and_(*outcome_filters))
            .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
        )
    ).all()

    outcomes_total = len(rows)
    if outcomes_total == 0:
        return {
            "decisions_total": decisions_total,
            "executed_total": executed_total,
            "rejected_total": rejected_total,
            "skipped_total": skipped_total,
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
        "decisions_total": decisions_total,
        "executed_total": executed_total,
        "rejected_total": rejected_total,
        "skipped_total": skipped_total,
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


@router.post("/test-executed")
async def explainability_test_executed(
    payload: TestExecutedRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    if not _flag_enabled("EXPLAINABILITY_ALLOW_TEST_BYPASS", "false"):
        logger.info(
            "explainability test-executed blocked: bypass flag disabled",
            extra={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
                "symbol": payload.symbol,
            },
        )
        raise HTTPException(
            status_code=403,
            detail="EXPLAINABILITY_ALLOW_TEST_BYPASS is disabled",
        )

    config = PaperTraderConfig.from_env()
    try:
        result = await place_order_unified(
            session,
            account_id=payload.account_id,
            symbol=payload.symbol,
            side=payload.side,
            qty=payload.qty,
            strategy_id=payload.strategy_config_id,
            fee_bps=config.fee_bps,
            slippage_bps=config.slippage_bps,
            price_override=payload.price_override,
            meta={"origin": "admin_explainability_test_executed"},
            bypass_max_order_notional=True,
        )
        if isinstance(result, RejectReason):
            await session.commit()
            return {
                "ok": False,
                "reject": result.dict(),
                "bypass_applied": "max_order_notional_usdt",
            }

        execution = result.execution
        await session.commit()
        return jsonable_encoder(
            {
                "ok": True,
                "bypass_applied": "max_order_notional_usdt",
                "execution": {
                    "order_id": execution.order.id,
                    "trade_id": execution.trade.id,
                    "symbol": result.prepared.symbol,
                    "side": result.prepared.side,
                    "qty": str(result.prepared.qty),
                    "price": str(result.prepared.price),
                    "price_source": result.price_source,
                    "strategy_id": str(result.strategy_id) if result.strategy_id is not None else None,
                    "strategy_config_id": payload.strategy_config_id,
                },
            }
        )
    except HTTPException:
        await session.rollback()
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception("explainability test-executed failed")
        raise HTTPException(status_code=500, detail=f"test-executed failed: {exc}")
