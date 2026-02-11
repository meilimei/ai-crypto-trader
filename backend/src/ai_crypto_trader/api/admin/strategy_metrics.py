from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.admin.pagination import clamp_limit_offset
from ai_crypto_trader.services.monitoring.strategy_metrics import run_strategy_metrics_once
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/strategy-metrics", tags=["admin"], dependencies=[Depends(require_admin_token)])


def meta_text(col, key):  # type: ignore[no-untyped-def]
    return col.op("->>")(key)


def meta_symbol_text(col):  # type: ignore[no-untyped-def]
    return func.coalesce(
        meta_text(col, "symbol_normalized"),
        meta_text(col, "symbol"),
        meta_text(col, "symbol_in"),
    )


def meta_symbol_upper(col):  # type: ignore[no-untyped-def]
    return func.upper(meta_symbol_text(col))


def _serialize_admin_action(row: AdminAction) -> dict[str, Any]:
    return {
        "id": row.id,
        "action": row.action,
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "dedupe_key": row.dedupe_key,
        "message": row.message,
        "meta": json_safe(row.meta) if row.meta is not None else {},
    }


def _snapshot_filters(
    *,
    account_id: int | None,
    strategy_config_id: int | None,
    symbol: str | None,
    window_minutes: int | None,
) -> list[Any]:
    filters: list[Any] = [AdminAction.action == "STRATEGY_METRICS_SNAPSHOT"]
    if account_id is not None:
        filters.append(meta_text(AdminAction.meta, "account_id") == str(account_id))
    if strategy_config_id is not None:
        filters.append(meta_text(AdminAction.meta, "strategy_config_id") == str(strategy_config_id))
    if symbol:
        filters.append(meta_symbol_upper(AdminAction.meta) == normalize_symbol(symbol))
    if window_minutes is not None:
        filters.append(meta_text(AdminAction.meta, "window_minutes") == str(max(int(window_minutes), 1)))
    return filters


class StrategyMetricsRunOnceRequest(BaseModel):
    account_id: int | None = None
    strategy_config_id: int | None = None
    symbol: str | None = None
    window_minutes: int | None = None
    max_pairs: int | None = None


@router.get("/snapshots")
async def get_strategy_metrics_snapshots(
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int | None = Query(default=None, ge=1),
    limit: int = Query(50),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, offset_value = clamp_limit_offset(limit=limit, offset=offset, default_limit=50, max_limit=200)
    filters = _snapshot_filters(
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        window_minutes=window_minutes,
    )
    total_stmt = select(func.count()).select_from(AdminAction).where(and_(*filters))
    total = int((await session.execute(total_stmt)).scalar_one() or 0)
    stmt = (
        select(AdminAction)
        .where(and_(*filters))
        .order_by(AdminAction.created_at.desc(), AdminAction.id.asc())
        .offset(offset_value)
        .limit(limit_value)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return {
        "items": [_serialize_admin_action(row) for row in rows],
        "total": total,
        "limit": limit_value,
        "offset": offset_value,
    }


@router.get("/latest")
async def get_strategy_metrics_latest(
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int | None = Query(default=None, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    filters = _snapshot_filters(
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        window_minutes=window_minutes,
    )
    row = await session.scalar(
        select(AdminAction)
        .where(and_(*filters))
        .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
        .limit(1)
    )
    return {"item": _serialize_admin_action(row) if row else {}}


@router.post("/run-once")
async def run_strategy_metrics_once_endpoint(
    body: StrategyMetricsRunOnceRequest | None = None,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    payload = body or StrategyMetricsRunOnceRequest()
    now_utc = datetime.now(timezone.utc)
    try:
        stats = await run_strategy_metrics_once(
            session,
            now_utc=now_utc,
            window_minutes=payload.window_minutes,
            max_pairs=payload.max_pairs,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            symbol=payload.symbol,
        )
        latest_filters = _snapshot_filters(
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            symbol=payload.symbol,
            window_minutes=payload.window_minutes,
        )
        latest_rows = (
            await session.execute(
                select(AdminAction)
                .where(and_(*latest_filters))
                .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
                .limit(10)
            )
        ).scalars().all()
        await session.commit()
        return {
            "ok": True,
            "stats": json_safe(stats),
            "latest_snapshots": [_serialize_admin_action(row) for row in latest_rows],
        }
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "strategy_metrics run-once failed",
            extra={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
                "symbol": payload.symbol,
                "window_minutes": payload.window_minutes,
                "max_pairs": payload.max_pairs,
            },
        )
        raise HTTPException(status_code=500, detail=f"Failed to run strategy metrics: {exc}") from exc
