from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.monitoring.strategy_monitor import compute_strategy_health
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

router = APIRouter(prefix="/admin/strategy-monitoring", tags=["admin"], dependencies=[Depends(require_admin_token)])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _bounded_limit(value: int) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if limit < 1:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


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


def meta_account_id_text(col):  # type: ignore[no-untyped-def]
    return meta_text(col, "account_id")


def meta_strategy_id(col):  # type: ignore[no-untyped-def]
    return meta_text(col, "strategy_id")


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


@router.get("/health")
async def get_strategy_monitor_health(
    limit: int = Query(DEFAULT_LIMIT, description="Number of rows to return (max 200)"),
    offset: int = Query(0, ge=0),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int = Query(60, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    symbol_norm = normalize_symbol(symbol) if symbol else None
    rows = await compute_strategy_health(
        session,
        window_minutes=window_minutes,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol_norm,
        now_utc=datetime.now(timezone.utc),
    )
    total = len(rows)
    paged = rows[offset : offset + bounded_limit]
    return {
        "items": [row.to_dict() for row in paged],
        "total": total,
        "limit": bounded_limit,
        "offset": offset,
    }


@router.get("/alerts")
async def get_strategy_monitor_alerts(
    limit: int = Query(DEFAULT_LIMIT, description="Number of rows to return (max 200)"),
    offset: int = Query(0, ge=0),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    type: str | None = Query(default=None),
    since_minutes: int = Query(1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    symbol_key = normalize_symbol(symbol) if symbol else None
    since = datetime.now(timezone.utc) - timedelta(minutes=max(since_minutes, 1))

    filters = [
        AdminAction.action == "STRATEGY_ALERT",
        AdminAction.created_at >= since,
    ]
    if type:
        filters.append(meta_text(AdminAction.meta, "type") == type.strip().upper())
    if account_id is not None:
        filters.append(meta_account_id_text(AdminAction.meta) == str(account_id))
    if strategy_config_id is not None:
        filters.append(meta_strategy_id(AdminAction.meta) == str(strategy_config_id))
    if symbol_key:
        filters.append(meta_symbol_upper(AdminAction.meta) == symbol_key)

    count_stmt = select(func.count()).select_from(AdminAction).where(and_(*filters))
    total = int((await session.execute(count_stmt)).scalar_one() or 0)

    stmt = (
        select(AdminAction)
        .where(and_(*filters))
        .order_by(AdminAction.created_at.desc(), AdminAction.id.asc())
        .offset(offset)
        .limit(bounded_limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return {
        "items": [_serialize_admin_action(row) for row in rows],
        "total": total,
        "limit": bounded_limit,
        "offset": offset,
    }


@router.get("/ticks")
async def get_strategy_monitor_ticks(
    limit: int = Query(DEFAULT_LIMIT, description="Number of rows to return (max 200)"),
    offset: int = Query(0, ge=0),
    since_minutes: int = Query(180, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    since = datetime.now(timezone.utc) - timedelta(minutes=max(since_minutes, 1))
    filters = [
        AdminAction.action == "STRATEGY_MONITOR_TICK",
        AdminAction.created_at >= since,
    ]

    count_stmt = select(func.count()).select_from(AdminAction).where(and_(*filters))
    total = int((await session.execute(count_stmt)).scalar_one() or 0)
    stmt = (
        select(AdminAction)
        .where(and_(*filters))
        .order_by(AdminAction.created_at.desc(), AdminAction.id.asc())
        .offset(offset)
        .limit(bounded_limit)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return {
        "items": [_serialize_admin_action(row) for row in rows],
        "total": total,
        "limit": bounded_limit,
        "offset": offset,
    }
