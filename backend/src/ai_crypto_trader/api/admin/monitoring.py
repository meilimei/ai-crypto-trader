from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import BigInteger, and_, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.utils.json_safe import json_safe

router = APIRouter(prefix="/admin/monitoring", tags=["admin"], dependencies=[Depends(require_admin_token)])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200

# Include STRATEGY_STALL_TICK to help inspect monitoring cadence alongside alerts.
MONITORING_ACTIONS: tuple[str, ...] = (
    "STRATEGY_ALERT",
    "STRATEGY_REJECT_COUNTER",
    "EQUITY_RISK_STATE",
    "STRATEGY_STALL_TICK",
)


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


def meta_account_id(col):  # type: ignore[no-untyped-def]
    return cast(meta_text(col, "account_id"), BigInteger)


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


@router.get("/strategy-alerts")
async def get_strategy_alerts(
    limit: int = Query(DEFAULT_LIMIT, description="Number of rows to return (max 200)"),
    account_id: int | None = Query(default=None, description="Filter by meta.account_id"),
    strategy_id: str | None = Query(default=None, description="Filter by meta.strategy_id"),
    symbol: str | None = Query(default=None, description="Filter by symbol (normalized, uppercased)"),
    status: str | None = Query(default=None, description="Filter by AdminAction.status"),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    filters = [AdminAction.action.in_(MONITORING_ACTIONS)]

    if status:
        filters.append(AdminAction.status == status.strip())
    if account_id is not None:
        filters.append(meta_account_id(AdminAction.meta) == account_id)
    if strategy_id:
        filters.append(meta_strategy_id(AdminAction.meta) == strategy_id.strip())
    if symbol:
        symbol_key = symbol.strip().upper()
        if symbol_key:
            filters.append(meta_symbol_upper(AdminAction.meta) == symbol_key)

    stmt = (
        select(AdminAction)
        .where(and_(*filters))
        .order_by(AdminAction.created_at.desc())
        .limit(bounded_limit)
    )
    result = await session.execute(stmt)
    items = [_serialize_admin_action(row) for row in result.scalars().all()]
    return {"ok": True, "items": items}


# Manual verification:
# curl -sS "$BASE_URL/api/admin/monitoring/strategy-alerts?limit=5&account_id=5&strategy_id=1&symbol=ETHUSDT" \
#   -H "X-Admin-Token: dev-admin-123"

