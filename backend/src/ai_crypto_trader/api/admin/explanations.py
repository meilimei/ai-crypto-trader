from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.services.admin.pagination import clamp_limit_offset
from ai_crypto_trader.services.explainability.store import (
    get_trade_explanation,
    list_trade_explanations,
    serialize_trade_explanation,
    summarize_trade_explanations,
)

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])


@router.get("/explanations")
async def list_explanations_endpoint(
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
    total, rows = await list_trade_explanations(
        session,
        limit=limit_value,
        offset=offset_value,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        status=status,
        since_minutes=since_minutes,
    )
    return {
        "items": [serialize_trade_explanation(row) for row in rows],
        "total": total,
        "limit": limit_value,
        "offset": offset_value,
    }


@router.get("/explanations/summary")
async def explanations_summary_endpoint(
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    window_minutes: int = Query(default=1440, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await summarize_trade_explanations(
        session,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        window_minutes=window_minutes,
    )


@router.get("/explanations/{explanation_id}")
async def get_explanation_endpoint(
    explanation_id: int,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    row = await get_trade_explanation(session, explanation_id=explanation_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Explanation not found")
    return {"item": serialize_trade_explanation(row)}

