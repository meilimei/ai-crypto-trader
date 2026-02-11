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
from ai_crypto_trader.services.universe_selector import rollback_universe, select_universe
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/universe", tags=["admin"], dependencies=[Depends(require_admin_token)])


def _meta_text(col, key):  # type: ignore[no-untyped-def]
    return col.op("->>")(key)


def _serialize_admin_action(row: AdminAction) -> dict[str, Any]:
    return {
        "id": row.id,
        "action": row.action,
        "status": row.status,
        "message": row.message,
        "dedupe_key": row.dedupe_key,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "meta": json_safe(row.meta) if row.meta is not None else {},
    }


class UniverseSelectRequest(BaseModel):
    strategy_config_id: int
    top_n: int | None = None
    window_minutes: int | None = None


class UniverseRollbackRequest(BaseModel):
    admin_action_id: int


@router.post("/select")
async def select_universe_endpoint(
    payload: UniverseSelectRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    try:
        result = await select_universe(
            session,
            strategy_config_id=payload.strategy_config_id,
            now_utc=now_utc,
            top_n=payload.top_n,
            window_minutes=payload.window_minutes,
        )
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "UNIVERSE_SELECT_FAILED",
            extra={
                "strategy_config_id": payload.strategy_config_id,
                "top_n": payload.top_n,
                "window_minutes": payload.window_minutes,
            },
        )
        raise HTTPException(status_code=500, detail=f"Universe selection failed: {exc}") from exc

    return {
        "ok": True,
        "admin_action_id": result.get("admin_action_id"),
        "strategy_config_id": result.get("strategy_config_id"),
        "previous_symbols": result.get("previous_symbols", []),
        "new_symbols": result.get("new_symbols", []),
        "picked_by": result.get("picked_by"),
        "source": result.get("source"),
        "source_reason": result.get("source_reason"),
        "stats_per_symbol": result.get("stats_per_symbol", []),
    }


@router.post("/rollback")
async def rollback_universe_endpoint(
    payload: UniverseRollbackRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    try:
        result = await rollback_universe(
            session,
            admin_action_id=payload.admin_action_id,
            now_utc=now_utc,
        )
        await session.commit()
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "UNIVERSE_ROLLBACK_FAILED",
            extra={"admin_action_id": payload.admin_action_id},
        )
        raise HTTPException(status_code=500, detail=f"Universe rollback failed: {exc}") from exc

    return {
        "ok": True,
        "strategy_config_id": result.get("strategy_config_id"),
        "selection_admin_action_id": result.get("selection_admin_action_id"),
        "rollback_admin_action_id": result.get("admin_action_id"),
        "restored_symbols": result.get("restored_symbols", []),
    }


@router.get("/history")
async def universe_history(
    strategy_config_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=50, ge=1),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    limit_value, offset_value = clamp_limit_offset(limit=limit, offset=offset, default_limit=50, max_limit=200)
    filters: list[Any] = [
        AdminAction.action.in_(("UNIVERSE_SELECTION", "UNIVERSE_ROLLBACK")),
    ]
    if strategy_config_id is not None:
        filters.append(_meta_text(AdminAction.meta, "strategy_config_id") == str(strategy_config_id))

    total_stmt = select(func.count()).select_from(AdminAction).where(and_(*filters))
    total = int((await session.execute(total_stmt)).scalar_one() or 0)

    rows = (
        await session.execute(
            select(AdminAction)
            .where(and_(*filters))
            .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
            .offset(offset_value)
            .limit(limit_value)
        )
    ).scalars().all()

    return {
        "items": [_serialize_admin_action(row) for row in rows],
        "limit": limit_value,
        "offset": offset_value,
        "total": total,
    }
