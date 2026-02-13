from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction, ExchangeOrder
from ai_crypto_trader.services.live_exchange import (
    create_live_exchange_order,
    dispatch_live_exchange_orders_once,
    get_live_pause_state,
    list_live_exchange_orders,
    pause_live_autopilot,
    resume_live_autopilot,
)
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/live", tags=["admin"], dependencies=[Depends(require_admin_token)])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _bounded_limit(value: int) -> int:
    try:
        limit = int(value)
    except Exception:
        return DEFAULT_LIMIT
    if limit < 1:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


def _serialize_exchange_order(row: ExchangeOrder | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": str(row.id),
        "exchange": row.exchange,
        "account_id": row.account_id,
        "strategy_config_id": row.strategy_config_id,
        "symbol": row.symbol,
        "side": row.side,
        "order_type": row.order_type,
        "qty": str(row.qty),
        "price": str(row.price) if row.price is not None else None,
        "idempotency_key": row.idempotency_key,
        "client_order_id": row.client_order_id,
        "status": row.status,
        "exchange_order_id": row.exchange_order_id,
        "attempts": row.attempts,
        "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
        "last_error": row.last_error,
        "meta": json_safe(row.meta) if row.meta is not None else {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


class CreateLiveOrderRequest(BaseModel):
    account_id: int = Field(..., ge=1)
    strategy_config_id: int | None = Field(default=None, ge=1)
    exchange: str | None = None
    symbol: str
    side: str
    qty: Decimal
    order_type: str | None = "market"
    price: Decimal | None = None
    idempotency_key: str | None = None


class PauseLiveRequest(BaseModel):
    reason: str | None = None
    paused_seconds: int | None = Field(default=None, ge=1)


class ResumeLiveRequest(BaseModel):
    reason: str | None = None


class TickLiveRequest(BaseModel):
    limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)


@router.post("/orders")
async def create_live_order(
    payload: CreateLiveOrderRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    if payload.qty <= 0:
        return {"ok": False, "reject": {"code": "QTY_ZERO", "reason": "Quantity must be positive"}}
    try:
        result = await create_live_exchange_order(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            exchange=payload.exchange,
            symbol=payload.symbol,
            side=payload.side,
            qty=payload.qty,
            order_type=payload.order_type,
            price=payload.price,
            idempotency_key=payload.idempotency_key,
        )
        if not result.ok:
            session.add(
                AdminAction(
                    action="LIVE_ORDER_REJECTED",
                    status="rejected",
                    message=(result.reject or {}).get("reason") if isinstance(result.reject, dict) else "Live order rejected",
                    meta=json_safe(
                        {
                            "account_id": payload.account_id,
                            "strategy_config_id": payload.strategy_config_id,
                            "exchange": payload.exchange,
                            "symbol": payload.symbol,
                            "side": payload.side,
                            "qty": str(payload.qty),
                            "order_type": payload.order_type,
                            "price": str(payload.price) if payload.price is not None else None,
                            "idempotency_key": payload.idempotency_key,
                            "reject": result.reject,
                            "policy_source": result.policy_source,
                            "policy_binding": result.policy_binding,
                            "computed_limits": result.computed_limits,
                        }
                    ),
                )
            )
            await session.commit()
            response = {
                "ok": False,
                "reject": result.reject,
                "policy_source": result.policy_source,
                "policy_binding": result.policy_binding,
                "computed_limits": result.computed_limits,
            }
            if result.order is not None:
                response["created"] = bool(result.created)
                response["exchange_order"] = _serialize_exchange_order(result.order)
            return response

        await session.commit()
        return {
            "ok": True,
            "created": result.created,
            "exchange_order": _serialize_exchange_order(result.order),
            "policy_source": result.policy_source,
            "policy_binding": result.policy_binding,
            "computed_limits": result.computed_limits,
        }
    except HTTPException:
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception("live order enqueue failed", extra={"account_id": payload.account_id, "symbol": payload.symbol})
        raise HTTPException(status_code=500, detail=f"Failed to enqueue live order: {exc}") from exc


@router.get("/orders")
@router.get("/exchange-orders")
async def get_live_orders(
    exchange: str | None = Query(default=None),
    account_id: int | None = Query(default=None),
    strategy_config_id: int | None = Query(default=None),
    symbol: str | None = Query(default=None),
    status: str | None = Query(default=None),
    since: datetime | None = Query(default=None),
    until: datetime | None = Query(default=None),
    limit: int = Query(DEFAULT_LIMIT),
    cursor: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    rows, next_cursor = await list_live_exchange_orders(
        session,
        exchange=exchange,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
        status=status,
        since=since,
        until=until,
        limit=bounded_limit,
        cursor=cursor,
    )
    return {
        "ok": True,
        "items": [_serialize_exchange_order(row) for row in rows],
        "next_cursor": next_cursor,
        "limit": bounded_limit,
    }


@router.get("/status")
async def get_live_status(
    account_id: int | None = Query(default=None, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    state = await get_live_pause_state(session, account_id=account_id)
    return {"ok": True, "status": state}


@router.get("/state")
async def get_live_state(
    account_id: int | None = Query(default=None, ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    state = await get_live_pause_state(session, account_id=account_id)
    return {"ok": True, "paused": bool(state.get("paused")), "reason": state.get("reason"), "updated_at": state.get("updated_at"), "meta": state.get("meta") or {}}


@router.post("/pause")
async def pause_live(
    payload: PauseLiveRequest = Body(default_factory=PauseLiveRequest),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        await pause_live_autopilot(
            session,
            reason=payload.reason or "manual_pause",
            actor="admin",
            paused_seconds=payload.paused_seconds,
            meta={"source": "admin_api"},
        )
        await session.commit()
        state = await get_live_pause_state(session)
        return {"ok": True, "status": state}
    except Exception as exc:
        await session.rollback()
        logger.exception("live pause failed")
        raise HTTPException(status_code=500, detail=f"Failed to pause live autopilot: {exc}") from exc


@router.post("/resume")
async def resume_live(
    payload: ResumeLiveRequest = Body(default_factory=ResumeLiveRequest),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        await resume_live_autopilot(
            session,
            reason=payload.reason or "manual_resume",
            actor="admin",
            meta={"source": "admin_api"},
        )
        await session.commit()
        state = await get_live_pause_state(session)
        return {"ok": True, "status": state}
    except Exception as exc:
        await session.rollback()
        logger.exception("live resume failed")
        raise HTTPException(status_code=500, detail=f"Failed to resume live autopilot: {exc}") from exc


@router.post("/tick")
@router.post("/exchange-orders/tick")
async def run_live_tick(
    payload: TickLiveRequest = Body(default_factory=TickLiveRequest),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    try:
        stats = await dispatch_live_exchange_orders_once(
            session,
            now_utc=now_utc,
            limit=_bounded_limit(payload.limit),
        )
        result = json_safe(stats)
        return {
            "ok": True,
            "picked": int(result.get("picked_count", 0) or 0),
            "submitted": int(result.get("sent_count", 0) or 0),
            "rejected": int(result.get("rejected_count", 0) or 0),
            "rejected_local": int(result.get("rejected_local_count", result.get("rejected_count", 0)) or 0),
            "retried": int(result.get("retried_count", 0) or 0),
            "failed": int(result.get("failed_count", 0) or 0),
            "paused_triggered": bool(result.get("paused", False)),
            "stats": result,
        }
    except Exception as exc:
        await session.rollback()
        logger.exception("live tick failed")
        raise HTTPException(status_code=500, detail=f"Failed to run live tick: {exc}") from exc


@router.get("/exchange-orders/{order_id}")
async def get_live_order_by_id(
    order_id: str,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        order_uuid = UUID(str(order_id))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid exchange order id")
    row = await session.get(ExchangeOrder, order_uuid)
    if row is None:
        raise HTTPException(status_code=404, detail="Exchange order not found")
    return {"ok": True, "item": _serialize_exchange_order(row)}
