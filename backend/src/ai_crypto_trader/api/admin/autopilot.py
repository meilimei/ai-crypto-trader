from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.autopilot.governor import (
    get_or_create_autopilot_state,
    pause_autopilot_state,
    resume_autopilot_state,
    serialize_autopilot_state,
)
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/autopilot", tags=["admin"], dependencies=[Depends(require_admin_token)])


class AutopilotPauseRequest(BaseModel):
    account_id: int = Field(..., ge=1)
    strategy_config_id: int = Field(..., ge=1)
    reason: str | None = None
    paused_seconds: int | None = Field(default=None, ge=1)


class AutopilotResumeRequest(BaseModel):
    account_id: int = Field(..., ge=1)
    strategy_config_id: int = Field(..., ge=1)
    reason: str | None = None


def _status_payload(state) -> dict[str, Any]:
    serialized = serialize_autopilot_state(state)
    return {
        "ok": True,
        "status": serialized,
        "current_status": serialized.get("status"),
        "reason": serialized.get("reason"),
        "meta": serialized.get("meta"),
        "updated_at": serialized.get("updated_at"),
    }


@router.post("/pause")
async def pause_autopilot(
    payload: AutopilotPauseRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        now_utc = datetime.now(timezone.utc)
        state = await pause_autopilot_state(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            reason=payload.reason or "manual_pause",
            paused_seconds=payload.paused_seconds,
            meta={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
                "reason": payload.reason or "manual_pause",
                "paused_seconds": payload.paused_seconds,
                "source": "admin_api",
            },
            actor="admin",
            audit_action="AUTOPILOT_PAUSED",
            now_utc=now_utc,
        )
        await session.commit()
        return _status_payload(state)
    except HTTPException:
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "autopilot pause failed",
            extra={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
            },
        )
        raise HTTPException(status_code=500, detail=f"autopilot pause failed: {exc}") from exc


@router.post("/resume")
async def resume_autopilot(
    payload: AutopilotResumeRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        now_utc = datetime.now(timezone.utc)
        state = await resume_autopilot_state(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            reason=payload.reason or "manual_resume",
            meta={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
                "reason": payload.reason or "manual_resume",
                "source": "admin_api",
            },
            actor="admin",
            audit_action="AUTOPILOT_RESUMED",
            now_utc=now_utc,
        )
        await session.commit()
        return _status_payload(state)
    except HTTPException:
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "autopilot resume failed",
            extra={
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
            },
        )
        raise HTTPException(status_code=500, detail=f"autopilot resume failed: {exc}") from exc


@router.get("/status")
async def get_autopilot_status(
    account_id: int = Query(..., ge=1),
    strategy_config_id: int = Query(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    try:
        now_utc = datetime.now(timezone.utc)
        state = await get_or_create_autopilot_state(
            session,
            account_id=account_id,
            strategy_config_id=strategy_config_id,
            now_utc=now_utc,
        )
        session.add(
            AdminAction(
                action="AUTOPILOT_STATUS",
                status=state.status,
                message="Autopilot status queried",
                meta=json_safe(
                    {
                        "account_id": account_id,
                        "strategy_config_id": strategy_config_id,
                        "autopilot_state": serialize_autopilot_state(state),
                        "source": "admin_api",
                    }
                ),
            )
        )
        await session.commit()
        return _status_payload(state)
    except HTTPException:
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception(
            "autopilot status failed",
            extra={"account_id": account_id, "strategy_config_id": strategy_config_id},
        )
        raise HTTPException(status_code=500, detail=f"autopilot status failed: {exc}") from exc
