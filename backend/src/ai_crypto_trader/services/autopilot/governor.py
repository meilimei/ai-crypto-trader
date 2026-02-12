from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, AutopilotState
from ai_crypto_trader.utils.json_safe import json_safe

AUTOPILOT_STATUS_RUNNING = "running"
AUTOPILOT_STATUS_PAUSED = "paused"
AUTOPILOT_STATUS_ERROR = "error"
AUTOPILOT_STATUSES = {
    AUTOPILOT_STATUS_RUNNING,
    AUTOPILOT_STATUS_PAUSED,
    AUTOPILOT_STATUS_ERROR,
}


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def is_autopilot_paused(
    *,
    status: str | None,
    paused_until: datetime | None,
    now_utc: datetime | None = None,
) -> bool:
    if (status or "").strip().lower() != AUTOPILOT_STATUS_PAUSED:
        return False
    if paused_until is None:
        return True
    now = _to_utc(now_utc or datetime.now(timezone.utc))
    return _to_utc(paused_until) > now


def serialize_autopilot_state(state: AutopilotState) -> dict[str, Any]:
    meta_safe = json_safe(state.meta if isinstance(state.meta, dict) else {})
    if not isinstance(meta_safe, dict):
        meta_safe = {"value": meta_safe}
    return {
        "id": state.id,
        "account_id": state.account_id,
        "strategy_config_id": state.strategy_config_id,
        "status": state.status,
        "reason": state.reason,
        "meta": meta_safe,
        "paused_until": state.paused_until.isoformat() if state.paused_until else None,
        "created_at": state.created_at.isoformat() if state.created_at else None,
        "updated_at": state.updated_at.isoformat() if state.updated_at else None,
    }


async def _write_audit_action(
    session: AsyncSession,
    *,
    action: str,
    state: AutopilotState,
    actor: str,
    reason: str | None,
    payload_meta: dict[str, Any] | None = None,
) -> None:
    meta = {
        "actor": actor,
        "reason": reason,
        "autopilot_state": serialize_autopilot_state(state),
    }
    if payload_meta:
        meta["payload"] = json_safe(payload_meta)
    session.add(
        AdminAction(
            action=action,
            status=state.status,
            message=reason or f"Autopilot {state.status}",
            meta=meta,
        )
    )


async def get_or_create_autopilot_state(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int,
    for_update: bool = False,
    now_utc: datetime | None = None,
) -> AutopilotState:
    now = _to_utc(now_utc or datetime.now(timezone.utc))
    stmt = select(AutopilotState).where(
        AutopilotState.account_id == int(account_id),
        AutopilotState.strategy_config_id == int(strategy_config_id),
    )
    if for_update:
        stmt = stmt.with_for_update()
    state = await session.scalar(stmt.limit(1))
    if state is None:
        state = AutopilotState(
            account_id=int(account_id),
            strategy_config_id=int(strategy_config_id),
            status=AUTOPILOT_STATUS_RUNNING,
            reason="initialized",
            meta={},
            paused_until=None,
            created_at=now,
            updated_at=now,
        )
        session.add(state)
        await session.flush()
        return state

    if (
        (state.status or "").strip().lower() == AUTOPILOT_STATUS_PAUSED
        and state.paused_until is not None
        and _to_utc(state.paused_until) <= now
    ):
        state.status = AUTOPILOT_STATUS_RUNNING
        state.reason = "pause_timeout_elapsed"
        state.paused_until = None
        state.updated_at = now
        await session.flush()

    return state


async def pause_autopilot_state(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int,
    reason: str | None = None,
    paused_seconds: int | None = None,
    meta: dict[str, Any] | None = None,
    actor: str = "admin",
    audit_action: str = "AUTOPILOT_PAUSED",
    force_audit: bool = True,
    now_utc: datetime | None = None,
) -> AutopilotState:
    now = _to_utc(now_utc or datetime.now(timezone.utc))
    state = await get_or_create_autopilot_state(
        session,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        for_update=True,
        now_utc=now,
    )
    previous_status = state.status
    previous_reason = state.reason
    previous_paused_until = state.paused_until
    state.status = AUTOPILOT_STATUS_PAUSED
    state.reason = reason or "paused"
    state.meta = json_safe(meta if isinstance(meta, dict) else {}) if meta is not None else state.meta
    if paused_seconds is not None and paused_seconds > 0:
        state.paused_until = now + timedelta(seconds=int(paused_seconds))
    else:
        state.paused_until = None
    state.updated_at = now
    await session.flush()
    state_changed = (
        previous_status != state.status
        or previous_reason != state.reason
        or previous_paused_until != state.paused_until
    )
    if force_audit or state_changed:
        await _write_audit_action(
            session,
            action=audit_action,
            state=state,
            actor=actor,
            reason=state.reason,
            payload_meta=meta,
        )
    return state


async def resume_autopilot_state(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int,
    reason: str | None = None,
    meta: dict[str, Any] | None = None,
    actor: str = "admin",
    audit_action: str = "AUTOPILOT_RESUMED",
    force_audit: bool = True,
    now_utc: datetime | None = None,
) -> AutopilotState:
    now = _to_utc(now_utc or datetime.now(timezone.utc))
    state = await get_or_create_autopilot_state(
        session,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        for_update=True,
        now_utc=now,
    )
    previous_status = state.status
    previous_reason = state.reason
    previous_paused_until = state.paused_until
    state.status = AUTOPILOT_STATUS_RUNNING
    state.reason = reason or "resumed"
    state.paused_until = None
    state.updated_at = now
    if meta is not None:
        state.meta = json_safe(meta if isinstance(meta, dict) else {})
    await session.flush()
    state_changed = (
        previous_status != state.status
        or previous_reason != state.reason
        or previous_paused_until != state.paused_until
    )
    if force_audit or state_changed:
        await _write_audit_action(
            session,
            action=audit_action,
            state=state,
            actor=actor,
            reason=state.reason,
            payload_meta=meta,
        )
    return state
