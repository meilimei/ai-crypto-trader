from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.admin_actions.throttled import write_admin_action_throttled
from ai_crypto_trader.services.notifications.outbox import enqueue_outbox_notification
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 60
THRESHOLD = 3


def _normalize_reject_code(code: object) -> str:
    raw = getattr(code, "value", None)
    text = str(raw) if raw is not None else str(code)
    text = text.strip()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper()


def _coerce_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _to_utc(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return _to_utc(parsed)
    return None


def _admin_action_values(
    *,
    account_id: int,
    strategy_id: UUID | str,
    symbol: str | None,
    dedupe_key: str,
    meta: dict,
) -> dict:
    values = {
        "action": "STRATEGY_REJECT_COUNTER",
        "status": "count",
        "message": None,
        "meta": meta,
        "dedupe_key": dedupe_key,
    }
    if hasattr(AdminAction, "account_id"):
        values["account_id"] = account_id
    if hasattr(AdminAction, "strategy_id"):
        values["strategy_id"] = strategy_id if isinstance(strategy_id, UUID) else str(strategy_id)
    if hasattr(AdminAction, "symbol"):
        values["symbol"] = symbol
    return values


async def bump_strategy_reject_counter(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id: UUID | str,
    symbol: str,
    reject_code: object,
    now_utc: datetime,
    window_seconds: int = WINDOW_SECONDS,
    threshold: int = THRESHOLD,
) -> Dict[str, Any]:
    _ = session
    now = _to_utc(now_utc)
    strategy_id_str = str(strategy_id)
    symbol_key = symbol or ""
    dedupe_key = f"STRATEGY_REJECT_COUNTER:{account_id}:{strategy_id_str}:{symbol_key}"
    code_norm = _normalize_reject_code(reject_code)

    count = 0
    payload_safe: dict | None = None

    async with AsyncSessionLocal() as db_session:
        try:
            existing = await db_session.scalar(
                select(AdminAction)
                .where(AdminAction.dedupe_key == dedupe_key)
                .order_by(AdminAction.created_at.desc())
                .limit(1)
            )
            existing_meta = existing.meta if isinstance(getattr(existing, "meta", None), dict) else {}
            first_seen_at = _parse_dt(existing_meta.get("first_seen_at"))
            window_delta = timedelta(seconds=window_seconds)

            reset_window = existing is None or first_seen_at is None or (now - first_seen_at) > window_delta

            by_code: Dict[str, int] = {}
            if not reset_window:
                raw_by_code = existing_meta.get("by_code") or {}
                if isinstance(raw_by_code, dict):
                    for key, value in raw_by_code.items():
                        by_code[str(key)] = _coerce_int(value, 0)

            if reset_window:
                count = 1
                first_seen_at = now
                by_code = {code_norm: 1}
            else:
                count = _coerce_int(existing_meta.get("count"), 0) + 1
                by_code[code_norm] = _coerce_int(by_code.get(code_norm), 0) + 1

            last_seen_at = now
            payload = {
                "account_id": str(account_id),
                "strategy_id": str(strategy_id),
                "symbol": str(symbol) if symbol is not None else None,
                "window_seconds": window_seconds,
                "threshold": threshold,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "count": count,
                "by_code": by_code,
                "last_reject_code": code_norm,
            }
            payload_safe = json_safe(payload)

            values = _admin_action_values(
                account_id=account_id,
                strategy_id=strategy_id,
                symbol=symbol,
                dedupe_key=dedupe_key,
                meta=payload_safe if isinstance(payload_safe, dict) else {"meta": payload_safe},
            )

            if existing is None:
                db_session.add(AdminAction(**values))
            else:
                await db_session.execute(
                    update(AdminAction)
                    .where(AdminAction.id == existing.id)
                    .values(**values)
                )
            await db_session.commit()
        except Exception:
            try:
                await db_session.rollback()
            finally:
                logger.exception(
                    "strategy_reject_counter failed",
                    extra={
                        "account_id": account_id,
                        "strategy_id": str(strategy_id),
                        "symbol": symbol,
                        "reject_code": code_norm,
                        "window_seconds": window_seconds,
                    },
                )
            raise

    logger.info(
        "strategy_reject_counter bumped",
        extra={
            "account_id": account_id,
            "strategy_id": str(strategy_id),
            "symbol": symbol,
            "reject_code": code_norm,
            "count": count,
            "window_seconds": window_seconds,
        },
    )

    if isinstance(payload_safe, dict):
        return payload_safe
    return {"meta": payload_safe}


async def maybe_alert_strategy_reject_burst(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id: UUID | str,
    symbol: str,
    reject_code: object,
    window_seconds: int = WINDOW_SECONDS,
    threshold: int = THRESHOLD,
    now_utc: datetime,
) -> None:
    counter_meta = await bump_strategy_reject_counter(
        session,
        account_id=account_id,
        strategy_id=strategy_id,
        symbol=symbol,
        reject_code=reject_code,
        now_utc=now_utc,
        window_seconds=window_seconds,
        threshold=threshold,
    )

    count = _coerce_int(counter_meta.get("count"), 0)
    if count < threshold:
        return

    symbol_key = symbol or ""
    inserted = await write_admin_action_throttled(
        None,
        action_type="STRATEGY_ALERT",
        account_id=account_id,
        symbol=symbol,
        status="REJECT_BURST",
        payload={
            "message": f"Strategy reject burst: {count} rejects in {window_seconds}s",
            "account_id": str(account_id),
            "strategy_id": str(strategy_id),
            "symbol": str(symbol) if symbol is not None else None,
            "window_seconds": window_seconds,
            "threshold": threshold,
            "count": count,
            "by_code": counter_meta.get("by_code") or {},
            "first_seen_at": counter_meta.get("first_seen_at"),
            "last_seen_at": counter_meta.get("last_seen_at"),
        },
        window_seconds=max(120, window_seconds),
        dedupe_key=f"STRATEGY_ALERT:REJECT_BURST:{account_id}:{strategy_id}:{symbol_key}",
        now_utc=now_utc,
    )
    if inserted:
        admin_action = await session.scalar(
            select(AdminAction)
            .where(
                AdminAction.dedupe_key == f"STRATEGY_ALERT:REJECT_BURST:{account_id}:{strategy_id}:{symbol_key}"
            )
            .order_by(AdminAction.created_at.desc())
            .limit(1)
        )
        if admin_action:
            try:
                await enqueue_outbox_notification(
                    session,
                    channel=None,
                    admin_action=admin_action,
                    dedupe_key=admin_action.dedupe_key,
                    payload={
                        "action": admin_action.action,
                        "status": admin_action.status,
                        "message": admin_action.message,
                        "meta": admin_action.meta,
                        "created_at": admin_action.created_at.isoformat() if admin_action.created_at else None,
                        "account_id": account_id,
                        "strategy_id": str(strategy_id),
                        "symbol": symbol,
                    },
                    now_utc=now_utc,
                )
            except Exception:
                logger.exception(
                    "outbox enqueue failed",
                    extra={
                        "admin_action_id": admin_action.id,
                        "status": admin_action.status,
                        "account_id": account_id,
                        "strategy_id": str(strategy_id),
                        "symbol": symbol,
                    },
                )
