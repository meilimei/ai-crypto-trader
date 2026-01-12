from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.paper_trader.maintenance import normalize_status
from ai_crypto_trader.utils.json_safe import json_safe


async def add_action_deduped(
    session: AsyncSession,
    *,
    action: str,
    status: str,
    message: str,
    meta: dict | None,
    cooldown_seconds: int = 60,
) -> bool:
    """
    Insert an AdminAction unless an equivalent recent row exists.

    Returns True if inserted, False if deduped or on error.
    """
    try:
        meta_safe = json_safe(meta or {})
        if not isinstance(meta_safe, dict):
            meta_safe = {"value": meta_safe}
        status_norm = normalize_status(status)
        window_start = datetime.now(timezone.utc) - timedelta(seconds=cooldown_seconds)

        filters: list[Any] = [
            AdminAction.action == action,
            AdminAction.message == message,
            AdminAction.created_at >= window_start,
        ]
        symbol_val = meta_safe.get("symbol")
        if symbol_val is not None:
            filters.append(AdminAction.meta["symbol"].as_string() == str(symbol_val))
        account_val = meta_safe.get("account_id")
        if account_val is not None:
            filters.append(AdminAction.meta["account_id"].as_string() == str(account_val))

        existing = await session.scalar(
            select(AdminAction).where(*filters).order_by(AdminAction.created_at.desc())
        )
        if existing:
            return False

        session.add(
            AdminAction(
                action=action,
                status=status_norm,
                message=message,
                meta=meta_safe,
            )
        )
        await session.commit()
        return True
    except Exception:
        try:
            await session.rollback()
        finally:
            return False
