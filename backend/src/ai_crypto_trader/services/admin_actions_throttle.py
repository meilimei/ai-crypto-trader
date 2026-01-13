from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.utils.json_safe import json_safe


async def log_admin_action_throttled(
    session: AsyncSession,
    *,
    action_type: str,
    status: str,
    message: str,
    account_id: Optional[int],
    symbol: Optional[str] = None,
    reason_code: Optional[str] = None,
    cooldown_seconds: int = 60,
    payload_json: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Insert an AdminAction with a dedupe key if no recent matching row exists.
    """
    try:
        dedupe_key = f"{action_type}:{account_id or '*'}:{symbol or '*'}:{reason_code or '*'}"
        window_start = datetime.now(timezone.utc) - timedelta(seconds=cooldown_seconds)
        existing = await session.scalar(
            select(AdminAction)
            .where(AdminAction.dedupe_key == dedupe_key, AdminAction.created_at >= window_start)
            .order_by(AdminAction.created_at.desc())
        )
        if existing:
            return False

        meta = payload_json or {}
        meta["dedupe_key"] = dedupe_key
        session.add(
            AdminAction(
                action=action_type,
                status=status,
                message=message,
                meta=json_safe(meta),
                dedupe_key=dedupe_key,
            )
        )
        await session.commit()
        return True
    except Exception:
        try:
            await session.rollback()
        finally:
            return False
