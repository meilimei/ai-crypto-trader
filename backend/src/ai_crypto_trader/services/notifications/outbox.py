from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

import os

from ai_crypto_trader.common.models import NotificationOutbox
from ai_crypto_trader.utils.json_safe import json_safe


async def enqueue_outbox_notification(
    session: AsyncSession,
    *,
    channel: str | None,
    admin_action_id: int,
    dedupe_key: str | None,
    payload: dict[str, Any],
    now_utc,
) -> bool:
    channel_value = (channel or os.getenv("NOTIFICATIONS_DEFAULT_CHANNEL") or "log").strip() or "log"
    payload_safe = json_safe(payload or {})
    clean_dedupe = dedupe_key.strip() if isinstance(dedupe_key, str) else None
    if clean_dedupe == "":
        clean_dedupe = None
    stmt = insert(NotificationOutbox).values(
        status="pending",
        channel=channel_value,
        admin_action_id=admin_action_id,
        dedupe_key=clean_dedupe,
        payload=payload_safe if isinstance(payload_safe, dict) else {"meta": payload_safe},
        attempt_count=0,
        next_attempt_at=now_utc,
    )
    if clean_dedupe:
        stmt = stmt.on_conflict_do_nothing(index_elements=["channel", "dedupe_key"])
    result = await session.execute(stmt)
    return bool(getattr(result, "rowcount", 0))


# Smoke verification:
# SELECT id, status, channel, admin_action_id, dedupe_key, created_at
# FROM notifications_outbox
# WHERE status = 'pending'
# ORDER BY created_at DESC
# LIMIT 10;
