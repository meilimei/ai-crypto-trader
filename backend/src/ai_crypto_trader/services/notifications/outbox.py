from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

import os

from ai_crypto_trader.common.models import NotificationOutbox
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


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
    stmt = stmt.on_conflict_do_nothing(index_elements=["admin_action_id"]).returning(NotificationOutbox.id)
    result = await session.execute(stmt)
    outbox_id = result.scalar()
    if outbox_id is not None:
        logger.info(
            "outbox enqueue created",
            extra={
                "admin_action_id": admin_action_id,
                "channel": channel_value,
                "dedupe_key": clean_dedupe,
                "outbox_id": outbox_id,
            },
        )
        return True

    logger.info(
        "outbox enqueue skipped",
        extra={
            "admin_action_id": admin_action_id,
            "channel": channel_value,
            "dedupe_key": clean_dedupe,
        },
    )
    return False


# Smoke verification:
# SELECT id, status, channel, admin_action_id, dedupe_key, created_at
# FROM notifications_outbox
# WHERE status = 'pending'
# ORDER BY created_at DESC
# LIMIT 10;
