from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, NotificationOutbox, utc_now

logger = logging.getLogger(__name__)


def _next_backoff_seconds(attempt_count: int) -> int:
    # Exponential backoff with a reasonable cap.
    base = max(attempt_count, 1)
    return min(300, 2 ** base)


def _resolve_channel(row: NotificationOutbox) -> str:
    raw = (row.channel or "").strip()
    if raw:
        return raw
    payload = row.payload if isinstance(row.payload, dict) else {}
    payload_channel = payload.get("channel") if isinstance(payload, dict) else None
    return str(payload_channel or "").strip()


async def _fetch_due_outbox(
    session: AsyncSession,
    *,
    now_utc: datetime,
    limit: int,
) -> list[NotificationOutbox]:
    stmt = (
        select(NotificationOutbox)
        .where(
            NotificationOutbox.status == "pending",
            or_(NotificationOutbox.next_attempt_at.is_(None), NotificationOutbox.next_attempt_at <= now_utc),
        )
        .order_by(NotificationOutbox.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def dispatch_outbox(
    session: AsyncSession,
    *,
    now_utc: datetime | None = None,
    limit: int = 50,
) -> int:
    now = now_utc or utc_now()
    delivered = 0
    rows = await _fetch_due_outbox(session, now_utc=now, limit=limit)
    if not rows:
        return 0

    for row in rows:
        row.attempt_count = (row.attempt_count or 0) + 1
        row.updated_at = now
        try:
            resolved_channel = _resolve_channel(row)
            channel = resolved_channel.lower()
            if channel in {"noop", "log"}:
                logger.info(
                    "outbox dispatch noop/log",
                    extra={
                        "outbox_id": row.id,
                        "admin_action_id": row.admin_action_id,
                        "channel": row.channel,
                        "dedupe_key": row.dedupe_key,
                    },
                )
                row.status = "sent"
                row.last_error = None
                row.next_attempt_at = None
                session.add(
                    AdminAction(
                        action="NOTIFICATION_SENT",
                        status="ok",
                        message="Notification dispatched to log channel",
                        meta={
                            "outbox_id": row.id,
                            "admin_action_id": row.admin_action_id,
                            "channel": row.channel,
                            "dedupe_key": row.dedupe_key,
                        },
                    )
                )
                delivered += 1
            else:
                row.status = "failed"
                row.last_error = f"Unsupported channel: {resolved_channel}"
                row.next_attempt_at = None
        except Exception as exc:
            row.last_error = str(exc)
            row.status = "pending"
            row.next_attempt_at = now + timedelta(seconds=_next_backoff_seconds(row.attempt_count))

    await session.commit()
    return delivered
