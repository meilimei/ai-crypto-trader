from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import NotificationOutbox
from ai_crypto_trader.utils.json_safe import json_safe

router = APIRouter(prefix="/admin/notifications", tags=["admin"], dependencies=[Depends(require_admin_token)])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _bounded_limit(value: int) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if limit < 1:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


@router.get("/outbox")
async def get_outbox(
    limit: int = Query(DEFAULT_LIMIT, description="Number of rows to return (max 200)"),
    status: str | None = Query(default=None, description="Filter by outbox status"),
    since_minutes: int | None = Query(default=None, description="Only rows created in the last N minutes"),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit)
    filters = []
    if status:
        filters.append(NotificationOutbox.status == status.strip())
    if since_minutes is not None:
        since = datetime.now(timezone.utc) - timedelta(minutes=max(since_minutes, 0))
        filters.append(NotificationOutbox.created_at >= since)

    stmt = select(NotificationOutbox)
    if filters:
        stmt = stmt.where(and_(*filters))
    stmt = stmt.order_by(NotificationOutbox.created_at.desc()).limit(bounded_limit)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    items = [
        {
            "id": row.id,
            "status": row.status,
            "channel": row.channel,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "admin_action_id": row.admin_action_id,
            "dedupe_key": row.dedupe_key,
            "payload": json_safe(row.payload) if row.payload is not None else {},
            "attempt_count": row.attempt_count,
            "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
            "last_error": row.last_error,
        }
        for row in rows
    ]
    return {"ok": True, "items": items}
