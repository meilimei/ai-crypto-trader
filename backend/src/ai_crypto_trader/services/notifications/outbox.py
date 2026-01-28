from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, NotificationOutbox
from ai_crypto_trader.utils.json_safe import json_safe


async def enqueue_outbox_for_admin_action(
    session: AsyncSession,
    admin_action: AdminAction | None,
    *,
    channel: str = "noop",
) -> bool:
    if not admin_action or admin_action.id is None:
        return False
    if admin_action.action != "STRATEGY_ALERT":
        return False

    payload_raw: dict[str, Any] = {
        "action": admin_action.action,
        "status": admin_action.status,
        "message": admin_action.message,
        "meta": admin_action.meta or {},
        "created_at": admin_action.created_at.isoformat() if admin_action.created_at else None,
    }
    payload_safe = json_safe(payload_raw)

    stmt = (
        insert(NotificationOutbox)
        .values(
            admin_action_id=admin_action.id,
            dedupe_key=admin_action.dedupe_key,
            status="pending",
            channel=channel,
            payload=payload_safe if isinstance(payload_safe, dict) else {"meta": payload_safe},
        )
        .on_conflict_do_nothing(index_elements=["admin_action_id"])
    )
    result = await session.execute(stmt)
    return bool(getattr(result, "rowcount", 0))
