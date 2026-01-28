from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


def normalize_status(value: object) -> str:
    if value is None:
        return ""
    raw = getattr(value, "value", None)
    text = str(raw) if raw is not None else str(value)
    text = text.strip()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    if not text:
        text = str(value)
    return text


async def write_admin_action_throttled(
    session: AsyncSession,
    *,
    action_type: str,
    account_id: int,
    symbol: str | None,
    status: str,
    payload: dict[str, Any],
    window_seconds: int = 120,
    dedupe_key: Optional[str] = None,
    now_utc: datetime | None = None,
) -> bool:
    """
    Insert an AdminAction unless a recent matching row exists for the dedupe key.
    """
    dedupe: Optional[str]
    try:
        status_norm = normalize_status(status)
        if isinstance(dedupe_key, str):
            dedupe = dedupe_key.strip() or None
        else:
            dedupe = dedupe_key
        if not dedupe:
            dedupe = f"{action_type}:{account_id}:{symbol or ''}:{status_norm}"
        window_start = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        payload_raw = json_safe(payload or {})
        payload_safe = json.loads(json.dumps(payload_raw, default=str))
        async with AsyncSessionLocal() as db_session:
            try:
                existing = await db_session.scalar(
                    select(AdminAction)
                    .where(AdminAction.dedupe_key == dedupe, AdminAction.created_at >= window_start)
                    .order_by(AdminAction.created_at.desc())
                )
                if existing:
                    return False

                message = None
                if isinstance(payload_safe, dict):
                    reject = payload_safe.get("reject")
                    if isinstance(reject, dict):
                        message = reject.get("reason") or payload_safe.get("message")
                    else:
                        message = payload_safe.get("message")
                admin_action = AdminAction(
                    action=action_type,
                    status=status_norm,
                    message=message,
                    meta=payload_safe,
                    dedupe_key=dedupe,
                )
                db_session.add(admin_action)
                await db_session.flush()
                await db_session.commit()
                return True
            except Exception:
                await db_session.rollback()
                raise
    except Exception:
        logger.exception("Admin action write failed")
        return False
