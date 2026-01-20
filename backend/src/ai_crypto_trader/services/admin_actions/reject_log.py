from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.services.admin_actions.throttled import normalize_status, write_admin_action_throttled
from ai_crypto_trader.utils.json_safe import json_safe


async def log_order_rejected_throttled(
    session: AsyncSession,
    *,
    account_id: int,
    symbol: str | None,
    reject_code: str,
    reject_reason: str,
    meta: dict[str, Any] | None,
    window_seconds: int = 120,
) -> bool:
    """
    Best-effort insert of an ORDER_REJECTED admin action with dedupe protection.
    """
    status_norm = normalize_status(reject_code)
    dedupe_key = f"ORDER_REJECTED:{account_id}:{symbol or ''}:{status_norm}"
    payload = json_safe(meta or {})
    if not isinstance(payload, dict):
        payload = {"meta": payload}
    payload["account_id"] = str(account_id)
    if symbol is not None:
        payload["symbol"] = str(symbol)
    payload["reject_code"] = str(status_norm)
    payload["reject_reason"] = str(reject_reason)
    payload.setdefault("reject", {"code": str(status_norm), "reason": str(reject_reason)})

    return await write_admin_action_throttled(
        session,
        action_type="ORDER_REJECTED",
        account_id=account_id,
        symbol=symbol,
        status=str(status_norm),
        payload=payload,
        window_seconds=window_seconds,
        dedupe_key=dedupe_key,
    )
