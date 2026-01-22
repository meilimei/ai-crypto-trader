from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel

from ai_crypto_trader.services.admin_actions.throttled import write_admin_action_throttled

logger = logging.getLogger(__name__)


class RejectCode(str, Enum):
    MIN_QTY = "MIN_QTY"
    MIN_ORDER_NOTIONAL = "MIN_ORDER_NOTIONAL"
    MAX_ORDER_NOTIONAL = "MAX_ORDER_NOTIONAL"
    MAX_POSITION_NOTIONAL = "MAX_POSITION_NOTIONAL"
    QTY_ZERO = "QTY_ZERO"
    INVALID_QTY = "INVALID_QTY"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    POSITION_LIMIT = "POSITION_LIMIT"
    RISK_MAX_DRAWDOWN = "RISK_MAX_DRAWDOWN"
    RISK_MAX_LOSS = "RISK_MAX_LOSS"
    ENGINE_NOT_RUNNING = "ENGINE_NOT_RUNNING"
    SYMBOL_DISABLED = "SYMBOL_DISABLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class RejectReason(BaseModel):
    code: RejectCode
    reason: str
    details: Optional[Dict[str, Any]] = None

    class Config:
        arbitrary_types_allowed = True


def normalize_reject_code(code: Any) -> str:
    if hasattr(code, "value"):
        text = str(getattr(code, "value"))
    else:
        text = str(code)
    text = text.strip()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.upper()


def make_reject(code: Any, reason: str, details: dict | None = None) -> dict:
    code_norm = normalize_reject_code(code)
    return {"code": code_norm, "reason": reason, "details": details}


async def log_reject_throttled(
    *,
    action: str = "ORDER_REJECTED",
    account_id: int | None,
    symbol: str | None,
    reject: dict,
    message: str | None = None,
    meta: dict | None = None,
    window_seconds: int = 120,
) -> bool:
    try:
        code = normalize_reject_code(reject.get("code"))
        dedupe_key = f"{action}:{account_id or ''}:{symbol or ''}:{code}"
        payload = dict(meta or {})
        payload["reject"] = reject
        if account_id is not None:
            payload["account_id"] = str(account_id)
        if symbol is not None:
            payload["symbol"] = str(symbol)
        payload["message"] = message or reject.get("reason")
        return await write_admin_action_throttled(
            None,
            action_type=action,
            account_id=account_id or 0,
            symbol=symbol,
            status=code,
            payload=payload,
            window_seconds=window_seconds,
            dedupe_key=dedupe_key,
        )
    except Exception:
        logger.exception("Reject logging failed")
        return False
