from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class RejectCode(str, Enum):
    MIN_QTY = "MIN_QTY"
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
