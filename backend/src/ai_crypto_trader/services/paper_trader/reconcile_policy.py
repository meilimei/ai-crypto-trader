from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.models.paper_policies import PaperReconcilePolicy


@dataclass(frozen=True)
class ReconcilePolicy:
    account_id: int
    mode: str
    baseline_cash_usdt: Optional[Decimal]
    ok_usdt_diff: Optional[Decimal]
    alert_usdt_diff: Optional[Decimal]
    log_window_seconds: Optional[int]


def _normalize_status(value: str) -> str:
    return (value or "").strip().lower()


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def policy_snapshot(policy: ReconcilePolicy | None) -> dict[str, str | None]:
    if policy is None:
        return {}
    return {
        "mode": policy.mode,
        "baseline_cash_usdt": str(policy.baseline_cash_usdt) if policy.baseline_cash_usdt is not None else None,
        "ok_usdt_diff": str(policy.ok_usdt_diff) if policy.ok_usdt_diff is not None else None,
        "alert_usdt_diff": str(policy.alert_usdt_diff) if policy.alert_usdt_diff is not None else None,
        "log_window_seconds": str(policy.log_window_seconds) if policy.log_window_seconds is not None else None,
    }


def apply_reconcile_policy_status(
    *,
    summary: dict[str, Any],
    base_status: str,
    policy: ReconcilePolicy | None,
) -> str:
    status = _normalize_status(base_status)
    if policy is None:
        return status
    mode = _normalize_status(policy.mode) or "strict"
    if mode == "disabled":
        return "ok"
    usdt_diff = _to_decimal(summary.get("usdt_diff"))
    if usdt_diff is not None:
        abs_diff = usdt_diff.copy_abs()
        if policy.ok_usdt_diff is not None and abs_diff <= policy.ok_usdt_diff:
            return "ok"
        if policy.alert_usdt_diff is not None and abs_diff <= policy.alert_usdt_diff:
            return "warn"
    return status


async def get_reconcile_policy(session: AsyncSession, account_id: int) -> ReconcilePolicy | None:
    if account_id is None:
        return None
    row = await session.scalar(
        select(PaperReconcilePolicy).where(PaperReconcilePolicy.account_id == account_id)
    )
    if row is None:
        return ReconcilePolicy(
            account_id=account_id,
            mode="strict",
            baseline_cash_usdt=None,
            ok_usdt_diff=None,
            alert_usdt_diff=None,
            log_window_seconds=None,
        )
    return ReconcilePolicy(
        account_id=row.account_id,
        mode=row.mode or "strict",
        baseline_cash_usdt=_to_decimal(row.baseline_cash_usdt),
        ok_usdt_diff=_to_decimal(row.ok_usdt_diff),
        alert_usdt_diff=_to_decimal(row.alert_usdt_diff),
        log_window_seconds=getattr(row, "log_window_seconds", None),
    )
