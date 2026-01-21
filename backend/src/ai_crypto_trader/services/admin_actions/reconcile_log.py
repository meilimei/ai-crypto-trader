from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


def _normalize_status(status: str) -> str:
    norm = (status or "").strip().lower()
    if norm == "warning":
        return "warn"
    if norm in {"ok", "warn", "alert", "error"}:
        return norm
    if norm in {"fail", "failed", "failure", "exception"}:
        return "alert"
    return norm or "warn"


def _extract_diffs_sample(payload: dict[str, Any]) -> list[Any]:
    diffs = payload.get("diffs_sample") or payload.get("diffs") or []
    if not isinstance(diffs, list):
        diffs = [diffs]
    return diffs[:20]


def _build_meta(account_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    diff_count = payload.get("diff_count", summary.get("diff_count"))
    usdt_diff = payload.get("usdt_diff", summary.get("usdt_diff"))
    has_negative_balance = payload.get("has_negative_balance", summary.get("has_negative_balance"))
    has_position_mismatch = payload.get("has_position_mismatch", summary.get("has_position_mismatch"))
    has_equity_mismatch = payload.get("has_equity_mismatch", summary.get("has_equity_mismatch"))
    ok_flag = payload.get("ok")
    if ok_flag is None and diff_count is not None:
        try:
            ok_flag = int(diff_count) == 0
        except Exception:
            ok_flag = None
    meta = {
        "account_id": str(account_id),
        "summary": summary or None,
        "diff_count": diff_count,
        "usdt_diff": usdt_diff,
        "has_negative_balance": has_negative_balance,
        "has_position_mismatch": has_position_mismatch,
        "has_equity_mismatch": has_equity_mismatch,
        "ok": ok_flag,
        "diffs_sample": _extract_diffs_sample(payload),
    }
    if "warning" in payload:
        meta["warning"] = payload.get("warning")
    if "warnings" in payload:
        meta["warnings"] = payload.get("warnings")
    if "policy" in payload:
        meta["policy"] = payload.get("policy")
    if "baseline_source" in payload:
        meta["baseline_source"] = payload.get("baseline_source")
    if "debug_counts" in payload:
        meta["debug_counts"] = payload.get("debug_counts")
    if "debug" in payload:
        meta["debug"] = payload.get("debug")
    return meta


async def log_reconcile_report_throttled(
    *,
    account_id: int,
    status: str,
    message: str,
    report_meta: dict[str, Any] | None,
    window_seconds: int = 120,
    dedupe_fingerprint: str | None = None,
) -> bool:
    """
    Best-effort insert of a RECONCILE_REPORT admin action with dedupe protection.
    """
    status_norm = _normalize_status(status)
    fingerprint = (dedupe_fingerprint or "").strip()
    if fingerprint:
        if len(fingerprint) > 32:
            fingerprint = fingerprint[:32]
        dedupe_key = f"RECONCILE_REPORT:{account_id}:{status_norm}:{fingerprint}"
    else:
        dedupe_key = f"RECONCILE_REPORT:{account_id}:{status_norm}"
    try:
        payload = json_safe(report_meta or {})
        if not isinstance(payload, dict):
            payload = {"meta": payload}
        payload_safe = _build_meta(account_id, payload)
        payload_safe = json.loads(json.dumps(payload_safe, default=str))
        window_start = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        async with AsyncSessionLocal() as session:
            try:
                existing = await session.scalar(
                    select(AdminAction)
                    .where(AdminAction.dedupe_key == dedupe_key, AdminAction.created_at >= window_start)
                    .order_by(AdminAction.created_at.desc())
                )
                if existing:
                    return False
                session.add(
                    AdminAction(
                        action="RECONCILE_REPORT",
                        status=status_norm,
                        message=message,
                        meta=payload_safe,
                        dedupe_key=dedupe_key,
                    )
                )
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                raise
    except Exception:
        logger.exception("Reconcile report log failed")
        return False
