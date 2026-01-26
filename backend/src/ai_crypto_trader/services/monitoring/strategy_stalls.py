from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import Integer, String, and_, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.admin_actions.throttled import write_admin_action_throttled
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

STALL_SECONDS = 900
ACTIVITY_GRACE_SECONDS = 300
ALERT_THROTTLE_SECONDS = 1800

ACTIVITY_ACTIONS: tuple[str, ...] = (
    "SMOKE_TRADE",
    "ORDER_REJECTED",
    "ORDER_SKIPPED",
    "STRATEGY_REJECT_COUNTER",
)


def meta_text(col, key):  # type: ignore[no-untyped-def]
    return col.op("->>")(key)


def meta_int(col, key):  # type: ignore[no-untyped-def]
    return cast(meta_text(col, key), Integer)


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _seconds_since(now: datetime, prior: datetime) -> int:
    delta = (now - prior).total_seconds()
    if delta < 0:
        return 0
    return int(delta)


def _get_strategy_pairs(rows: Iterable[object]) -> list[tuple[int, str, str]]:
    pairs: list[tuple[int, str, str]] = []
    for row in rows:
        account_val = getattr(row, "account_id", None)
        strategy_val = getattr(row, "strategy_id", None)
        symbol_val = getattr(row, "symbol", None)
        if account_val is None or strategy_val is None or symbol_val is None:
            continue
        strategy_text = str(strategy_val).strip()
        symbol_text = str(symbol_val).strip()
        if not strategy_text or not symbol_text:
            continue
        try:
            account_id = int(account_val)
        except (TypeError, ValueError):
            continue
        pairs.append((account_id, strategy_text, symbol_text))
    return pairs


async def maybe_alert_strategy_stalls(
    session: AsyncSession,
    *,
    now_utc: datetime,
    stall_seconds: int = STALL_SECONDS,
    activity_grace_seconds: int = ACTIVITY_GRACE_SECONDS,
    throttle_seconds: int = ALERT_THROTTLE_SECONDS,
) -> None:
    now = _to_utc(now_utc)
    try:
        meta = {"now_utc": now.isoformat()}
        reconcile_tick_count = session.info.get("reconcile_tick_count")
        if isinstance(reconcile_tick_count, int):
            meta["reconcile_tick_count"] = reconcile_tick_count
        await write_admin_action_throttled(
            session,
            action_type="STRATEGY_STALL_TICK",
            account_id=0,
            symbol=None,
            status="ok",
            payload={
                "message": "strategy stall monitoring tick",
                **meta,
            },
            window_seconds=60,
            dedupe_key="STRATEGY_STALL_TICK:global",
        )
    except Exception:
        logger.exception("Strategy stall tick heartbeat write failed")

    stall_seconds = _parse_int(os.getenv("PAPER_STALL_SECONDS"), stall_seconds)
    activity_grace_seconds = _parse_int(os.getenv("PAPER_ACTIVITY_GRACE_SECONDS"), activity_grace_seconds)
    throttle_seconds = _parse_int(os.getenv("PAPER_STALL_THROTTLE_SECONDS"), throttle_seconds)

    window_start = now - timedelta(hours=24)
    account_text = meta_text(AdminAction.meta, "account_id")
    account_expr = cast(account_text, Integer)
    strategy_expr = meta_text(AdminAction.meta, "strategy_id")
    symbol_expr = func.upper(cast(meta_text(AdminAction.meta, "symbol"), String))
    account_filters = [account_text.isnot(None), account_text != ""]
    strategy_filters = [strategy_expr.isnot(None), strategy_expr != ""]
    symbol_filters = [symbol_expr.isnot(None), symbol_expr != ""]

    try:
        rows = await session.execute(
            select(
                account_expr.label("account_id"),
                strategy_expr.label("strategy_id"),
                symbol_expr.label("symbol"),
            )
            .where(
                and_(
                    AdminAction.action == "STRATEGY_REJECT_COUNTER",
                    AdminAction.created_at >= window_start,
                    *account_filters,
                    *strategy_filters,
                    *symbol_filters,
                )
            )
            .distinct()
        )
    except Exception:
        logger.exception("strategy_stall pair scan failed")
        return

    pairs = _get_strategy_pairs(rows.all())
    logger.info("strategy_stalls tick pairs=%s", len(pairs))
    if not pairs:
        return

    for account_id, strategy_id, symbol in pairs:
        try:
            symbol_match = or_(symbol_expr == symbol.upper(), symbol_expr == symbol)
            last_trade_at = await session.scalar(
                select(func.max(AdminAction.created_at)).where(
                    and_(
                        AdminAction.action == "SMOKE_TRADE",
                        AdminAction.status == "ok",
                        account_expr == account_id,
                        strategy_expr == strategy_id,
                        symbol_match,
                    )
                )
            )
            if isinstance(last_trade_at, datetime):
                last_trade_at = _to_utc(last_trade_at)

            activity_filters = [
                AdminAction.action.in_(ACTIVITY_ACTIONS),
                account_expr == account_id,
                strategy_expr == strategy_id,
                symbol_match,
            ]

            last_activity_at = await session.scalar(
                select(func.max(AdminAction.created_at)).where(*activity_filters)
            )
            if isinstance(last_activity_at, datetime):
                last_activity_at = _to_utc(last_activity_at)

            counter_meta = await session.scalar(
                select(AdminAction.meta)
                .where(
                    and_(
                        AdminAction.action == "STRATEGY_REJECT_COUNTER",
                        account_expr == account_id,
                        strategy_expr == strategy_id,
                        symbol_match,
                    )
                )
                .order_by(AdminAction.created_at.desc())
                .limit(1)
            )
            first_seen_at = None
            if isinstance(counter_meta, dict):
                first_seen_at = counter_meta.get("first_seen_at")
            first_seen_at = _to_utc(first_seen_at) if isinstance(first_seen_at, datetime) else None
            if isinstance(first_seen_at, str):
                try:
                    first_seen_at = _to_utc(datetime.fromisoformat(first_seen_at.replace("Z", "+00:00")))
                except ValueError:
                    first_seen_at = None

            effective_last_trade_at = last_trade_at or first_seen_at or last_activity_at
            if not effective_last_trade_at or not last_activity_at:
                continue

            seconds_since_trade = _seconds_since(now, effective_last_trade_at)
            seconds_since_activity = _seconds_since(now, last_activity_at)

            if seconds_since_trade <= stall_seconds or seconds_since_activity <= activity_grace_seconds:
                continue

            message = (
                f"Strategy stalled: no trades for {seconds_since_trade}s and "
                f"no activity for {seconds_since_activity}s"
            )
            payload = json_safe(
                {
                    "message": message,
                    "account_id": account_id,
                    "strategy_id": str(strategy_id),
                    "symbol": symbol,
                    "last_trade_at": last_trade_at,
                    "first_seen_at": first_seen_at,
                    "last_activity_at": last_activity_at,
                    "stall_seconds": stall_seconds,
                    "activity_grace_seconds": activity_grace_seconds,
                    "seconds_since_trade": seconds_since_trade,
                    "seconds_since_activity": seconds_since_activity,
                }
            )

            await write_admin_action_throttled(
                None,
                action_type="STRATEGY_ALERT",
                account_id=account_id,
                symbol=None,
                status="NO_TRADE_STALL",
                payload=payload if isinstance(payload, dict) else {"meta": payload},
                window_seconds=max(throttle_seconds, 1),
                dedupe_key=f"STRATEGY_ALERT:NO_TRADE_STALL:{account_id}:{strategy_id}:{symbol}",
            )
            logger.info(
                "strategy_stall alert fired",
                extra={
                    "account_id": account_id,
                    "strategy_id": str(strategy_id),
                    "symbol": symbol,
                    "seconds_since_trade": seconds_since_trade,
                    "seconds_since_activity": seconds_since_activity,
                },
            )
        except Exception:
            logger.exception(
                "strategy_stall check failed",
                extra={"account_id": account_id, "strategy_id": str(strategy_id)},
            )
