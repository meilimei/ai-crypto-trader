from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import BigInteger, Integer, and_, cast, func, select, union
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


def meta_symbol_text(col):  # type: ignore[no-untyped-def]
    return func.coalesce(
        meta_text(col, "symbol_normalized"),
        meta_text(col, "symbol"),
        meta_text(col, "symbol_in"),
    )


def meta_symbol_upper(col):  # type: ignore[no-untyped-def]
    return func.upper(meta_symbol_text(col))


def meta_int(col, key):  # type: ignore[no-untyped-def]
    return cast(meta_text(col, key), Integer)


def meta_account_id(col):  # type: ignore[no-untyped-def]
    return cast(meta_text(col, "account_id"), BigInteger)


def meta_strategy_id(col):  # type: ignore[no-untyped-def]
    return meta_text(col, "strategy_id")


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
    stall_seconds = _parse_int(os.getenv("PAPER_STALL_SECONDS"), stall_seconds)
    activity_grace_seconds = _parse_int(os.getenv("PAPER_ACTIVITY_GRACE_SECONDS"), activity_grace_seconds)
    throttle_seconds = _parse_int(os.getenv("PAPER_STALL_THROTTLE_SECONDS"), throttle_seconds)

    window_start = now - timedelta(hours=24)
    account_text_meta = meta_text(AdminAction.meta, "account_id")
    strategy_text_meta = meta_text(AdminAction.meta, "strategy_id")
    symbol_text_meta = meta_symbol_text(AdminAction.meta)
    account_expr_meta = meta_account_id(AdminAction.meta)
    strategy_expr_meta = meta_strategy_id(AdminAction.meta)
    symbol_expr_meta = meta_symbol_upper(AdminAction.meta)

    # AdminAction does not have account_id/strategy_id/symbol columns in the DB; read from meta only.
    account_text_any = meta_text(AdminAction.meta, "account_id")
    strategy_text_any = meta_strategy_id(AdminAction.meta)
    symbol_text_any = meta_symbol_text(AdminAction.meta)
    account_expr = meta_account_id(AdminAction.meta)
    strategy_expr = meta_strategy_id(AdminAction.meta)
    symbol_expr = meta_symbol_upper(AdminAction.meta)

    account_filters = [account_text_any.isnot(None), account_text_any != ""]
    strategy_filters = [strategy_text_any.isnot(None), strategy_text_any != ""]
    symbol_filters = [symbol_text_any.isnot(None), symbol_text_any != ""]
    counter_filters = [
        account_text_meta.isnot(None),
        account_text_meta != "",
        strategy_text_meta.isnot(None),
        strategy_text_meta != "",
        symbol_text_meta.isnot(None),
        symbol_text_meta != "",
    ]

    try:
        counter_select = (
            select(
                account_expr_meta.label("account_id"),
                strategy_expr_meta.label("strategy_id"),
                symbol_expr_meta.label("symbol"),
            )
            .where(
                and_(
                    AdminAction.action == "STRATEGY_REJECT_COUNTER",
                    AdminAction.created_at >= window_start,
                    *counter_filters,
                )
            )
        )
        any_select = (
            select(
                account_expr.label("account_id"),
                strategy_expr.label("strategy_id"),
                symbol_expr.label("symbol"),
            )
            .where(
                and_(
                    AdminAction.created_at >= window_start,
                    *account_filters,
                    *strategy_filters,
                    *symbol_filters,
                )
            )
        )
        pair_union = union(counter_select, any_select).subquery()
        rows = await session.execute(
            select(pair_union.c.account_id, pair_union.c.strategy_id, pair_union.c.symbol)
        )
    except Exception:
        logger.exception("strategy_stall pair scan failed")
        return

    pairs = _get_strategy_pairs(rows.all())
    try:
        heartbeat_meta = {
            "message": "strategy stall monitoring tick",
            "now_utc": now.isoformat(),
            "pairs_scanned": len(pairs),
            "pairs_sample": [
                {"account_id": acct, "strategy_id": strat, "symbol": sym}
                for acct, strat, sym in pairs[:3]
            ],
            "stall_seconds": stall_seconds,
            "activity_grace_seconds": activity_grace_seconds,
            "throttle_seconds": throttle_seconds,
        }
        reconcile_tick_count = session.info.get("reconcile_tick_count")
        if isinstance(reconcile_tick_count, int):
            heartbeat_meta["reconcile_tick_count"] = reconcile_tick_count
        await write_admin_action_throttled(
            session,
            action_type="STRATEGY_STALL_TICK",
            account_id=0,
            symbol=None,
            status="ok",
            payload=heartbeat_meta,
            window_seconds=60,
            dedupe_key="STRATEGY_STALL_TICK:global",
        )
    except Exception:
        logger.exception("Strategy stall tick heartbeat write failed")
    logger.info("strategy_stalls tick pairs=%s", len(pairs))
    if not pairs:
        return

    for account_id, strategy_id, symbol in pairs:
        try:
            symbol_key = symbol.upper()
            last_trade_at = await session.scalar(
                select(func.max(AdminAction.created_at)).where(
                    and_(
                        AdminAction.action == "SMOKE_TRADE",
                        AdminAction.status == "ok",
                        account_expr == account_id,
                        strategy_expr == strategy_id,
                        symbol_expr == symbol_key,
                    )
                )
            )
            if isinstance(last_trade_at, datetime):
                last_trade_at = _to_utc(last_trade_at)

            last_activity_at = await session.scalar(
                select(func.max(AdminAction.created_at)).where(
                    and_(
                        AdminAction.action.in_(ACTIVITY_ACTIONS),
                        account_expr == account_id,
                        strategy_expr == strategy_id,
                        symbol_expr == symbol_key,
                    )
                )
            )
            if isinstance(last_activity_at, datetime):
                last_activity_at = _to_utc(last_activity_at)

            first_seen_at = await session.scalar(
                select(func.min(AdminAction.created_at)).where(
                    and_(
                        AdminAction.action == "STRATEGY_REJECT_COUNTER",
                        account_expr_meta == account_id,
                        strategy_expr_meta == strategy_id,
                        symbol_expr_meta == symbol_key,
                    )
                )
            )
            if isinstance(first_seen_at, datetime):
                first_seen_at = _to_utc(first_seen_at)

            effective_last_trade_at = last_trade_at or first_seen_at or last_activity_at
            if not effective_last_trade_at:
                continue

            last_trade_at_meta = last_trade_at.isoformat() if isinstance(last_trade_at, datetime) else None
            first_seen_at_meta = first_seen_at.isoformat() if isinstance(first_seen_at, datetime) else None
            last_activity_at_meta = last_activity_at.isoformat() if isinstance(last_activity_at, datetime) else None
            effective_last_trade_at_meta = (
                effective_last_trade_at.isoformat()
                if isinstance(effective_last_trade_at, datetime)
                else effective_last_trade_at
            )

            seconds_since_trade = _seconds_since(now, effective_last_trade_at)
            seconds_since_activity = (
                _seconds_since(now, last_activity_at) if isinstance(last_activity_at, datetime) else seconds_since_trade
            )

            if seconds_since_trade <= stall_seconds or seconds_since_activity <= activity_grace_seconds:
                continue

            message = (
                f"No trades for {seconds_since_trade}s (activity {seconds_since_activity}s) > {stall_seconds}s"
            )
            payload = json_safe(
                {
                    "message": message,
                    "account_id": account_id,
                    "strategy_id": str(strategy_id),
                    "symbol": symbol_key,
                    "effective_last_trade_at": effective_last_trade_at_meta,
                    "last_trade_at": last_trade_at_meta,
                    "first_seen_at": first_seen_at_meta,
                    "last_activity_at": last_activity_at_meta,
                    "stall_seconds": stall_seconds,
                    "activity_grace_seconds": activity_grace_seconds,
                    "throttle_seconds": throttle_seconds,
                    "seconds_since_trade": seconds_since_trade,
                    "seconds_since_activity": seconds_since_activity,
                    "now_utc": now.isoformat(),
                }
            )

            await write_admin_action_throttled(
                session,
                action_type="STRATEGY_ALERT",
                account_id=account_id,
                symbol=None,
                status="NO_TRADE_STALL",
                payload=payload if isinstance(payload, dict) else {"meta": payload},
                window_seconds=max(throttle_seconds, 1),
                dedupe_key=f"STRATEGY_ALERT:NO_TRADE_STALL:{account_id}:{strategy_id}:{symbol_key}",
            )
            logger.info(
                "strategy_stall alert fired",
                extra={
                    "account_id": account_id,
                    "strategy_id": str(strategy_id),
                    "symbol": symbol_key,
                    "seconds_since_trade": seconds_since_trade,
                    "seconds_since_activity": seconds_since_activity,
                },
            )
        except Exception:
            logger.exception(
                "strategy_stall check failed",
                extra={"account_id": account_id, "strategy_id": str(strategy_id)},
            )
