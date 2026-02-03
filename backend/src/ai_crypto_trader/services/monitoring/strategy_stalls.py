from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import BigInteger, Integer, and_, cast, func, select, union
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.admin_actions.throttled import normalize_status, write_admin_action_throttled
from ai_crypto_trader.services.notifications.outbox import enqueue_outbox_notification
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

def _parse_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _env_int_default(key: str, default: int) -> int:
    return _parse_int(os.getenv(key), default)


STALL_SECONDS = _env_int_default("STRATEGY_STALL_SECONDS", 900)
ACTIVITY_GRACE_SECONDS = _env_int_default("STRATEGY_ACTIVITY_GRACE_SECONDS", 300)
ALERT_THROTTLE_SECONDS = _env_int_default("STRATEGY_STALL_ALERT_THROTTLE_SECONDS", 1800)
STRATEGY_ALERT_OUTBOX_CHANNEL = os.getenv("STRATEGY_ALERT_OUTBOX_CHANNEL", "log").strip() or "log"

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


def _seconds_since(now: datetime, prior: datetime) -> int:
    delta = (now - prior).total_seconds()
    if delta < 0:
        return 0
    return int(delta)


def _latest_datetime(*values: datetime | None) -> datetime | None:
    candidates = [value for value in values if isinstance(value, datetime)]
    return max(candidates) if candidates else None


def _env_int(primary_key: str, fallback_key: str, default: int) -> int:
    """
    Resolve an int threshold from env vars.
    Primary keys are STRATEGY_STALL_*; PAPER_* are supported as a fallback.

    Fast dev test:
      STRATEGY_STALL_SECONDS=20 STRATEGY_ACTIVITY_GRACE_SECONDS=5
    """
    primary_val = os.getenv(primary_key)
    if primary_val:
        return _parse_int(primary_val, default)
    return _parse_int(os.getenv(fallback_key), default)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return _to_utc(parsed)


async def _insert_admin_action_throttled(
    session: AsyncSession,
    *,
    action_type: str,
    status: str,
    payload: dict[str, object],
    dedupe_key: str,
    window_seconds: int,
    now_utc: datetime,
) -> AdminAction | None:
    window_start = now_utc - timedelta(seconds=window_seconds)
    existing = await session.scalar(
        select(AdminAction.id)
        .where(AdminAction.dedupe_key == dedupe_key, AdminAction.created_at >= window_start)
        .order_by(AdminAction.created_at.desc())
        .limit(1)
    )
    if existing:
        return None
    payload_safe = json_safe(payload or {})
    status_norm = normalize_status(status)
    message = payload_safe.get("message") if isinstance(payload_safe, dict) else None
    admin_action = AdminAction(
        action=action_type,
        status=status_norm,
        message=message,
        meta=payload_safe,
        dedupe_key=dedupe_key,
    )
    session.add(admin_action)
    await session.flush()
    return admin_action


async def _latest_strategy_alert(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id_filter_val: str,
    symbol_key: str,
) -> AdminAction | None:
    strategy_expr_alert = func.coalesce(meta_strategy_id(AdminAction.meta), "")
    stmt = (
        select(AdminAction)
        .where(
            and_(
                AdminAction.action == "STRATEGY_ALERT",
                AdminAction.status.in_(("NO_TRADE_STALL", "STALL_RECOVERED")),
                meta_account_id(AdminAction.meta) == account_id,
                strategy_expr_alert == strategy_id_filter_val,
                meta_symbol_upper(AdminAction.meta) == symbol_key,
            )
        )
        .order_by(AdminAction.created_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first()


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
    stall_seconds = _env_int("STRATEGY_STALL_SECONDS", "PAPER_STALL_SECONDS", stall_seconds)
    activity_grace_seconds = _env_int(
        "STRATEGY_ACTIVITY_GRACE_SECONDS",
        "PAPER_ACTIVITY_GRACE_SECONDS",
        activity_grace_seconds,
    )
    throttle_seconds = _env_int(
        "STRATEGY_STALL_ALERT_THROTTLE_SECONDS",
        "PAPER_STALL_THROTTLE_SECONDS",
        throttle_seconds,
    )

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
            strategy_id_key = str(strategy_id).strip() or None
            strategy_id_filter_val = strategy_id_key or ""
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

            last_signal_at = _latest_datetime(last_trade_at, last_activity_at)
            if isinstance(last_signal_at, datetime):
                last_signal_at = _to_utc(last_signal_at)
            last_signal_at_meta = last_signal_at.isoformat() if isinstance(last_signal_at, datetime) else None

            latest_alert_row = await _latest_strategy_alert(
                session,
                account_id=account_id,
                strategy_id_filter_val=strategy_id_filter_val,
                symbol_key=symbol_key,
            )
            last_alert_status = latest_alert_row.status if latest_alert_row else None
            last_alert_created_at: datetime | None = None
            last_alert_effective_at: datetime | None = None
            if latest_alert_row and isinstance(latest_alert_row.created_at, datetime):
                last_alert_created_at = _to_utc(latest_alert_row.created_at)
            if latest_alert_row and isinstance(latest_alert_row.meta, dict):
                last_alert_meta = latest_alert_row.meta
                last_alert_effective_at = _parse_iso_datetime(
                    last_alert_meta.get("effective_last_trade_at") or last_alert_meta.get("last_trade_at")
                )
            last_alert_created_at_meta = (
                last_alert_created_at.isoformat() if isinstance(last_alert_created_at, datetime) else None
            )
            recovery_reference_at = last_alert_created_at or last_alert_effective_at
            recovery_reference_at_meta = (
                recovery_reference_at.isoformat() if isinstance(recovery_reference_at, datetime) else None
            )

            is_stalled = seconds_since_trade > stall_seconds and seconds_since_activity > activity_grace_seconds
            if is_stalled:
                if last_alert_status == "NO_TRADE_STALL":
                    continue

                message = (
                    f"No trades for {seconds_since_trade}s (activity {seconds_since_activity}s) > {stall_seconds}s"
                )
                payload = json_safe(
                    {
                        "message": message,
                        "account_id": account_id,
                        "strategy_id": strategy_id_key,
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

                payload_dict = payload if isinstance(payload, dict) else {"meta": payload}
                dedupe_key = f"STRATEGY_ALERT:NO_TRADE_STALL:{account_id}:{strategy_id_filter_val}:{symbol_key}"
                admin_action = await _insert_admin_action_throttled(
                    session,
                    action_type="STRATEGY_ALERT",
                    status="NO_TRADE_STALL",
                    payload=payload_dict,
                    dedupe_key=dedupe_key,
                    window_seconds=max(throttle_seconds, 1),
                    now_utc=now,
                )
                if admin_action:
                    try:
                        logger.info(
                            "strategy alert enqueue outbox",
                            extra={
                                "admin_action_id": admin_action.id,
                                "dedupe_key": admin_action.dedupe_key,
                                "channel": STRATEGY_ALERT_OUTBOX_CHANNEL,
                            },
                        )
                        await enqueue_outbox_notification(
                            session,
                            channel=STRATEGY_ALERT_OUTBOX_CHANNEL,
                            admin_action=admin_action,
                            dedupe_key=admin_action.dedupe_key,
                            payload={
                                "action": admin_action.action,
                                "status": admin_action.status,
                                "message": admin_action.message,
                                "meta": admin_action.meta,
                                "created_at": admin_action.created_at.isoformat()
                                if admin_action.created_at
                                else None,
                                "account_id": account_id,
                                "strategy_id": strategy_id_key,
                                "symbol": symbol_key,
                            },
                            now_utc=now,
                        )
                        await session.commit()
                        logger.info(
                            "strategy_stall alert fired",
                            extra={
                                "account_id": account_id,
                                "strategy_id": strategy_id_key,
                                "symbol": symbol_key,
                                "seconds_since_trade": seconds_since_trade,
                                "seconds_since_activity": seconds_since_activity,
                            },
                        )
                    except Exception:
                        await session.rollback()
                        logger.exception(
                            "strategy stall alert enqueue failed",
                            extra={
                                "admin_action_id": admin_action.id,
                                "dedupe_key": admin_action.dedupe_key,
                                "account_id": account_id,
                                "strategy_id": strategy_id_key,
                                "symbol": symbol_key,
                            },
                        )
                continue

            if last_alert_status != "NO_TRADE_STALL":
                continue
            if not last_signal_at or not recovery_reference_at:
                continue
            if last_signal_at <= recovery_reference_at:
                continue

            recovery_payload = json_safe(
                {
                    "message": "Strategy stall recovered",
                    "account_id": account_id,
                    "strategy_id": strategy_id_key,
                    "symbol": symbol_key,
                    "effective_last_trade_at": effective_last_trade_at_meta,
                    "last_trade_at": last_trade_at_meta,
                    "first_seen_at": first_seen_at_meta,
                    "last_activity_at": last_activity_at_meta,
                    "last_signal_at": last_signal_at_meta,
                    "last_alert_at": last_alert_created_at_meta,
                    "recovery_reference_at": recovery_reference_at_meta,
                    "stall_seconds": stall_seconds,
                    "activity_grace_seconds": activity_grace_seconds,
                    "throttle_seconds": throttle_seconds,
                    "seconds_since_trade": seconds_since_trade,
                    "seconds_since_activity": seconds_since_activity,
                    "now_utc": now.isoformat(),
                }
            )
            recovery_payload_dict = (
                recovery_payload if isinstance(recovery_payload, dict) else {"meta": recovery_payload}
            )
            recovery_dedupe_key = (
                f"STRATEGY_ALERT:STALL_RECOVERED:{account_id}:{strategy_id_filter_val}:{symbol_key}"
            )
            admin_action = await _insert_admin_action_throttled(
                session,
                action_type="STRATEGY_ALERT",
                status="STALL_RECOVERED",
                payload=recovery_payload_dict,
                dedupe_key=recovery_dedupe_key,
                window_seconds=max(throttle_seconds, 1),
                now_utc=now,
            )
            if admin_action:
                try:
                    logger.info(
                        "strategy alert enqueue outbox",
                        extra={
                            "admin_action_id": admin_action.id,
                            "dedupe_key": admin_action.dedupe_key,
                            "channel": STRATEGY_ALERT_OUTBOX_CHANNEL,
                        },
                    )
                    await enqueue_outbox_notification(
                        session,
                        channel=STRATEGY_ALERT_OUTBOX_CHANNEL,
                        admin_action=admin_action,
                        dedupe_key=admin_action.dedupe_key,
                        payload={
                            "action": admin_action.action,
                            "status": admin_action.status,
                            "message": admin_action.message,
                            "meta": admin_action.meta,
                            "created_at": admin_action.created_at.isoformat()
                            if admin_action.created_at
                            else None,
                            "account_id": account_id,
                            "strategy_id": strategy_id_key,
                            "symbol": symbol_key,
                        },
                        now_utc=now,
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    logger.exception(
                        "strategy stall alert enqueue failed",
                        extra={
                            "admin_action_id": admin_action.id,
                            "dedupe_key": admin_action.dedupe_key,
                            "account_id": account_id,
                            "strategy_id": strategy_id_key,
                            "symbol": symbol_key,
                        },
                    )
        except Exception:
            logger.exception(
                "strategy_stall check failed",
                extra={"account_id": account_id, "strategy_id": str(strategy_id)},
            )
