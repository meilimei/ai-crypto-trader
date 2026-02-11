from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, EquitySnapshot, PaperAccount, PaperTrade, StrategyConfig
from ai_crypto_trader.services.notifications.outbox import enqueue_outbox_notification
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


def _parse_int(value: str | None, default: int) -> int:
    if not value:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


DEFAULT_REJECT_SPIKE_THRESHOLD = _parse_int(os.getenv("STRATEGY_MONITOR_REJECT_SPIKE_THRESHOLD"), 10)
DEFAULT_REJECT_SPIKE_WINDOW_SECONDS = _parse_int(os.getenv("STRATEGY_MONITOR_REJECT_SPIKE_WINDOW_SECONDS"), 60)
DEFAULT_ALERT_THROTTLE_SECONDS = _parse_int(os.getenv("STRATEGY_MONITOR_ALERT_THROTTLE_SECONDS"), 1800)
STRATEGY_ALERT_OUTBOX_CHANNEL = os.getenv("STRATEGY_ALERT_OUTBOX_CHANNEL", "log").strip() or "log"


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _to_utc(value).isoformat()


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _monitor_thresholds(thresholds: Any) -> dict[str, Any]:
    root = thresholds if isinstance(thresholds, dict) else {}
    monitor = root.get("monitor")
    monitor_cfg = monitor if isinstance(monitor, dict) else {}
    reject_spike_threshold = _to_int(monitor_cfg.get("reject_spike_threshold"), DEFAULT_REJECT_SPIKE_THRESHOLD)
    reject_spike_window_seconds = _to_int(
        monitor_cfg.get("reject_spike_window_seconds"),
        DEFAULT_REJECT_SPIKE_WINDOW_SECONDS,
    )
    alert_throttle_seconds = _to_int(
        monitor_cfg.get("alert_throttle_seconds"),
        DEFAULT_ALERT_THROTTLE_SECONDS,
    )
    code_spike_thresholds = monitor_cfg.get("reject_code_spike_thresholds")
    if not isinstance(code_spike_thresholds, dict):
        code_spike_thresholds = {}
    return {
        "reject_spike_threshold": max(reject_spike_threshold, 1),
        "reject_spike_window_seconds": max(reject_spike_window_seconds, 1),
        "alert_throttle_seconds": max(alert_throttle_seconds, 1),
        "reject_code_spike_thresholds": {
            str(k).upper(): max(_to_int(v, 0), 0)
            for k, v in code_spike_thresholds.items()
        },
    }


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


def meta_account_id_text(col):  # type: ignore[no-untyped-def]
    return meta_text(col, "account_id")


def meta_strategy_id_text(col):  # type: ignore[no-untyped-def]
    return meta_text(col, "strategy_id")


def reject_code_expr(col):  # type: ignore[no-untyped-def]
    return func.coalesce(
        col.op("->")("reject").op("->>")("code"),
        meta_text(col, "reject_code"),
        meta_text(col, "code"),
    )


@dataclass
class HealthRow:
    account_id: int
    strategy_config_id: int
    symbol: str
    last_trade_at: datetime | None
    trades_count_window: int
    rejects_count_window: int
    reject_by_code_window: dict[str, int]
    equity_latest: Decimal | None
    thresholds_monitor: dict[str, Any]
    reject_events_window: list[tuple[datetime, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "strategy_config_id": self.strategy_config_id,
            "symbol": self.symbol,
            "last_trade_at": _to_iso(self.last_trade_at),
            "trades_count_window": self.trades_count_window,
            "rejects_count_window": self.rejects_count_window,
            "reject_by_code_window": self.reject_by_code_window,
            "equity_latest": str(self.equity_latest) if self.equity_latest is not None else None,
            "thresholds_monitor": self.thresholds_monitor,
        }


@dataclass
class AlertToEmit:
    alert_type: str
    dedupe_key: str
    throttle_seconds: int
    payload: dict[str, Any]


def _pair_key(account_id: int, strategy_config_id: int, symbol: str) -> tuple[int, int, str]:
    return (account_id, strategy_config_id, symbol.upper())


async def _insert_admin_action_throttled(
    session: AsyncSession,
    *,
    action_type: str,
    status: str,
    payload: dict[str, Any],
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
    message = payload_safe.get("message") if isinstance(payload_safe, dict) else None
    admin_action = AdminAction(
        action=action_type,
        status=status,
        message=message,
        meta=payload_safe,
        dedupe_key=dedupe_key,
    )
    session.add(admin_action)
    await session.flush()
    return admin_action


async def compute_strategy_health(
    session: AsyncSession,
    *,
    window_minutes: int,
    limit_pairs: int | None = None,
    account_id: int | None = None,
    strategy_config_id: int | None = None,
    symbol: str | None = None,
    now_utc: datetime | None = None,
) -> list[HealthRow]:
    now = _to_utc(now_utc or datetime.now(timezone.utc))
    window_minutes_safe = max(int(window_minutes), 1)
    window_start = now - timedelta(minutes=window_minutes_safe)
    symbol_filter = normalize_symbol(symbol) if symbol else None

    strategy_stmt = select(
        StrategyConfig.id,
        StrategyConfig.symbols,
        StrategyConfig.thresholds,
        StrategyConfig.is_active,
    )
    if strategy_config_id is not None:
        strategy_stmt = strategy_stmt.where(StrategyConfig.id == strategy_config_id)
    else:
        strategy_stmt = strategy_stmt.where(StrategyConfig.is_active.is_(True))
    strategy_rows = (await session.execute(strategy_stmt)).all()
    if not strategy_rows:
        return []

    account_stmt = select(PaperAccount.id)
    if account_id is not None:
        account_stmt = account_stmt.where(PaperAccount.id == account_id)
    account_ids = [int(x) for x in (await session.scalars(account_stmt)).all()]
    if not account_ids:
        return []

    thresholds_by_strategy: dict[int, dict[str, Any]] = {}
    pairs: dict[tuple[int, int, str], dict[str, Any]] = {}
    for row in strategy_rows:
        strategy_id_val = int(row.id)
        thresholds_monitor = _monitor_thresholds(row.thresholds)
        thresholds_by_strategy[strategy_id_val] = thresholds_monitor
        symbols = row.symbols if isinstance(row.symbols, list) else []
        for symbol_raw in symbols:
            symbol_norm = normalize_symbol(symbol_raw)
            if not symbol_norm:
                continue
            if symbol_filter and symbol_norm != symbol_filter:
                continue
            for acct in account_ids:
                pairs[_pair_key(acct, strategy_id_val, symbol_norm)] = thresholds_monitor

    # Add recent runtime-discovered pairs so monitoring still surfaces non-active or ad-hoc pairs.
    recent_pairs_stmt = (
        select(
            meta_account_id_text(AdminAction.meta).label("account_id"),
            meta_strategy_id_text(AdminAction.meta).label("strategy_config_id"),
            meta_symbol_upper(AdminAction.meta).label("symbol"),
        )
        .where(
            and_(
                AdminAction.created_at >= window_start,
                meta_account_id_text(AdminAction.meta).isnot(None),
                meta_account_id_text(AdminAction.meta) != "",
                meta_strategy_id_text(AdminAction.meta).isnot(None),
                meta_strategy_id_text(AdminAction.meta) != "",
                meta_symbol_text(AdminAction.meta).isnot(None),
                meta_symbol_text(AdminAction.meta) != "",
            )
        )
        .distinct()
    )
    if account_id is not None:
        recent_pairs_stmt = recent_pairs_stmt.where(meta_account_id_text(AdminAction.meta) == str(account_id))
    if strategy_config_id is not None:
        recent_pairs_stmt = recent_pairs_stmt.where(meta_strategy_id_text(AdminAction.meta) == str(strategy_config_id))
    if symbol_filter:
        recent_pairs_stmt = recent_pairs_stmt.where(meta_symbol_upper(AdminAction.meta) == symbol_filter)
    for recent in (await session.execute(recent_pairs_stmt)).all():
        if recent.account_id is None or recent.strategy_config_id is None or recent.symbol is None:
            continue
        try:
            account_id_val = int(str(recent.account_id))
            strategy_id_val = int(str(recent.strategy_config_id))
        except (TypeError, ValueError):
            continue
        key = _pair_key(account_id_val, strategy_id_val, str(recent.symbol))
        pairs.setdefault(key, thresholds_by_strategy.get(strategy_id_val, _monitor_thresholds({})))

    if not pairs:
        return []

    sorted_keys = sorted(pairs.keys(), key=lambda item: (item[0], item[1], item[2]))
    if limit_pairs is not None and limit_pairs > 0:
        sorted_keys = sorted_keys[:limit_pairs]
    key_set = set(sorted_keys)

    account_set = sorted({k[0] for k in sorted_keys})
    strategy_set = sorted({k[1] for k in sorted_keys})
    symbol_set = sorted({k[2] for k in sorted_keys})
    strategy_text_set = [str(x) for x in strategy_set]

    last_trade_map: dict[tuple[int, str], datetime] = {}
    if account_set and symbol_set:
        last_trade_stmt = (
            select(
                PaperTrade.account_id.label("account_id"),
                func.upper(PaperTrade.symbol).label("symbol"),
                func.max(PaperTrade.created_at).label("last_trade_at"),
            )
            .where(
                and_(
                    PaperTrade.account_id.in_(account_set),
                    func.upper(PaperTrade.symbol).in_(symbol_set),
                )
            )
            .group_by(PaperTrade.account_id, func.upper(PaperTrade.symbol))
        )
        for trade_row in (await session.execute(last_trade_stmt)).all():
            if trade_row.last_trade_at is not None:
                last_trade_map[(int(trade_row.account_id), str(trade_row.symbol))] = _to_utc(trade_row.last_trade_at)

    trades_count_map: dict[tuple[int, str], int] = {}
    if account_set and symbol_set:
        trades_count_stmt = (
            select(
                PaperTrade.account_id.label("account_id"),
                func.upper(PaperTrade.symbol).label("symbol"),
                func.count(PaperTrade.id).label("trades_count"),
            )
            .where(
                and_(
                    PaperTrade.account_id.in_(account_set),
                    func.upper(PaperTrade.symbol).in_(symbol_set),
                    PaperTrade.created_at >= window_start,
                )
            )
            .group_by(PaperTrade.account_id, func.upper(PaperTrade.symbol))
        )
        for count_row in (await session.execute(trades_count_stmt)).all():
            trades_count_map[(int(count_row.account_id), str(count_row.symbol))] = int(count_row.trades_count or 0)

    reject_count_map: dict[tuple[int, int, str], int] = {key: 0 for key in sorted_keys}
    reject_by_code_map: dict[tuple[int, int, str], dict[str, int]] = {key: {} for key in sorted_keys}
    reject_events_map: dict[tuple[int, int, str], list[tuple[datetime, str]]] = {key: [] for key in sorted_keys}
    reject_stmt = (
        select(
            meta_account_id_text(AdminAction.meta).label("account_id"),
            meta_strategy_id_text(AdminAction.meta).label("strategy_config_id"),
            meta_symbol_upper(AdminAction.meta).label("symbol"),
            AdminAction.created_at.label("created_at"),
            func.upper(reject_code_expr(AdminAction.meta)).label("reject_code"),
        )
        .where(
            and_(
                AdminAction.action.in_(("ORDER_REJECTED", "ORDER_SKIPPED")),
                AdminAction.created_at >= window_start,
                meta_account_id_text(AdminAction.meta).in_([str(x) for x in account_set]),
                meta_strategy_id_text(AdminAction.meta).in_(strategy_text_set),
                meta_symbol_upper(AdminAction.meta).in_(symbol_set),
            )
        )
        .order_by(AdminAction.created_at.desc())
    )
    reject_rows = (await session.execute(reject_stmt)).all()
    for reject_row in reject_rows:
        if (
            reject_row.account_id is None
            or reject_row.strategy_config_id is None
            or reject_row.symbol is None
            or reject_row.created_at is None
        ):
            continue
        try:
            account_id_val = int(str(reject_row.account_id))
            strategy_id_val = int(str(reject_row.strategy_config_id))
        except (TypeError, ValueError):
            continue
        key = _pair_key(account_id_val, strategy_id_val, str(reject_row.symbol))
        if key not in key_set:
            continue
        code = str(reject_row.reject_code or "UNKNOWN").upper()
        reject_count_map[key] = reject_count_map.get(key, 0) + 1
        code_map = reject_by_code_map.setdefault(key, {})
        code_map[code] = int(code_map.get(code, 0)) + 1
        reject_events_map.setdefault(key, []).append((_to_utc(reject_row.created_at), code))

    equity_map: dict[int, Decimal] = {}
    if account_set:
        latest_equity_subq = (
            select(
                EquitySnapshot.account_id.label("account_id"),
                func.max(EquitySnapshot.created_at).label("created_at"),
            )
            .where(EquitySnapshot.account_id.in_(account_set))
            .group_by(EquitySnapshot.account_id)
            .subquery()
        )
        equity_stmt = (
            select(
                EquitySnapshot.account_id,
                func.coalesce(EquitySnapshot.equity_usdt, EquitySnapshot.equity).label("equity_latest"),
            )
            .join(
                latest_equity_subq,
                and_(
                    EquitySnapshot.account_id == latest_equity_subq.c.account_id,
                    EquitySnapshot.created_at == latest_equity_subq.c.created_at,
                ),
            )
        )
        for eq_row in (await session.execute(equity_stmt)).all():
            equity_decimal = _to_decimal(eq_row.equity_latest)
            if eq_row.account_id is not None and equity_decimal is not None:
                equity_map[int(eq_row.account_id)] = equity_decimal

    rows: list[HealthRow] = []
    for key in sorted_keys:
        account_key, strategy_key, symbol_key = key
        rows.append(
            HealthRow(
                account_id=account_key,
                strategy_config_id=strategy_key,
                symbol=symbol_key,
                last_trade_at=last_trade_map.get((account_key, symbol_key)),
                trades_count_window=trades_count_map.get((account_key, symbol_key), 0),
                rejects_count_window=reject_count_map.get(key, 0),
                reject_by_code_window=reject_by_code_map.get(key, {}),
                equity_latest=equity_map.get(account_key),
                thresholds_monitor=pairs.get(key, _monitor_thresholds({})),
                reject_events_window=reject_events_map.get(key, []),
            )
        )
    return rows


def evaluate_rules(
    row: HealthRow,
    *,
    now_utc: datetime,
) -> list[AlertToEmit]:
    now = _to_utc(now_utc)
    monitor = row.thresholds_monitor if isinstance(row.thresholds_monitor, dict) else _monitor_thresholds({})
    spike_threshold = max(_to_int(monitor.get("reject_spike_threshold"), DEFAULT_REJECT_SPIKE_THRESHOLD), 1)
    spike_window_seconds = max(
        _to_int(monitor.get("reject_spike_window_seconds"), DEFAULT_REJECT_SPIKE_WINDOW_SECONDS),
        1,
    )
    alert_throttle_seconds = max(
        _to_int(monitor.get("alert_throttle_seconds"), DEFAULT_ALERT_THROTTLE_SECONDS),
        1,
    )
    rule_window_start = now - timedelta(seconds=spike_window_seconds)
    rule_window_events = [event for event in row.reject_events_window if event[0] >= rule_window_start]
    count_in_rule_window = len(rule_window_events)
    by_code_in_rule_window: dict[str, int] = {}
    for _, code in rule_window_events:
        by_code_in_rule_window[code] = int(by_code_in_rule_window.get(code, 0)) + 1

    alerts: list[AlertToEmit] = []
    if count_in_rule_window >= spike_threshold:
        dedupe_key = (
            f"STRATEGY_ALERT:REJECT_SPIKE:{row.account_id}:{row.strategy_config_id}:{row.symbol}"
        )
        payload = {
            "type": "REJECT_SPIKE",
            "message": (
                f"Reject spike detected: {count_in_rule_window} rejects in {spike_window_seconds}s"
            ),
            "account_id": row.account_id,
            "strategy_id": str(row.strategy_config_id),
            "symbol": row.symbol,
            "rule": {
                "reject_spike_threshold": spike_threshold,
                "reject_spike_window_seconds": spike_window_seconds,
                "alert_throttle_seconds": alert_throttle_seconds,
            },
            "metrics": row.to_dict(),
            "rejects_count_rule_window": count_in_rule_window,
            "reject_by_code_rule_window": by_code_in_rule_window,
        }
        alerts.append(
            AlertToEmit(
                alert_type="REJECT_SPIKE",
                dedupe_key=dedupe_key,
                throttle_seconds=alert_throttle_seconds,
                payload=payload,
            )
        )

    code_thresholds = monitor.get("reject_code_spike_thresholds")
    if isinstance(code_thresholds, dict):
        for code, threshold_raw in code_thresholds.items():
            threshold = _to_int(threshold_raw, 0)
            if threshold <= 0:
                continue
            code_key = str(code).upper()
            code_count = int(by_code_in_rule_window.get(code_key, 0))
            if code_count < threshold:
                continue
            dedupe_key = (
                f"STRATEGY_ALERT:REJECT_CODE_SPIKE:{row.account_id}:{row.strategy_config_id}:{row.symbol}:{code_key}"
            )
            payload = {
                "type": "REJECT_CODE_SPIKE",
                "message": (
                    f"Reject code spike detected: {code_key} count={code_count} in {spike_window_seconds}s"
                ),
                "account_id": row.account_id,
                "strategy_id": str(row.strategy_config_id),
                "symbol": row.symbol,
                "reject_code": code_key,
                "rule": {
                    "reject_code_spike_threshold": threshold,
                    "reject_spike_window_seconds": spike_window_seconds,
                    "alert_throttle_seconds": alert_throttle_seconds,
                },
                "metrics": row.to_dict(),
                "rejects_count_rule_window": code_count,
            }
            alerts.append(
                AlertToEmit(
                    alert_type="REJECT_CODE_SPIKE",
                    dedupe_key=dedupe_key,
                    throttle_seconds=alert_throttle_seconds,
                    payload=payload,
                )
            )

    return alerts


async def run_strategy_monitor_tick(
    session: AsyncSession,
    *,
    now_utc: datetime,
    window_seconds: int,
    max_pairs: int,
) -> dict[str, Any]:
    now = _to_utc(now_utc)
    window_minutes = max(int(window_seconds / 60), 1)
    rows = await compute_strategy_health(
        session,
        window_minutes=window_minutes,
        limit_pairs=max_pairs,
        now_utc=now,
    )

    alerts_emitted = 0
    alerts_sample: list[dict[str, Any]] = []
    for row in rows:
        alerts = evaluate_rules(row, now_utc=now)
        for alert in alerts:
            inserted = await _insert_admin_action_throttled(
                session,
                action_type="STRATEGY_ALERT",
                status=alert.alert_type,
                payload=alert.payload,
                dedupe_key=alert.dedupe_key,
                window_seconds=alert.throttle_seconds,
                now_utc=now,
            )
            if inserted is None:
                continue
            alerts_emitted += 1
            alert_sample = {
                "type": alert.alert_type,
                "account_id": row.account_id,
                "strategy_config_id": row.strategy_config_id,
                "symbol": row.symbol,
                "dedupe_key": alert.dedupe_key,
                "rejects_count_window": row.rejects_count_window,
            }
            if len(alerts_sample) < 5:
                alerts_sample.append(alert_sample)
            outbox_id: int | None = None
            try:
                outbox_id = await enqueue_outbox_notification(
                    session,
                    channel=STRATEGY_ALERT_OUTBOX_CHANNEL,
                    admin_action=inserted,
                    admin_action_id=inserted.id,
                    dedupe_key=inserted.dedupe_key,
                    payload={
                        "action": inserted.action,
                        "status": inserted.status,
                        "message": inserted.message,
                        "meta": inserted.meta,
                        "created_at": inserted.created_at.isoformat() if inserted.created_at else None,
                        "account_id": row.account_id,
                        "strategy_id": str(row.strategy_config_id),
                        "symbol": row.symbol,
                        "channel": STRATEGY_ALERT_OUTBOX_CHANNEL,
                    },
                    now_utc=now,
                )
            except Exception:
                logger.exception(
                    "strategy_monitor outbox enqueue failed",
                    extra={
                        "admin_action_id": inserted.id,
                        "type": alert.alert_type,
                        "account_id": row.account_id,
                        "strategy_config_id": row.strategy_config_id,
                        "symbol": row.symbol,
                    },
                )
            logger.info(
                "strategy_monitor alert emitted",
                extra={
                    "type": alert.alert_type,
                    "account_id": row.account_id,
                    "strategy_config_id": row.strategy_config_id,
                    "symbol": row.symbol,
                    "dedupe_key": alert.dedupe_key,
                    "admin_action_id": inserted.id,
                    "outbox_id": outbox_id,
                    "metrics_summary": {
                        "trades_count_window": row.trades_count_window,
                        "rejects_count_window": row.rejects_count_window,
                        "last_trade_at": _to_iso(row.last_trade_at),
                    },
                },
            )

    return {
        "pairs_scanned": len(rows),
        "alerts_emitted_count": alerts_emitted,
        "alerts_sample": alerts_sample,
        "window_seconds": window_seconds,
        "reject_spike_window_seconds": DEFAULT_REJECT_SPIKE_WINDOW_SECONDS,
        "reject_spike_threshold": DEFAULT_REJECT_SPIKE_THRESHOLD,
        "alert_throttle_seconds": DEFAULT_ALERT_THROTTLE_SECONDS,
    }
