from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, PaperAccount, StrategyConfig
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


def _parse_decimal(value: str | None, default: Decimal) -> Decimal:
    if not value:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


DEFAULT_WINDOW_MINUTES = _parse_int(os.getenv("STRATEGY_METRICS_WINDOW_MINUTES"), 1440)
DEFAULT_MAX_PAIRS = _parse_int(os.getenv("STRATEGY_METRICS_MAX_PAIRS"), 200)
DEFAULT_MIN_SAMPLES = _parse_int(os.getenv("STRATEGY_METRICS_MIN_SAMPLES"), 5)
DEFAULT_LOSS_STREAK_THRESHOLD = _parse_int(os.getenv("STRATEGY_METRICS_LOSS_STREAK_THRESHOLD"), 3)
DEFAULT_MIN_WIN_RATE = _parse_decimal(os.getenv("STRATEGY_METRICS_MIN_WIN_RATE"), Decimal("0.2"))
DEFAULT_MIN_TOTAL_PNL_USDT = _parse_decimal(os.getenv("STRATEGY_METRICS_MIN_TOTAL_PNL_USDT"), Decimal("-1"))
DEFAULT_ALERT_THROTTLE_SECONDS = _parse_int(
    os.getenv("STRATEGY_METRICS_ALERT_THROTTLE_SECONDS")
    or os.getenv("STRATEGY_MONITOR_ALERT_THROTTLE_SECONDS"),
    1800,
)
STRATEGY_ALERT_OUTBOX_CHANNEL = os.getenv("STRATEGY_ALERT_OUTBOX_CHANNEL", "log").strip() or "log"


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


def meta_strategy_config_id_text(col):  # type: ignore[no-untyped-def]
    return func.coalesce(meta_text(col, "strategy_config_id"), meta_text(col, "strategy_id"))


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


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


@dataclass
class StrategyMetricsRow:
    account_id: int
    strategy_config_id: int
    symbol: str
    outcomes_total: int
    win_rate: Decimal | None
    avg_return_pct_signed: Decimal | None
    total_pnl_usdt_est: Decimal
    loss_streak: int
    last_outcome_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "strategy_config_id": self.strategy_config_id,
            "symbol": self.symbol,
            "outcomes_total": self.outcomes_total,
            "win_rate": str(self.win_rate) if self.win_rate is not None else None,
            "avg_return_pct_signed": str(self.avg_return_pct_signed) if self.avg_return_pct_signed is not None else None,
            "total_pnl_usdt_est": str(self.total_pnl_usdt_est),
            "loss_streak": self.loss_streak,
            "last_outcome_at": _to_iso(self.last_outcome_at),
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
    window_start = now_utc - timedelta(seconds=max(int(window_seconds), 1))
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


async def compute_strategy_metrics(
    session: AsyncSession,
    *,
    now_utc: datetime,
    window_minutes: int,
    max_pairs: int,
    account_id: int | None = None,
    strategy_config_id: int | None = None,
    symbol: str | None = None,
) -> list[StrategyMetricsRow]:
    now = _to_utc(now_utc)
    window_minutes_safe = max(int(window_minutes), 1)
    window_start = now - timedelta(minutes=window_minutes_safe)
    symbol_filter = normalize_symbol(symbol) if symbol else None

    strategy_stmt = select(StrategyConfig.id, StrategyConfig.symbols, StrategyConfig.is_active)
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
    account_ids = [int(value) for value in (await session.scalars(account_stmt)).all()]
    if not account_ids:
        return []

    pairs: dict[tuple[int, int, str], None] = {}
    for row in strategy_rows:
        strategy_id_val = int(row.id)
        symbols = row.symbols if isinstance(row.symbols, list) else []
        for symbol_raw in symbols:
            symbol_norm = normalize_symbol(symbol_raw)
            if not symbol_norm:
                continue
            if symbol_filter and symbol_filter != symbol_norm:
                continue
            for account_id_val in account_ids:
                pairs[_pair_key(account_id_val, strategy_id_val, symbol_norm)] = None
    if not pairs:
        return []

    keys = sorted(pairs.keys(), key=lambda item: (item[0], item[1], item[2]))
    if max_pairs > 0:
        keys = keys[: max(int(max_pairs), 1)]
    key_set = set(keys)

    account_set = sorted({key[0] for key in keys})
    strategy_set = sorted({key[1] for key in keys})
    symbol_set = sorted({key[2] for key in keys})

    # Query only outcomes relevant to selected pairs.
    outcome_stmt = (
        select(
            meta_account_id_text(AdminAction.meta).label("account_id"),
            meta_strategy_config_id_text(AdminAction.meta).label("strategy_config_id"),
            meta_symbol_upper(AdminAction.meta).label("symbol"),
            AdminAction.created_at.label("created_at"),
            meta_text(AdminAction.meta, "win").label("win"),
            meta_text(AdminAction.meta, "return_pct_signed").label("return_pct_signed"),
            meta_text(AdminAction.meta, "pnl_usdt_est").label("pnl_usdt_est"),
        )
        .where(
            and_(
                AdminAction.action == "TRADE_OUTCOME",
                AdminAction.created_at >= window_start,
                meta_account_id_text(AdminAction.meta).in_([str(x) for x in account_set]),
                meta_strategy_config_id_text(AdminAction.meta).in_([str(x) for x in strategy_set]),
                meta_symbol_upper(AdminAction.meta).in_(symbol_set),
            )
        )
        .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
    )
    outcome_rows = (await session.execute(outcome_stmt)).all()

    metrics_map: dict[tuple[int, int, str], dict[str, Any]] = {}
    for row in outcome_rows:
        if row.account_id is None or row.strategy_config_id is None or row.symbol is None:
            continue
        try:
            account_id_val = int(str(row.account_id))
            strategy_id_val = int(str(row.strategy_config_id))
        except (TypeError, ValueError):
            continue
        key = _pair_key(account_id_val, strategy_id_val, str(row.symbol))
        if key not in key_set:
            continue

        state = metrics_map.setdefault(
            key,
            {
                "outcomes_total": 0,
                "wins_total": 0,
                "return_sum": Decimal("0"),
                "return_count": 0,
                "pnl_sum": Decimal("0"),
                "last_outcome_at": None,
                "wins_sequence": [],
            },
        )
        state["outcomes_total"] += 1

        win_bool = _to_bool(row.win)
        if win_bool is True:
            state["wins_total"] += 1
        state["wins_sequence"].append(win_bool)

        return_pct = _to_decimal(row.return_pct_signed)
        if return_pct is not None:
            state["return_sum"] += return_pct
            state["return_count"] += 1

        pnl = _to_decimal(row.pnl_usdt_est)
        if pnl is not None:
            state["pnl_sum"] += pnl

        created_at = _to_utc(row.created_at) if row.created_at is not None else None
        if state["last_outcome_at"] is None and created_at is not None:
            state["last_outcome_at"] = created_at

    rows: list[StrategyMetricsRow] = []
    for key in keys:
        state = metrics_map.get(
            key,
            {
                "outcomes_total": 0,
                "wins_total": 0,
                "return_sum": Decimal("0"),
                "return_count": 0,
                "pnl_sum": Decimal("0"),
                "last_outcome_at": None,
                "wins_sequence": [],
            },
        )
        outcomes_total = int(state["outcomes_total"])
        wins_total = int(state["wins_total"])
        win_rate = None
        if outcomes_total > 0:
            win_rate = Decimal(wins_total) / Decimal(outcomes_total)
        avg_return = None
        if int(state["return_count"]) > 0:
            avg_return = state["return_sum"] / Decimal(int(state["return_count"]))

        loss_streak = 0
        for win_value in state["wins_sequence"]:
            if win_value is False:
                loss_streak += 1
                continue
            break

        rows.append(
            StrategyMetricsRow(
                account_id=key[0],
                strategy_config_id=key[1],
                symbol=key[2],
                outcomes_total=outcomes_total,
                win_rate=win_rate,
                avg_return_pct_signed=avg_return,
                total_pnl_usdt_est=state["pnl_sum"],
                loss_streak=loss_streak,
                last_outcome_at=state["last_outcome_at"],
            )
        )
    return rows


def evaluate_rules(
    row: StrategyMetricsRow,
    *,
    window_minutes: int,
    min_samples: int,
    loss_streak_threshold: int,
    min_win_rate: Decimal,
    min_total_pnl_usdt: Decimal,
    alert_throttle_seconds: int,
) -> list[AlertToEmit]:
    outcomes_total = int(row.outcomes_total)
    if outcomes_total < min_samples:
        return []

    alerts: list[AlertToEmit] = []
    if row.loss_streak >= loss_streak_threshold:
        dedupe_key = (
            f"STRATEGY_ALERT:LOSS_STREAK:{row.account_id}:{row.strategy_config_id}:{row.symbol}:{window_minutes}"
        )
        payload = {
            "type": "LOSS_STREAK",
            "message": f"Loss streak threshold exceeded: {row.loss_streak} >= {loss_streak_threshold}",
            "account_id": row.account_id,
            "strategy_id": str(row.strategy_config_id),
            "strategy_config_id": row.strategy_config_id,
            "symbol": row.symbol,
            "window_minutes": window_minutes,
            "metrics": row.to_dict(),
            "rule": {
                "loss_streak_threshold": loss_streak_threshold,
                "min_samples": min_samples,
                "alert_throttle_seconds": alert_throttle_seconds,
            },
        }
        alerts.append(
            AlertToEmit(
                alert_type="LOSS_STREAK",
                dedupe_key=dedupe_key,
                throttle_seconds=alert_throttle_seconds,
                payload=payload,
            )
        )

    if row.win_rate is not None and row.win_rate <= min_win_rate:
        dedupe_key = (
            f"STRATEGY_ALERT:LOW_WIN_RATE:{row.account_id}:{row.strategy_config_id}:{row.symbol}:{window_minutes}"
        )
        payload = {
            "type": "LOW_WIN_RATE",
            "message": f"Win rate below threshold: {row.win_rate} <= {min_win_rate}",
            "account_id": row.account_id,
            "strategy_id": str(row.strategy_config_id),
            "strategy_config_id": row.strategy_config_id,
            "symbol": row.symbol,
            "window_minutes": window_minutes,
            "metrics": row.to_dict(),
            "rule": {
                "min_win_rate": str(min_win_rate),
                "min_samples": min_samples,
                "alert_throttle_seconds": alert_throttle_seconds,
            },
        }
        alerts.append(
            AlertToEmit(
                alert_type="LOW_WIN_RATE",
                dedupe_key=dedupe_key,
                throttle_seconds=alert_throttle_seconds,
                payload=payload,
            )
        )

    if row.total_pnl_usdt_est <= min_total_pnl_usdt:
        dedupe_key = (
            f"STRATEGY_ALERT:NEGATIVE_PNL:{row.account_id}:{row.strategy_config_id}:{row.symbol}:{window_minutes}"
        )
        payload = {
            "type": "NEGATIVE_PNL",
            "message": f"Total pnl below threshold: {row.total_pnl_usdt_est} <= {min_total_pnl_usdt}",
            "account_id": row.account_id,
            "strategy_id": str(row.strategy_config_id),
            "strategy_config_id": row.strategy_config_id,
            "symbol": row.symbol,
            "window_minutes": window_minutes,
            "metrics": row.to_dict(),
            "rule": {
                "min_total_pnl_usdt": str(min_total_pnl_usdt),
                "min_samples": min_samples,
                "alert_throttle_seconds": alert_throttle_seconds,
            },
        }
        alerts.append(
            AlertToEmit(
                alert_type="NEGATIVE_PNL",
                dedupe_key=dedupe_key,
                throttle_seconds=alert_throttle_seconds,
                payload=payload,
            )
        )
    return alerts


async def run_strategy_metrics_once(
    session: AsyncSession,
    *,
    now_utc: datetime | None = None,
    window_minutes: int | None = None,
    max_pairs: int | None = None,
    account_id: int | None = None,
    strategy_config_id: int | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    now = _to_utc(now_utc or started_at)
    window_minutes_val = max(int(window_minutes or DEFAULT_WINDOW_MINUTES), 1)
    max_pairs_val = max(int(max_pairs or DEFAULT_MAX_PAIRS), 1)
    min_samples = max(DEFAULT_MIN_SAMPLES, 1)
    loss_streak_threshold = max(DEFAULT_LOSS_STREAK_THRESHOLD, 1)
    alert_throttle_seconds = max(DEFAULT_ALERT_THROTTLE_SECONDS, 1)
    min_win_rate = DEFAULT_MIN_WIN_RATE
    min_total_pnl_usdt = DEFAULT_MIN_TOTAL_PNL_USDT

    rows = await compute_strategy_metrics(
        session,
        now_utc=now,
        window_minutes=window_minutes_val,
        max_pairs=max_pairs_val,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol,
    )

    snapshots_written = 0
    alerts_emitted_count = 0
    errors_count = 0
    alerts_sample: list[dict[str, Any]] = []
    snapshots_sample: list[dict[str, Any]] = []

    for row in rows:
        try:
            snapshot_meta = {
                "account_id": str(row.account_id),
                "strategy_id": str(row.strategy_config_id),
                "strategy_config_id": row.strategy_config_id,
                "symbol": row.symbol,
                "window_minutes": window_minutes_val,
                "metrics": row.to_dict(),
            }
            session.add(
                AdminAction(
                    action="STRATEGY_METRICS_SNAPSHOT",
                    status="ok",
                    message="strategy metrics snapshot",
                    meta=json_safe(snapshot_meta),
                )
            )
            snapshots_written += 1
            if len(snapshots_sample) < 5:
                snapshots_sample.append(snapshot_meta)

            alerts = evaluate_rules(
                row,
                window_minutes=window_minutes_val,
                min_samples=min_samples,
                loss_streak_threshold=loss_streak_threshold,
                min_win_rate=min_win_rate,
                min_total_pnl_usdt=min_total_pnl_usdt,
                alert_throttle_seconds=alert_throttle_seconds,
            )
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
                alerts_emitted_count += 1
                if len(alerts_sample) < 5:
                    alerts_sample.append(
                        {
                            "type": alert.alert_type,
                            "account_id": row.account_id,
                            "strategy_config_id": row.strategy_config_id,
                            "symbol": row.symbol,
                            "dedupe_key": alert.dedupe_key,
                            "metrics_summary": {
                                "outcomes_total": row.outcomes_total,
                                "win_rate": str(row.win_rate) if row.win_rate is not None else None,
                                "loss_streak": row.loss_streak,
                                "total_pnl_usdt_est": str(row.total_pnl_usdt_est),
                            },
                        }
                    )
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
                        "strategy_metrics outbox enqueue failed",
                        extra={
                            "admin_action_id": inserted.id,
                            "type": alert.alert_type,
                            "account_id": row.account_id,
                            "strategy_config_id": row.strategy_config_id,
                            "symbol": row.symbol,
                        },
                    )
                logger.info(
                    "strategy_metrics alert emitted",
                    extra={
                        "type": alert.alert_type,
                        "account_id": row.account_id,
                        "strategy_config_id": row.strategy_config_id,
                        "symbol": row.symbol,
                        "dedupe_key": alert.dedupe_key,
                        "admin_action_id": inserted.id,
                        "outbox_id": outbox_id,
                        "metrics_summary": {
                            "outcomes_total": row.outcomes_total,
                            "win_rate": str(row.win_rate) if row.win_rate is not None else None,
                            "loss_streak": row.loss_streak,
                            "total_pnl_usdt_est": str(row.total_pnl_usdt_est),
                        },
                    },
                )
        except Exception:
            errors_count += 1
            logger.exception(
                "strategy_metrics pair processing failed",
                extra={
                    "account_id": row.account_id,
                    "strategy_config_id": row.strategy_config_id,
                    "symbol": row.symbol,
                },
            )

    duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
    tick_meta = {
        "now_utc": now.isoformat(),
        "window_minutes": window_minutes_val,
        "pairs_scanned": len(rows),
        "snapshots_written": snapshots_written,
        "alerts_emitted_count": alerts_emitted_count,
        "alerts_sample": alerts_sample,
        "duration_ms": duration_ms,
        "errors_count": errors_count,
        "min_samples": min_samples,
        "loss_streak_threshold": loss_streak_threshold,
        "min_win_rate": str(min_win_rate),
        "min_total_pnl_usdt": str(min_total_pnl_usdt),
        "alert_throttle_seconds": alert_throttle_seconds,
        "limit_pairs": max_pairs_val,
    }
    tick_bucket = now.strftime("%Y%m%d%H%M")
    tick_dedupe_key = f"STRATEGY_METRICS_TICK:{tick_bucket}"
    existing_tick = await session.scalar(
        select(AdminAction)
        .where(AdminAction.dedupe_key == tick_dedupe_key)
        .limit(1)
    )
    if existing_tick:
        existing_tick.status = "ok"
        existing_tick.message = "strategy metrics tick"
        existing_tick.meta = json_safe(tick_meta)
    else:
        session.add(
            AdminAction(
                action="STRATEGY_METRICS_TICK",
                status="ok",
                message="strategy metrics tick",
                dedupe_key=tick_dedupe_key,
                meta=json_safe(tick_meta),
            )
        )
    logger.info("strategy metrics tick", extra=tick_meta)

    return {
        "now_utc": now.isoformat(),
        "window_minutes": window_minutes_val,
        "pairs_scanned": len(rows),
        "snapshots_written": snapshots_written,
        "alerts_emitted_count": alerts_emitted_count,
        "alerts_sample": alerts_sample,
        "errors_count": errors_count,
        "duration_ms": duration_ms,
        "snapshot_samples": snapshots_sample,
        "limit_pairs": max_pairs_val,
    }
