from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, Candle, EquitySnapshot, PaperTrade
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


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
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _latest_price_from_payload(payload: dict[str, Any]) -> Decimal | None:
    result = payload.get("result")
    if isinstance(result, dict):
        price = (
            result.get("executed_price")
            or result.get("price")
            or result.get("fill_price")
            or result.get("market_price")
        )
        if price is not None:
            dec = _to_decimal(price)
            if dec is not None and dec > 0:
                return dec

    inputs = payload.get("inputs")
    if isinstance(inputs, dict):
        price = inputs.get("market_price_used")
        if price is not None:
            dec = _to_decimal(price)
            if dec is not None and dec > 0:
                return dec

    return None


def build_decision_key(
    *,
    created_now_utc: datetime,
    account_id: str | int | None,
    strategy_id: str | None,
    strategy_config_id: int | None,
    symbol_normalized: str,
    side: str,
    qty_requested: str | Decimal | None,
    status: str,
) -> str:
    timestamp = _to_utc(created_now_utc).strftime("%Y%m%d%H%M%S%f")
    raw = "|".join(
        [
            str(account_id or ""),
            str(strategy_id or ""),
            str(strategy_config_id or ""),
            normalize_symbol(symbol_normalized),
            (side or "").lower().strip(),
            str(qty_requested or ""),
            (status or "").lower().strip(),
            timestamp,
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"DEC:{timestamp}:{digest}"


async def emit_trade_decision(
    session: AsyncSession,
    payload: dict[str, Any],
) -> AdminAction | None:
    meta = dict(payload or {})
    now = meta.get("created_now_utc")
    if isinstance(now, datetime):
        now_utc = _to_utc(now)
    elif isinstance(now, str):
        try:
            now_utc = _to_utc(datetime.fromisoformat(now))
        except ValueError:
            now_utc = datetime.now(timezone.utc)
    else:
        now_utc = datetime.now(timezone.utc)

    symbol_normalized = normalize_symbol(
        str(meta.get("symbol_normalized") or meta.get("symbol_in") or meta.get("symbol") or "")
    )
    if not symbol_normalized:
        logger.warning("trade decision emit skipped: missing symbol")
        return None

    side = str(meta.get("side") or "").lower().strip()
    status = str(meta.get("status") or "").lower().strip()
    if status not in {"executed", "rejected", "skipped", "proposed"}:
        logger.warning("trade decision emit skipped: invalid status", extra={"status": status})
        return None

    account_id_raw = meta.get("account_id")
    strategy_config_id = _to_int(meta.get("strategy_config_id"))
    strategy_id = meta.get("strategy_id")
    qty_requested = meta.get("qty_requested")
    decision_key = str(meta.get("decision_key") or "").strip()
    if not decision_key:
        decision_key = build_decision_key(
            created_now_utc=now_utc,
            account_id=account_id_raw,
            strategy_id=str(strategy_id) if strategy_id is not None else None,
            strategy_config_id=strategy_config_id,
            symbol_normalized=symbol_normalized,
            side=side,
            qty_requested=qty_requested,
            status=status,
        )

    meta["decision_key"] = decision_key
    meta["created_now_utc"] = _to_iso(now_utc)
    meta["account_id"] = str(account_id_raw) if account_id_raw is not None else None
    meta["strategy_id"] = str(strategy_id) if strategy_id is not None else None
    meta["strategy_config_id"] = strategy_config_id
    meta["symbol"] = symbol_normalized
    meta["symbol_normalized"] = symbol_normalized
    meta["side"] = side
    meta["status"] = status
    if "qty" not in meta:
        meta["qty"] = str(qty_requested) if qty_requested is not None else None
    if "market_price" not in meta or meta.get("market_price") is None:
        market_price = _latest_price_from_payload(meta)
        if market_price is not None:
            meta["market_price"] = str(market_price)
    if "price_source" not in meta or meta.get("price_source") is None:
        inputs = meta.get("inputs")
        if isinstance(inputs, dict):
            meta["price_source"] = inputs.get("price_source")
    if "reject" not in meta:
        result = meta.get("result")
        if isinstance(result, dict) and isinstance(result.get("reject"), dict):
            meta["reject"] = result.get("reject")

    row = AdminAction(
        action="TRADE_DECISION",
        status=status,
        message=str(meta.get("rationale") or "trade decision"),
        dedupe_key=f"TRADE_DECISION:{decision_key}",
        meta=json_safe(meta),
    )
    session.add(row)
    await session.flush()
    result = meta.get("result") if isinstance(meta.get("result"), dict) else {}
    reject = result.get("reject") if isinstance(result, dict) else {}
    logger.info(
        "trade explainability decision",
        extra={
            "action": "TRADE_DECISION",
            "decision_key": decision_key,
            "account_id": meta.get("account_id"),
            "strategy_config_id": strategy_config_id,
            "symbol": symbol_normalized,
            "side": side,
            "status": status,
            "trade_id": result.get("trade_id") if isinstance(result, dict) else None,
            "order_id": result.get("order_id") if isinstance(result, dict) else None,
            "reject_code": reject.get("code") if isinstance(reject, dict) else None,
        },
    )
    return row


async def emit_trade_outcome(
    session: AsyncSession,
    payload: dict[str, Any],
) -> AdminAction | None:
    meta = dict(payload or {})
    decision_key = str(meta.get("decision_key") or "").strip()
    if not decision_key:
        logger.warning("trade outcome emit skipped: missing decision_key")
        return None

    meta["evaluated_at_utc"] = _to_iso(datetime.now(timezone.utc))
    meta["decision_key"] = decision_key
    row = AdminAction(
        action="TRADE_OUTCOME",
        status="evaluated",
        message="trade outcome evaluated",
        dedupe_key=f"TRADE_OUTCOME:{decision_key}",
        meta=json_safe(meta),
    )
    session.add(row)
    await session.flush()
    logger.info(
        "trade explainability outcome",
        extra={
            "action": "TRADE_OUTCOME",
            "decision_key": decision_key,
            "account_id": meta.get("account_id"),
            "strategy_config_id": meta.get("strategy_config_id"),
            "symbol": meta.get("symbol"),
            "side": meta.get("side"),
            "status": "evaluated",
            "horizon_seconds": meta.get("horizon_seconds"),
            "win": meta.get("win"),
            "return_pct": meta.get("return_pct_signed"),
        },
    )
    return row


async def _resolve_market_price(
    session: AsyncSession,
    *,
    symbol_in: str,
    symbol_norm: str,
) -> tuple[Decimal | None, str | None]:
    candle_price = await session.scalar(
        select(Candle.close).where(Candle.symbol == symbol_norm).order_by(Candle.open_time.desc()).limit(1)
    )
    if candle_price is not None:
        candidate = _to_decimal(candle_price)
        if candidate is not None and candidate > 0:
            return candidate, "latest"

    symbol_slash = None
    if "/" in symbol_in:
        symbol_slash = symbol_in
    elif symbol_norm.endswith("USDT") and len(symbol_norm) > 4:
        symbol_slash = f"{symbol_norm[:-4]}/USDT"
    symbols = [symbol_norm]
    if symbol_slash and symbol_slash not in symbols:
        symbols.append(symbol_slash)
    row = await session.execute(
        select(PaperTrade.price)
        .where(PaperTrade.symbol.in_(symbols))
        .order_by(desc(PaperTrade.created_at).nullslast(), PaperTrade.id.desc())
        .limit(1)
    )
    latest_trade = row.first()
    if latest_trade and latest_trade[0] is not None:
        candidate = _to_decimal(latest_trade[0])
        if candidate is not None and candidate > 0:
            return candidate, "paper_trades_last"
    return None, None


async def run_outcome_tick(
    session: AsyncSession,
    *,
    limit: int = 200,
    min_age_seconds: int = 900,
    horizon_seconds: int = 900,
) -> dict[str, int]:
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(seconds=max(int(min_age_seconds), 0))
    picked = 0
    emitted = 0
    skipped_existing = 0
    errors = 0

    decisions = (
        await session.execute(
            select(AdminAction)
            .where(
                and_(
                    AdminAction.action == "TRADE_DECISION",
                    meta_text(AdminAction.meta, "status") == "executed",
                    AdminAction.created_at <= cutoff,
                )
            )
            .order_by(AdminAction.created_at.asc(), AdminAction.id.asc())
            .limit(max(int(limit), 1))
        )
    ).scalars().all()

    for decision in decisions:
        picked += 1
        meta = decision.meta if isinstance(decision.meta, dict) else {}
        decision_key = str(meta.get("decision_key") or "").strip()
        if not decision_key:
            errors += 1
            continue

        existing = await session.scalar(
            select(AdminAction.id)
            .where(
                and_(
                    AdminAction.action == "TRADE_OUTCOME",
                    meta_text(AdminAction.meta, "decision_key") == decision_key,
                )
            )
            .limit(1)
        )
        if existing is not None:
            skipped_existing += 1
            continue

        try:
            account_id = _to_int(meta.get("account_id"))
            strategy_config_id = _to_int(meta.get("strategy_config_id"))
            strategy_id = meta.get("strategy_id")
            symbol = normalize_symbol(
                str(meta.get("symbol_normalized") or meta.get("symbol_in") or meta.get("symbol") or "")
            )
            side = str(meta.get("side") or "").lower().strip()
            qty = _to_decimal(meta.get("qty_requested"))
            entry_price = _latest_price_from_payload(meta)
            if entry_price is None:
                result = meta.get("result")
                if isinstance(result, dict):
                    entry_price = _to_decimal(result.get("executed_price"))

            if account_id is None or not symbol or side not in {"buy", "sell"} or qty is None or qty <= 0 or entry_price is None or entry_price <= 0:
                errors += 1
                continue

            eval_price, price_source = await _resolve_market_price(
                session,
                symbol_in=str(meta.get("symbol_in") or symbol),
                symbol_norm=symbol,
            )
            if eval_price is None or eval_price <= 0:
                errors += 1
                continue

            latest_equity_row = await session.execute(
                select(
                    EquitySnapshot.id,
                    EquitySnapshot.created_at,
                    func.coalesce(EquitySnapshot.equity_usdt, EquitySnapshot.equity),
                )
                .where(EquitySnapshot.account_id == account_id)
                .order_by(EquitySnapshot.created_at.desc(), EquitySnapshot.id.desc())
                .limit(1)
            )
            equity_snapshot = latest_equity_row.first()

            sign = Decimal("1") if side == "buy" else Decimal("-1")
            pnl_usdt_est = qty * (eval_price - entry_price) * sign
            return_pct_signed = (
                ((eval_price - entry_price) / entry_price) * sign if entry_price > 0 else Decimal("0")
            )
            payload = {
                "decision_key": decision_key,
                "account_id": str(account_id),
                "strategy_id": str(strategy_id) if strategy_id is not None else None,
                "strategy_config_id": strategy_config_id,
                "symbol": symbol,
                "side": side,
                "qty": str(qty),
                "entry_price": str(entry_price),
                "horizon_seconds": int(horizon_seconds),
                "decision_action_id": decision.id,
                "decision_action_created_at": _to_iso(decision.created_at),
                "eval_price": str(eval_price),
                "eval_price_source": price_source,
                "return_pct_signed": str(return_pct_signed),
                "pnl_usdt_est": str(pnl_usdt_est),
                "win": pnl_usdt_est > 0,
            }
            if equity_snapshot:
                payload["latest_equity_snapshot_id"] = equity_snapshot[0]
                payload["latest_equity_snapshot_created_at"] = _to_iso(equity_snapshot[1])
                payload["latest_equity_usdt"] = (
                    str(equity_snapshot[2]) if equity_snapshot[2] is not None else None
                )
            emitted_row = await emit_trade_outcome(session, payload)
            if emitted_row is not None:
                emitted += 1
        except Exception:
            errors += 1
            logger.exception("trade explainability outcome tick failed for decision", extra={"decision_key": decision_key})

    stats = {
        "picked": picked,
        "emitted": emitted,
        "skipped_existing": skipped_existing,
        "errors": errors,
    }
    logger.info(
        "trade explainability outcome tick",
        extra={
            "picked_count": picked,
            "wrote_count": emitted,
            "skipped_existing": skipped_existing,
            "errors": errors,
            "min_age_seconds": int(min_age_seconds),
            "horizon_seconds": int(horizon_seconds),
            "limit": int(limit),
        },
    )
    return stats
