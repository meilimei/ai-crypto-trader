from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from sqlalchemy import String, and_, cast, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, Candle, EquitySnapshot, ExchangeOrder, PaperPosition
from ai_crypto_trader.exchange import (
    ExchangeError,
    ExchangeOrderRequest,
    get_exchange_client,
)
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.policies.effective import resolve_effective_policy_snapshot
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)

LIVE_PAUSE_SCOPE = "live_exchange"
DEFAULT_EXCHANGE = "binance_spot_testnet"
PENDING_STATES = {"pending", "retry"}
TERMINAL_STATES = {"accepted", "filled", "rejected", "failed", "canceled"}
_rate_limit_locks: dict[tuple[str, int], asyncio.Lock] = {}
_next_request_at: dict[tuple[str, int], datetime] = {}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        parsed = int(raw)
    except Exception:
        return default
    if parsed < minimum:
        return minimum
    return parsed


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _normalize_side(side: str) -> str:
    return (side or "").strip().lower()


def _normalize_order_type(order_type: str | None) -> str:
    return (order_type or "market").strip().lower()


def _is_live_enabled() -> bool:
    return _env_bool("LIVE_TRADING_ENABLED", default=False)


def _build_client_order_id(idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:28]
    return f"act{digest}"


def _calc_backoff_seconds(*, attempts: int, retry_after_seconds: int | None = None) -> int:
    base = _env_int("LIVE_EXCHANGE_RETRY_BASE_SECONDS", 5, minimum=1)
    cap = _env_int("LIVE_EXCHANGE_RETRY_CAP_SECONDS", 300, minimum=1)
    jitter_max = _env_int("LIVE_EXCHANGE_RETRY_JITTER_SECONDS", 3, minimum=0)
    exponent = max(0, attempts)
    delay = min(cap, base * (2 ** exponent))
    if retry_after_seconds is not None and retry_after_seconds > 0:
        delay = min(cap, max(delay, retry_after_seconds))
    if jitter_max > 0:
        delay = min(cap, int(delay + random.uniform(0, jitter_max)))
    return max(1, int(delay))


def _extract_filter_failure_type(exc: ExchangeError) -> str | None:
    details = exc.details if isinstance(exc.details, dict) else {}
    filter_type = details.get("filter_type")
    if isinstance(filter_type, str) and filter_type.strip():
        return filter_type.strip().upper()
    message = str(exc)
    match = re.search(r"Filter failure:\s*([A-Z_]+)", message)
    if match:
        return match.group(1).strip().upper()
    return None


def _is_terminal_status(status: str | None) -> bool:
    return (status or "").strip().lower() in TERMINAL_STATES


@dataclass(frozen=True)
class ExchangeErrorDecision:
    reason_code: str
    target_status: str
    retriable: bool
    pause_trigger: bool
    retry_after_seconds: int | None
    attempts: int
    filter_type: str | None
    local_precheck: bool


def _classify_exchange_error(
    exc: ExchangeError,
    *,
    current_attempts: int,
    max_attempts: int,
) -> ExchangeErrorDecision:
    details = exc.details if isinstance(exc.details, dict) else {}
    filter_type = _extract_filter_failure_type(exc)
    source = str(details.get("source") or "exchange_rejection").strip().lower()
    local_precheck = source == "local_precheck"
    attempts = int(current_attempts if local_precheck else current_attempts + 1)
    message_lower = str(exc).lower()

    if exc.is_auth_error:
        return ExchangeErrorDecision(
            reason_code="AUTH_ERROR",
            target_status="failed",
            retriable=False,
            pause_trigger=True,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type=filter_type,
            local_precheck=local_precheck,
        )

    if filter_type is not None:
        return ExchangeErrorDecision(
            reason_code=f"FILTER_{filter_type}",
            target_status="rejected",
            retriable=False,
            pause_trigger=False,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type=filter_type,
            local_precheck=local_precheck,
        )

    if exc.error_code in {-2010, -2011} or ("insufficient" in message_lower and "balance" in message_lower):
        return ExchangeErrorDecision(
            reason_code="INSUFFICIENT_BALANCE",
            target_status="rejected",
            retriable=False,
            pause_trigger=False,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type=None,
            local_precheck=False,
        )

    if "price filter" in message_lower:
        return ExchangeErrorDecision(
            reason_code="PRICE_FILTER",
            target_status="rejected",
            retriable=False,
            pause_trigger=False,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type="PRICE_FILTER",
            local_precheck=False,
        )

    if exc.error_code == -1021 or "outside of the recvwindow" in message_lower or "invalid timestamp" in message_lower:
        reason_code = "INVALID_TIMESTAMP"
    elif exc.status_code == 418 or "ip banned" in message_lower:
        reason_code = "IP_BANNED"
    elif exc.status_code == 429 or exc.error_code in {-1003, -1015}:
        reason_code = "RATE_LIMIT"
    elif exc.retriable:
        reason_code = "RETRIABLE_ERROR"
    else:
        reason_code = "NON_RETRIABLE"

    if not exc.retriable:
        return ExchangeErrorDecision(
            reason_code=reason_code,
            target_status="rejected",
            retriable=False,
            pause_trigger=False,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type=None,
            local_precheck=False,
        )

    critical_retry_threshold = _env_int("LIVE_EXCHANGE_CRITICAL_RETRY_THRESHOLD", 5, minimum=1)
    pause_trigger = reason_code == "IP_BANNED" or (
        reason_code in {"INVALID_TIMESTAMP", "RATE_LIMIT", "RETRIABLE_ERROR"}
        and attempts >= critical_retry_threshold
    )
    if attempts >= max_attempts:
        return ExchangeErrorDecision(
            reason_code="MAX_ATTEMPTS_EXCEEDED",
            target_status="failed",
            retriable=False,
            pause_trigger=True if pause_trigger or reason_code in {"INVALID_TIMESTAMP", "RATE_LIMIT", "RETRIABLE_ERROR"} else False,
            retry_after_seconds=exc.retry_after_seconds,
            attempts=attempts,
            filter_type=None,
            local_precheck=False,
        )
    return ExchangeErrorDecision(
        reason_code=reason_code,
        target_status="pending",
        retriable=True,
        pause_trigger=pause_trigger,
        retry_after_seconds=exc.retry_after_seconds,
        attempts=attempts,
        filter_type=None,
        local_precheck=False,
    )


def _parse_symbol_variants(symbol_normalized: str) -> tuple[str, str]:
    norm = normalize_symbol(symbol_normalized)
    if len(norm) >= 6 and norm.endswith("USDT"):
        return norm, f"{norm[:-4]}/USDT"
    return norm, norm


async def _resolve_market_price(session: AsyncSession, symbol_normalized: str) -> Decimal | None:
    norm, slash = _parse_symbol_variants(symbol_normalized)
    row = await session.execute(
        select(Candle.close)
        .where(
            Candle.symbol.in_([norm, slash]),
        )
        .order_by(Candle.close_time.desc())
        .limit(1)
    )
    close_value = row.scalar()
    return _to_decimal(close_value)


async def _resolve_exchange_market_price(exchange: str, symbol_normalized: str) -> Decimal | None:
    try:
        client = get_exchange_client(exchange)
    except Exception:
        return None
    try:
        return await client.get_market_price(symbol_normalized)
    except ExchangeError:
        return None
    except Exception:
        return None


async def _load_current_qty(session: AsyncSession, account_id: int, symbol: str) -> Decimal:
    qty_value = await session.scalar(
        select(PaperPosition.qty)
        .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol)
        .limit(1)
    )
    qty = _to_decimal(qty_value)
    return qty if qty is not None else Decimal("0")


async def _load_latest_equity(session: AsyncSession, account_id: int) -> Decimal | None:
    row = await session.execute(
        select(EquitySnapshot.equity)
        .where(EquitySnapshot.account_id == account_id)
        .order_by(EquitySnapshot.created_at.desc())
        .limit(1)
    )
    return _to_decimal(row.scalar())


def _is_meta_true(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _meta_account_id(meta: dict[str, Any] | None) -> int | None:
    if not isinstance(meta, dict):
        return None
    for candidate in (meta.get("account_id"), (meta.get("details") or {}).get("account_id")):
        if candidate is None:
            continue
        try:
            return int(str(candidate))
        except Exception:
            continue
    return None


async def get_live_pause_state(session: AsyncSession, *, account_id: int | None = None) -> dict[str, Any]:
    rows = (
        await session.execute(
            select(AdminAction)
            .where(
                AdminAction.action.in_(["AUTOPILOT_PAUSED", "AUTOPILOT_RESUMED"]),
                AdminAction.meta.op("->>")("scope") == LIVE_PAUSE_SCOPE,
            )
            .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
            .limit(200)
        )
    ).scalars().all()
    if not rows:
        return {"paused": False, "reason": None, "updated_at": None, "meta": {}}

    selected: AdminAction | None = None
    selected_meta: dict[str, Any] = {}
    for row in rows:
        meta = row.meta if isinstance(row.meta, dict) else {}
        event_account_id = _meta_account_id(meta)
        if account_id is None:
            selected = row
            selected_meta = meta
            break
        if event_account_id is None or event_account_id == int(account_id):
            selected = row
            selected_meta = meta
            break

    if selected is None:
        return {"paused": False, "reason": None, "updated_at": None, "meta": {}}

    paused = selected.action == "AUTOPILOT_PAUSED"
    paused_until_raw = selected_meta.get("paused_until")
    paused_until = None
    if isinstance(paused_until_raw, str):
        try:
            paused_until = datetime.fromisoformat(paused_until_raw.replace("Z", "+00:00"))
        except Exception:
            paused_until = None
    if paused and paused_until is not None:
        now_utc = _now_utc()
        if paused_until.tzinfo is None:
            paused_until = paused_until.replace(tzinfo=timezone.utc)
        if paused_until <= now_utc:
            paused = False
    return {
        "paused": paused,
        "reason": selected.message,
        "updated_at": selected.created_at.isoformat() if selected.created_at else None,
        "meta": json_safe(selected_meta),
    }


async def pause_live_autopilot(
    session: AsyncSession,
    *,
    reason: str,
    actor: str,
    paused_seconds: int | None = None,
    account_id: int | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    now_utc = _now_utc()
    paused_until = None
    if paused_seconds is not None and paused_seconds > 0:
        paused_until = now_utc + timedelta(seconds=paused_seconds)
    payload = {
        "scope": LIVE_PAUSE_SCOPE,
        "actor": actor,
        "reason": reason,
        "account_id": account_id,
        "paused_seconds": paused_seconds,
        "paused_until": paused_until.isoformat() if paused_until else None,
    }
    if isinstance(meta, dict):
        payload["details"] = meta
    session.add(
        AdminAction(
            action="AUTOPILOT_PAUSED",
            status="paused",
            message=reason,
            meta=json_safe(payload),
            dedupe_key=f"AUTOPILOT_PAUSED:{LIVE_PAUSE_SCOPE}",
        )
    )


async def resume_live_autopilot(
    session: AsyncSession,
    *,
    reason: str,
    actor: str,
    account_id: int | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    payload = {
        "scope": LIVE_PAUSE_SCOPE,
        "actor": actor,
        "reason": reason,
        "account_id": account_id,
    }
    if isinstance(meta, dict):
        payload["details"] = meta
    session.add(
        AdminAction(
            action="AUTOPILOT_RESUMED",
            status="running",
            message=reason,
            meta=json_safe(payload),
            dedupe_key=f"AUTOPILOT_RESUMED:{LIVE_PAUSE_SCOPE}",
        )
    )


@dataclass
class LiveOrderValidation:
    ok: bool
    reject: dict[str, Any] | None
    policy_source: str
    policy_binding: dict[str, Any]
    computed_limits: dict[str, Any]
    market_price: Decimal
    price_source: str
    symbol: str
    side: str
    qty: Decimal
    order_type: str


async def _validate_live_order(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int | None,
    symbol: str,
    side: str,
    qty: Decimal,
    order_type: str,
    price: Decimal | None,
    market_price_override: Decimal | None = None,
    market_price_source: str = "db",
) -> LiveOrderValidation:
    symbol_norm = normalize_symbol(symbol)
    side_norm = _normalize_side(side)
    order_type_norm = _normalize_order_type(order_type)
    policy_source = "account_default"
    policy_binding: dict[str, Any] = {"strategy_config_id": strategy_config_id}
    computed_limits: dict[str, Any] = {}

    if strategy_config_id is not None:
        snapshot = await resolve_effective_policy_snapshot(
            session,
            strategy_config_id=int(strategy_config_id),
            symbol=symbol_norm,
        )
        if snapshot is not None:
            policy_source = snapshot.policy_source
            policy_binding = snapshot.policy_binding
            computed_limits = snapshot.computed_limits

    market_price = price
    price_source = "price_override"
    if market_price is None:
        market_price = market_price_override
        price_source = market_price_source
    if market_price is None:
        market_price = await _resolve_market_price(session, symbol_norm)
        price_source = "db_candle"
    if market_price is None or market_price <= 0:
        return LiveOrderValidation(
            ok=False,
            reject={
                "code": "SYMBOL_DISABLED",
                "reason": f"No market price available for {symbol_norm}",
                "details": {"symbol": symbol_norm, "price_source": price_source},
            },
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
            market_price=Decimal("0"),
            price_source=price_source,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty,
            order_type=order_type_norm,
        )

    notional = qty.copy_abs() * market_price
    risk_limits = computed_limits.get("risk_limits") if isinstance(computed_limits, dict) else {}
    symbol_limits = computed_limits.get("symbol_limits") if isinstance(computed_limits, dict) else {}

    max_order_notional = _to_decimal((risk_limits or {}).get("max_order_notional_usdt"))
    if max_order_notional is not None and max_order_notional > 0 and notional > max_order_notional:
        return LiveOrderValidation(
            ok=False,
            reject={
                "code": "MAX_ORDER_NOTIONAL",
                "reason": "Order notional above maximum",
                "details": {
                    "max_order_notional_usdt": str(max_order_notional),
                    "notional": str(notional),
                    "qty": str(qty),
                    "market_price": str(market_price),
                    "policy_binding": policy_binding,
                },
            },
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
            market_price=market_price,
            price_source=price_source,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty,
            order_type=order_type_norm,
        )

    current_qty = await _load_current_qty(session, account_id=account_id, symbol=symbol_norm)
    delta_qty = qty if side_norm == "buy" else -qty
    next_qty = current_qty + delta_qty
    next_notional = next_qty.copy_abs() * market_price

    max_position_qty = _to_decimal((symbol_limits or {}).get("max_position_qty"))
    if max_position_qty is not None and max_position_qty > 0 and next_qty.copy_abs() > max_position_qty:
        return LiveOrderValidation(
            ok=False,
            reject={
                "code": "MAX_POSITION_QTY",
                "reason": "Position quantity exceeds policy max",
                "details": {
                    "max_position_qty": str(max_position_qty),
                    "current_qty": str(current_qty),
                    "delta_qty": str(delta_qty),
                    "next_qty": str(next_qty),
                    "next_notional": str(next_notional),
                    "market_price": str(market_price),
                    "policy_binding": policy_binding,
                    "symbol_limit_source": computed_limits.get("symbol_limit_source"),
                },
            },
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
            market_price=market_price,
            price_source=price_source,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty,
            order_type=order_type_norm,
        )

    max_position_notional = _to_decimal((symbol_limits or {}).get("max_position_notional_usdt"))
    if max_position_notional is not None and max_position_notional > 0 and next_notional > max_position_notional:
        return LiveOrderValidation(
            ok=False,
            reject={
                "code": "MAX_POSITION_NOTIONAL",
                "reason": "Position notional above maximum",
                "details": {
                    "max_position_notional_usdt": str(max_position_notional),
                    "current_qty": str(current_qty),
                    "delta_qty": str(delta_qty),
                    "next_qty": str(next_qty),
                    "next_notional": str(next_notional),
                    "market_price": str(market_price),
                    "policy_binding": policy_binding,
                    "symbol_limit_source": computed_limits.get("symbol_limit_source"),
                },
            },
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
            market_price=market_price,
            price_source=price_source,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty,
            order_type=order_type_norm,
        )

    max_position_pct_equity = _to_decimal((symbol_limits or {}).get("max_position_pct_equity"))
    if max_position_pct_equity is not None and max_position_pct_equity > 0:
        equity = await _load_latest_equity(session, account_id=account_id)
        if equity is not None and equity > 0:
            pct = max_position_pct_equity / Decimal("100") if max_position_pct_equity > 1 else max_position_pct_equity
            cap = equity * pct
            if next_notional > cap:
                return LiveOrderValidation(
                    ok=False,
                    reject={
                        "code": "MAX_POSITION_PCT_EQUITY",
                        "reason": "Position exceeds equity percentage cap",
                        "details": {
                            "max_position_pct_equity": str(max_position_pct_equity),
                            "equity_usdt": str(equity),
                            "limit": str(cap),
                            "current_qty": str(current_qty),
                            "delta_qty": str(delta_qty),
                            "next_qty": str(next_qty),
                            "next_notional": str(next_notional),
                            "market_price": str(market_price),
                            "policy_binding": policy_binding,
                        },
                    },
                    policy_source=policy_source,
                    policy_binding=policy_binding,
                    computed_limits=computed_limits,
                    market_price=market_price,
                    price_source=price_source,
                    symbol=symbol_norm,
                    side=side_norm,
                    qty=qty,
                    order_type=order_type_norm,
                )

    return LiveOrderValidation(
        ok=True,
        reject=None,
        policy_source=policy_source,
        policy_binding=policy_binding,
        computed_limits=computed_limits,
        market_price=market_price,
        price_source=price_source,
        symbol=symbol_norm,
        side=side_norm,
        qty=qty,
        order_type=order_type_norm,
    )


@dataclass
class LiveOrderCreateResult:
    ok: bool
    order: ExchangeOrder | None
    created: bool
    reject: dict[str, Any] | None = None
    policy_source: str | None = None
    policy_binding: dict[str, Any] | None = None
    computed_limits: dict[str, Any] | None = None


def _build_local_precheck_reject(exc: ExchangeError) -> dict[str, Any]:
    details = exc.details if isinstance(exc.details, dict) else {}
    filter_type = _extract_filter_failure_type(exc)
    return {
        "code": filter_type or "FILTER_FAILURE",
        "reason": "Exchange local precheck rejected order",
        "details": {
            "kind": "FILTER_FAILURE",
            "source": str(details.get("source") or "local_precheck"),
            "retriable": False,
            "filter_type": filter_type,
            **details,
        },
    }


async def create_live_exchange_order(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int | None,
    exchange: str | None,
    symbol: str,
    side: str,
    qty: Decimal,
    order_type: str | None = None,
    price: Decimal | None = None,
    idempotency_key: str | None = None,
) -> LiveOrderCreateResult:
    exchange_name = (exchange or DEFAULT_EXCHANGE).strip().lower()
    symbol_norm = normalize_symbol(symbol)
    idempotency_value = (idempotency_key or "").strip()
    if not idempotency_value:
        raw = f"{exchange_name}:{account_id}:{strategy_config_id or ''}:{symbol_norm}:{_normalize_side(side)}:{qty}:{_normalize_order_type(order_type)}:{price or ''}"
        idempotency_value = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    if not _is_live_enabled():
        return LiveOrderCreateResult(
            ok=False,
            order=None,
            created=False,
            reject={"code": "LIVE_TRADING_DISABLED", "reason": "LIVE_TRADING_ENABLED is not set"},
        )

    pause_state = await get_live_pause_state(session, account_id=account_id)
    if pause_state.get("paused"):
        return LiveOrderCreateResult(
            ok=False,
            order=None,
            created=False,
            reject={
                "code": "AUTOPILOT_PAUSED",
                "reason": "Autopilot is paused",
                "details": pause_state,
            },
        )

    exchange_market_price: Decimal | None = None
    market_price_source = "db_candle"
    if price is None:
        exchange_market_price = await _resolve_exchange_market_price(exchange_name, symbol_norm)
        if exchange_market_price is not None and exchange_market_price > 0:
            market_price_source = "exchange"
        else:
            return LiveOrderCreateResult(
                ok=False,
                order=None,
                created=False,
                reject={
                    "code": "EXCHANGE_PRICE_UNAVAILABLE",
                    "reason": f"Unable to fetch exchange market price for {symbol_norm}",
                    "details": {
                        "exchange": exchange_name,
                        "symbol": symbol_norm,
                        "price_source": "exchange",
                    },
                },
            )

    validation = await _validate_live_order(
        session,
        account_id=account_id,
        strategy_config_id=strategy_config_id,
        symbol=symbol_norm,
        side=side,
        qty=qty,
        order_type=_normalize_order_type(order_type),
        price=price,
        market_price_override=exchange_market_price,
        market_price_source=market_price_source,
    )
    if not validation.ok:
        return LiveOrderCreateResult(
            ok=False,
            order=None,
            created=False,
            reject=validation.reject,
            policy_source=validation.policy_source,
            policy_binding=validation.policy_binding,
            computed_limits=validation.computed_limits,
        )

    client_order_id = _build_client_order_id(idempotency_value)
    existing = await session.scalar(
        select(ExchangeOrder)
        .where(
            ExchangeOrder.exchange == exchange_name,
            ExchangeOrder.account_id == account_id,
            ExchangeOrder.idempotency_key == idempotency_value,
        )
        .limit(1)
    )
    if existing is not None:
        session.add(
            AdminAction(
                action="IDEMPOTENCY_HIT",
                status=existing.status,
                message="Live order idempotency hit",
                meta=json_safe(
                    {
                        "exchange_order_id": str(existing.id),
                        "exchange": existing.exchange,
                        "account_id": existing.account_id,
                        "strategy_config_id": existing.strategy_config_id,
                        "idempotency_key": existing.idempotency_key,
                        "status": existing.status,
                    }
                ),
            )
        )
        if existing.status == "rejected":
            parsed_last_error: dict[str, Any] = {}
            try:
                if existing.last_error:
                    maybe_last_error = json.loads(existing.last_error)
                    if isinstance(maybe_last_error, dict):
                        parsed_last_error = maybe_last_error
            except Exception:
                parsed_last_error = {}
            details = parsed_last_error.get("details") if isinstance(parsed_last_error, dict) else {}
            if isinstance(details, dict) and str(details.get("source") or "").lower() == "local_precheck":
                return LiveOrderCreateResult(
                    ok=False,
                    order=existing,
                    created=False,
                    reject={
                        "code": str(parsed_last_error.get("filter_type") or "FILTER_FAILURE"),
                        "reason": "Exchange local precheck rejected order",
                        "details": details,
                    },
                    policy_source=validation.policy_source,
                    policy_binding=validation.policy_binding,
                    computed_limits=validation.computed_limits,
                )
        return LiveOrderCreateResult(
            ok=True,
            order=existing,
            created=False,
            policy_source=validation.policy_source,
            policy_binding=validation.policy_binding,
            computed_limits=validation.computed_limits,
        )

    precheck_request = ExchangeOrderRequest(
        symbol=validation.symbol,
        side=validation.side,
        order_type=validation.order_type,
        qty=validation.qty,
        price=price,
        market_price=validation.market_price,
        client_order_id=client_order_id,
    )
    try:
        client = get_exchange_client(exchange_name)
        await client.precheck_order(precheck_request)
    except ExchangeError as exc:
        filter_failure_type = _extract_filter_failure_type(exc)
        details = exc.details if isinstance(exc.details, dict) else {}
        reject_source = str(details.get("source") or "").strip().lower()
        if filter_failure_type is not None and reject_source == "local_precheck":
            error_payload = {
                "kind": "FILTER_FAILURE",
                "message": str(exc),
                "status_code": exc.status_code,
                "retry_after_seconds": exc.retry_after_seconds,
                "error_code": exc.error_code,
                "retriable": False,
                "filter_type": filter_failure_type,
                "details": details,
            }
            order_meta = {
                "policy_source": validation.policy_source,
                "policy_binding": validation.policy_binding,
                "computed_limits": validation.computed_limits,
                "market_price": str(validation.market_price),
                "price_source": validation.price_source,
                "created_by": "admin_live_api",
                "last_exchange_error": {
                    "message": str(exc),
                    "status_code": exc.status_code,
                    "retry_after_seconds": exc.retry_after_seconds,
                    "error_code": exc.error_code,
                    "retriable": False,
                    "kind": "FILTER_FAILURE",
                    "filter_type": filter_failure_type,
                    "details": details,
                },
            }
            stmt_rejected = (
                insert(ExchangeOrder)
                .values(
                    exchange=exchange_name,
                    account_id=account_id,
                    strategy_config_id=strategy_config_id,
                    symbol=validation.symbol,
                    side=validation.side,
                    order_type=validation.order_type,
                    qty=validation.qty,
                    price=price,
                    idempotency_key=idempotency_value,
                    client_order_id=client_order_id,
                    status="rejected",
                    attempts=0,
                    next_attempt_at=None,
                    last_error=json.dumps(error_payload, default=str),
                    meta=json_safe(order_meta),
                    updated_at=_now_utc(),
                )
                .on_conflict_do_nothing(index_elements=["exchange", "account_id", "idempotency_key"])
                .returning(ExchangeOrder.id)
            )
            inserted_rejected_id = (await session.execute(stmt_rejected)).scalar()
            rejected_row = None
            if inserted_rejected_id is not None:
                rejected_row = await session.scalar(
                    select(ExchangeOrder).where(ExchangeOrder.id == inserted_rejected_id).limit(1)
                )
            if rejected_row is None:
                rejected_row = await session.scalar(
                    select(ExchangeOrder)
                    .where(
                        ExchangeOrder.exchange == exchange_name,
                        ExchangeOrder.account_id == account_id,
                        ExchangeOrder.idempotency_key == idempotency_value,
                    )
                    .limit(1)
                )
            session.add(
                AdminAction(
                    action="EXCHANGE_FILTER_REJECT_LOCAL",
                    status="rejected",
                    message="Live order rejected by local filter precheck",
                    meta=json_safe(
                        {
                            "exchange_order_row_id": str(rejected_row.id) if rejected_row is not None else None,
                            "exchange": exchange_name,
                            "account_id": account_id,
                            "strategy_config_id": strategy_config_id,
                            "status": "rejected",
                            "attempts": 0,
                            "error_code": exc.error_code,
                            "error": str(exc),
                            "filter_type": filter_failure_type,
                            "idempotency_key": idempotency_value,
                            "symbol": validation.symbol,
                            "qty": str(validation.qty),
                            "policy_binding": validation.policy_binding,
                            "details": details,
                            "source": "local_precheck",
                        }
                    ),
                )
            )
            return LiveOrderCreateResult(
                ok=False,
                order=rejected_row,
                created=bool(inserted_rejected_id),
                reject=_build_local_precheck_reject(exc),
                policy_source=validation.policy_source,
                policy_binding=validation.policy_binding,
                computed_limits=validation.computed_limits,
            )
        return LiveOrderCreateResult(
            ok=False,
            order=None,
            created=False,
            reject={
                "code": "EXCHANGE_PRECHECK_UNAVAILABLE" if exc.retriable else "EXCHANGE_PRECHECK_FAILED",
                "reason": str(exc),
                "details": exc.details if isinstance(exc.details, dict) else {},
            },
            policy_source=validation.policy_source,
            policy_binding=validation.policy_binding,
            computed_limits=validation.computed_limits,
        )

    order_meta = {
        "policy_source": validation.policy_source,
        "policy_binding": validation.policy_binding,
        "computed_limits": validation.computed_limits,
        "market_price": str(validation.market_price),
        "price_source": validation.price_source,
        "created_by": "admin_live_api",
    }
    stmt = (
        insert(ExchangeOrder)
        .values(
            exchange=exchange_name,
            account_id=account_id,
            strategy_config_id=strategy_config_id,
            symbol=validation.symbol,
            side=validation.side,
            order_type=validation.order_type,
            qty=validation.qty,
            price=price,
            idempotency_key=idempotency_value,
            client_order_id=client_order_id,
            status="pending",
            attempts=0,
            next_attempt_at=_now_utc(),
            meta=json_safe(order_meta),
            updated_at=_now_utc(),
        )
        .on_conflict_do_nothing(index_elements=["exchange", "account_id", "idempotency_key"])
        .returning(ExchangeOrder.id)
    )
    inserted_id = (await session.execute(stmt)).scalar()
    if inserted_id is None:
        existing = await session.scalar(
            select(ExchangeOrder)
            .where(
                ExchangeOrder.exchange == exchange_name,
                ExchangeOrder.account_id == account_id,
                ExchangeOrder.idempotency_key == idempotency_value,
            )
            .limit(1)
        )
        return LiveOrderCreateResult(
            ok=True,
            order=existing,
            created=False,
            policy_source=validation.policy_source,
            policy_binding=validation.policy_binding,
            computed_limits=validation.computed_limits,
        )

    row = await session.scalar(select(ExchangeOrder).where(ExchangeOrder.id == inserted_id).limit(1))
    if row is not None:
        session.add(
            AdminAction(
                action="LIVE_ORDER_ENQUEUED",
                status="pending",
                message="Live order enqueued",
                meta=json_safe(
                    {
                        "exchange_order_id": str(row.id),
                        "exchange": row.exchange,
                        "account_id": str(row.account_id),
                        "strategy_config_id": row.strategy_config_id,
                        "symbol": row.symbol,
                        "side": row.side,
                        "qty": str(row.qty),
                        "order_type": row.order_type,
                        "idempotency_key": row.idempotency_key,
                        "client_order_id": row.client_order_id,
                        "market_price": str(validation.market_price),
                        "price_source": validation.price_source,
                        "policy_source": validation.policy_source,
                        "policy_binding": validation.policy_binding,
                        "computed_limits": validation.computed_limits,
                    }
                ),
            )
        )
    return LiveOrderCreateResult(
        ok=True,
        order=row,
        created=True,
        policy_source=validation.policy_source,
        policy_binding=validation.policy_binding,
        computed_limits=validation.computed_limits,
    )


async def list_live_exchange_orders(
    session: AsyncSession,
    *,
    exchange: str | None,
    account_id: int | None,
    strategy_config_id: int | None,
    symbol: str | None,
    status: str | None,
    since: datetime | None,
    until: datetime | None,
    limit: int,
    cursor: str | None,
) -> tuple[list[ExchangeOrder], str | None]:
    filters: list[Any] = []
    if exchange:
        filters.append(ExchangeOrder.exchange == exchange.strip().lower())
    if account_id is not None:
        filters.append(ExchangeOrder.account_id == account_id)
    if strategy_config_id is not None:
        filters.append(ExchangeOrder.strategy_config_id == strategy_config_id)
    if symbol:
        filters.append(ExchangeOrder.symbol == normalize_symbol(symbol))
    if status:
        filters.append(ExchangeOrder.status == status.strip().lower())
    if since is not None:
        filters.append(ExchangeOrder.created_at >= since)
    if until is not None:
        filters.append(ExchangeOrder.created_at <= until)

    cursor_row: ExchangeOrder | None = None
    if cursor:
        try:
            cursor_uuid = UUID(cursor)
            cursor_row = await session.scalar(
                select(ExchangeOrder).where(ExchangeOrder.id == cursor_uuid).limit(1)
            )
        except Exception:
            cursor_row = None
    if cursor_row is not None and cursor_row.created_at is not None:
        filters.append(
            or_(
                ExchangeOrder.created_at < cursor_row.created_at,
                and_(
                    ExchangeOrder.created_at == cursor_row.created_at,
                    cast(ExchangeOrder.id, String()) < str(cursor_row.id),
                ),
            )
        )

    stmt = select(ExchangeOrder)
    if filters:
        stmt = stmt.where(and_(*filters))
    stmt = stmt.order_by(ExchangeOrder.created_at.desc(), ExchangeOrder.id.desc()).limit(limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    next_cursor = None
    if len(rows) > limit:
        tail = rows[limit - 1]
        next_cursor = str(tail.id)
        rows = rows[:limit]
    return rows, next_cursor


async def _rate_limit_sleep(exchange: str, account_id: int) -> None:
    key = (exchange, int(account_id))
    lock = _rate_limit_locks.setdefault(key, asyncio.Lock())
    async with lock:
        now_utc = _now_utc()
        next_at = _next_request_at.get(key)
        if next_at is not None and next_at > now_utc:
            await asyncio.sleep((next_at - now_utc).total_seconds())


def _set_rate_limit_cooldown(exchange: str, account_id: int, seconds: int) -> None:
    if seconds <= 0:
        return
    key = (exchange, int(account_id))
    now_utc = _now_utc()
    candidate = now_utc + timedelta(seconds=seconds)
    current = _next_request_at.get(key)
    if current is None or candidate > current:
        _next_request_at[key] = candidate


async def _maybe_pause_circuit_breaker(
    session: AsyncSession,
    *,
    now_utc: datetime,
    recent_error: str | None,
) -> bool:
    threshold = _env_int("LIVE_CIRCUIT_BREAKER_CONSECUTIVE_FAILURES", 5, minimum=1)
    window_minutes = _env_int("LIVE_CIRCUIT_BREAKER_WINDOW_MINUTES", 5, minimum=1)
    window_start = now_utc - timedelta(minutes=window_minutes)

    critical_count = await session.scalar(
        select(func.count())
        .select_from(AdminAction)
        .where(
            AdminAction.action.in_(["LIVE_ORDER_RETRY_SCHEDULED", "LIVE_ORDER_FAILED"]),
            AdminAction.created_at >= window_start,
            AdminAction.meta.op("->>")("critical") == "true",
        )
    )
    if int(critical_count or 0) < threshold:
        return False
    pause_meta = {
        "scope": LIVE_PAUSE_SCOPE,
        "reason": "LIVE_EXCHANGE_FAILURE",
        "failure_mode": "circuit_breaker",
        "threshold": threshold,
        "window_minutes": window_minutes,
        "critical_count": int(critical_count or 0),
        "error_sample": recent_error,
    }
    await pause_live_autopilot(
        session,
        reason="LIVE_EXCHANGE_FAILURE",
        actor="system",
        meta=pause_meta,
    )
    return True


async def dispatch_live_exchange_orders_once(
    session: AsyncSession,
    *,
    limit: int = 50,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now = now_utc or _now_utc()
    if not _is_live_enabled():
        return {
            "picked_count": 0,
            "sent_count": 0,
            "rejected_count": 0,
            "rejected_local_count": 0,
            "failed_count": 0,
            "retried_count": 0,
            "pending_due_count": 0,
            "pending_remaining": 0,
            "paused": False,
            "disabled": True,
        }

    due_query = (
        select(ExchangeOrder)
        .where(ExchangeOrder.status.in_(list(PENDING_STATES)), ExchangeOrder.next_attempt_at <= now)
        .order_by(ExchangeOrder.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    due_before = int(
        (
            await session.execute(
                select(func.count())
                .select_from(ExchangeOrder)
                .where(ExchangeOrder.status.in_(list(PENDING_STATES)), ExchangeOrder.next_attempt_at <= now)
            )
        ).scalar()
        or 0
    )
    rows = (await session.execute(due_query)).scalars().all()

    picked = 0
    sent = 0
    rejected = 0
    rejected_local = 0
    failed = 0
    retried = 0
    error_sample = None
    paused_triggered = False

    max_attempts = _env_int("LIVE_EXCHANGE_MAX_ATTEMPTS", 8, minimum=1)
    account_pause_cache: dict[int, dict[str, Any]] = {}
    for row in rows:
        picked += 1
        account_pause = account_pause_cache.get(row.account_id)
        if account_pause is None:
            account_pause = await get_live_pause_state(session, account_id=row.account_id)
            account_pause_cache[row.account_id] = account_pause
        if account_pause.get("paused"):
            row.status = "pending"
            row.updated_at = now
            row.last_error = "AUTOPILOT_PAUSED"
            row.next_attempt_at = now + timedelta(seconds=30)
            retried += 1
            session.add(
                AdminAction(
                    action="LIVE_ORDER_RETRY_SCHEDULED",
                    status="pending",
                    message="Live order paused by autopilot state",
                    meta=json_safe(
                        {
                            "exchange_order_row_id": str(row.id),
                            "exchange": row.exchange,
                            "account_id": row.account_id,
                            "status": "pending",
                            "reason": "AUTOPILOT_PAUSED",
                            "autopilot_state": account_pause,
                            "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
                            "critical": False,
                        }
                    ),
                )
            )
            continue
        row.status = "sending"
        row.updated_at = now
        attempts_before_send = int(row.attempts or 0)
        send_attempted = False

        critical = False
        try:
            await _rate_limit_sleep(row.exchange, row.account_id)
            client = get_exchange_client(row.exchange)
            row_meta = row.meta if isinstance(row.meta, dict) else {}
            request = ExchangeOrderRequest(
                symbol=row.symbol,
                side=row.side,
                order_type=row.order_type,
                qty=Decimal(str(row.qty)),
                price=Decimal(str(row.price)) if row.price is not None else None,
                market_price=_to_decimal(row_meta.get("market_price")),
                client_order_id=row.client_order_id,
            )
            row.attempts = attempts_before_send + 1
            send_attempted = True
            result = await client.place_order(request)
            status = result.status.lower()
            row.exchange_order_id = result.exchange_order_id
            row.status = "filled" if status == "filled" else "accepted"
            row.last_error = None
            row.next_attempt_at = None if _is_terminal_status(row.status) else now
            row.updated_at = now
            meta = row.meta if isinstance(row.meta, dict) else {}
            meta["used_weight_headers"] = result.used_weight_headers
            meta["exchange_response"] = json_safe(result.response_payload)
            row.meta = json_safe(meta)
            if result.used_weight_headers:
                _set_rate_limit_cooldown(
                    row.exchange,
                    row.account_id,
                    _env_int("LIVE_EXCHANGE_MIN_SPACING_SECONDS", 1, minimum=0),
                )

            session.add(
                AdminAction(
                    action="LIVE_ORDER_SUBMITTED",
                    status=row.status,
                    message="Live order accepted by exchange",
                    meta=json_safe(
                        {
                            "exchange_order_row_id": str(row.id),
                            "exchange": row.exchange,
                            "account_id": row.account_id,
                            "exchange_order_id": row.exchange_order_id,
                            "status": row.status,
                            "attempts": row.attempts,
                            "used_weight_headers": result.used_weight_headers,
                        }
                    ),
                )
            )
            sent += 1
        except ExchangeError as exc:
            error_sample = str(exc)
            current_attempts = attempts_before_send
            decision = _classify_exchange_error(
                exc,
                current_attempts=current_attempts,
                max_attempts=max_attempts,
            )
            row.attempts = decision.attempts
            filter_failure_type = decision.filter_type
            details = exc.details if isinstance(exc.details, dict) else {}
            if decision.retry_after_seconds is not None:
                _set_rate_limit_cooldown(row.exchange, row.account_id, decision.retry_after_seconds)
            if decision.target_status == "rejected":
                row.status = "rejected"
                row.next_attempt_at = None
                rejected += 1
                if decision.local_precheck:
                    rejected_local += 1
                    action = "EXCHANGE_FILTER_REJECT_LOCAL"
                else:
                    action = "EXCHANGE_FILTER_REJECT_EXCHANGE"
            elif decision.target_status == "pending":
                row.status = "pending"
                delay = _calc_backoff_seconds(
                    attempts=max(0, int(row.attempts or 0)),
                    retry_after_seconds=decision.retry_after_seconds,
                )
                row.next_attempt_at = now + timedelta(seconds=delay)
                retried += 1
                action = "LIVE_ORDER_RETRY_SCHEDULED"
            else:
                row.status = "failed"
                row.next_attempt_at = None
                failed += 1
                action = "LIVE_ORDER_FAILED"
            critical = bool(decision.pause_trigger)
            error_payload = {
                "kind": "FILTER_FAILURE" if filter_failure_type is not None else "EXCHANGE_ERROR",
                "message": str(exc),
                "status_code": exc.status_code,
                "retry_after_seconds": decision.retry_after_seconds,
                "error_code": exc.error_code,
                "retriable": bool(decision.retriable),
                "filter_type": filter_failure_type,
                "reason_code": decision.reason_code,
                "details": exc.details if isinstance(exc.details, dict) else {},
            }
            row.last_error = json.dumps(error_payload, default=str)
            row.updated_at = now
            meta = row.meta if isinstance(row.meta, dict) else {}
            meta["last_exchange_error"] = {
                "message": str(exc),
                "status_code": exc.status_code,
                "retry_after_seconds": decision.retry_after_seconds,
                "error_code": exc.error_code,
                "retriable": bool(decision.retriable),
                "critical": critical,
                "used_weight_headers": exc.used_weight_headers,
                "kind": "FILTER_FAILURE" if filter_failure_type is not None else "EXCHANGE_ERROR",
                "filter_type": filter_failure_type,
                "reason_code": decision.reason_code,
                "details": exc.details if isinstance(exc.details, dict) else {},
            }
            row.meta = json_safe(meta)
            session.add(
                AdminAction(
                    action=action,
                    status=row.status,
                    message=(
                        "Live order rejected by local filter precheck"
                        if filter_failure_type is not None and str((exc.details or {}).get("source", "")).lower() == "local_precheck"
                        else "Live order rejected by exchange filter"
                        if filter_failure_type is not None
                        else "Live order exchange error"
                    ),
                    meta=json_safe(
                        {
                            "exchange_order_row_id": str(row.id),
                            "exchange": row.exchange,
                            "account_id": row.account_id,
                            "attempts": row.attempts,
                            "status_code": exc.status_code,
                            "retry_after_seconds": decision.retry_after_seconds,
                            "error_code": exc.error_code,
                            "error": str(exc),
                            "critical": critical,
                            "reason_code": decision.reason_code,
                            "filter_type": filter_failure_type,
                            "idempotency_key": row.idempotency_key,
                            "symbol": row.symbol,
                            "qty": str(row.qty),
                            "estimated_notional": ((exc.details or {}).get("estimated_notional") if isinstance(exc.details, dict) else None),
                            "policy_binding": (meta.get("policy_binding") if isinstance(meta, dict) else None),
                            "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
                            "kind": "FILTER_FAILURE" if filter_failure_type is not None else "EXCHANGE_ERROR",
                            "details": exc.details if isinstance(exc.details, dict) else {},
                            "source": (exc.details or {}).get("source") if isinstance(exc.details, dict) else None,
                        }
                    ),
                )
            )
            if filter_failure_type is not None:
                logger.warning(
                    "EXCHANGE_ORDER_REJECTED_FILTER_FAILURE",
                    extra={
                        "exchange_order_row_id": str(row.id),
                        "exchange": row.exchange,
                        "account_id": row.account_id,
                        "symbol": row.symbol,
                        "filter_type": filter_failure_type,
                        "source": (exc.details or {}).get("source") if isinstance(exc.details, dict) else None,
                        "idempotency_key": row.idempotency_key,
                    },
                )
            if decision.pause_trigger and filter_failure_type is None:
                await pause_live_autopilot(
                    session,
                    reason="EXCHANGE_ORDER_CRITICAL",
                    actor="system",
                    account_id=row.account_id,
                    meta={
                        "scope": LIVE_PAUSE_SCOPE,
                        "exchange": row.exchange,
                        "account_id": row.account_id,
                        "exchange_order_row_id": str(row.id),
                        "attempts": row.attempts,
                        "error": str(exc),
                        "status_code": exc.status_code,
                        "error_code": exc.error_code,
                        "reason_code": decision.reason_code,
                    },
                )
                paused_triggered = True
        except Exception as exc:  # safety net
            error_sample = str(exc)
            row.attempts = attempts_before_send + 1 if send_attempted else attempts_before_send
            delay = _calc_backoff_seconds(attempts=max(0, int(row.attempts or 0)))
            row.status = "pending" if row.attempts < max_attempts else "failed"
            row.next_attempt_at = now + timedelta(seconds=delay) if row.status == "pending" else None
            row.last_error = str(exc)
            row.updated_at = now
            if row.status == "pending":
                retried += 1
                action = "LIVE_ORDER_RETRY_SCHEDULED"
            else:
                failed += 1
                action = "LIVE_ORDER_FAILED"
            session.add(
                AdminAction(
                    action=action,
                    status=row.status,
                    message="Live order dispatcher exception",
                    meta=json_safe(
                        {
                            "exchange_order_row_id": str(row.id),
                            "exchange": row.exchange,
                            "account_id": row.account_id,
                            "attempts": row.attempts,
                            "error": str(exc),
                            "critical": False,
                            "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
                        }
                    ),
                )
            )
            if row.status == "failed":
                severe_attempts_threshold = _env_int("LIVE_EXCHANGE_SEVERE_FAILURE_ATTEMPTS", 5, minimum=1)
                if row.attempts >= severe_attempts_threshold:
                    await pause_live_autopilot(
                        session,
                        reason="LIVE_EXCHANGE_FAILURE",
                        actor="system",
                        account_id=row.account_id,
                        meta={
                            "scope": LIVE_PAUSE_SCOPE,
                            "exchange": row.exchange,
                            "account_id": row.account_id,
                            "exchange_order_row_id": str(row.id),
                            "attempts": row.attempts,
                            "error": str(exc),
                        },
                    )
                    paused_triggered = True

    paused_by_circuit = False
    if picked > 0:
        paused_by_circuit = await _maybe_pause_circuit_breaker(
            session,
            now_utc=now,
            recent_error=error_sample,
        )

    pending_due_after = int(
        (
            await session.execute(
                select(func.count())
                .select_from(ExchangeOrder)
                .where(ExchangeOrder.status.in_(list(PENDING_STATES)), ExchangeOrder.next_attempt_at <= now)
            )
        ).scalar()
        or 0
    )
    pending_remaining = int(
        (
            await session.execute(
                select(func.count()).select_from(ExchangeOrder).where(ExchangeOrder.status.in_(list(PENDING_STATES)))
            )
        ).scalar()
        or 0
    )
    await session.commit()
    return {
        "picked_count": picked,
        "sent_count": sent,
        "rejected_count": rejected,
        "rejected_local_count": rejected_local,
        "failed_count": failed,
        "retried_count": retried,
        "pending_due_count": pending_due_after,
        "pending_due_before_count": due_before,
        "pending_remaining": pending_remaining,
        "paused": bool(paused_by_circuit or paused_triggered),
        "paused_triggered": bool(paused_by_circuit or paused_triggered),
        "disabled": False,
    }
