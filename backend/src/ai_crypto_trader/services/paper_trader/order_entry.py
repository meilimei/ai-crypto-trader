from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import Candle, EquitySnapshot, PaperPosition, PaperTrade, StrategyConfig
from ai_crypto_trader.services.explainability.explainability import emit_trade_decision
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.execution import ExecutionResult, execute_market_order_with_costs
from ai_crypto_trader.services.paper_trader.equity import compute_equity
from ai_crypto_trader.services.paper_trader.equity_risk import check_equity_risk
from ai_crypto_trader.services.paper_trader.rejects import RejectCode, RejectReason, log_reject_throttled, make_reject
from ai_crypto_trader.services.paper_trader.policies_effective import (
    get_effective_position_policy,
    get_effective_risk_policy,
)
from ai_crypto_trader.services.policies.loader import (
    get_effective_policy_ids,
    load_position_policy,
    load_risk_policy,
)
from ai_crypto_trader.services.monitoring.strategy_alerts import maybe_alert_strategy_reject_burst
from ai_crypto_trader.services.paper_trader.rate_limit import check_and_record_rate_limit
from ai_crypto_trader.services.paper_trader.symbol_limits import get_symbol_limit
from ai_crypto_trader.services.paper_trader.utils import PreparedOrder, RiskRejected, prepare_order_inputs


@dataclass
class UnifiedOrderResult:
    execution: ExecutionResult
    prepared: PreparedOrder
    price_source: Optional[str]
    policy_source: Optional[str]
    strategy_id: Optional[str]


logger = logging.getLogger(__name__)


def _reject_from_reason(reason: str) -> RejectReason:
    reason_norm = (reason or "").strip().upper()
    if reason_norm in {"QTY_NOT_POSITIVE", "QTY_ZERO"}:
        return RejectReason(code=RejectCode.QTY_ZERO, reason="Quantity must be positive")
    if reason_norm in {"QTY_INVALID", "SYMBOL_EMPTY"}:
        return RejectReason(code=RejectCode.INVALID_QTY, reason="Quantity is invalid")
    if reason_norm.startswith("PRICE_"):
        return RejectReason(code=RejectCode.INVALID_QTY, reason="Price is invalid")
    if reason_norm in {"SYMBOL_CONTAINS_SLASH"}:
        return RejectReason(code=RejectCode.SYMBOL_DISABLED, reason="Symbol not supported")
    if reason_norm in {"INSUFFICIENT_CASH"}:
        return RejectReason(code=RejectCode.INSUFFICIENT_BALANCE, reason="Insufficient balance for order")
    if reason_norm in {"SHORT_NOT_ALLOWED", "INSUFFICIENT_POSITION"}:
        return RejectReason(code=RejectCode.POSITION_LIMIT, reason="Position limits prevent this trade")
    return RejectReason(code=RejectCode.INTERNAL_ERROR, reason=reason or "Rejected")


def _to_decimal(value: object | None) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: object | None) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_strategy_config_id(value: object | None) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_pct(value: Decimal) -> Decimal:
    return value / Decimal("100") if value > 1 else value


async def _load_latest_equity_state(
    session: AsyncSession,
    *,
    account_id: int,
) -> dict[str, Decimal] | None:
    row = await session.execute(
        select(
            EquitySnapshot.equity_usdt,
            EquitySnapshot.positions_notional_usdt,
        )
        .where(EquitySnapshot.account_id == account_id)
        .order_by(EquitySnapshot.created_at.desc())
        .limit(1)
    )
    latest = row.first()
    if latest and latest[0] is not None and latest[1] is not None:
        return {
            "equity_usdt": Decimal(str(latest[0])),
            "positions_notional_usdt": Decimal(str(latest[1])),
        }
    data = await compute_equity(session, account_id, price_source="order_entry")
    return {
        "equity_usdt": Decimal(str(data.get("equity_usdt", "0"))),
        "positions_notional_usdt": Decimal(str(data.get("positions_notional_usdt", "0"))),
    }


def _apply_risk_params(policy, params: dict) -> object:
    if not params:
        return policy
    lookback_minutes = _to_int(params.get("lookback_minutes"))
    lookback_hours = _to_int(params.get("equity_lookback_hours"))
    if lookback_minutes is not None and lookback_minutes > 0:
        lookback_hours = max(1, int(lookback_minutes / 60))
    return replace(
        policy,
        max_drawdown_pct=_to_decimal(params.get("max_drawdown_pct", policy.max_drawdown_pct)),
        max_daily_loss_usdt=_to_decimal(params.get("max_daily_loss_usdt", policy.max_daily_loss_usdt)),
        max_order_notional_usdt=_to_decimal(
            params.get("max_order_notional_usdt", policy.max_order_notional_usdt)
        ),
        max_drawdown_usdt=_to_decimal(params.get("max_drawdown_usdt", policy.max_drawdown_usdt)),
        equity_lookback_hours=lookback_hours or policy.equity_lookback_hours,
        max_leverage=_to_decimal(params.get("max_leverage", getattr(policy, "max_leverage", None))),
    )


def merge_policy_params(
    params: dict | None,
    symbol: str,
) -> tuple[dict, str | None, dict | None]:
    if not isinstance(params, dict) or not params:
        return {}, None, None

    default_params = params.get("default")
    per_symbol = params.get("per_symbol")
    has_structured = isinstance(default_params, dict) or isinstance(per_symbol, dict)

    merged: dict = {}
    source: str | None = None
    symbol_override: dict | None = None

    if has_structured:
        inline_defaults = {
            key: value
            for key, value in params.items()
            if key not in {"default", "per_symbol"}
        }
        if inline_defaults:
            merged.update(inline_defaults)
            source = "default"
        if isinstance(default_params, dict):
            merged.update(default_params)
            source = "default"

        if isinstance(per_symbol, dict):
            symbol_norm = normalize_symbol(symbol)
            symbol_params = per_symbol.get(symbol_norm)
            if symbol_params is None:
                symbol_params = per_symbol.get(symbol_norm.upper())
            if symbol_params is None and symbol:
                symbol_params = per_symbol.get(symbol)
            if isinstance(symbol_params, dict):
                merged.update(symbol_params)
                symbol_override = dict(symbol_params)
                source = "per_symbol"
        return merged, source, symbol_override

    # Backward-compatible flat policy params: treat as default.
    return dict(params), "default", None


def _resolve_position_params(
    *,
    params: dict,
    symbol: str,
) -> tuple[dict, str | None, dict | None]:
    merged, raw_source, symbol_override = merge_policy_params(params, symbol)
    source = None
    if raw_source == "default":
        source = "position_policy.default"
    elif raw_source == "per_symbol":
        source = "position_policy.per_symbol"
    return merged, source, symbol_override


def get_effective_position_limits(
    position_policy_params: dict | None,
    symbol_normalized: str,
) -> tuple[dict[str, Decimal | None], str | None, dict | None]:
    if not isinstance(position_policy_params, dict):
        return (
            {
                "max_position_qty": None,
                "max_position_notional_usdt": None,
                "max_position_pct_equity": None,
            },
            None,
            None,
        )
    merged, source, symbol_override = _resolve_position_params(
        params=position_policy_params,
        symbol=symbol_normalized,
    )
    limits = {
        "max_position_qty": _to_decimal(merged.get("max_position_qty")),
        "max_position_notional_usdt": _to_decimal(merged.get("max_position_notional_usdt")),
        "max_position_pct_equity": _to_decimal(merged.get("max_position_pct_equity")),
    }
    return limits, source, symbol_override


def get_effective_risk_limits(
    risk_policy_params: dict | None,
    symbol_normalized: str,
) -> tuple[dict[str, Decimal | int | None], str | None, dict | None]:
    if not isinstance(risk_policy_params, dict):
        return (
            {
                "max_order_notional_usdt": None,
                "min_order_notional_usdt": None,
                "max_leverage": None,
                "max_drawdown_pct": None,
                "max_drawdown_usdt": None,
                "max_daily_loss_usdt": None,
                "equity_lookback_hours": None,
                "lookback_minutes": None,
            },
            None,
            None,
        )
    merged, raw_source, symbol_override = merge_policy_params(risk_policy_params, symbol_normalized)
    source = None
    if raw_source == "default":
        source = "risk_policy.default"
    elif raw_source == "per_symbol":
        source = "risk_policy.per_symbol"
    limits: dict[str, Decimal | int | None] = {
        "max_order_notional_usdt": _to_decimal(merged.get("max_order_notional_usdt")),
        "min_order_notional_usdt": _to_decimal(merged.get("min_order_notional_usdt")),
        "max_leverage": _to_decimal(merged.get("max_leverage")),
        "max_drawdown_pct": _to_decimal(merged.get("max_drawdown_pct")),
        "max_drawdown_usdt": _to_decimal(merged.get("max_drawdown_usdt")),
        "max_daily_loss_usdt": _to_decimal(merged.get("max_daily_loss_usdt")),
        "equity_lookback_hours": _to_int(merged.get("equity_lookback_hours")),
        "lookback_minutes": _to_int(merged.get("lookback_minutes")),
    }
    return limits, source, symbol_override


def _apply_position_params(policy, params: dict, symbol: str) -> tuple[object, dict]:
    if not params:
        return policy, {}
    merged, _, _ = _resolve_position_params(params=params, symbol=symbol)
    if not merged:
        return policy, {}
    return (
        replace(
            policy,
            min_qty=_to_decimal(merged.get("min_qty", policy.min_qty)),
            min_order_notional_usdt=_to_decimal(
                merged.get("min_order_notional_usdt", policy.min_order_notional_usdt)
            ),
            max_position_notional_per_symbol_usdt=_to_decimal(
                merged.get(
                    "max_position_notional_usdt",
                    policy.max_position_notional_per_symbol_usdt,
                )
            ),
            max_total_notional_usdt=_to_decimal(
                merged.get("max_total_notional_usdt", policy.max_total_notional_usdt)
            ),
            max_position_qty=_to_decimal(
                merged.get("max_position_qty", getattr(policy, "max_position_qty", None))
            ),
            max_position_pct_equity=_to_decimal(
                merged.get("max_position_pct_equity", getattr(policy, "max_position_pct_equity", None))
            ),
        ),
        merged,
    )


async def _latest_price(session: AsyncSession, symbol: str) -> Optional[Decimal]:
    result = await session.execute(
        select(Candle.close)
        .where(Candle.symbol == symbol)
        .order_by(Candle.open_time.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _resolve_market_price(
    session: AsyncSession,
    *,
    symbol_in: str,
    symbol_norm: str,
) -> tuple[Optional[Decimal], Optional[str]]:
    price = await _latest_price(session, symbol_norm)
    if price is not None:
        candidate = Decimal(str(price))
        if candidate.is_finite() and candidate > 0:
            return candidate, "latest"

    symbol_slash = None
    if "/" in symbol_in:
        symbol_slash = symbol_in
    elif symbol_norm.endswith("USDT") and len(symbol_norm) > 4:
        symbol_slash = f"{symbol_norm[:-4]}/USDT"
    symbols = [symbol_norm]
    if symbol_slash and symbol_slash not in symbols:
        symbols.append(symbol_slash)
    last_trade = await session.execute(
        select(PaperTrade.price)
        .where(PaperTrade.symbol.in_(symbols))
        .order_by(desc(PaperTrade.created_at).nullslast(), PaperTrade.id.desc())
        .limit(1)
    )
    row = last_trade.first()
    if row and row[0] is not None:
        candidate = Decimal(str(row[0]))
        if candidate.is_finite() and candidate > 0:
            return candidate, "paper_trades_last"
    return None, None


async def place_order_unified(
    session: AsyncSession,
    *,
    account_id: int,
    symbol: str,
    side: str,
    qty: object,
    strategy_id: object | None = None,
    fee_bps: Decimal,
    slippage_bps: Decimal,
    price_override: object | None = None,
    market_price: object | None = None,
    market_price_source: str | None = None,
    meta: Optional[dict[str, str]] = None,
    reject_action_type: str = "ORDER_REJECTED",
    reject_window_seconds: int = 120,
) -> UnifiedOrderResult | RejectReason:
    symbol_in = (symbol or "ETHUSDT").strip()
    symbol_norm = normalize_symbol(symbol_in)
    side_norm = (side or "buy").lower().strip()
    qty_dec_raw: Decimal | None = None
    price_source: Optional[str] = None
    policy_source = "account_default"
    strategy_id_norm = strategy_id
    strategy_config_id = _normalize_strategy_config_id(strategy_id)
    strategy_id_for_meta = (
        str(strategy_config_id)
        if strategy_config_id is not None
        else (str(strategy_id_norm) if strategy_id_norm is not None else None)
    )
    symbol_limit = None
    bound_policy_meta: dict | None = None
    policy_ids = None
    bound_position_policy_row = None
    position_policy_limits = {
        "max_position_qty": None,
        "max_position_notional_usdt": None,
        "max_position_pct_equity": None,
    }
    position_policy_limit_source: str | None = None
    position_policy_symbol_override: dict | None = None
    risk_policy_limit_source: str | None = None
    risk_policy_symbol_override: dict | None = None
    risk_policy_limits: dict[str, Decimal | int | None] = {
        "max_order_notional_usdt": None,
        "min_order_notional_usdt": None,
        "max_leverage": None,
        "max_drawdown_pct": None,
        "max_drawdown_usdt": None,
        "max_daily_loss_usdt": None,
        "equity_lookback_hours": None,
        "lookback_minutes": None,
    }
    strategy_thresholds = None
    strategy_timeframe = None
    strategy_symbols_snapshot = None

    if strategy_config_id is not None:
        strategy_row = (
            await session.execute(
                select(
                    StrategyConfig.thresholds,
                    StrategyConfig.timeframe,
                    StrategyConfig.symbols,
                )
                .where(StrategyConfig.id == strategy_config_id)
                .limit(1)
            )
        ).first()
        if strategy_row is not None:
            strategy_thresholds = strategy_row[0] if isinstance(strategy_row[0], dict) else None
            strategy_timeframe = strategy_row[1]
            strategy_symbols_snapshot = strategy_row[2] if isinstance(strategy_row[2], list) else None
        policy_ids = await get_effective_policy_ids(session, strategy_config_id=strategy_config_id)
        if policy_ids is not None:
            bound_policy_meta = {
                "strategy_config_id": strategy_config_id,
                "legacy_risk_policy_id": policy_ids.legacy_risk_policy_id,
                "bound_risk_policy_id": policy_ids.bound_risk_policy_id,
                "effective_risk_policy_id": policy_ids.effective_risk_policy_id,
                "bound_position_policy_id": str(policy_ids.position_policy_id)
                if policy_ids.position_policy_id
                else None,
                "effective_position_policy_id": str(policy_ids.position_policy_id)
                if policy_ids.position_policy_id
                else None,
            }
            if policy_ids.bound_risk_policy_id is not None or policy_ids.position_policy_id is not None:
                policy_source = "binding"
            elif policy_ids.effective_risk_policy_id is not None:
                policy_source = "strategy_legacy"
            if strategy_symbols_snapshot is None and policy_ids.strategy_symbols:
                strategy_symbols_snapshot = list(policy_ids.strategy_symbols)
            if policy_ids.position_policy_id is not None:
                bound_position_policy_row = await load_position_policy(
                    session,
                    policy_id=policy_ids.position_policy_id,
                )
                if bound_position_policy_row is not None:
                    (
                        position_policy_limits,
                        position_policy_limit_source,
                        position_policy_symbol_override,
                    ) = get_effective_position_limits(
                        bound_position_policy_row.params or {},
                        symbol_norm,
                    )

    async def _log_reject(
        reject: RejectReason,
        *,
        prepared: PreparedOrder | None = None,
        notional: Decimal | None = None,
        risk_policy=None,
        position_policy=None,
        extra_meta: dict | None = None,
    ) -> None:
        policy_snapshot = {}
        if position_policy is not None:
            policy_snapshot.update(
                {
                    "min_qty": str(position_policy.min_qty) if position_policy.min_qty is not None else None,
                    "min_order_notional_usdt": str(position_policy.min_order_notional_usdt)
                    if position_policy.min_order_notional_usdt is not None
                    else None,
                    "max_position_notional_per_symbol_usdt": str(position_policy.max_position_notional_per_symbol_usdt)
                    if position_policy.max_position_notional_per_symbol_usdt is not None
                    else None,
                    "max_position_qty": str(position_policy.max_position_qty)
                    if getattr(position_policy, "max_position_qty", None) is not None
                    else None,
                    "max_position_pct_equity": str(position_policy.max_position_pct_equity)
                    if getattr(position_policy, "max_position_pct_equity", None) is not None
                    else None,
                    "max_total_notional_usdt": str(position_policy.max_total_notional_usdt)
                    if position_policy.max_total_notional_usdt is not None
                    else None,
                }
            )
        if risk_policy is not None:
            policy_snapshot.update(
                {
                    "max_order_notional_usdt": str(risk_policy.max_order_notional_usdt)
                    if getattr(risk_policy, "max_order_notional_usdt", None) is not None
                    else None,
                    "max_daily_loss_usdt": str(risk_policy.max_daily_loss_usdt)
                    if getattr(risk_policy, "max_daily_loss_usdt", None) is not None
                    else None,
                    "max_drawdown_usdt": str(risk_policy.max_drawdown_usdt)
                    if getattr(risk_policy, "max_drawdown_usdt", None) is not None
                    else None,
                    "max_drawdown_pct": str(risk_policy.max_drawdown_pct)
                    if getattr(risk_policy, "max_drawdown_pct", None) is not None
                    else None,
                    "max_leverage": str(risk_policy.max_leverage)
                    if getattr(risk_policy, "max_leverage", None) is not None
                    else None,
                    "equity_lookback_hours": str(risk_policy.equity_lookback_hours)
                    if getattr(risk_policy, "equity_lookback_hours", None) is not None
                    else None,
                    "order_rate_limit_max": str(risk_policy.order_rate_limit_max)
                    if getattr(risk_policy, "order_rate_limit_max", None) is not None
                    else None,
                    "order_rate_limit_window_seconds": str(risk_policy.order_rate_limit_window_seconds)
                    if getattr(risk_policy, "order_rate_limit_window_seconds", None) is not None
                    else None,
                }
            )
        reject_payload = make_reject(reject.code, reject.reason, reject.details)
        policy_source_local = policy_source
        if strategy_id_norm and (
            getattr(risk_policy, "source", None) == "strategy_override"
            or getattr(position_policy, "source", None) == "strategy_override"
        ):
            policy_source_local = "strategy_override"
        symbol_limit_source = position_policy_limit_source
        symbol_limits_payload = {
            "max_order_qty": None,
            "max_position_qty": None,
            "max_position_notional_usdt": None,
            "max_position_pct_equity": None,
        }
        if symbol_limit is not None and symbol_limit.max_order_qty is not None:
            symbol_limits_payload["max_order_qty"] = str(symbol_limit.max_order_qty)
        if position_policy_limits.get("max_position_qty") is not None:
            symbol_limits_payload["max_position_qty"] = str(position_policy_limits["max_position_qty"])
        elif symbol_limit is not None and symbol_limit.max_position_qty is not None:
            symbol_limits_payload["max_position_qty"] = str(symbol_limit.max_position_qty)
        if position_policy_limits.get("max_position_notional_usdt") is not None:
            symbol_limits_payload["max_position_notional_usdt"] = str(
                position_policy_limits["max_position_notional_usdt"]
            )
        elif symbol_limit is not None and symbol_limit.max_position_notional_usdt is not None:
            symbol_limits_payload["max_position_notional_usdt"] = str(symbol_limit.max_position_notional_usdt)
        if position_policy_limits.get("max_position_pct_equity") is not None:
            symbol_limits_payload["max_position_pct_equity"] = str(
                position_policy_limits["max_position_pct_equity"]
            )
        payload = {
            "message": "Execution skipped" if reject_action_type == "ORDER_SKIPPED" else "Order rejected",
            "reject": reject_payload,
            "account_id": account_id,
            "symbol": symbol_norm,
            "request": {
                "account_id": account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side_norm,
                "qty": str(qty),
                "price_override": str(price_override) if price_override is not None else None,
                "strategy_id": strategy_id_for_meta,
                "strategy_config_id": strategy_config_id,
            },
            "market_price": str(prepared.price) if prepared is not None else str(market_price) if market_price is not None else None,
            "price_source": price_source or market_price_source,
            "notional": str(notional) if notional is not None else None,
            "policies": policy_snapshot,
            "policy_binding": bound_policy_meta,
            "position_policy_overrides": position_policy_symbol_override,
            "risk_limit_source": risk_policy_limit_source,
            "risk_policy_source": risk_policy_limit_source,
            "risk_limits": {
                "max_order_notional_usdt": str(risk_policy_limits.get("max_order_notional_usdt"))
                if risk_policy_limits.get("max_order_notional_usdt") is not None
                else None,
                "min_order_notional_usdt": str(risk_policy_limits.get("min_order_notional_usdt"))
                if risk_policy_limits.get("min_order_notional_usdt") is not None
                else None,
                "max_leverage": str(risk_policy_limits.get("max_leverage"))
                if risk_policy_limits.get("max_leverage") is not None
                else None,
                "max_drawdown_pct": str(risk_policy_limits.get("max_drawdown_pct"))
                if risk_policy_limits.get("max_drawdown_pct") is not None
                else None,
                "max_drawdown_usdt": str(risk_policy_limits.get("max_drawdown_usdt"))
                if risk_policy_limits.get("max_drawdown_usdt") is not None
                else None,
                "max_daily_loss_usdt": str(risk_policy_limits.get("max_daily_loss_usdt"))
                if risk_policy_limits.get("max_daily_loss_usdt") is not None
                else None,
                "equity_lookback_hours": risk_policy_limits.get("equity_lookback_hours"),
                "lookback_minutes": risk_policy_limits.get("lookback_minutes"),
            },
            "risk_policy_overrides": risk_policy_symbol_override,
            "policy_source": policy_source_local,
            "strategy_id": strategy_id_for_meta,
            "strategy_config_id": strategy_config_id,
            "symbol_limit_source": symbol_limit_source,
            "symbol_limits": symbol_limits_payload,
        }
        if extra_meta:
            payload.update(extra_meta)
        try:
            await emit_trade_decision(
                session,
                {
                    "created_now_utc": datetime.now(timezone.utc),
                    "account_id": str(account_id),
                    "strategy_id": strategy_id_for_meta,
                    "strategy_config_id": strategy_config_id,
                    "symbol_in": symbol_in,
                    "symbol_normalized": symbol_norm,
                    "side": side_norm,
                    "qty_requested": str(
                        prepared.qty
                        if prepared is not None
                        else (qty_dec_raw if qty_dec_raw is not None else qty)
                    ),
                    "status": "skipped" if reject_action_type == "ORDER_SKIPPED" else "rejected",
                    "rationale": reject.reason,
                    "inputs": {
                        "strategy_thresholds": strategy_thresholds,
                        "timeframe": strategy_timeframe,
                        "symbols": strategy_symbols_snapshot,
                        "price_source": price_source or market_price_source,
                        "market_price_used": str(prepared.price)
                        if prepared is not None
                        else (str(market_price) if market_price is not None else None),
                    },
                    "policy_source": policy_source_local,
                    "policy_binding": bound_policy_meta,
                    "computed_limits": {
                        "risk_limits": payload.get("risk_limits"),
                        "risk_limit_source": payload.get("risk_limit_source"),
                        "risk_policy_overrides": payload.get("risk_policy_overrides"),
                        "symbol_limits": payload.get("symbol_limits"),
                        "symbol_limit_source": payload.get("symbol_limit_source"),
                        "position_policy_overrides": payload.get("position_policy_overrides"),
                    },
                    "result": {"reject": reject_payload},
                },
            )
        except Exception:
            logger.exception(
                "Failed to emit trade decision for reject",
                extra={
                    "account_id": account_id,
                    "strategy_config_id": strategy_config_id,
                    "symbol": symbol_norm,
                    "side": side_norm,
                    "reject_code": str(reject.code),
                },
            )
        await log_reject_throttled(
            action=reject_action_type,
            account_id=account_id,
            symbol=symbol_norm,
            reject=reject_payload,
            message=payload.get("message"),
            meta=payload,
            window_seconds=reject_window_seconds,
        )
        if strategy_id_norm is not None:
            try:
                await maybe_alert_strategy_reject_burst(
                    session,
                    account_id=account_id,
                    strategy_id=strategy_id_norm,
                    symbol=symbol_norm,
                    reject_code=reject.code,
                    now_utc=datetime.now(timezone.utc),
                )
            except Exception:
                logger.exception("Strategy reject burst alert failed")

    if "/" in symbol_in:
        reject = RejectReason(code=RejectCode.SYMBOL_DISABLED, reason="Symbol not supported")
        await _log_reject(reject)
        return reject
    if policy_ids is not None and policy_ids.strategy_symbols:
        allowed = {normalize_symbol(sym) for sym in policy_ids.strategy_symbols if sym}
        if allowed and symbol_norm not in allowed:
            reject = RejectReason(
                code=RejectCode.SYMBOL_DISABLED,
                reason="Symbol not enabled for strategy",
                details={
                    "strategy_config_id": strategy_config_id,
                    "symbol": symbol_norm,
                    "allowlist_count": len(allowed),
                    "allowlist_missing": True,
                },
            )
            await _log_reject(reject)
            return reject
    if side_norm not in {"buy", "sell"}:
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="side must be buy or sell")
        await _log_reject(reject)
        return reject

    try:
        qty_dec_raw = Decimal(str(qty))
    except Exception:
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="qty must be a valid decimal")
        await _log_reject(reject)
        return reject
    if qty_dec_raw <= 0:
        reject = RejectReason(code=RejectCode.QTY_ZERO, reason="qty must be positive")
        await _log_reject(reject)
        return reject

    price = None
    if price_override is not None:
        try:
            price = Decimal(str(price_override))
        except (InvalidOperation, TypeError, ValueError):
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price must be a valid decimal")
            await _log_reject(reject)
            return reject
        if not price.is_finite() or price <= 0:
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price must be positive")
            await _log_reject(reject)
            return reject
        price_source = "override"
    elif market_price is not None:
        try:
            price = Decimal(str(market_price))
        except (InvalidOperation, TypeError, ValueError):
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price must be a valid decimal")
            await _log_reject(reject)
            return reject
        if not price.is_finite() or price <= 0:
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price must be positive")
            await _log_reject(reject)
            return reject
        price_source = market_price_source or "engine"
    else:
        price, price_source = await _resolve_market_price(session, symbol_in=symbol_in, symbol_norm=symbol_norm)
        if price is None:
            reject = RejectReason(
                code=RejectCode.SYMBOL_DISABLED,
                reason=f"No price data for symbol {symbol_norm}; pass price to override",
            )
            await _log_reject(reject)
            return reject

    try:
        prepared = prepare_order_inputs(symbol_in, qty_dec_raw, price)
    except ValueError as exc:
        reject = _reject_from_reason(str(exc))
        await _log_reject(reject)
        return reject

    risk_policy = await get_effective_risk_policy(session, account_id, strategy_id_norm)
    position_policy = await get_effective_position_policy(session, account_id, strategy_id_norm)
    if policy_ids is not None:
        if policy_ids.effective_risk_policy_id is not None:
            risk_policy_row = await load_risk_policy(
                session,
                policy_id=policy_ids.effective_risk_policy_id,
            )
            if risk_policy_row is not None:
                (
                    risk_policy_limits,
                    risk_policy_limit_source,
                    risk_policy_symbol_override,
                ) = get_effective_risk_limits(risk_policy_row.params or {}, prepared.symbol)
                risk_params, risk_source_raw, risk_symbol_override = merge_policy_params(
                    risk_policy_row.params or {},
                    prepared.symbol,
                )
                if risk_source_raw == "default":
                    risk_policy_limit_source = "risk_policy.default"
                elif risk_source_raw == "per_symbol":
                    risk_policy_limit_source = "risk_policy.per_symbol"
                risk_policy_symbol_override = risk_symbol_override
                risk_policy = _apply_risk_params(risk_policy, risk_params)
        if policy_ids.position_policy_id is not None:
            if bound_position_policy_row is None:
                bound_position_policy_row = await load_position_policy(
                    session,
                    policy_id=policy_ids.position_policy_id,
                )
            if bound_position_policy_row is not None:
                position_policy, _ = _apply_position_params(
                    position_policy,
                    bound_position_policy_row.params or {},
                    prepared.symbol,
                )
                (
                    position_policy_limits,
                    position_policy_limit_source,
                    position_policy_symbol_override,
                ) = get_effective_position_limits(
                    bound_position_policy_row.params or {},
                    prepared.symbol,
                )
    if position_policy_limits.get("max_position_qty") is None and getattr(position_policy, "max_position_qty", None) is not None:
        position_policy_limits["max_position_qty"] = _to_decimal(getattr(position_policy, "max_position_qty", None))
    if (
        position_policy_limits.get("max_position_notional_usdt") is None
        and getattr(position_policy, "max_position_notional_per_symbol_usdt", None) is not None
    ):
        position_policy_limits["max_position_notional_usdt"] = _to_decimal(
            getattr(position_policy, "max_position_notional_per_symbol_usdt", None)
        )
    if (
        position_policy_limits.get("max_position_pct_equity") is None
        and getattr(position_policy, "max_position_pct_equity", None) is not None
    ):
        position_policy_limits["max_position_pct_equity"] = _to_decimal(
            getattr(position_policy, "max_position_pct_equity", None)
        )
    strategy_id_norm = (
        getattr(risk_policy, "strategy_id", None)
        or getattr(position_policy, "strategy_id", None)
        or strategy_id_norm
    )
    if strategy_id_norm is not None and not isinstance(strategy_id_norm, UUID):
        try:
            strategy_id_norm = UUID(str(strategy_id_norm))
        except Exception:
            strategy_id_norm = None
    if strategy_id_norm and (
        getattr(risk_policy, "source", None) == "strategy_override"
        or getattr(position_policy, "source", None) == "strategy_override"
    ):
        policy_source = "strategy_override"

    rate_limit_max = getattr(risk_policy, "order_rate_limit_max", None)
    rate_limit_window = getattr(risk_policy, "order_rate_limit_window_seconds", None)
    rate_limit_max_int = None
    rate_limit_window_int = None
    if rate_limit_max is not None and rate_limit_window is not None:
        try:
            rate_limit_max_int = int(rate_limit_max)
            rate_limit_window_int = int(rate_limit_window)
        except (TypeError, ValueError):
            rate_limit_max_int = None
            rate_limit_window_int = None
    if rate_limit_max_int and rate_limit_window_int and rate_limit_max_int > 0 and rate_limit_window_int > 0:
        allowed, count = await check_and_record_rate_limit(
            session,
            account_id=account_id,
            strategy_id=strategy_id_norm,
            symbol=prepared.symbol,
            max_requests=rate_limit_max_int,
            window_seconds=rate_limit_window_int,
        )
        if not allowed:
            reject = RejectReason(
                code=RejectCode.RATE_LIMITED,
                reason="Too many order requests in time window",
                details={
                    "max": rate_limit_max_int,
                    "window_seconds": rate_limit_window_int,
                    "count": count,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                risk_policy=risk_policy,
                position_policy=position_policy,
                extra_meta={
                    "rate_limit": {
                        "max": rate_limit_max_int,
                        "window_seconds": rate_limit_window_int,
                        "count": count,
                        "account_id": account_id,
                        "strategy_id": str(strategy_id_norm) if strategy_id_norm is not None else None,
                        "symbol": prepared.symbol,
                    }
                },
            )
            return reject

    equity_reject = await check_equity_risk(
        session,
        account_id=account_id,
        strategy_id=strategy_id_norm,
        policy=risk_policy,
        now_utc=datetime.now(timezone.utc),
    )
    if equity_reject:
        details = equity_reject.get("details") or {}
        if not isinstance(details, dict):
            details = {"raw_details": str(details)}
        details = dict(details)
        details["policy_binding"] = bound_policy_meta
        details["risk_policy_source"] = risk_policy_limit_source
        details["risk_policy_overrides"] = risk_policy_symbol_override
        reject = RejectReason(
            code=RejectCode(equity_reject.get("code")),
            reason=equity_reject.get("reason", "Equity risk limit exceeded"),
            details=details,
        )
        await _log_reject(
            reject,
            prepared=prepared,
            risk_policy=risk_policy,
            position_policy=position_policy,
            extra_meta={"equity_risk": equity_reject.get("details")},
        )
        return reject

    if position_policy.min_qty is not None:
        min_qty = Decimal(str(position_policy.min_qty))
        if min_qty > 0 and prepared.qty.copy_abs() < min_qty:
            reject = RejectReason(
                code=RejectCode.MIN_QTY,
                reason="Quantity below minimum",
                details={"min_qty": str(min_qty)},
            )
            await _log_reject(reject, prepared=prepared, risk_policy=risk_policy, position_policy=position_policy)
            return reject

    symbol_limit = await get_symbol_limit(session, account_id, strategy_id_norm, prepared.symbol)
    if symbol_limit is not None:
        if symbol_limit.source == "strategy":
            policy_source = "strategy_override"
    if symbol_limit is not None and symbol_limit.max_order_qty is not None:
        max_order_qty = Decimal(str(symbol_limit.max_order_qty))
        if prepared.qty.copy_abs() > max_order_qty:
            reject = RejectReason(
                code=RejectCode.SYMBOL_MAX_ORDER_QTY,
                reason="Order quantity exceeds symbol max",
                details={
                    "max_order_qty": str(max_order_qty),
                    "qty": str(prepared.qty),
                    "symbol": prepared.symbol,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    notional = prepared.qty.copy_abs() * prepared.price
    if position_policy.min_order_notional_usdt is not None:
        min_notional = Decimal(str(position_policy.min_order_notional_usdt))
        if notional < min_notional:
            reject = RejectReason(
                code=RejectCode.MIN_ORDER_NOTIONAL,
                reason="Order notional below minimum",
                details={
                    "min_order_notional_usdt": str(min_notional),
                    "notional": str(notional),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    risk_min_notional = risk_policy_limits.get("min_order_notional_usdt")
    if risk_min_notional is not None:
        min_notional = Decimal(str(risk_min_notional))
        if min_notional > 0 and notional < min_notional:
            reject = RejectReason(
                code=RejectCode.MIN_ORDER_NOTIONAL,
                reason="Order notional below risk policy minimum",
                details={
                    "min_order_notional_usdt": str(min_notional),
                    "notional": str(notional),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                    "risk_limit_source": risk_policy_limit_source,
                    "policy_binding": bound_policy_meta,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    if risk_policy.max_order_notional_usdt is not None:
        max_notional = Decimal(str(risk_policy.max_order_notional_usdt))
        if notional > max_notional:
            reject = RejectReason(
                code=RejectCode.MAX_ORDER_NOTIONAL,
                reason="Order notional above maximum",
                details={
                    "max_order_notional_usdt": str(max_notional),
                    "notional": str(notional),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    current_qty_dec = None
    next_qty = None
    delta_qty = None
    if (
        position_policy_limits.get("max_position_notional_usdt") is not None
        or position_policy.max_total_notional_usdt is not None
        or position_policy_limits.get("max_position_qty") is not None
        or position_policy_limits.get("max_position_pct_equity") is not None
        or getattr(risk_policy, "max_leverage", None) is not None
        or (symbol_limit is not None and (symbol_limit.max_position_qty is not None or symbol_limit.max_position_notional_usdt is not None))
    ):
        current_qty = await session.scalar(
            select(PaperPosition.qty)
            .where(PaperPosition.account_id == account_id, PaperPosition.symbol == prepared.symbol)
            .limit(1)
        )
        current_qty_dec = Decimal(str(current_qty)) if current_qty is not None else Decimal("0")
        delta_qty = prepared.qty if side_norm == "buy" else -prepared.qty
        next_qty = current_qty_dec + delta_qty
        next_notional = next_qty.copy_abs() * prepared.price
    else:
        next_notional = None

    if symbol_limit is not None and symbol_limit.max_position_qty is not None:
        max_pos_qty = Decimal(str(symbol_limit.max_position_qty))
        if next_qty is not None and next_qty.copy_abs() > max_pos_qty:
            reject = RejectReason(
                code=RejectCode.SYMBOL_MAX_POSITION_QTY,
                reason="Position quantity exceeds symbol max",
                details={
                    "max_position_qty": str(max_pos_qty),
                    "current_qty": str(current_qty_dec),
                    "next_qty": str(next_qty),
                    "qty": str(prepared.qty),
                    "symbol": prepared.symbol,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    policy_max_position_qty = position_policy_limits.get("max_position_qty")
    if policy_max_position_qty is not None and next_qty is not None:
        max_pos_qty = Decimal(str(policy_max_position_qty))
        if max_pos_qty > 0 and next_qty.copy_abs() > max_pos_qty:
            reject = RejectReason(
                code=RejectCode.MAX_POSITION_QTY,
                reason="Position quantity exceeds policy max",
                details={
                    "max_position_qty": str(max_pos_qty),
                    "limit": str(max_pos_qty),
                    "current_qty": str(current_qty_dec),
                    "delta_qty": str(delta_qty) if delta_qty is not None else None,
                    "next_qty": str(next_qty),
                    "qty": str(prepared.qty),
                    "symbol": prepared.symbol,
                    "symbol_limit_source": position_policy_limit_source,
                    "policy_binding": bound_policy_meta,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    if symbol_limit is not None and symbol_limit.max_position_notional_usdt is not None:
        limit_notional = Decimal(str(symbol_limit.max_position_notional_usdt))
        if next_notional is not None:
            if next_notional > limit_notional:
                reject = RejectReason(
                    code=RejectCode.SYMBOL_MAX_POSITION_NOTIONAL,
                    reason="Position notional exceeds symbol max",
                    details={
                        "max_position_notional_usdt": str(limit_notional),
                        "next_notional": str(next_notional),
                        "current_qty": str(current_qty_dec),
                        "next_qty": str(next_qty),
                        "qty": str(prepared.qty),
                        "market_price": str(prepared.price),
                    },
                )
                await _log_reject(
                    reject,
                    prepared=prepared,
                    notional=next_notional,
                    risk_policy=risk_policy,
                    position_policy=position_policy,
                )
                return reject

    policy_max_position_notional = position_policy_limits.get("max_position_notional_usdt")
    if policy_max_position_notional is not None:
        limit = Decimal(str(policy_max_position_notional))
        if next_notional is not None and next_notional > limit:
            reject = RejectReason(
                code=RejectCode.MAX_POSITION_NOTIONAL,
                reason="Position notional above maximum",
                details={
                    "max_position_notional_usdt": str(limit),
                    "limit": str(limit),
                    "next_notional": str(next_notional),
                    "current_qty": str(current_qty_dec),
                    "delta_qty": str(delta_qty) if delta_qty is not None else None,
                    "next_qty": str(next_qty),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                    "symbol_limit_source": position_policy_limit_source,
                    "policy_binding": bound_policy_meta,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=next_notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

    max_position_pct_equity = position_policy_limits.get("max_position_pct_equity")
    max_leverage = getattr(risk_policy, "max_leverage", None)
    equity_state = None
    equity_usdt = None
    if (max_position_pct_equity is not None or max_leverage is not None) and next_notional is not None:
        equity_state = await _load_latest_equity_state(session, account_id=account_id)
        if equity_state is not None:
            equity_usdt = equity_state.get("equity_usdt")

    if max_position_pct_equity is not None and next_notional is not None:
        pct_raw = Decimal(str(max_position_pct_equity))
        pct = _normalize_pct(pct_raw)
        if equity_usdt is not None and equity_usdt > 0 and pct > 0:
            cap = equity_usdt * pct
            if next_notional > cap:
                reject = RejectReason(
                    code=RejectCode.MAX_POSITION_PCT_EQUITY,
                    reason="Position exceeds equity percentage cap",
                    details={
                        "max_position_pct_equity": str(pct_raw),
                        "limit": str(cap),
                        "equity_usdt": str(equity_usdt),
                        "max_position_notional_usdt": str(cap),
                        "next_notional": str(next_notional),
                        "current_qty": str(current_qty_dec),
                        "delta_qty": str(delta_qty) if delta_qty is not None else None,
                        "next_qty": str(next_qty),
                        "qty": str(prepared.qty),
                        "market_price": str(prepared.price),
                        "symbol_limit_source": position_policy_limit_source,
                        "policy_binding": bound_policy_meta,
                    },
                )
                await _log_reject(
                    reject,
                    prepared=prepared,
                    notional=next_notional,
                    risk_policy=risk_policy,
                    position_policy=position_policy,
                )
                return reject

    if position_policy.max_total_notional_usdt is not None or max_leverage is not None:
        limit = None
        if position_policy.max_total_notional_usdt is not None:
            limit = Decimal(str(position_policy.max_total_notional_usdt))
        positions = await session.execute(
            select(PaperPosition.symbol, PaperPosition.qty, PaperPosition.avg_entry_price)
            .where(PaperPosition.account_id == account_id)
        )
        total_notional = Decimal("0")
        for sym, qty_val, avg_price in positions.all():
            sym_norm = normalize_symbol(sym)
            if sym_norm == prepared.symbol:
                qty_for_total = next_qty
                price_for_total = prepared.price
            else:
                qty_for_total = Decimal(str(qty_val or 0))
                price_for_total = Decimal(str(avg_price or 0))
            total_notional += qty_for_total.copy_abs() * price_for_total
        if limit is not None and total_notional > limit:
            reject = RejectReason(
                code=RejectCode.POSITION_LIMIT,
                reason="Account notional above maximum",
                details={
                    "max_total_notional_usdt": str(limit),
                    "total_notional": str(total_notional),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=total_notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

        if max_leverage is not None:
            max_leverage_dec = Decimal(str(max_leverage))
            if equity_usdt is None:
                equity_state = equity_state or await _load_latest_equity_state(session, account_id=account_id)
                equity_usdt = equity_state.get("equity_usdt") if equity_state else None
            if equity_usdt is not None and equity_usdt > 0 and max_leverage_dec > 0:
                if total_notional > equity_usdt * max_leverage_dec:
                    leverage = (
                        (total_notional / equity_usdt) if equity_usdt and equity_usdt > 0 else None
                    )
                    reject = RejectReason(
                        code=RejectCode.MAX_LEVERAGE,
                        reason="Account leverage exceeds maximum",
                        details={
                            "max_leverage": str(max_leverage_dec),
                            "equity_usdt": str(equity_usdt),
                            "total_notional": str(total_notional),
                            "leverage": str(leverage) if leverage is not None else None,
                            "qty": str(prepared.qty),
                            "market_price": str(prepared.price),
                            "policy_binding": bound_policy_meta,
                            "risk_policy_source": risk_policy_limit_source,
                            "risk_policy_overrides": risk_policy_symbol_override,
                        },
                    )
                    await _log_reject(
                        reject,
                        prepared=prepared,
                        notional=total_notional,
                        risk_policy=risk_policy,
                        position_policy=position_policy,
                    )
                    return reject

    # Final hard guard immediately before order creation to ensure position-policy
    # limits cannot be bypassed by earlier path variance.
    if (
        position_policy_limits.get("max_position_notional_usdt") is not None
        or position_policy_limits.get("max_position_qty") is not None
        or position_policy_limits.get("max_position_pct_equity") is not None
    ):
        if current_qty_dec is None or next_qty is None or next_notional is None:
            current_qty = await session.scalar(
                select(PaperPosition.qty)
                .where(PaperPosition.account_id == account_id, PaperPosition.symbol == prepared.symbol)
                .limit(1)
            )
            current_qty_dec = Decimal(str(current_qty)) if current_qty is not None else Decimal("0")
            delta_qty = prepared.qty if side_norm == "buy" else -prepared.qty
            next_qty = current_qty_dec + delta_qty
            next_notional = next_qty.copy_abs() * prepared.price

        limit_qty = position_policy_limits.get("max_position_qty")
        if limit_qty is not None and Decimal(str(limit_qty)) > 0 and next_qty.copy_abs() > Decimal(str(limit_qty)):
            reject = RejectReason(
                code=RejectCode.MAX_POSITION_QTY,
                reason="Position quantity exceeds policy max",
                details={
                    "max_position_qty": str(limit_qty),
                    "limit": str(limit_qty),
                    "current_qty": str(current_qty_dec),
                    "delta_qty": str(delta_qty),
                    "next_qty": str(next_qty),
                    "qty": str(prepared.qty),
                    "symbol": prepared.symbol,
                    "market_price": str(prepared.price),
                    "next_notional": str(next_notional),
                    "symbol_limit_source": position_policy_limit_source,
                    "policy_binding": bound_policy_meta,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=next_notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

        limit_notional = position_policy_limits.get("max_position_notional_usdt")
        if limit_notional is not None and Decimal(str(limit_notional)) > 0 and next_notional > Decimal(str(limit_notional)):
            reject = RejectReason(
                code=RejectCode.MAX_POSITION_NOTIONAL,
                reason="Position notional above maximum",
                details={
                    "max_position_notional_usdt": str(limit_notional),
                    "limit": str(limit_notional),
                    "current_qty": str(current_qty_dec),
                    "delta_qty": str(delta_qty),
                    "next_qty": str(next_qty),
                    "qty": str(prepared.qty),
                    "market_price": str(prepared.price),
                    "next_notional": str(next_notional),
                    "symbol_limit_source": position_policy_limit_source,
                    "policy_binding": bound_policy_meta,
                },
            )
            await _log_reject(
                reject,
                prepared=prepared,
                notional=next_notional,
                risk_policy=risk_policy,
                position_policy=position_policy,
            )
            return reject

        pct_limit = position_policy_limits.get("max_position_pct_equity")
        if pct_limit is not None:
            pct_raw = Decimal(str(pct_limit))
            pct = _normalize_pct(pct_raw)
            if equity_usdt is None:
                equity_state = equity_state or await _load_latest_equity_state(session, account_id=account_id)
                equity_usdt = equity_state.get("equity_usdt") if equity_state else None
            if equity_usdt is not None and equity_usdt > 0 and pct > 0:
                cap = equity_usdt * pct
                if next_notional > cap:
                    reject = RejectReason(
                        code=RejectCode.MAX_POSITION_PCT_EQUITY,
                        reason="Position exceeds equity percentage cap",
                        details={
                            "max_position_pct_equity": str(pct_raw),
                            "limit": str(cap),
                            "equity_usdt": str(equity_usdt),
                            "current_qty": str(current_qty_dec),
                            "delta_qty": str(delta_qty),
                            "next_qty": str(next_qty),
                            "qty": str(prepared.qty),
                            "market_price": str(prepared.price),
                            "next_notional": str(next_notional),
                            "symbol_limit_source": position_policy_limit_source,
                            "policy_binding": bound_policy_meta,
                        },
                    )
                    await _log_reject(
                        reject,
                        prepared=prepared,
                        notional=next_notional,
                        risk_policy=risk_policy,
                        position_policy=position_policy,
                    )
                    return reject

    try:
        execution = await execute_market_order_with_costs(
            session=session,
            account_id=account_id,
            symbol=prepared.symbol,
            side=side_norm,
            qty=prepared.qty,
            mid_price=prepared.price,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            meta=meta,
        )
    except (RiskRejected, ValueError) as exc:
        reject = _reject_from_reason(str(exc))
        await _log_reject(
            reject,
            prepared=prepared,
            notional=notional,
            risk_policy=risk_policy,
            position_policy=position_policy,
        )
        return reject

    decision_payload = {
        "created_now_utc": datetime.now(timezone.utc),
        "account_id": str(account_id),
        "strategy_id": strategy_id_for_meta,
        "strategy_config_id": strategy_config_id,
        "symbol_in": symbol_in,
        "symbol_normalized": prepared.symbol,
        "side": side_norm,
        "qty_requested": str(prepared.qty),
        "status": "executed",
        "rationale": "Risk and position checks passed; order executed",
        "inputs": {
            "strategy_thresholds": strategy_thresholds,
            "timeframe": strategy_timeframe,
            "symbols": strategy_symbols_snapshot,
            "price_source": price_source or market_price_source,
            "market_price_used": str(prepared.price),
        },
        "policy_source": policy_source,
        "policy_binding": bound_policy_meta,
        "computed_limits": {
            "risk_limits": {
                "max_order_notional_usdt": str(risk_policy_limits.get("max_order_notional_usdt"))
                if risk_policy_limits.get("max_order_notional_usdt") is not None
                else None,
                "min_order_notional_usdt": str(risk_policy_limits.get("min_order_notional_usdt"))
                if risk_policy_limits.get("min_order_notional_usdt") is not None
                else None,
                "max_leverage": str(risk_policy_limits.get("max_leverage"))
                if risk_policy_limits.get("max_leverage") is not None
                else None,
                "max_drawdown_pct": str(risk_policy_limits.get("max_drawdown_pct"))
                if risk_policy_limits.get("max_drawdown_pct") is not None
                else None,
                "max_drawdown_usdt": str(risk_policy_limits.get("max_drawdown_usdt"))
                if risk_policy_limits.get("max_drawdown_usdt") is not None
                else None,
                "max_daily_loss_usdt": str(risk_policy_limits.get("max_daily_loss_usdt"))
                if risk_policy_limits.get("max_daily_loss_usdt") is not None
                else None,
                "equity_lookback_hours": risk_policy_limits.get("equity_lookback_hours"),
                "lookback_minutes": risk_policy_limits.get("lookback_minutes"),
            },
            "risk_limit_source": risk_policy_limit_source,
            "risk_policy_overrides": risk_policy_symbol_override,
            "symbol_limits": {
                "max_order_qty": str(symbol_limit.max_order_qty)
                if symbol_limit is not None and symbol_limit.max_order_qty is not None
                else None,
                "max_position_qty": str(position_policy_limits.get("max_position_qty"))
                if position_policy_limits.get("max_position_qty") is not None
                else None,
                "max_position_notional_usdt": str(position_policy_limits.get("max_position_notional_usdt"))
                if position_policy_limits.get("max_position_notional_usdt") is not None
                else None,
                "max_position_pct_equity": str(position_policy_limits.get("max_position_pct_equity"))
                if position_policy_limits.get("max_position_pct_equity") is not None
                else None,
            },
            "symbol_limit_source": position_policy_limit_source,
            "position_policy_overrides": position_policy_symbol_override,
        },
        "result": {
            "order_id": execution.order.id,
            "trade_id": execution.trade.id,
            "executed_qty": str(prepared.qty),
            "executed_price": str(execution.order.avg_fill_price)
            if execution.order.avg_fill_price is not None
            else str(prepared.price),
            "fee": str(execution.order.fee_paid) if execution.order.fee_paid is not None else None,
            "created_at": execution.trade.created_at.isoformat() if execution.trade.created_at else None,
        },
    }
    try:
        await emit_trade_decision(session, decision_payload)
    except Exception:
        logger.exception(
            "Failed to emit trade decision for executed order",
            extra={
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "symbol": prepared.symbol,
                "side": side_norm,
                "order_id": execution.order.id,
                "trade_id": execution.trade.id,
            },
        )

    return UnifiedOrderResult(
        execution=execution,
        prepared=prepared,
        price_source=price_source,
        policy_source=policy_source,
        strategy_id=strategy_id_for_meta,
    )
