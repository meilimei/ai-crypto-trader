from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import Candle, PaperPosition, PaperTrade
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.execution import ExecutionResult, execute_market_order_with_costs
from ai_crypto_trader.services.paper_trader.rejects import RejectCode, RejectReason, log_reject_throttled, make_reject
from ai_crypto_trader.services.paper_trader.policies_effective import (
    get_effective_position_policy,
    get_effective_risk_policy,
)
from ai_crypto_trader.services.paper_trader.symbol_limits import get_symbol_limit
from ai_crypto_trader.services.paper_trader.utils import PreparedOrder, RiskRejected, prepare_order_inputs


@dataclass
class UnifiedOrderResult:
    execution: ExecutionResult
    prepared: PreparedOrder
    price_source: Optional[str]
    policy_source: Optional[str]
    strategy_id: Optional[str]


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
    price_source: Optional[str] = None
    policy_source = "account_default"
    strategy_id_norm = strategy_id
    symbol_limit = None

    async def _log_reject(
        reject: RejectReason,
        *,
        prepared: PreparedOrder | None = None,
        notional: Decimal | None = None,
        risk_policy=None,
        position_policy=None,
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
                }
            )
        reject_payload = make_reject(reject.code, reject.reason, reject.details)
        policy_source_local = policy_source
        if strategy_id_norm and (
            getattr(risk_policy, "source", None) == "strategy_override"
            or getattr(position_policy, "source", None) == "strategy_override"
        ):
            policy_source_local = "strategy_override"
        symbol_limit_source = getattr(symbol_limit, "source", None)
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
                "strategy_id": str(strategy_id_norm) if strategy_id_norm is not None else None,
            },
            "market_price": str(prepared.price) if prepared is not None else str(market_price) if market_price is not None else None,
            "price_source": price_source or market_price_source,
            "notional": str(notional) if notional is not None else None,
            "policies": policy_snapshot,
            "policy_source": policy_source_local,
            "strategy_id": str(strategy_id_norm) if strategy_id_norm is not None else None,
            "symbol_limit_source": symbol_limit_source,
            "symbol_limits": {
                "max_order_qty": str(symbol_limit.max_order_qty) if symbol_limit and symbol_limit.max_order_qty is not None else None,
                "max_position_qty": str(symbol_limit.max_position_qty) if symbol_limit and symbol_limit.max_position_qty is not None else None,
                "max_position_notional_usdt": str(symbol_limit.max_position_notional_usdt)
                if symbol_limit and symbol_limit.max_position_notional_usdt is not None
                else None,
            },
        }
        await log_reject_throttled(
            action=reject_action_type,
            account_id=account_id,
            symbol=symbol_norm,
            reject=reject_payload,
            message=payload.get("message"),
            meta=payload,
            window_seconds=reject_window_seconds,
        )

    if "/" in symbol_in:
        reject = RejectReason(code=RejectCode.SYMBOL_DISABLED, reason="Symbol not supported")
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
    strategy_id_norm = (
        getattr(risk_policy, "strategy_id", None)
        or getattr(position_policy, "strategy_id", None)
        or strategy_id_norm
    )
    if strategy_id_norm and (
        getattr(risk_policy, "source", None) == "strategy_override"
        or getattr(position_policy, "source", None) == "strategy_override"
    ):
        policy_source = "strategy_override"

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
    if (
        position_policy.max_position_notional_per_symbol_usdt is not None
        or position_policy.max_total_notional_usdt is not None
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

    if symbol_limit is not None and symbol_limit.max_position_notional_usdt is not None:
        limit_notional = Decimal(str(symbol_limit.max_position_notional_usdt))
        if next_qty is not None:
            next_notional = next_qty.copy_abs() * prepared.price
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

    if position_policy.max_position_notional_per_symbol_usdt is not None:
        limit = Decimal(str(position_policy.max_position_notional_per_symbol_usdt))
        next_notional = next_qty.copy_abs() * prepared.price
        if next_notional > limit:
            reject = RejectReason(
                code=RejectCode.MAX_POSITION_NOTIONAL,
                reason="Position notional above maximum",
                details={
                    "max_position_notional_usdt": str(limit),
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
        if total_notional > limit:
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

    return UnifiedOrderResult(
        execution=execution,
        prepared=prepared,
        price_source=price_source,
        policy_source=policy_source,
        strategy_id=str(strategy_id_norm) if strategy_id_norm is not None else None,
    )
