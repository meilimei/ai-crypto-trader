import os
import logging
import traceback
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from ipaddress import IPv4Address, IPv6Address

from fastapi import APIRouter, Depends, HTTPException, Query, Header, Body
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, desc, delete
from sqlalchemy.exc import ProgrammingError
try:
    from asyncpg.exceptions import UndefinedTableError as AsyncpgUndefinedTableError
except Exception:  # pragma: no cover - fallback when asyncpg not installed
    AsyncpgUndefinedTableError = None

from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.services.paper_trader.runner import PaperTraderRunner, reset_paper_data
from ai_crypto_trader.services.paper_trader.maintenance import flatten_positions
from ai_crypto_trader.common.models import (
    Candle,
    EquitySnapshot,
    PaperAccount,
    PaperBalance,
    PaperOrder,
    PaperPosition,
    PaperTrade,
    AdminAction,
    utc_now,
)

from ai_crypto_trader.common.log_buffer import get_log_buffer
from ai_crypto_trader.services.paper_trader.config import get_active_bundle, ActiveConfigMissingError, PaperTraderConfig
from ai_crypto_trader.services.agent.auto_trade import AutoTradeAgent, AutoTradeSettings
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.risk import evaluate_and_size
from ai_crypto_trader.services.paper_trader.execution import execute_market_order_with_costs
from ai_crypto_trader.services.paper_trader.order_entry import (
    place_order_unified,
)
from ai_crypto_trader.services.explainability.explainability import emit_trade_decision
from ai_crypto_trader.services.policies.effective import resolve_effective_policy_snapshot
from ai_crypto_trader.services.paper_trader.policies import DEFAULT_POSITION_POLICY, DEFAULT_RISK_POLICY, validate_order
from ai_crypto_trader.services.paper_trader.rejects import RejectCode, RejectReason
from ai_crypto_trader.services.paper_trader.utils import prepare_order_inputs, RiskRejected
from ai_crypto_trader.services.paper_trader.maintenance import reconcile_report, reconcile_fix, reconcile_apply, normalize_status
from ai_crypto_trader.services.paper_trader.reconcile_policy import (
    apply_reconcile_policy_status,
    get_reconcile_policy,
    policy_snapshot,
)
from ai_crypto_trader.common.jsonable import to_jsonable
from ai_crypto_trader.services.admin_actions.helpers import add_action_deduped
from ai_crypto_trader.services.admin_actions.reconcile_log import log_reconcile_report_throttled
from ai_crypto_trader.services.admin_actions.reject_log import log_order_rejected_throttled
from ai_crypto_trader.utils.json_safe import json_safe
from sqlalchemy import text
from fastapi.responses import StreamingResponse, JSONResponse
import asyncio
import json

logger = logging.getLogger(__name__)

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")


def require_admin_token(
    admin_header: str | None = Header(default=None, alias="X-Admin-Token"),
    token_query: str | None = Query(default=None, alias="token"),
    token_query_alt: str | None = Query(default=None, alias="admin_token"),
) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="ADMIN_TOKEN not configured")
    provided = admin_header or token_query or token_query_alt
    if provided != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing admin token")


def _reject_response(reason: RejectReason, status_code: int = 400) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"ok": False, "reject": reason.dict()})


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


def _resolve_strategy_config_id(
    *,
    strategy_config_id: int | None,
    strategy_id: str | None,
) -> int | None:
    if strategy_config_id is not None:
        return int(strategy_config_id)
    if strategy_id is None:
        return None
    text_value = str(strategy_id).strip()
    if not text_value:
        return None
    if text_value.isdigit():
        return int(text_value)
    return None


def _to_decimal_or_none(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _is_auto_symbol(value: str | None) -> bool:
    if value is None:
        return True
    return normalize_symbol(value) == "AUTO"


async def _resolve_auto_smoke_decision(
    payload: "SmokeTradeRequest",
    session: AsyncSession,
) -> tuple["SmokeTradeRequest", dict]:
    strategy_config_id = _resolve_strategy_config_id(
        strategy_config_id=payload.strategy_config_id,
        strategy_id=payload.strategy_id,
    )
    if strategy_config_id is None:
        raise HTTPException(status_code=400, detail="strategy_config_id is required for AUTO symbol")

    agent = AutoTradeAgent(AutoTradeSettings.from_env())
    try:
        decision_bundle = await agent.decide_for_strategy(
            session,
            account_id=payload.account_id,
            strategy_config_id=strategy_config_id,
            symbol="AUTO",
            timeframe=None,
            window_minutes=60,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    decision = decision_bundle["decision"]
    universe = decision_bundle["universe"]
    timeframe = str(universe.get("timeframe") or PaperTraderConfig.from_env().timeframe)
    if "candidates" not in decision:
        decision["candidates"] = universe.get("fresh_candidates") or universe.get("candidates") or []
    decision["candidates_count"] = len(decision.get("candidates") or [])
    decision["candidates_sample"] = list((decision.get("candidates") or [])[:10])
    decision["timeframe"] = timeframe
    decision["strategy_config_id"] = strategy_config_id
    decision["universe"] = {
        "source": universe.get("source"),
        "reason": universe.get("reason"),
        "candidates_count": universe.get("candidates_count"),
        "fresh_candidates_count": universe.get("fresh_candidates_count"),
    }

    selected_symbol = normalize_symbol(str(decision.get("selected_symbol") or ""))
    selected_side = str(decision.get("side") or "hold").lower().strip()
    qty_text = str(decision.get("qty") or "0")
    try:
        selected_qty = Decimal(qty_text)
    except Exception:
        selected_qty = Decimal("0")

    update_payload = {
        "symbol": selected_symbol if selected_symbol else "AUTO",
        "side": selected_side,
        "qty": selected_qty if selected_qty > 0 else Decimal("0"),
        "strategy_config_id": strategy_config_id,
        "strategy_id": str(strategy_config_id),
    }
    if hasattr(payload, "model_copy"):
        resolved_payload = payload.model_copy(update=update_payload)
    else:
        resolved_payload = payload.copy(update=update_payload)
    return resolved_payload, decision


async def _emit_agent_decision_event(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int,
    strategy_id: str | None,
    decision: dict,
    status: str,
    rationale: str,
    policy_source: str,
    policy_binding: dict,
    computed_limits: dict,
    reject: dict | None = None,
    execution: dict | None = None,
) -> None:
    selected_symbol = normalize_symbol(str(decision.get("selected_symbol") or ""))
    side = str(decision.get("side") or "hold").lower().strip()
    qty = str(decision.get("qty") or "0")
    await emit_trade_decision(
        session,
        {
            "created_now_utc": datetime.now(timezone.utc),
            "account_id": str(account_id),
            "strategy_id": strategy_id,
            "strategy_config_id": strategy_config_id,
            "symbol_in": selected_symbol or decision.get("symbol_in") or "AUTO",
            "symbol_normalized": selected_symbol or "AUTO",
            "side": side,
            "qty_requested": qty,
            "status": status,
            "rationale": rationale,
            "candidates_count": int(decision.get("candidates_count") or len(decision.get("candidates") or [])),
            "candidates_sample": list((decision.get("candidates_sample") or decision.get("candidates") or [])[:10]),
            "confidence": decision.get("confidence"),
            "rationale_bullets": decision.get("rationale_bullets"),
            "signals": decision.get("signals"),
            "inputs": {
                "strategy_thresholds": None,
                "timeframe": decision.get("timeframe"),
                "symbols": decision.get("candidates"),
                "price_source": "agent_auto",
                "market_price_used": None,
            },
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "agent_decision": decision,
            "result": {
                "reject": reject,
                **({"execution": execution} if execution else {}),
            },
        },
    )


router = APIRouter(prefix="/admin/paper-trader", tags=["admin"], dependencies=[Depends(require_admin_token)])
admin_router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])

runner = PaperTraderRunner.instance()

OPEN_STATUSES = {"open", "new", "partially_filled", "partial", "partially-filled"}


def _account_name() -> str:
    from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig

    return os.getenv("PAPER_ACCOUNT_NAME") or f"paper-{PaperTraderConfig.from_env().exchange_name}"


async def _latest_price(session: AsyncSession, symbol: str) -> Decimal | None:
    try:
        result = await session.execute(
            select(Candle.close)
            .where(Candle.symbol == symbol)
            .order_by(Candle.open_time.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()
    except Exception:
        return None


@router.post("/start")
async def start_paper_trader(session: AsyncSession = Depends(get_db_session)) -> dict:
    if runner.is_running:
        await _record_action(session, "start", "error", "Paper trader already running")
        raise HTTPException(status_code=400, detail="Paper trader is already running")
    await runner.start()
    await _record_action(session, "start", "ok", "Paper trader started")
    return runner.status()


@router.post("/stop")
async def stop_paper_trader(session: AsyncSession = Depends(get_db_session)) -> dict:
    if not runner.is_running:
        await _record_action(session, "stop", "error", "Paper trader not running")
        raise HTTPException(status_code=400, detail="Paper trader is not running")
    await runner.stop()
    await _record_action(session, "stop", "ok", "Paper trader stopped")
    return runner.status()


@router.get("/status")
async def paper_trader_status() -> JSONResponse:
    defaults = {
        "running": False,
        "started_at": None,
        "last_cycle_at": None,
        "last_error": None,
        "engine_task_state": None,
        "engine_task_exception": None,
        "reconcile_task_running": False,
        "reconcile_task_state": None,
        "reconcile_task_exception": None,
        "reconcile_tick_count": 0,
        "last_reconcile_at": None,
        "reconcile_interval_seconds": 0,
        "last_reconcile_error": None,
        "last_reconcile_action_at": None,
        "last_reconcile_action_status": None,
    }
    try:
        runner_present = runner is not None
        payload = dict(defaults)
        if runner_present:
            status = runner.status()
            if isinstance(status, dict):
                payload.update(status)
            else:
                payload["last_error"] = f"runner.status returned {type(status).__name__}"
        payload["debug"] = {
            "pid": os.getpid(),
            "runner_present": runner_present,
            "now": utc_now().isoformat(),
        }
        encoded = jsonable_encoder(
            payload,
            custom_encoder={
                Decimal: lambda x: str(x),
                IPv4Address: lambda x: str(x),
                IPv6Address: lambda x: str(x),
                Exception: lambda x: repr(x),
            },
        )
        return JSONResponse(content=encoded, status_code=200)
    except Exception as exc:
        trace_lines = traceback.format_exception(exc.__class__, exc, exc.__traceback__)
        trace_short = "\n".join(trace_lines[:20]) if trace_lines else None
        payload = dict(defaults)
        payload["last_error"] = repr(exc)
        payload["debug"] = {
            "pid": os.getpid(),
            "runner_present": runner is not None,
            "now": utc_now().isoformat(),
            "traceback": trace_short,
        }
        encoded = jsonable_encoder(
            payload,
            custom_encoder={
                Decimal: lambda x: str(x),
                IPv4Address: lambda x: str(x),
                IPv6Address: lambda x: str(x),
                Exception: lambda x: repr(x),
            },
        )
        return JSONResponse(content=encoded, status_code=200)


@router.post("/reset")
async def reset_paper_trader(session: AsyncSession = Depends(get_db_session)) -> dict:
    if runner.is_running:
        raise HTTPException(status_code=409, detail="Paper trader is running; stop it before resetting data")
    result = await reset_paper_data(session)
    await _record_action(session, "reset", "ok", "Paper data reset", meta=result)
    return result


@router.post("/flatten")
async def flatten_paper_trader(session: AsyncSession = Depends(get_db_session)) -> dict:
    if runner.is_running:
        raise HTTPException(status_code=409, detail="Paper trader is running; stop it before flatten.")
    result = await flatten_positions(session)
    flattened_count = result.get("flattened", 0) if isinstance(result, dict) else 0
    await _record_action(
        session,
        "flatten",
        "ok",
        f"Flattened {flattened_count} positions",
        meta={"flattened": flattened_count},
    )
    return {**result, "message": f"Flattened {flattened_count} positions"}


@router.get("/summary")
async def paper_trader_summary(session: AsyncSession = Depends(get_db_session)) -> dict:
    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == _account_name()))
    if account is None:
        raise HTTPException(status_code=404, detail="Paper account not found")

    latest_balance = await session.scalar(
        select(PaperBalance)
        .where(PaperBalance.account_id == account.id, PaperBalance.ccy == account.base_ccy)
        .order_by(desc(PaperBalance.updated_at))
        .limit(1)
    )
    latest_snapshot = await session.scalar(
        select(EquitySnapshot).where(EquitySnapshot.account_id == account.id).order_by(desc(EquitySnapshot.created_at)).limit(1)
    )

    positions_count = await session.scalar(
        select(func.count()).where(PaperPosition.account_id == account.id, PaperPosition.qty != Decimal("0"))
    )
    orders_open_count = await session.scalar(
        select(func.count()).where(PaperOrder.account_id == account.id, PaperOrder.status.in_(OPEN_STATUSES))
    )
    trades_count = await session.scalar(select(func.count()).where(PaperTrade.account_id == account.id))

    balance_val = latest_balance.available if latest_balance else Decimal("0")
    equity_val = latest_snapshot.equity if latest_snapshot else balance_val
    unrealized_val = latest_snapshot.unrealized_pnl if latest_snapshot else Decimal("0")
    last_cycle = latest_snapshot.created_at if latest_snapshot else None

    started_at = getattr(runner, "started_at", None)
    last_error = getattr(runner, "last_error", None)
    runner_last_cycle = getattr(getattr(runner, "_engine", None), "last_cycle_at", None)
    engine_last_cycle = runner_last_cycle or last_cycle
    uptime_sec = None
    if started_at:
        uptime_sec = int((datetime.now(timezone.utc) - started_at).total_seconds())

    return {
        "engine": {
            "running": runner.is_running,
            "started_at": started_at.isoformat() if started_at else None,
            "uptime_sec": uptime_sec,
            "last_cycle_at": engine_last_cycle.isoformat() if engine_last_cycle else None,
            "last_error": last_error,
        },
        "account": {
            "id": account.id,
            "name": account.name,
            "base_ccy": account.base_ccy,
            "balance": str(balance_val),
            "equity": str(equity_val),
            "unrealized_pnl": str(unrealized_val),
        },
        "counts": {
            "positions": int(positions_count or 0),
            "orders_open": int(orders_open_count or 0),
            "trades": int(trades_count or 0),
        },
    }


class RiskCheckRequest(BaseModel):
    account_id: int
    symbol: str
    side: str
    notional_usd: Decimal
    price: Decimal


class PlaceOrderRequest(RiskCheckRequest):
    pass


class ReconcileApplyBody(BaseModel):
    account_id: int
    apply_balances: bool | None = False
    apply_positions: bool | None = True


class SmokeTradeRequest(BaseModel):
    account_id: int
    symbol: str | None = "ETHUSDT"
    side: str = "buy"
    qty: Decimal = Decimal("0.01")
    price: str | None = None
    price_override: str | None = None
    strategy_id: str | None = None
    strategy_config_id: int | None = None


class TestOrderRequest(BaseModel):
    account_id: int
    strategy_config_id: int
    symbol: str
    side: str
    qty: Decimal | str
    price_override: Decimal | str | None = None
    execute: bool = False


class AgentDecideRequest(BaseModel):
    account_id: int
    strategy_config_id: int
    symbol: str | None = "AUTO"
    timeframe: str | None = None
    window_minutes: int = 60
    dry_run: bool = True
    price_override: Decimal | str | None = None


class PaperAccountCreateRequest(BaseModel):
    initial_cash_usd: Decimal | None = None


class PaperAccountResetRequest(BaseModel):
    account_id: int
    reset_to_initial_cash: bool | None = True


@router.post("/risk-check")
async def risk_check(payload: RiskCheckRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    try:
        bundle = await get_active_bundle(session)
    except ActiveConfigMissingError as e:
        raise HTTPException(status_code=409, detail=e.reason)
    result = await evaluate_and_size(
        session=session,
        account_id=payload.account_id,
        symbol=payload.symbol,
        side=payload.side,
        desired_notional_usd=Decimal(str(payload.notional_usd)),
        price=Decimal(str(payload.price)),
        bundle=bundle,
    )
    return result


@router.post("/place-order")
async def place_order(payload: PlaceOrderRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    try:
        bundle = await get_active_bundle(session)
    except ActiveConfigMissingError as e:
        raise HTTPException(status_code=409, detail=e.reason)

    risk_eval = await evaluate_and_size(
        session=session,
        account_id=payload.account_id,
        symbol=payload.symbol,
        side=payload.side,
        desired_notional_usd=Decimal(str(payload.notional_usd)),
        price=Decimal(str(payload.price)),
        bundle=bundle,
    )

    meta_payload = {
        "request": {
            "account_id": payload.account_id,
            "symbol": payload.symbol,
            "side": payload.side,
            "notional_usd": str(payload.notional_usd),
            "price": str(payload.price),
        },
        "risk": risk_eval,
    }

    if not risk_eval.get("allowed"):
        reject = RejectReason(
            code=RejectCode.POSITION_LIMIT,
            reason="Risk checks rejected order",
            details={"reasons": risk_eval.get("reasons")},
        )
        await _record_action(
            session,
            "RISK_REJECT",
            "rejected",
            "Risk rejected order",
            meta={**meta_payload, "reject": reject.dict()},
        )
        return _reject_response(reject, status_code=400)

    qty = Decimal(str(risk_eval.get("qty", "0")))
    if qty <= 0:
        reject = RejectReason(code=RejectCode.QTY_ZERO, reason="Risk sizing returned non-positive qty")
        await _record_action(
            session,
            "RISK_REJECT",
            "rejected",
            "Risk sizing returned non-positive qty",
            meta={**meta_payload, "reject": reject.dict()},
        )
        return _reject_response(reject, status_code=400)

    fee_bps = Decimal(str(bundle["risk_policy"]["fee_bps"]))
    slippage_bps = Decimal(str(bundle["risk_policy"]["slippage_bps"]))

    try:
        prepared = prepare_order_inputs(payload.symbol, qty, payload.price)
        reject_reason = await validate_order(
            session,
            account_id=payload.account_id,
            symbol=prepared.symbol,
            side=payload.side,
            qty=prepared.qty,
            price=prepared.price,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            risk_policy=DEFAULT_RISK_POLICY,
            position_policy=DEFAULT_POSITION_POLICY,
        )
        if reject_reason:
            await log_order_rejected_throttled(
                session,
                account_id=payload.account_id,
                symbol=normalize_symbol(payload.symbol),
                reject_code=str(reject_reason.code),
                reject_reason=reject_reason.reason,
                meta={
                    "message": "Order rejected",
                    "reject": reject_reason.dict(),
                    "request": {
                        "account_id": payload.account_id,
                        "symbol_in": payload.symbol,
                        "side": payload.side,
                        "qty": str(qty),
                        "price_override": str(payload.price),
                    },
                },
                window_seconds=120,
            )
            return _reject_response(reject_reason, status_code=400)
        execution = await execute_market_order_with_costs(
            session=session,
            account_id=payload.account_id,
            symbol=prepared.symbol,
            side=payload.side,
            qty=prepared.qty,
            mid_price=prepared.price,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            meta={"origin": "admin_manual"},
        )
    except (RiskRejected, ValueError) as exc:
        reject_reason = _reject_from_reason(str(exc))
        await log_order_rejected_throttled(
            session,
            account_id=payload.account_id,
            symbol=normalize_symbol(payload.symbol),
            reject_code=str(reject_reason.code),
            reject_reason=reject_reason.reason,
            meta={
                "message": "Order rejected",
                "reject": reject_reason.dict(),
                "request": {
                    "account_id": payload.account_id,
                    "symbol_in": payload.symbol,
                    "side": payload.side,
                    "qty": str(qty),
                    "price_override": str(payload.price),
                },
            },
            window_seconds=120,
        )
        return _reject_response(reject_reason, status_code=400)

    exec_payload = {
        **meta_payload,
        "execution": execution.costs,
    }
    await _record_action(session, "PAPER_ORDER", "filled", "Paper order executed", meta=exec_payload)

    return {
        "allowed": True,
        "risk": risk_eval,
        "execution": {
            "order_id": execution.order.id,
            "trade_id": execution.trade.id,
            "position_qty": str(execution.position.qty),
            "position_avg_price": str(execution.position.avg_entry_price),
            "balance_available": str(execution.balance.available),
            "costs": execution.costs,
        },
    }


async def _smoke_trade_impl(
    payload: SmokeTradeRequest,
    session: AsyncSession,
    *,
    agent_decision: dict | None = None,
) -> dict | JSONResponse:
    # Self-test: NEW_ID=3 smoke buy 0.01 + sell 0.005 (with price), then verify positions/balances,
    # reconcile?account_id=3 diff_count ok, /status has no InvalidStateError, admin_actions meta serializes.
    symbol_in = (payload.symbol or "ETHUSDT").strip()
    symbol_norm = normalize_symbol(symbol_in)
    side = (payload.side or "buy").lower().strip()
    strategy_config_id = _resolve_strategy_config_id(
        strategy_config_id=payload.strategy_config_id,
        strategy_id=payload.strategy_id,
    )
    price_override_value = payload.price_override if payload.price_override is not None else payload.price

    policy_source = "account_default"
    policy_binding = {
        "strategy_config_id": strategy_config_id,
        "legacy_risk_policy_id": None,
        "bound_risk_policy_id": None,
        "effective_risk_policy_id": None,
        "bound_position_policy_id": None,
        "effective_position_policy_id": None,
    }
    position_limits = {
        "max_position_qty": None,
        "max_position_notional_usdt": None,
        "max_position_pct_equity": None,
    }
    position_limits_source = None
    position_policy_overrides = None
    computed_limits = {
        "risk_limits": {
            "max_order_notional_usdt": None,
            "min_order_notional_usdt": None,
            "max_leverage": None,
            "max_drawdown_pct": None,
            "max_drawdown_usdt": None,
            "max_daily_loss_usdt": None,
            "equity_lookback_hours": None,
            "lookback_minutes": None,
        },
        "risk_limit_source": None,
        "risk_policy_overrides": None,
        "symbol_limits": {
            "max_order_qty": None,
            "max_position_qty": None,
            "max_position_notional_usdt": None,
            "max_position_pct_equity": None,
        },
        "symbol_limit_source": None,
        "position_policy_overrides": None,
    }

    request_meta = {
        "account_id": payload.account_id,
        "strategy_id": payload.strategy_id,
        "strategy_config_id": strategy_config_id,
        "symbol_in": symbol_in,
        "symbol_normalized": symbol_norm,
        "side": side,
        "qty": str(payload.qty),
        "price_override": str(price_override_value) if price_override_value is not None else None,
        "agent_decision": agent_decision,
    }

    async def _reject_smoke(reject: RejectReason, *, status_code: int = 400):
        decision_payload = {
            "created_now_utc": datetime.now(timezone.utc),
            "message": "Smoke trade rejected",
            "account_id": str(payload.account_id),
            "strategy_id": payload.strategy_id,
            "strategy_config_id": strategy_config_id,
            "symbol_in": symbol_in,
            "symbol_normalized": symbol_norm,
            "side": side,
            "qty_requested": str(payload.qty),
            "status": "rejected",
            "rationale": reject.reason,
            "inputs": {
                "strategy_thresholds": None,
                "timeframe": None,
                "symbols": None,
                "price_source": "override" if price_override_value is not None else "latest",
                "market_price_used": None,
            },
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "agent_decision": agent_decision,
            "reject": reject.dict(),
            "result": {"reject": reject.dict()},
        }
        try:
            await emit_trade_decision(
                session,
                decision_payload,
            )
        except Exception:
            logger.exception(
                "Failed to emit smoke-trade decision",
                extra={
                    "account_id": payload.account_id,
                    "strategy_config_id": strategy_config_id,
                    "symbol": symbol_norm,
                    "side": side,
                    "reject_code": str(reject.code),
                },
            )
        await log_order_rejected_throttled(
            session,
            account_id=payload.account_id,
            symbol=symbol_norm,
            reject_code=str(reject.code),
            reject_reason=reject.reason,
            meta=decision_payload,
            window_seconds=120,
        )
        try:
            await session.commit()
        except Exception:
            await session.rollback()
        return JSONResponse(
            status_code=status_code,
            content=jsonable_encoder(
                {
                    "ok": False,
                    "reject": reject.dict(),
                    "policy_source": policy_source,
                    "policy_binding": policy_binding,
                    "computed_limits": computed_limits,
                    "agent_decision": agent_decision,
                }
            ),
        )

    if strategy_config_id is None:
        reject = RejectReason(
            code=RejectCode.INTERNAL_ERROR,
            reason="Unable to resolve strategy_config_id",
            details={
                "strategy_id": payload.strategy_id,
                "strategy_config_id": payload.strategy_config_id,
            },
        )
        return await _reject_smoke(reject)

    snapshot = await resolve_effective_policy_snapshot(
        session,
        strategy_config_id=strategy_config_id,
        symbol=symbol_norm,
    )
    if snapshot is not None:
        policy_source = snapshot.policy_source
        policy_binding = snapshot.policy_binding
        computed_limits = snapshot.computed_limits
        symbol_limits = computed_limits.get("symbol_limits") if isinstance(computed_limits, dict) else None
        if isinstance(symbol_limits, dict):
            position_limits = {
                "max_position_qty": _to_decimal_or_none(symbol_limits.get("max_position_qty")),
                "max_position_notional_usdt": _to_decimal_or_none(symbol_limits.get("max_position_notional_usdt")),
                "max_position_pct_equity": _to_decimal_or_none(symbol_limits.get("max_position_pct_equity")),
            }
    position_limits_source = (
        computed_limits.get("symbol_limit_source")
        if isinstance(computed_limits, dict)
        else None
    )
    position_policy_overrides = (
        computed_limits.get("position_policy_overrides")
        if isinstance(computed_limits, dict)
        else None
    )

    if side not in {"buy", "sell"}:
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="side must be buy or sell")
        return await _reject_smoke(reject)

    try:
        qty_dec = Decimal(str(payload.qty))
    except (InvalidOperation, TypeError, ValueError):
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="qty must be a valid decimal")
        return await _reject_smoke(reject)
    if qty_dec <= 0:
        reject = RejectReason(code=RejectCode.QTY_ZERO, reason="qty must be positive")
        return await _reject_smoke(reject)

    if price_override_value is not None:
        try:
            market_price = Decimal(str(price_override_value))
        except (InvalidOperation, TypeError, ValueError):
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price_override must be a valid decimal")
            return await _reject_smoke(reject)
        if market_price <= 0:
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price_override must be positive")
            return await _reject_smoke(reject)
    else:
        market_price = await _latest_price(session, symbol_norm)
        if market_price is None:
            reject = RejectReason(code=RejectCode.SYMBOL_DISABLED, reason=f"No price data for symbol {symbol_norm}")
            return await _reject_smoke(reject)
        market_price = Decimal(str(market_price))

    current_qty_val = await session.scalar(
        select(PaperPosition.qty)
        .where(PaperPosition.account_id == payload.account_id, PaperPosition.symbol == symbol_norm)
        .limit(1)
    )
    current_qty = Decimal(str(current_qty_val)) if current_qty_val is not None else Decimal("0")
    delta_qty = qty_dec if side == "buy" else -qty_dec
    next_qty = current_qty + delta_qty
    next_notional = next_qty.copy_abs() * market_price

    limit_qty = position_limits.get("max_position_qty")
    if limit_qty is not None and Decimal(str(limit_qty)) > 0 and next_qty.copy_abs() > Decimal(str(limit_qty)):
        reject = RejectReason(
            code=RejectCode.MAX_POSITION_QTY,
            reason="Position quantity exceeds policy max",
            details={
                "current_qty": str(current_qty),
                "delta_qty": str(delta_qty),
                "next_qty": str(next_qty),
                "market_price": str(market_price),
                "next_notional": str(next_notional),
                "limit": str(limit_qty),
                "max_position_qty": str(limit_qty),
                "symbol_limit_source": position_limits_source,
                "policy_binding": policy_binding,
            },
        )
        return await _reject_smoke(reject)

    limit_notional = position_limits.get("max_position_notional_usdt")
    if limit_notional is not None and Decimal(str(limit_notional)) > 0 and next_notional > Decimal(str(limit_notional)):
        reject = RejectReason(
            code=RejectCode.MAX_POSITION_NOTIONAL,
            reason="Position notional above maximum",
            details={
                "current_qty": str(current_qty),
                "delta_qty": str(delta_qty),
                "next_qty": str(next_qty),
                "qty": str(qty_dec),
                "market_price": str(market_price),
                "next_notional": str(next_notional),
                "limit": str(limit_notional),
                "max_position_notional_usdt": str(limit_notional),
                "symbol_limit_source": position_limits_source,
                "policy_binding": policy_binding,
            },
        )
        return await _reject_smoke(reject)

    config = PaperTraderConfig.from_env()
    result = await place_order_unified(
        session,
        account_id=payload.account_id,
        symbol=symbol_in,
        side=side,
        qty=payload.qty,
        strategy_id=strategy_config_id,
        price_override=price_override_value,
        fee_bps=config.fee_bps,
        slippage_bps=config.slippage_bps,
        meta={
            "origin": "admin_smoke",
            "agent_decision": agent_decision,
        },
    )
    if isinstance(result, RejectReason):
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "Smoke trade rejected",
            meta=json_safe(
                {
                    "request": request_meta,
                    "reject": result.dict(),
                    "policy_source": policy_source,
                    "policy_binding": policy_binding,
                    "computed_limits": computed_limits,
                }
            ),
        )
        return JSONResponse(
            status_code=400,
            content=jsonable_encoder(
                {
                    "ok": False,
                    "reject": result.dict(),
                    "policy_source": policy_source,
                    "policy_binding": policy_binding,
                    "computed_limits": computed_limits,
                    "agent_decision": agent_decision,
                }
            ),
        )

    execution = result.execution
    prepared = result.prepared
    symbol_norm = prepared.symbol
    price_source = result.price_source

    await _record_action(
        session,
        "SMOKE_TRADE",
        "ok",
        "Smoke trade executed",
        meta=json_safe({
            "account_id": payload.account_id,
            "symbol_in": symbol_in,
            "symbol_normalized": symbol_norm,
            "side": side,
            "qty": str(prepared.qty),
            "price": str(prepared.price),
            "price_source": price_source,
            "trade_id": execution.trade.id,
            "strategy_id": result.strategy_id,
            "policy_source": result.policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "agent_decision": agent_decision,
        }),
    )
    await _record_action(
        session,
        "SMOKE_TRADE_EXECUTED",
        "ok",
        "Smoke trade executed",
        meta=json_safe(
            {
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(prepared.qty),
                "price": str(prepared.price),
                "price_source": price_source,
                "order_id": execution.order.id,
                "trade_id": execution.trade.id,
                "strategy_id": result.strategy_id,
                "policy_source": result.policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "agent_decision": agent_decision,
            }
        ),
    )

    positions = (
        await session.scalars(
            select(PaperPosition)
            .where(PaperPosition.account_id == payload.account_id)
            .order_by(PaperPosition.symbol.asc())
        )
    ).all()
    balance = await session.scalar(
        select(PaperBalance)
        .where(PaperBalance.account_id == payload.account_id, PaperBalance.ccy == "USDT")
    )
    report = await reconcile_report(session, payload.account_id)

    return {
        "ok": True,
        "account_id": payload.account_id,
        "symbol_in": symbol_in,
        "symbol_normalized": symbol_norm,
        "selected_symbol": (
            normalize_symbol(str(agent_decision.get("selected_symbol")))
            if isinstance(agent_decision, dict) and agent_decision.get("selected_symbol")
            else symbol_norm
        ),
        "decision_side": (
            str(agent_decision.get("side")).lower().strip()
            if isinstance(agent_decision, dict) and agent_decision.get("side")
            else side
        ),
        "decision_confidence": (
            agent_decision.get("confidence")
            if isinstance(agent_decision, dict)
            else None
        ),
        "price_source": price_source,
        "policy_source": policy_source,
        "policy_binding": policy_binding,
        "computed_limits": computed_limits,
        "agent_decision": agent_decision,
        "trade": {
            "id": execution.trade.id,
            "symbol": execution.trade.symbol,
            "side": execution.trade.side,
            "qty": str(execution.trade.qty),
            "price": str(execution.trade.price),
            "fee": str(execution.trade.fee),
            "created_at": execution.trade.created_at.isoformat() if execution.trade.created_at else None,
        },
        "positions": [
            {
                "symbol": pos.symbol,
                "side": pos.side,
                "qty": str(pos.qty),
                "avg_entry_price": str(pos.avg_entry_price),
                "updated_at": pos.updated_at.isoformat() if pos.updated_at else None,
            }
            for pos in positions
        ],
        "balance_usdt": {
            "ccy": balance.ccy,
            "available": str(balance.available),
            "updated_at": balance.updated_at.isoformat() if balance.updated_at else None,
        }
        if balance
        else None,
        "reconcile": report,
    }


@router.post("/test-order")
async def test_order(payload: TestOrderRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    symbol_norm = normalize_symbol(payload.symbol)
    policy_source = "account_default"
    position_limits_source = None
    position_limits = {
        "max_position_qty": None,
        "max_position_notional_usdt": None,
        "max_position_pct_equity": None,
    }
    snapshot = await resolve_effective_policy_snapshot(
        session,
        strategy_config_id=payload.strategy_config_id,
        symbol=symbol_norm,
    )
    policy_binding = {
        "strategy_config_id": payload.strategy_config_id,
        "legacy_risk_policy_id": None,
        "bound_risk_policy_id": None,
        "effective_risk_policy_id": None,
        "bound_position_policy_id": None,
        "effective_position_policy_id": None,
    }
    computed_limits = {
        "risk_limits": {
            "max_order_notional_usdt": None,
            "min_order_notional_usdt": None,
            "max_leverage": None,
            "max_drawdown_pct": None,
            "max_drawdown_usdt": None,
            "max_daily_loss_usdt": None,
            "equity_lookback_hours": None,
            "lookback_minutes": None,
        },
        "risk_limit_source": None,
        "risk_policy_overrides": None,
        "symbol_limits": {
            "max_order_qty": None,
            "max_position_qty": None,
            "max_position_notional_usdt": None,
            "max_position_pct_equity": None,
        },
        "symbol_limit_source": None,
        "position_policy_overrides": None,
    }
    if snapshot is not None:
        policy_source = snapshot.policy_source
        policy_binding = snapshot.policy_binding
        computed_limits = snapshot.computed_limits
        symbol_limits = computed_limits.get("symbol_limits") if isinstance(computed_limits, dict) else None
        if isinstance(symbol_limits, dict):
            position_limits = {
                "max_position_qty": _to_decimal_or_none(symbol_limits.get("max_position_qty")),
                "max_position_notional_usdt": _to_decimal_or_none(symbol_limits.get("max_position_notional_usdt")),
                "max_position_pct_equity": _to_decimal_or_none(symbol_limits.get("max_position_pct_equity")),
            }
        position_limits_source = (
            computed_limits.get("symbol_limit_source")
            if isinstance(computed_limits, dict)
            else None
        )

    side_norm = (payload.side or "").strip().lower()
    if side_norm not in {"buy", "sell"}:
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="side must be buy or sell")
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": bool(payload.execute),
                },
                "reject": reject.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": not bool(payload.execute),
            },
        )
        return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}

    try:
        qty_dec = Decimal(str(payload.qty))
    except Exception:
        reject = RejectReason(code=RejectCode.INVALID_QTY, reason="qty must be a valid decimal")
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": bool(payload.execute),
                },
                "reject": reject.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": not bool(payload.execute),
            },
        )
        return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}
    if qty_dec <= 0:
        reject = RejectReason(code=RejectCode.QTY_ZERO, reason="qty must be positive")
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": bool(payload.execute),
                },
                "reject": reject.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": not bool(payload.execute),
            },
        )
        return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}

    if payload.price_override is not None:
        try:
            market_price = Decimal(str(payload.price_override))
        except (InvalidOperation, TypeError, ValueError):
            reject = RejectReason(code=RejectCode.INVALID_QTY, reason="price_override must be a valid decimal")
            await _record_action(
                session,
                "ORDER_REJECTED",
                "error",
                "test-order rejected",
                meta={
                    "request": {
                        "account_id": payload.account_id,
                        "strategy_config_id": payload.strategy_config_id,
                        "symbol": symbol_norm,
                        "side": payload.side,
                        "qty": str(payload.qty),
                        "price_override": str(payload.price_override),
                        "execute": bool(payload.execute),
                    },
                    "reject": reject.dict(),
                    "policy_source": policy_source,
                    "policy_binding": policy_binding,
                    "computed_limits": computed_limits,
                    "dry_run": not bool(payload.execute),
                },
            )
            return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}
    else:
        market_price = await _latest_price(session, symbol_norm)
    if market_price is None:
        reject = RejectReason(code=RejectCode.SYMBOL_DISABLED, reason=f"No price data for symbol {symbol_norm}")
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": bool(payload.execute),
                },
                "reject": reject.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": not bool(payload.execute),
            },
        )
        return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}
    market_price = Decimal(str(market_price))

    current_qty_val = await session.scalar(
        select(PaperPosition.qty)
        .where(PaperPosition.account_id == payload.account_id, PaperPosition.symbol == symbol_norm)
        .limit(1)
    )
    current_qty = Decimal(str(current_qty_val)) if current_qty_val is not None else Decimal("0")
    delta_qty = qty_dec if side_norm == "buy" else -qty_dec
    next_qty = current_qty + delta_qty
    next_notional = next_qty.copy_abs() * market_price

    limit_notional = position_limits.get("max_position_notional_usdt")
    if limit_notional is not None and Decimal(str(limit_notional)) > 0 and next_notional > Decimal(str(limit_notional)):
        reject = RejectReason(
            code=RejectCode.MAX_POSITION_NOTIONAL,
            reason="Position notional above maximum",
            details={
                "current_qty": str(current_qty),
                "delta_qty": str(delta_qty),
                "next_qty": str(next_qty),
                "market_price": str(market_price),
                "next_notional": str(next_notional),
                "limit": str(limit_notional),
                "symbol_limit_source": position_limits_source,
                "policy_binding": policy_binding,
            },
        )
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": bool(payload.execute),
                },
                "reject": reject.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": not bool(payload.execute),
            },
        )
        return {"ok": False, "reject": reject.dict(), "policy_source": policy_source, "policy_binding": policy_binding, "computed_limits": computed_limits, "dry_run": not bool(payload.execute)}

    config = PaperTraderConfig.from_env()
    if not payload.execute:
        await _record_action(
            session,
            "ORDER_SIMULATED",
            "ok",
            "test-order simulated",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": False,
                },
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "simulation": {
                    "current_qty": str(current_qty),
                    "delta_qty": str(delta_qty),
                    "next_qty": str(next_qty),
                    "market_price": str(market_price),
                    "next_notional": str(next_notional),
                },
                "dry_run": True,
            },
        )
        return {
            "ok": True,
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "dry_run": True,
            "execution": None,
        }

    result = await place_order_unified(
        session,
        account_id=payload.account_id,
        symbol=payload.symbol,
        side=payload.side,
        qty=payload.qty,
        strategy_id=payload.strategy_config_id,
        price_override=payload.price_override,
        fee_bps=config.fee_bps,
        slippage_bps=config.slippage_bps,
        meta={"origin": "admin_test_order"},
    )

    if isinstance(result, RejectReason):
        await _record_action(
            session,
            "ORDER_REJECTED",
            "error",
            "test-order rejected",
            meta={
                "request": {
                    "account_id": payload.account_id,
                    "strategy_config_id": payload.strategy_config_id,
                    "symbol": symbol_norm,
                    "side": payload.side,
                    "qty": str(payload.qty),
                    "price_override": str(payload.price_override) if payload.price_override is not None else None,
                    "execute": True,
                },
                "reject": result.dict(),
                "policy_source": policy_source,
                "policy_binding": policy_binding,
                "computed_limits": computed_limits,
                "dry_run": False,
            },
        )
        return {
            "ok": False,
            "reject": result.dict(),
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "dry_run": False,
        }

    execution = result.execution
    prepared = result.prepared
    await session.commit()
    await _record_action(
        session,
        "ORDER_SIMULATED",
        "ok",
        "test-order executed",
        meta={
            "request": {
                "account_id": payload.account_id,
                "strategy_config_id": payload.strategy_config_id,
                "symbol": symbol_norm,
                "side": payload.side,
                "qty": str(payload.qty),
                "price_override": str(payload.price_override) if payload.price_override is not None else None,
                "execute": True,
            },
            "policy_source": result.policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "execution": {
                "order_id": execution.order.id,
                "trade_id": execution.trade.id,
                "symbol": prepared.symbol,
                "side": execution.order.side,
                "qty": str(prepared.qty),
                "price": str(prepared.price),
                "price_source": result.price_source,
            },
            "dry_run": False,
        },
    )
    return {
        "ok": True,
        "policy_source": result.policy_source,
        "policy_binding": policy_binding,
        "computed_limits": computed_limits,
        "dry_run": False,
        "execution": {
            "order_id": execution.order.id,
            "trade_id": execution.trade.id,
            "symbol": prepared.symbol,
            "side": execution.order.side,
            "qty": str(prepared.qty),
            "price": str(prepared.price),
            "price_source": result.price_source,
        },
    }


@router.post("/smoke-trade")
async def smoke_trade(payload: SmokeTradeRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    resolved_payload = payload
    agent_decision: dict | None = None
    if _is_auto_symbol(payload.symbol):
        resolved_payload, agent_decision = await _resolve_auto_smoke_decision(payload, session)
        if str(agent_decision.get("side") or "hold").lower().strip() == "hold":
            strategy_config_id = _resolve_strategy_config_id(
                strategy_config_id=resolved_payload.strategy_config_id,
                strategy_id=resolved_payload.strategy_id,
            )
            selected_symbol = normalize_symbol(str(agent_decision.get("selected_symbol") or "AUTO"))
            snapshot = None
            if strategy_config_id is not None:
                snapshot = await resolve_effective_policy_snapshot(
                    session,
                    strategy_config_id=strategy_config_id,
                    symbol=selected_symbol,
                )
            rationale = (
                (agent_decision.get("rationale_bullets") or [agent_decision.get("reason") or "Agent decided hold"])[0]
                if isinstance(agent_decision.get("rationale_bullets"), list)
                else str(agent_decision.get("reason") or "Agent decided hold")
            )
            await emit_trade_decision(
                session,
                {
                    "created_now_utc": datetime.now(timezone.utc),
                    "account_id": str(resolved_payload.account_id),
                    "strategy_id": resolved_payload.strategy_id,
                    "strategy_config_id": strategy_config_id,
                    "symbol_in": selected_symbol,
                    "symbol_normalized": selected_symbol,
                    "side": "hold",
                    "qty_requested": "0",
                    "status": "skipped",
                    "rationale": rationale,
                    "inputs": {
                        "strategy_thresholds": None,
                        "timeframe": agent_decision.get("timeframe"),
                        "symbols": agent_decision.get("candidates"),
                        "price_source": "agent_auto",
                        "market_price_used": None,
                    },
                    "policy_source": snapshot.policy_source if snapshot else "account_default",
                    "policy_binding": snapshot.policy_binding if snapshot else {"strategy_config_id": strategy_config_id},
                    "computed_limits": snapshot.computed_limits if snapshot else {},
                    "agent_decision": agent_decision,
                    "result": {
                        "reject": {
                            "code": "AUTO_HOLD",
                            "reason": str(agent_decision.get("reason") or "Agent decided hold"),
                            "details": {"selected_symbol": selected_symbol},
                        }
                    },
                },
            )
            await _record_action(
                session,
                "SMOKE_TRADE",
                "ok",
                "Smoke trade skipped (agent hold)",
                meta=json_safe(
                    {
                        "account_id": resolved_payload.account_id,
                        "strategy_config_id": strategy_config_id,
                        "symbol": selected_symbol,
                        "status": "skipped",
                        "agent_decision": agent_decision,
                    }
                ),
            )
            return {
                "ok": True,
                "status": "skipped",
                "reason": agent_decision.get("reason") or "Agent decided hold",
                "selected_symbol": selected_symbol,
                "agent_decision": agent_decision,
                "policy_source": snapshot.policy_source if snapshot else "account_default",
                "policy_binding": snapshot.policy_binding if snapshot else {"strategy_config_id": strategy_config_id},
                "computed_limits": snapshot.computed_limits if snapshot else {},
            }

    price_override_value = (
        resolved_payload.price_override
        if resolved_payload.price_override is not None
        else resolved_payload.price
    )
    if not runner.is_running:
        reject = RejectReason(code=RejectCode.ENGINE_NOT_RUNNING, reason="Paper trader engine is not running")
        strategy_config_id = _resolve_strategy_config_id(
            strategy_config_id=resolved_payload.strategy_config_id,
            strategy_id=resolved_payload.strategy_id,
        )
        symbol_norm = normalize_symbol(getattr(resolved_payload, "symbol", ""))
        side_norm = (getattr(resolved_payload, "side", "") or "").lower().strip()
        try:
            await emit_trade_decision(
                session,
                {
                    "created_now_utc": datetime.now(timezone.utc),
                    "account_id": str(resolved_payload.account_id),
                    "strategy_id": resolved_payload.strategy_id,
                    "strategy_config_id": strategy_config_id,
                    "symbol_in": getattr(resolved_payload, "symbol", None),
                    "symbol_normalized": symbol_norm,
                    "side": side_norm or "buy",
                    "qty_requested": str(resolved_payload.qty),
                    "status": "rejected",
                    "rationale": reject.reason,
                    "inputs": {
                        "strategy_thresholds": None,
                        "timeframe": None,
                        "symbols": None,
                        "price_source": "override" if price_override_value is not None else "latest",
                        "market_price_used": None,
                    },
                    "policy_source": "account_default",
                    "policy_binding": {"strategy_config_id": strategy_config_id},
                    "computed_limits": {},
                    "agent_decision": agent_decision,
                    "result": {
                        "reject": reject.dict(),
                        "endpoint": "POST /admin/paper-trader/smoke-trade",
                    },
                },
            )
        except Exception:
            logger.exception("Failed to emit smoke-trade decision when engine is not running")
        engine_status = runner.status()
        await log_order_rejected_throttled(
            session,
            account_id=resolved_payload.account_id,
            symbol=symbol_norm,
            reject_code=reject.code,
            reject_reason=reject.reason,
            meta={
                "message": "Smoke trade rejected",
                "reject": reject.dict(),
                "endpoint": "POST /admin/paper-trader/smoke-trade",
                "engine_running": runner.is_running,
                "engine_task_state": engine_status.get("engine_task_state"),
                "engine_last_cycle_at": engine_status.get("last_cycle_at"),
                "engine_last_error": engine_status.get("last_error"),
                "request": {
                    "account_id": resolved_payload.account_id,
                    "symbol_in": getattr(resolved_payload, "symbol", None),
                    "side": getattr(resolved_payload, "side", None),
                    "qty": str(getattr(resolved_payload, "qty", None)),
                    "price_override": str(price_override_value) if price_override_value is not None else None,
                    "strategy_id": getattr(resolved_payload, "strategy_id", None),
                    "strategy_config_id": getattr(resolved_payload, "strategy_config_id", None),
                    "agent_decision": agent_decision,
                },
            },
            window_seconds=120,
        )
        try:
            await session.commit()
        except Exception:
            await session.rollback()
        return _reject_response(reject, status_code=400)
    try:
        return await _smoke_trade_impl(
            resolved_payload,
            session,
            agent_decision=agent_decision,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Smoke trade failed")
        await _record_action(
            session,
            "SMOKE_TRADE",
            "error",
            "Smoke trade failed",
            meta=json_safe(
                {
                    "account_id": resolved_payload.account_id,
                    "symbol_in": getattr(resolved_payload, "symbol", None),
                    "side": getattr(resolved_payload, "side", None),
                    "qty": str(getattr(resolved_payload, "qty", None)),
                    "price_override": str(price_override_value) if price_override_value is not None else None,
                    "error": str(exc),
                    "error_type": exc.__class__.__name__,
                    "agent_decision": agent_decision,
                }
            ),
        )
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(exc), "error_type": exc.__class__.__name__},
        )


@router.get("/agent/universe")
async def agent_universe(
    strategy_config_id: int = Query(..., ge=1),
    timeframe: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    agent = AutoTradeAgent(AutoTradeSettings.from_env())
    try:
        universe = await agent.build_universe_for_strategy(
            session,
            strategy_config_id=strategy_config_id,
            timeframe=timeframe,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    logger.info(
        "AGENT_UNIVERSE_QUERY",
        extra={
            "strategy_config_id": strategy_config_id,
            "timeframe": timeframe,
            "candidates_count": universe.get("candidates_count"),
            "fresh_candidates_count": universe.get("fresh_candidates_count"),
            "source": universe.get("source"),
        },
    )
    return {"ok": True, **universe}


@router.post("/agent/decide")
async def agent_decide(
    payload: AgentDecideRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    agent = AutoTradeAgent(AutoTradeSettings.from_env())
    try:
        bundle = await agent.decide_for_strategy(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            window_minutes=payload.window_minutes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    decision = bundle["decision"]
    universe = bundle["universe"]
    selected_symbol = normalize_symbol(str(decision.get("selected_symbol") or ""))
    side = str(decision.get("side") or "hold").lower().strip()
    qty_text = str(decision.get("qty") or "0")
    try:
        qty = Decimal(qty_text)
    except Exception:
        qty = Decimal("0")

    symbol_for_policy = selected_symbol or (
        (decision.get("candidates") or [None])[0]
        if isinstance(decision.get("candidates"), list)
        else None
    ) or "AUTO"
    snapshot = await resolve_effective_policy_snapshot(
        session,
        strategy_config_id=payload.strategy_config_id,
        symbol=str(symbol_for_policy),
    )
    policy_source = snapshot.policy_source if snapshot else "account_default"
    policy_binding = (
        snapshot.policy_binding
        if snapshot
        else {"strategy_config_id": payload.strategy_config_id}
    )
    computed_limits = snapshot.computed_limits if snapshot else {}

    logger.info(
        "AGENT_DECIDE",
        extra={
            "account_id": payload.account_id,
            "strategy_config_id": payload.strategy_config_id,
            "symbol": selected_symbol,
            "side": side,
            "qty": str(qty),
            "dry_run": payload.dry_run,
            "confidence": decision.get("confidence"),
            "candidates_count": decision.get("candidates_count"),
        },
    )

    if side == "hold" or not selected_symbol or qty <= 0:
        reason = str(decision.get("reason") or "Agent decided hold")
        reject_payload = {
            "code": "AUTO_HOLD",
            "reason": reason,
            "details": {
                "selected_symbol": selected_symbol or None,
                "candidates_count": decision.get("candidates_count"),
                "candidates_sample": decision.get("candidates_sample"),
            },
        }
        await _emit_agent_decision_event(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            strategy_id=str(payload.strategy_config_id),
            decision=decision,
            status="skipped",
            rationale=reason,
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
            reject=reject_payload,
        )
        await session.commit()
        return {
            "ok": True,
            "status": "skipped",
            "agent_decision": decision,
            "selected_symbol": selected_symbol or None,
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "universe": universe,
        }

    if payload.dry_run:
        await _emit_agent_decision_event(
            session,
            account_id=payload.account_id,
            strategy_config_id=payload.strategy_config_id,
            strategy_id=str(payload.strategy_config_id),
            decision=decision,
            status="proposed",
            rationale="Agent decision proposed (dry run)",
            policy_source=policy_source,
            policy_binding=policy_binding,
            computed_limits=computed_limits,
        )
        await session.commit()
        return {
            "ok": True,
            "status": "proposed",
            "dry_run": True,
            "agent_decision": decision,
            "selected_symbol": selected_symbol,
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "universe": universe,
        }

    config = PaperTraderConfig.from_env()
    try:
        result = await place_order_unified(
            session,
            account_id=payload.account_id,
            symbol=selected_symbol,
            side=side,
            qty=qty,
            strategy_id=payload.strategy_config_id,
            price_override=payload.price_override,
            fee_bps=config.fee_bps,
            slippage_bps=config.slippage_bps,
            meta={
                "origin": "admin_agent_decide",
                "agent_decision": decision,
                "agent_universe": {
                    "source": universe.get("source"),
                    "candidates_count": universe.get("candidates_count"),
                    "fresh_candidates_count": universe.get("fresh_candidates_count"),
                },
            },
            reject_action_type="ORDER_REJECTED",
            reject_window_seconds=120,
        )
    except HTTPException:
        await session.rollback()
        raise
    except Exception as exc:
        await session.rollback()
        logger.exception("AGENT_DECIDE_EXECUTION_FAILED")
        raise HTTPException(status_code=500, detail=f"agent decide execution failed: {exc}")

    if isinstance(result, RejectReason):
        await session.commit()
        return {
            "ok": False,
            "status": "rejected",
            "agent_decision": decision,
            "selected_symbol": selected_symbol,
            "reject": result.dict(),
            "policy_source": policy_source,
            "policy_binding": policy_binding,
            "computed_limits": computed_limits,
            "universe": universe,
        }

    execution = result.execution
    await session.commit()
    return {
        "ok": True,
        "status": "executed",
        "agent_decision": decision,
        "selected_symbol": selected_symbol,
        "policy_source": result.policy_source or policy_source,
        "policy_binding": policy_binding,
        "computed_limits": computed_limits,
        "execution": {
            "order_id": execution.order.id,
            "trade_id": execution.trade.id,
            "symbol": result.prepared.symbol,
            "side": execution.order.side,
            "qty": str(result.prepared.qty),
            "price": str(result.prepared.price),
            "price_source": result.price_source,
        },
        "universe": universe,
    }


@admin_router.post("/paper-accounts/create")
async def create_paper_account(
    payload: PaperAccountCreateRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    config = PaperTraderConfig.from_env()
    initial_cash = (
        Decimal(str(payload.initial_cash_usd))
        if payload.initial_cash_usd is not None
        else Decimal("10000.00")
    )
    if not initial_cash.is_finite() or initial_cash < 0:
        raise HTTPException(status_code=400, detail="initial_cash_usd must be a valid decimal")

    account_name = f"paper-{config.exchange_name}"
    base_ccy = config.base_ccy

    async def _create() -> PaperAccount:
        account = await session.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
        if account is None:
            account = PaperAccount(name=account_name, base_ccy=base_ccy, initial_cash_usd=initial_cash)
            session.add(account)
            await session.flush()
        elif payload.initial_cash_usd is not None or account.initial_cash_usd is None:
            account.initial_cash_usd = initial_cash
            await session.flush()

        balance = await session.scalar(
            select(PaperBalance).where(PaperBalance.account_id == account.id, PaperBalance.ccy == "USDT")
        )
        if balance is None:
            balance = PaperBalance(account_id=account.id, ccy="USDT", available=initial_cash)
            session.add(balance)
        else:
            balance.available = initial_cash
        await session.flush()
        return account

    if session.in_transaction():
        account = await _create()
    else:
        async with session.begin():
            account = await _create()

    return {
        "account_id": account.id,
        "name": account.name,
        "initial_cash_usd": str(account.initial_cash_usd),
    }


@admin_router.post("/paper-accounts/reset")
async def reset_paper_account(
    payload: PaperAccountResetRequest,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    reset_to_initial_cash = bool(payload.reset_to_initial_cash) if payload.reset_to_initial_cash is not None else True
    account = await session.get(PaperAccount, payload.account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Paper account not found")

    async def _reset() -> None:
        initial_cash = account.initial_cash_usd if account.initial_cash_usd is not None else Decimal("10000.00")
        await session.execute(delete(PaperOrder).where(PaperOrder.account_id == account.id))
        await session.execute(delete(PaperTrade).where(PaperTrade.account_id == account.id))
        await session.execute(delete(PaperPosition).where(PaperPosition.account_id == account.id))
        await session.execute(delete(PaperBalance).where(PaperBalance.account_id == account.id))
        if reset_to_initial_cash:
            session.add(PaperBalance(account_id=account.id, ccy="USDT", available=Decimal(str(initial_cash))))
        session.add(
            AdminAction(
                action="ACCOUNT_RESET",
                status=normalize_status("ok"),
                message="Paper account reset",
                meta=to_jsonable(
                    {
                        "account_id": account.id,
                        "reset_to_initial_cash": reset_to_initial_cash,
                        "initial_cash_usd": str(initial_cash),
                    }
                ),
            )
        )
        await session.flush()

    try:
        if session.in_transaction():
            await _reset()
        else:
            async with session.begin():
                await _reset()
    except Exception as exc:
        try:
            if session.in_transaction():
                await session.rollback()
        except Exception:
            pass
        try:
            session.add(
                AdminAction(
                    action="ACCOUNT_RESET",
                    status=normalize_status("error"),
                    message="Paper account reset error",
                    meta=to_jsonable(
                        {
                            "account_id": account.id,
                            "reset_to_initial_cash": reset_to_initial_cash,
                            "initial_cash_usd": str(account.initial_cash_usd),
                            "error": str(exc),
                        }
                    ),
                )
            )
            await session.commit()
        except Exception:
            await session.rollback()
        raise

    return {
        "account_id": account.id,
        "reset_to_initial_cash": reset_to_initial_cash,
    }


@router.get("/equity")
async def paper_trader_equity(limit: int = Query(200, ge=1, le=2000), session: AsyncSession = Depends(get_db_session)) -> dict:
    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == _account_name()))
    if account is None:
        raise HTTPException(status_code=404, detail="Paper account not found")

    snapshots = (
        await session.scalars(
            select(EquitySnapshot)
            .where(EquitySnapshot.account_id == account.id)
            .order_by(desc(EquitySnapshot.created_at))
            .limit(limit)
        )
    ).all()
    points = [
        {
            "ts": snap.created_at.isoformat(),
            "equity": str(snap.equity),
            "balance": str(snap.balance),
            "unrealized_pnl": str(snap.unrealized_pnl),
        }
        for snap in reversed(snapshots)
    ]
    return {"points": points}


@router.get("/reconcile")
async def reconcile(account_id: int = Query(..., ge=1), mode: str | None = Query(default=None), session: AsyncSession = Depends(get_db_session)) -> dict:
    if mode and mode.lower().strip() == "apply":
        raise HTTPException(status_code=400, detail="GET reconcile is report-only; use POST /reconcile/apply")
    policy = await get_reconcile_policy(session, account_id)
    report = await reconcile_report(session, account_id, policy=policy)
    status = normalize_status("ok" if report.get("ok") else "alert")
    status = apply_reconcile_policy_status(
        summary=report.get("summary") or {},
        base_status=status,
        policy=policy,
    )
    diffs_sample = report.get("diffs", [])[:10]
    window_seconds = 600
    if policy and policy.log_window_seconds:
        window_seconds = int(policy.log_window_seconds)
    await log_reconcile_report_throttled(
        account_id=account_id,
        status=status,
        message="Paper trader reconcile report",
        report_meta={
            "summary": report.get("summary"),
            "diff_count": len(report.get("diffs", [])),
            "diffs_sample": diffs_sample,
            "warnings": report.get("warnings"),
            "policy": policy_snapshot(policy),
            "baseline_source": (report.get("derived") or {}).get("baseline_source"),
        },
        window_seconds=window_seconds,
    )
    return report


@admin_router.get("/db-fingerprint")
async def db_fingerprint(session: AsyncSession = Depends(get_db_session)) -> dict:
    row = await session.execute(text("select current_database(), current_schema(), current_user, inet_server_addr(), inet_server_port(), now()"))
    vals = row.fetchone()
    return {
        "current_database": vals[0],
        "current_schema": vals[1],
        "current_user": vals[2],
        "server_addr": str(vals[3]),
        "server_port": vals[4],
        "now": vals[5].isoformat() if vals[5] else None,
    }


@router.post("/reconcile/apply")
async def reconcile_apply_endpoint(
    body: ReconcileApplyBody = Body(...),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    from ai_crypto_trader.services.paper_trader.runner import PaperTraderRunner

    runner = PaperTraderRunner.instance()
    if runner.is_running:
        raise HTTPException(status_code=409, detail="Paper trader runner is active; stop it before applying reconciliation")
    return await reconcile_apply(
        session,
        body.account_id,
        apply_positions=bool(body.apply_positions),
        apply_balances=bool(body.apply_balances),
    )


async def _record_action(session: AsyncSession, action: str, status: str, message: str | None = None, meta: dict | None = None) -> None:
    try:
        session.add(
            AdminAction(
                action=action,
                status=normalize_status(status),
                message=message,
                meta=to_jsonable(meta or {}),
            )
        )
        await session.commit()
    except Exception:
        await session.rollback()


@router.get("/logs")
async def paper_trader_logs(tail: int = Query(200, ge=1, le=5000)) -> dict:
    buf = get_log_buffer()
    lines = buf.tail(tail)
    return {"lines": lines}


@router.get("/logs/stream")
async def paper_trader_logs_stream() -> StreamingResponse:
    """
    Server-sent events streaming new log lines.

    Manual test:
      curl -N http://127.0.0.1:8000/api/admin/paper-trader/logs/stream
    """
    buf = get_log_buffer()
    queue: asyncio.Queue[str] = asyncio.Queue()

    class QueueHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                queue.put_nowait(self.format(record))
            except asyncio.QueueFull:
                pass

    handler = QueueHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger("ai_crypto_trader")
    root.addHandler(handler)

    async def event_generator():
        try:
            # send current tail initially
            for line in buf.tail(100):
                yield f"data: {json.dumps(line)}\n\n"
            while True:
                line = await queue.get()
                yield f"data: {json.dumps(line)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            root.removeHandler(handler)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@admin_router.get("/actions")
async def list_admin_actions(limit: int = Query(50, ge=1, le=500), session: AsyncSession = Depends(get_db_session)) -> dict:
    try:
        result = await session.execute(select(AdminAction).order_by(AdminAction.created_at.desc()).limit(limit))
        actions = result.scalars().all()
        return {
            "actions": [
                {
                    "id": a.id,
                    "action": a.action,
                    "status": a.status,
                    "message": a.message,
                    "meta": a.meta,
                    "created_at": a.created_at.isoformat(),
                }
                for a in actions
            ]
        }
    except ProgrammingError as e:
        if AsyncpgUndefinedTableError and isinstance(e.orig, AsyncpgUndefinedTableError):
            logger.warning("admin_actions table missing; returning empty actions list")
            return {"actions": []}
        raise
