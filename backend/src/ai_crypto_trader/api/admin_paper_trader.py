import os
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, HTTPException, Query, Header, Body
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
)

from ai_crypto_trader.common.log_buffer import get_log_buffer
from ai_crypto_trader.services.paper_trader.config import get_active_bundle, ActiveConfigMissingError, PaperTraderConfig
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.risk import evaluate_and_size
from ai_crypto_trader.services.paper_trader.execution import execute_market_order_with_costs
from ai_crypto_trader.services.paper_trader.maintenance import reconcile_report, reconcile_fix, reconcile_apply, normalize_status
from ai_crypto_trader.common.jsonable import to_jsonable
from sqlalchemy import text
from fastapi.responses import StreamingResponse
import asyncio
import json

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
async def paper_trader_status() -> dict:
    return runner.status()


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
    symbol: str = "ETHUSDT"
    side: str = "buy"
    qty: Decimal = Decimal("0.01")
    price: str | None = None


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
        await _record_action(session, "RISK_REJECT", "rejected", "Risk rejected order", meta=meta_payload)
        return {"allowed": False, "risk": risk_eval}

    qty = Decimal(str(risk_eval.get("qty", "0")))
    if qty <= 0:
        await _record_action(session, "RISK_REJECT", "rejected", "Risk sizing returned non-positive qty", meta=meta_payload)
        return {"allowed": False, "risk": risk_eval}

    fee_bps = Decimal(str(bundle["risk_policy"]["fee_bps"]))
    slippage_bps = Decimal(str(bundle["risk_policy"]["slippage_bps"]))

    try:
        execution = await execute_market_order_with_costs(
            session=session,
            account_id=payload.account_id,
            symbol=payload.symbol,
            side=payload.side,
            qty=qty,
            mid_price=Decimal(str(payload.price)),
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            meta={"origin": "admin_manual"},
        )
    except ValueError as exc:
        await _record_action(
            session,
            "ORDER_REJECTED",
            "alert",
            "Order rejected",
            meta={
                "account_id": payload.account_id,
                "symbol": payload.symbol,
                "side": payload.side,
                "qty": str(qty),
                "price": str(payload.price),
                "reason": str(exc),
            },
        )
        raise HTTPException(status_code=400, detail=str(exc))

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


@router.post("/smoke-trade")
async def smoke_trade(payload: SmokeTradeRequest, session: AsyncSession = Depends(get_db_session)) -> dict:
    # Self-test: NEW_ID=3 smoke buy 0.01 + sell 0.005 (with price), then verify positions/balances,
    # reconcile?account_id=3 diff_count ok, /status has no InvalidStateError, admin_actions meta serializes.
    symbol_in = (payload.symbol or "ETHUSDT").strip()
    symbol_norm = normalize_symbol(symbol_in)
    side = (payload.side or "buy").lower().strip()
    if side not in {"buy", "sell"}:
        await _record_action(
            session,
            "ORDER_REJECTED",
            "alert",
            "Order rejected",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(payload.qty),
                "reason": "side must be buy or sell",
            },
        )
        raise HTTPException(status_code=400, detail="side must be buy or sell")
    qty = Decimal(str(payload.qty))
    if qty <= 0:
        await _record_action(
            session,
            "ORDER_REJECTED",
            "alert",
            "Order rejected",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(payload.qty),
                "reason": "qty must be positive",
            },
        )
        raise HTTPException(status_code=400, detail="qty must be positive")
    if not symbol_norm:
        await _record_action(
            session,
            "ORDER_REJECTED",
            "alert",
            "Order rejected",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(payload.qty),
                "reason": "symbol must be non-empty",
            },
        )
        raise HTTPException(status_code=400, detail="symbol must be non-empty")

    price = None
    price_source = None
    if payload.price is not None:
        try:
            price = Decimal(str(payload.price))
        except (InvalidOperation, TypeError, ValueError):
            await _record_action(
                session,
                "ORDER_REJECTED",
                "alert",
                "Order rejected",
                meta={
                    "account_id": payload.account_id,
                    "symbol_in": symbol_in,
                    "symbol_normalized": symbol_norm,
                    "side": side,
                    "qty": str(qty),
                    "price": payload.price,
                    "reason": "price must be a valid decimal",
                },
            )
            raise HTTPException(status_code=400, detail="price must be a valid decimal")
        if not price.is_finite() or price <= 0:
            await _record_action(
                session,
                "ORDER_REJECTED",
                "alert",
                "Order rejected",
                meta={
                    "account_id": payload.account_id,
                    "symbol_in": symbol_in,
                    "symbol_normalized": symbol_norm,
                    "side": side,
                    "qty": str(qty),
                    "price": str(price),
                    "reason": "price must be positive",
                },
            )
            raise HTTPException(status_code=400, detail="price must be positive")
        price_source = "override"
    else:
        price = await _latest_price(session, symbol_norm)
        if price is not None and price.is_finite():
            price_source = "latest"
        else:
            price = None
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
                    price = candidate
                    price_source = "paper_trades_last"

    if price is None:
        await _record_action(
            session,
            "SMOKE_TRADE",
            "error",
            "Smoke trade price unavailable",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(qty),
                "price": None,
                "price_source": price_source,
                "trade_id": None,
            },
        )
        raise HTTPException(
            status_code=400,
            detail=f"No price data for symbol {symbol_norm}; pass price to override",
        )

    config = PaperTraderConfig.from_env()
    try:
        execution = await execute_market_order_with_costs(
            session=session,
            account_id=payload.account_id,
            symbol=symbol_norm,
            side=side,
            qty=qty,
            mid_price=Decimal(str(price)),
            fee_bps=config.fee_bps,
            slippage_bps=config.slippage_bps,
            meta={"origin": "admin_smoke"},
        )
    except ValueError as exc:
        await _record_action(
            session,
            "ORDER_REJECTED",
            "alert",
            "Order rejected",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(qty),
                "price": str(price) if price is not None else None,
                "price_source": price_source,
                "reason": str(exc),
            },
        )
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        await _record_action(
            session,
            "SMOKE_TRADE",
            "error",
            "Smoke trade failed",
            meta={
                "account_id": payload.account_id,
                "symbol_in": symbol_in,
                "symbol_normalized": symbol_norm,
                "side": side,
                "qty": str(qty),
                "price": str(price) if price is not None else None,
                "price_source": price_source,
                "trade_id": None,
                "error": str(exc),
            },
        )
        raise

    await _record_action(
        session,
        "SMOKE_TRADE",
        "ok",
        "Smoke trade executed",
        meta={
            "account_id": payload.account_id,
            "symbol_in": symbol_in,
            "symbol_normalized": symbol_norm,
            "side": side,
            "qty": str(qty),
            "price": str(price) if price is not None else None,
            "price_source": price_source,
            "trade_id": execution.trade.id,
        },
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
        "symbol_in": symbol_in,
        "symbol_normalized": symbol_norm,
        "price_source": price_source,
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
    report = await reconcile_report(session, account_id)
    status = normalize_status("ok" if report.get("ok") else "alert")
    diffs_sample = report.get("diffs", [])[:10]
    await _record_action(
        session,
        "RECONCILE_REPORT",
        status,
        "Paper trader reconcile report",
        meta={
            "account_id": account_id,
            "summary": report.get("summary"),
            "diff_count": len(report.get("diffs", [])),
            "diffs_sample": diffs_sample,
        },
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
