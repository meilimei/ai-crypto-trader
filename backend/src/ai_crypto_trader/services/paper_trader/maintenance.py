import logging
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

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
from ai_crypto_trader.common.jsonable import to_jsonable
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol, MONEY_EXP
from ai_crypto_trader.services.paper_trader.ledger import get_initial_usdt, simulate_positions_and_cash

logger = logging.getLogger(__name__)


async def _latest_price(session: AsyncSession, symbol: str) -> Optional[Decimal]:
    result = await session.execute(
        select(Candle.close)
        .where(Candle.symbol == symbol)
        .order_by(Candle.open_time.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def flatten_positions(session: AsyncSession) -> Dict[str, object]:
    """
    Close all open paper positions for the configured account by inserting filled market
    orders/trades and zeroing out positions. Adds an equity snapshot at the end.
    """
    config = PaperTraderConfig.from_env()
    account_name = f"paper-{config.exchange_name}"

    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
    if account is None:
        logger.info("Flatten requested but paper account missing", extra={"account": account_name})
        return {"ok": True, "account_name": account_name, "flattened": 0, "symbols": []}

    positions: List[PaperPosition] = (
        await session.scalars(
            select(PaperPosition).where(PaperPosition.account_id == account.id, PaperPosition.qty != Decimal("0"))
        )
    ).all()
    if not positions:
        return {"ok": True, "account_name": account_name, "flattened": 0, "symbols": []}

    flattened = 0
    symbols: List[str] = []
    now = utc_now()

    for pos in positions:
        qty_abs = pos.qty.copy_abs()
        side = "sell" if pos.qty > 0 else "buy"
        symbol_norm = normalize_symbol(pos.symbol)
        symbols.append(symbol_norm)

        price = await _latest_price(session, symbol_norm)
        fill_price = price if price is not None else Decimal("0")

        order = PaperOrder(
            account_id=account.id,
            symbol=symbol_norm,
            side=side,
            type="market",
            status="filled",
            requested_qty=qty_abs,
            filled_qty=qty_abs,
            avg_fill_price=fill_price,
            fee_paid=Decimal("0"),
            created_at=now,
        )
        session.add(order)

        trade = PaperTrade(
            account_id=account.id,
            symbol=symbol_norm,
            side=side,
            qty=qty_abs,
            price=fill_price,
            fee=Decimal("0"),
            realized_pnl=Decimal("0"),
            created_at=now,
        )
        session.add(trade)

        pos.qty = Decimal("0")
        pos.avg_entry_price = Decimal("0")
        pos.unrealized_pnl = Decimal("0")
        pos.updated_at = now
        flattened += 1

    balances: List[PaperBalance] = (
        await session.scalars(select(PaperBalance).where(PaperBalance.account_id == account.id))
    ).all()
    total_balance = sum((bal.available for bal in balances), Decimal("0"))
    snapshot = EquitySnapshot(
        account_id=account.id,
        equity=total_balance,
        balance=total_balance,
        unrealized_pnl=Decimal("0"),
        created_at=now,
    )
    session.add(snapshot)

    await session.commit()

    logger.info(
        "Flattened paper positions",
        extra={"account": account_name, "flattened": flattened, "symbols": symbols},
    )

    return {"ok": True, "account_name": account_name, "flattened": flattened, "symbols": symbols}


def normalize_asset(asset: str) -> str:
    return (asset or "").upper().strip()


def _quant(val: Decimal, exp: str = "0.00000001") -> Decimal:
    return Decimal(val).quantize(Decimal(exp))


def to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def is_effectively_zero(value: Any, eps: Decimal) -> bool:
    return to_decimal(value).copy_abs() <= to_decimal(eps)


def normalize_zero(value: Any, eps: Decimal = Decimal("0")) -> Decimal:
    dec = to_decimal(value)
    if is_effectively_zero(dec, eps):
        return Decimal("0")
    return dec


def normalize_status(status: str) -> str:
    return (status or "").strip().lower()


def json_safe(obj: Any) -> Any:
    return to_jsonable(obj)


def _safe_order_cols(model) -> List[Any]:
    cols: List[Any] = []
    if hasattr(model, "created_at"):
        cols.append(getattr(model, "created_at").asc())
    elif hasattr(model, "updated_at"):
        cols.append(getattr(model, "updated_at").asc())
    if hasattr(model, "id"):
        cols.append(getattr(model, "id").asc())
    return cols


async def _ensure_unique_index(
    session: AsyncSession,
    *,
    table_name: str,
    index_name: str,
    columns: List[str],
    alt_names: Optional[List[str]] = None,
) -> None:
    bind = session.get_bind()
    if not bind or bind.dialect.name != "postgresql":
        return
    names = [index_name] + (alt_names or [])
    for name in names:
        exists = await session.execute(text("select to_regclass(:name)"), {"name": name})
        if exists.scalar():
            return
    col_sql = ", ".join(columns)
    await session.execute(text(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col_sql})"))


async def compute_derived_state(session: AsyncSession, account_id: int) -> Dict[str, Any]:
    """
    Derive positions and balances from trades (best-effort).
    """
    baseline_cash, baseline_source = await get_initial_usdt(session, account_id)

    trades: List[PaperTrade] = (
        await session.scalars(
            select(PaperTrade)
            .where(PaperTrade.account_id == account_id)
            .order_by(PaperTrade.created_at.asc(), PaperTrade.id.asc())
        )
    ).all()
    ledger = simulate_positions_and_cash(trades, baseline_cash)
    positions_by_symbol = {
        sym: {"qty": str(data["qty"]), "avg_entry_price": str(data["avg_entry"])}
        for sym, data in ledger.get("positions", {}).items()
    }
    cash_usd = _quant(ledger.get("cash", {}).get("USDT", {}).get("free", Decimal("0")), MONEY_EXP)
    stats = ledger.get("stats", {})
    total_fees = _quant(stats.get("fees_usd", Decimal("0")), MONEY_EXP)
    total_slippage = _quant(stats.get("slippage_usd", Decimal("0")), MONEY_EXP)

    derived = {
        "positions_by_symbol": positions_by_symbol,
        "cash_usd": str(cash_usd),
        "fees_usd": str(total_fees),
        "slippage_usd": str(total_slippage),
        "realized_pnl_usd": str(_quant(stats.get("realized_pnl_usd", Decimal("0")), MONEY_EXP)),
        "balances_by_asset": {"USDT": str(cash_usd)},
        "baseline_source": baseline_source,
        "debug_cash_components": {
            "baseline_cash": str(_quant(baseline_cash, MONEY_EXP)),
            "sum_buy_notional": str(_quant(stats.get("sum_buy_notional", Decimal("0")), MONEY_EXP)),
            "sum_sell_notional": str(_quant(stats.get("sum_sell_notional", Decimal("0")), MONEY_EXP)),
            "sum_fees": str(total_fees),
            "sum_slippage": str(total_slippage),
        },
    }
    return derived


def diff_states(
    db_state: Dict[str, Any],
    derived_state: Dict[str, Any],
    qty_tol: Decimal = Decimal("0.00000001"),
    usd_tol: Decimal = Decimal("0.01"),
    price_tol: Decimal = Decimal("0.00000001"),
) -> Dict[str, Any]:
    diffs: List[Dict[str, Any]] = []

    # Positions
    db_positions = db_state.get("positions_by_symbol", {}) or {}
    derived_positions = derived_state.get("positions_by_symbol", {}) or {}
    symbols = set(db_positions.keys()) | set(derived_positions.keys())
    for sym in sorted(symbols):
        db_qty = Decimal(str(db_positions.get(sym, {}).get("qty", "0")))
        deriv_qty = Decimal(str(derived_positions.get(sym, {}).get("qty", "0")))
        delta = db_qty - deriv_qty
        if delta.copy_abs() > qty_tol:
            diffs.append(
                {
                    "type": "position_qty",
                    "symbol": sym,
                    "db_value": str(db_qty),
                    "derived_value": str(deriv_qty),
                    "delta": str(delta),
                }
            )
        if db_qty.copy_abs() > qty_tol or deriv_qty.copy_abs() > qty_tol:
            db_avg = Decimal(str(db_positions.get(sym, {}).get("avg_entry_price", "0")))
            deriv_avg = Decimal(str(derived_positions.get(sym, {}).get("avg_entry_price", "0")))
            avg_delta = db_avg - deriv_avg
            if avg_delta.copy_abs() > price_tol:
                diffs.append(
                    {
                        "type": "position_avg",
                        "symbol": sym,
                        "db_value": str(db_avg),
                        "derived_value": str(deriv_avg),
                        "delta": str(avg_delta),
                    }
                )

    # Balances
    db_balances = db_state.get("balances_by_asset", {}) or {}
    derived_balances = derived_state.get("balances_by_asset", {}) or {}
    assets = set(db_balances.keys()) | set(derived_balances.keys())
    for asset in sorted(assets):
        db_amt = Decimal(str(db_balances.get(asset, "0")))
        deriv_amt = Decimal(str(derived_balances.get(asset, "0")))
        delta = db_amt - deriv_amt
        if delta.copy_abs() > usd_tol:
            diffs.append(
                {
                    "type": "balance",
                    "asset": asset,
                    "db_value": str(db_amt),
                    "derived_value": str(deriv_amt),
                    "delta": str(delta),
                }
            )

    usdt_delta = Decimal(str(db_balances.get("USDT", "0"))) - Decimal(str(derived_balances.get("USDT", "0")))
    per_symbol_diffs = {
        d["symbol"]: d["delta"] for d in diffs if d.get("type") == "position_qty"
    }

    summary = {
        "diff_count": len(diffs),
        "has_negative_balance": any(Decimal(str(v)) < 0 for v in derived_state.get("balances_by_asset", {}).values()),
        "has_position_mismatch": any(d.get("type") == "position_qty" for d in diffs),
        "has_equity_mismatch": False,  # placeholder; can be extended
        "tolerance_qty": str(qty_tol),
        "tolerance_usd": str(usd_tol),
        "tolerance_price": str(price_tol),
        "per_symbol_diffs": per_symbol_diffs,
        "usdt_diff": str(usdt_delta),
    }
    return {"diffs": diffs, "summary": summary}


async def reconcile_report(session: AsyncSession, account_id: int) -> Dict[str, Any]:
    try:
        account = await session.get(PaperAccount, account_id)
        if not account:
            return {"ok": True, "account_id": account_id, "db": {}, "derived": {}, "diffs": [], "summary": {}}

        warnings: List[str] = []
        db_positions_query = await session.scalars(
            select(PaperPosition).where(PaperPosition.account_id == account_id)
        )
        db_positions: Dict[str, Dict[str, Decimal]] = {}
        symbol_counts: Dict[str, int] = {}
        symbol_raws: Dict[str, set[str]] = {}
        for pos in db_positions_query:
            sym_norm = normalize_symbol(pos.symbol)
            symbol_counts[sym_norm] = symbol_counts.get(sym_norm, 0) + 1
            symbol_raws.setdefault(sym_norm, set()).add(pos.symbol)
            entry = db_positions.setdefault(sym_norm, {"qty": Decimal("0"), "avg": Decimal("0")})
            qty = Decimal(str(pos.qty or 0))
            avg = Decimal(str(pos.avg_entry_price or 0))
            if entry["qty"] == 0:
                entry["qty"] = qty
                entry["avg"] = avg
            else:
                new_qty = entry["qty"] + qty
                denom = abs(new_qty)
                if denom == 0:
                    entry["qty"] = Decimal("0")
                    entry["avg"] = Decimal("0")
                else:
                    weighted = (abs(entry["qty"]) * entry["avg"] + abs(qty) * avg) / denom
                    entry["qty"] = new_qty
                    entry["avg"] = weighted
        if any(count > 1 for count in symbol_counts.values()) or any(len(raws) > 1 for raws in symbol_raws.values()):
            warnings.append("DUP_SYMBOL_ROWS")

        # Check duplicates in trades/orders too
        trade_rows = await session.scalars(select(PaperTrade.symbol).where(PaperTrade.account_id == account_id))
        trade_sets: Dict[str, set[str]] = {}
        for sym in trade_rows:
            sym_norm = normalize_symbol(sym)
            trade_sets.setdefault(sym_norm, set()).add(sym)
        order_rows = await session.scalars(select(PaperOrder.symbol).where(PaperOrder.account_id == account_id))
        order_sets: Dict[str, set[str]] = {}
        for sym in order_rows:
            sym_norm = normalize_symbol(sym)
            order_sets.setdefault(sym_norm, set()).add(sym)
        if any(len(raws) > 1 for raws in trade_sets.values()) or any(len(raws) > 1 for raws in order_sets.values()):
            if "DUP_SYMBOL_ROWS" not in warnings:
                warnings.append("DUP_SYMBOL_ROWS")

        db_balances_query = await session.scalars(select(PaperBalance).where(PaperBalance.account_id == account_id))
        db_balances = {normalize_asset(bal.ccy): str(bal.available) for bal in db_balances_query}

        latest_snapshot = await session.scalar(
            select(EquitySnapshot).where(EquitySnapshot.account_id == account_id).order_by(EquitySnapshot.created_at.desc()).limit(1)
        )
        db_state = {
            "positions_by_symbol": {sym: {"qty": str(val["qty"]), "avg_entry_price": str(val["avg"])} for sym, val in db_positions.items()},
            "balances_by_asset": db_balances,
            "latest_equity_snapshot": {
                "equity": str(latest_snapshot.equity),
                "created_at": latest_snapshot.created_at.isoformat(),
            }
            if latest_snapshot
            else None,
        }

        try:
            derived_state = await compute_derived_state(session, account_id)
        except (InvalidOperation, ZeroDivisionError) as exc:
            warnings.append(f"DERIVED_STATE_ERROR:{exc.__class__.__name__}")
            derived_state = {
                "positions_by_symbol": {},
                "balances_by_asset": {},
                "baseline_source": "error",
            }
        diff_result = diff_states(db_state, derived_state)
        diffs = diff_result.get("diffs", [])
        diff_type_counts: Dict[str, int] = {}
        for diff in diffs:
            diff_type = str(diff.get("type") or "unknown")
            diff_type_counts[diff_type] = diff_type_counts.get(diff_type, 0) + 1
        pos_qty_count = diff_type_counts.get("position_qty", 0)
        pos_avg_count = diff_type_counts.get("position_avg", 0)
        balance_count = diff_type_counts.get("balance", 0)
        equity_count = diff_type_counts.get("equity", 0)
        debug_counts = {
            "diff_items_total": len(diffs),
            "diff_items_positions": pos_qty_count + pos_avg_count,
            "diff_items_position_qty": pos_qty_count,
            "diff_items_position_avg": pos_avg_count,
            "diff_items_balances": balance_count,
            "diff_items_equity": equity_count,
            "warnings": len(warnings),
            "extra_rows": 0,
            "diff_types": diff_type_counts,
        }

        return {
            "ok": diff_result["summary"]["diff_count"] == 0,
            "account_id": account_id,
            "db": db_state,
            "derived": derived_state,
            "diffs": diff_result["diffs"],
            "summary": diff_result["summary"],
            "warnings": warnings,
            "debug_counts": debug_counts,
        }
    except Exception as exc:
        error_payload = {
            "status": "error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
            "diff_count": None,
            "generated_at": utc_now().isoformat(),
            "account_id": account_id,
        }
        try:
            session.add(
                AdminAction(
                    action="RECONCILE_REPORT",
                    status=normalize_status("error"),
                    message="Reconcile report error",
                    meta=to_jsonable(
                        {
                            "account_id": account_id,
                            "error_type": exc.__class__.__name__,
                            "error": str(exc),
                        }
                    ),
                )
            )
            if not session.in_transaction():
                await session.commit()
        except Exception:
            try:
                await session.rollback()
            except Exception:
                pass
        return error_payload


async def reconcile_fix(session: AsyncSession, account_id: int, apply: bool) -> Dict[str, Any]:
    # Deprecated in favor of reconcile_apply; keep for compatibility.
    report_before = await reconcile_report(session, account_id)
    diffs = report_before.get("diffs", [])
    applied_counts = {"positions_upserted": 0, "positions_zeroed": 0, "balances_upserted": 0}
    derived_cash = Decimal(str(report_before.get("derived", {}).get("balances_by_asset", {}).get("USDT", "0")))
    equity_snapshot = report_before.get("db", {}).get("latest_equity_snapshot")
    equity_mismatch = False
    equity_epsilon = Decimal("0.01")
    if equity_snapshot and equity_snapshot.get("equity") is not None:
        try:
            equity_val = Decimal(str(equity_snapshot.get("equity")))
            if (equity_val - derived_cash).copy_abs() > equity_epsilon:
                equity_mismatch = True
        except Exception:
            equity_mismatch = False

    if not apply:
        status = normalize_status("ok" if report_before.get("ok") else "alert")
        session.add(
            AdminAction(
                action="RECONCILE_REPORT",
                status=status,
                message="Reconcile report (dry run)",
                meta=to_jsonable({
                    "account_id": account_id,
                    "summary": report_before.get("summary"),
                    "diffs_sample": diffs[:20],
                }),
            )
        )
        if not session.in_transaction():
            await session.commit()
        return {
            "mode": "dry_run",
            "before": report_before,
            "after": report_before,
            "applied": False,
            "actions": [],
        }

    if report_before.get("ok"):
        return {
            "mode": "apply",
            "before": report_before,
            "after": report_before,
            "applied": False,
            "actions": ["no_diffs"],
        }

    if derived_cash < 0 or equity_mismatch:
        session.add(
            AdminAction(
                action="RECONCILE_APPLY",
                status=normalize_status("blocked"),
                message="Reconcile blocked",
                meta=to_jsonable(
                    {"account_id": account_id, "derived_cash": str(derived_cash), "equity_mismatch": equity_mismatch}
                ),
            )
        )
        if not session.in_transaction():
            await session.commit()
        return {
            "mode": "apply",
            "before": report_before,
            "after": report_before,
            "applied": False,
            "actions": ["blocked_negative_cash" if derived_cash < 0 else "equity_mismatch"],
        }

    started_tx = not session.in_transaction()
    tx = session.begin_nested() if not started_tx else session.begin()
    async with tx:
        # Positions upsert/zero
        derived_positions = report_before.get("derived", {}).get("positions_by_symbol", {})
        for sym_norm, pdata in derived_positions.items():
            qty = Decimal(str(pdata.get("qty", "0")))
            avg = Decimal(str(pdata.get("avg_entry_price", "0")))
            existing = await session.scalar(
                select(PaperPosition).where(PaperPosition.account_id == account_id, PaperPosition.symbol == sym_norm)
            )
            if not existing:
                existing = PaperPosition(
                    account_id=account_id,
                    symbol=sym_norm,
                    side="long",
                    qty=qty,
                    avg_entry_price=avg,
                    unrealized_pnl=Decimal("0"),
                )
                session.add(existing)
            else:
                existing.symbol = sym_norm
                existing.qty = qty
                existing.avg_entry_price = avg
            applied_counts["positions_upserted"] += 1

        db_positions_query = await session.scalars(
            select(PaperPosition).where(PaperPosition.account_id == account_id)
        )
        for pos in db_positions_query:
            sym_norm = normalize_symbol(pos.symbol)
            if sym_norm not in derived_positions or Decimal(str(derived_positions.get(sym_norm, {}).get("qty", "0"))) == 0:
                if pos.qty != 0 or pos.avg_entry_price != 0:
                    pos.qty = Decimal("0")
                    pos.avg_entry_price = Decimal("0")
                    applied_counts["positions_zeroed"] += 1

        # Balance upsert for USDT
        usdt_balance = await session.scalar(
            select(PaperBalance).where(PaperBalance.account_id == account_id, PaperBalance.ccy == "USDT")
        )
        if not usdt_balance:
            usdt_balance = PaperBalance(account_id=account_id, ccy="USDT", available=derived_cash)
            session.add(usdt_balance)
        else:
            usdt_balance.available = derived_cash
        applied_counts["balances_upserted"] += 1

        await session.flush()

    report_after = await reconcile_report(session, account_id)
    status = "fixed" if report_after.get("ok") else "ALERT"
    session.add(
        AdminAction(
            action="RECONCILE_APPLY",
            status=status,
            message="Reconcile fix applied",
            meta=to_jsonable({
                "account_id": account_id,
                "before_summary": report_before.get("summary"),
                "after_summary": report_after.get("summary"),
                "diffs_sample_before": diffs[:20],
                "diffs_sample_after": report_after.get("diffs", [])[:20],
                "applied": applied_counts,
            }),
        )
    )
    if started_tx:
        await session.commit()

    return {
        "mode": "apply",
        "before": report_before,
        "after": report_after,
        "applied": True,
        "actions": ["fixed"],
    }


async def reconcile_apply(
    session: AsyncSession,
    account_id: int,
    apply_positions: bool = True,
    apply_balances: bool = False,
) -> Dict[str, Any]:
    db_ident_row = (
        await session.execute(
            text("select current_database() as db, inet_server_addr() as host, inet_server_port() as port")
        )
    ).mappings().first()
    if db_ident_row:
        db_ident = {
            "db": db_ident_row.get("db"),
            "host": str(db_ident_row.get("host")) if db_ident_row.get("host") is not None else None,
            "port": db_ident_row.get("port"),
        }
    else:
        db_ident = None
    warnings: List[str] = []
    before = await reconcile_report(session, account_id)
    before_summary = before.get("summary") or {}
    diff_before = before_summary.get("diff_count", 0)
    baseline_source = before.get("derived", {}).get("baseline_source")
    if baseline_source != "account":
        warnings.append("BALANCE_BASELINE_UNKNOWN")

    derived_positions = before.get("derived", {}).get("positions_by_symbol", {}) or {}
    db_positions = before.get("db", {}).get("positions_by_symbol", {}) or {}
    derived_balances = before.get("derived", {}).get("balances_by_asset", {}) or {}
    db_balances = before.get("db", {}).get("balances_by_asset", {}) or {}
    before_keys = {
        "diff_before": diff_before,
        "apply_flags": {"positions": apply_positions, "balances": apply_balances},
        "derived_pos_keys_n": len(derived_positions),
        "db_pos_keys_n": len(db_positions),
        "derived_bal_keys_n": len(derived_balances),
        "db_bal_keys_n": len(db_balances),
        "derived_pos_keys_sample": list(derived_positions.keys())[:10],
        "db_pos_keys_sample": list(db_positions.keys())[:10],
    }
    pos_cnt = await session.execute(
        text("select count(*)::int from paper_positions where account_id=:aid"),
        {"aid": account_id},
    )
    bal_cnt = await session.execute(
        text("select count(*)::int from paper_balances where account_id=:aid"),
        {"aid": account_id},
    )
    trd_cnt = await session.execute(
        text("select count(*)::int from paper_trades where account_id=:aid"),
        {"aid": account_id},
    )
    before_db_counts = {
        "paper_positions": int(pos_cnt.scalar_one()),
        "paper_balances": int(bal_cnt.scalar_one()),
        "paper_trades": int(trd_cnt.scalar_one()),
    }

    debug_counts: Dict[str, Any] = {"positions_after": None, "balances_after": None}
    applied_counts = {"positions": 0, "balances": 0}
    attempted_counts = {"positions": 0, "balances": 0}
    debug_payload = {
        "db_ident": db_ident,
        "counts_after": debug_counts,
        "attempted": attempted_counts,
        "rowcount": applied_counts,
        "before_keys": before_keys,
        "before_db_counts": before_db_counts,
    }

    if diff_before == 0:
        session.add(
            AdminAction(
                action="RECONCILE_APPLY",
                status=normalize_status("ok"),
                message="No diffs to apply",
                meta=to_jsonable({
                    "account_id": account_id,
                    "summary_before": before_summary,
                    "warnings": warnings,
                    "apply_positions": apply_positions,
                    "apply_balances": apply_balances,
                    "diff_count": diff_before,
                    "diffs_sample": before.get("diffs", [])[:10],
                    "debug": debug_payload,
                }),
            )
        )
        if not session.in_transaction():
            await session.commit()
        return {
            "mode": "apply",
            "applied": False,
            "before": before,
            "after": before,
            "warnings": warnings,
            "debug": debug_payload,
        }

    async def _apply_updates() -> None:
        if apply_positions:
            await _ensure_unique_index(
                session,
                table_name="paper_positions",
                index_name="ux_paper_positions_account_symbol",
                columns=["account_id", "symbol"],
                alt_names=["uq_paper_positions_account_symbol"],
            )
            derived_positions = before.get("derived", {}).get("positions_by_symbol", {})
            db_positions = before.get("db", {}).get("positions_by_symbol", {})
            symbols = set(derived_positions.keys()) | set(db_positions.keys())
            now = utc_now()
            values = []
            for sym in symbols:
                target = derived_positions.get(sym, {"qty": "0", "avg_entry_price": "0"})
                qty = Decimal(str(target.get("qty", "0")))
                avg = Decimal(str(target.get("avg_entry_price", "0")))
                side = "short" if qty < 0 else "long"
                values.append(
                    {
                        "account_id": account_id,
                        "symbol": sym,
                        "side": side,
                        "qty": qty,
                        "avg_entry_price": avg,
                        "updated_at": now,
                    }
                )
            attempted_counts["positions"] = len(values)
            if values:
                stmt = pg_insert(PaperPosition).values(values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["account_id", "symbol"],
                    set_={
                        "qty": stmt.excluded.qty,
                        "avg_entry_price": stmt.excluded.avg_entry_price,
                        "side": stmt.excluded.side,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                result = await session.execute(stmt)
                applied_counts["positions"] = int(result.rowcount or 0)
            pos_cnt = await session.execute(
                text("select count(*)::int from paper_positions where account_id=:aid"),
                {"aid": account_id},
            )
            debug_counts["positions_after"] = int(pos_cnt.scalar_one())

        if apply_balances:
            derived_balances_raw = before.get("derived", {}).get("balances_by_asset", {}) or {}
            derived_balances = {normalize_asset(k): Decimal(str(v)) for k, v in derived_balances_raw.items()}
            db_balances = {normalize_asset(k): Decimal(str(v)) for k, v in (before.get("db", {}).get("balances_by_asset", {}) or {}).items()}
            assets = set(derived_balances.keys()) | set(db_balances.keys())
            now = utc_now()
            values = []
            for asset in assets:
                amount = Decimal(str(derived_balances.get(asset, "0")))
                values.append(
                    {
                        "account_id": account_id,
                        "ccy": asset,
                        "available": amount,
                        "updated_at": now,
                    }
                )
            attempted_counts["balances"] = len(values)
            if values:
                stmt = pg_insert(PaperBalance).values(values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["account_id", "ccy"],
                    set_={
                        "available": stmt.excluded.available,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                result = await session.execute(stmt)
                applied_counts["balances"] = int(result.rowcount or 0)
            bal_cnt = await session.execute(
                text("select count(*)::int from paper_balances where account_id=:aid"),
                {"aid": account_id},
            )
            debug_counts["balances_after"] = int(bal_cnt.scalar_one())

        await session.flush()

    started_tx = not session.in_transaction()
    if started_tx:
        async with session.begin():
            await _apply_updates()
    else:
        await _apply_updates()

    after = await reconcile_report(session, account_id)
    after_summary = after.get("summary") or {}
    diff_after = after_summary.get("diff_count", 0)
    changed = (diff_after < diff_before) or diff_after == 0
    status = normalize_status("fixed" if (apply_positions or apply_balances) and changed and diff_after == 0 else ("alert" if diff_after > 0 else "noop"))
    session.add(
        AdminAction(
            action="RECONCILE_APPLY",
            status=status,
            message="Reconcile apply",
            meta=to_jsonable({
                "account_id": account_id,
                "summary_before": before_summary,
                "summary_after": after_summary,
                "diffs_sample_before": before.get("diffs", [])[:20],
                "diffs_sample_after": after.get("diffs", [])[:20],
                "warnings": warnings,
                "applied": {"positions": apply_positions, "balances": apply_balances},
                "rowcount": applied_counts,
                "attempted": attempted_counts,
                "debug": debug_payload,
                "diff_count_before": diff_before,
                "diff_count_after": diff_after,
            }),
        )
    )
    await session.commit()

    return {
        "mode": "apply",
        "applied": changed and (apply_positions or apply_balances),
        "before": before,
        "after": after,
        "warnings": warnings,
        "debug": debug_payload,
    }
