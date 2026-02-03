import asyncio
import logging
import os
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import (
    AdminAction,
    EquitySnapshot,
    PaperAccount,
    PaperBalance,
    PaperOrder,
    PaperPosition,
    PaperTrade,
    utc_now,
)
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.services.paper_trader.engine import PaperTradingEngine
from ai_crypto_trader.common.maintenance import ensure_and_sync_paper_id_sequences
from ai_crypto_trader.services.paper_trader.maintenance import reconcile_report, reconcile_apply, normalize_status
from ai_crypto_trader.services.paper_trader.equity import (
    compute_equity,
    log_equity_snapshot_failed,
    write_equity_snapshot,
)
from ai_crypto_trader.services.paper_trader.equity_risk import check_equity_risk
from ai_crypto_trader.services.paper_trader.policies_effective import get_effective_risk_policy
from ai_crypto_trader.services.admin_actions.throttled import write_admin_action_throttled
from ai_crypto_trader.services.monitoring.strategy_stalls import maybe_alert_strategy_stalls
from ai_crypto_trader.services.notifications.dispatcher import dispatch_outbox_once
from ai_crypto_trader.services.paper_trader.reconcile_policy import (
    apply_reconcile_policy_status,
    get_reconcile_policy,
    policy_snapshot,
)
from ai_crypto_trader.services.admin_actions.reconcile_log import log_reconcile_report_throttled
from ai_crypto_trader.common.jsonable import to_jsonable
from ai_crypto_trader.common.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


class PaperTraderRunner:
    _instance: Optional["PaperTraderRunner"] = None

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._engine: Optional[PaperTradingEngine] = None
        self.started_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self._reconcile_task: Optional[asyncio.Task[None]] = None
        self._stall_monitor_task: Optional[asyncio.Task[None]] = None
        self._outbox_task: Optional[asyncio.Task[None]] = None
        self.last_reconcile_at: Optional[datetime] = None
        self.reconcile_tick_count: int = 0
        self.reconcile_interval_seconds: int = 0
        self.last_reconcile_error: Optional[str] = None
        self._last_reconcile_summary: Optional[Dict[str, object]] = None
        self._last_reconcile_action_at: Optional[float] = None
        self._last_reconcile_action_status: Optional[str] = None
        self._equity_risk_state: Dict[int, Dict[str, object]] = {}

    @classmethod
    def instance(cls) -> "PaperTraderRunner":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            raise RuntimeError("Paper trader already running")

        self._stop_event = asyncio.Event()
        config = PaperTraderConfig.from_env()
        self._engine = PaperTradingEngine(config, stop_event=self._stop_event)
        self.started_at = datetime.now(timezone.utc)
        self.last_error = None

        await ensure_and_sync_paper_id_sequences()

        async def _run() -> None:
            try:
                await self._engine.run()
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("Paper trader runner encountered an error")
            finally:
                if self._stop_event:
                    self._stop_event.set()

        auto_reconcile = os.getenv("PAPER_TRADER_AUTO_RECONCILE", "false").lower() in {"1", "true", "yes", "on"}
        self.reconcile_interval_seconds = int(
            os.getenv("PAPER_TRADER_RECONCILE_INTERVAL_SECONDS", os.getenv("PAPER_TRADER_RECONCILE_INTERVAL", "60"))
        )
        if self.reconcile_interval_seconds > 0:
            self._reconcile_task = asyncio.create_task(self._reconcile_loop(auto_apply=auto_reconcile), name="paper_reconcile")
            self._reconcile_task.add_done_callback(self._reconcile_done)
        self._stall_monitor_task = asyncio.create_task(self._stall_monitor_loop(), name="paper_stall_monitor")
        self._outbox_task = asyncio.create_task(self._outbox_loop(), name="notifications_outbox")
        self._task = asyncio.create_task(_run(), name="paper-trader")

    async def stop(self) -> None:
        if not self.is_running:
            return

        if self._stop_event:
            self._stop_event.set()
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        if self._reconcile_task:
            self._reconcile_task.cancel()
            try:
                await self._reconcile_task
            except asyncio.CancelledError:
                pass
        self._reconcile_task = None
        if self._outbox_task:
            self._outbox_task.cancel()
            try:
                await self._outbox_task
            except asyncio.CancelledError:
                pass
        self._outbox_task = None
        if self._stall_monitor_task:
            self._stall_monitor_task.cancel()
            try:
                await self._stall_monitor_task
            except asyncio.CancelledError:
                pass
        self._stall_monitor_task = None
        self.last_reconcile_at = None
        self.reconcile_tick_count = 0
        self.last_reconcile_error = None
        self._last_reconcile_summary = None
        self._last_reconcile_action_at = None
        self._last_reconcile_action_status = None

    def status(self) -> Dict[str, object]:
        engine_cycle_at = self._engine.last_cycle_at.isoformat() if self._engine and self._engine.last_cycle_at else None
        engine_error = self._engine.last_error if self._engine else None
        engine_task_state = "none"
        engine_task_exc = None
        reconcile_state = "none"
        reconcile_exc = None
        running = False
        if self._task:
            if self._task.cancelled():
                engine_task_state = "cancelled"
            elif self._task.done():
                engine_task_state = "done"
                engine_task_exc = self._task.exception()
            else:
                engine_task_state = "pending"
        if self._reconcile_task:
            if self._reconcile_task.cancelled():
                reconcile_state = "cancelled"
            elif self._reconcile_task.done():
                reconcile_state = "done"
                reconcile_exc = self._reconcile_task.exception()
            else:
                reconcile_state = "pending"
                running = True
                reconcile_exc = None
        return {
            "running": self.is_running,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_cycle_at": engine_cycle_at,
            "last_error": engine_error or self.last_error,
            "engine_task_state": engine_task_state,
            "engine_task_exception": str(engine_task_exc) if engine_task_exc else None,
            "reconcile_task_running": running,
            "reconcile_task_state": reconcile_state,
            "reconcile_task_exception": str(reconcile_exc) if reconcile_exc else None,
            "reconcile_tick_count": self.reconcile_tick_count,
            "last_reconcile_at": self.last_reconcile_at.isoformat() if self.last_reconcile_at else None,
            "reconcile_interval_seconds": self.reconcile_interval_seconds,
            "last_reconcile_error": self.last_reconcile_error,
            "last_reconcile_action_at": datetime.fromtimestamp(self._last_reconcile_action_at, tz=timezone.utc).isoformat()
            if self._last_reconcile_action_at
            else None,
            "last_reconcile_action_status": self._last_reconcile_action_status,
            "equity_risk_state": {
                str(account_id): {
                    "risk_last_checked_at": state.get("risk_last_checked_at").isoformat()
                    if state.get("risk_last_checked_at")
                    else None,
                    "equity_risk_blocked": state.get("equity_risk_blocked", False),
                    "blocked_reason": state.get("blocked_reason"),
                    "last_details": state.get("last_details"),
                }
                for account_id, state in self._equity_risk_state.items()
            },
        }

    def _reconcile_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            self.last_reconcile_error = repr(exc)

    async def _reconcile_loop(self, auto_apply: bool) -> None:
        config = PaperTraderConfig.from_env()
        account_name = f"paper-{config.exchange_name}"
        emit_ok = os.getenv("PAPER_TRADER_RECONCILE_EMIT_OK", "true").lower() in {"1", "true", "yes", "on"}
        usdt_tolerance = Decimal(os.getenv("PAPER_RECONCILE_USDT_TOLERANCE", "0") or "0")
        suppress_accounts = {
            int(x)
            for x in os.getenv("PAPER_RECONCILE_SUPPRESS_ACCOUNTS", "")
            .replace(" ", "")
            .split(",")
            if x.strip().isdigit()
        }
        while self.is_running and self.reconcile_interval_seconds > 0:
            try:
                async with AsyncSessionLocal() as session:
                    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
                    if not account:
                        await asyncio.sleep(self.reconcile_interval_seconds)
                        continue
                    self.last_reconcile_at = datetime.now(timezone.utc)
                    suppressed = account.id in suppress_accounts
                    policy = await get_reconcile_policy(session, account.id)
                    report = await reconcile_report(session, account.id, policy=policy)
                    summary = report.get("summary") or {}
                    diff_count = summary.get("diff_count", 0) or 0
                    usdt_diff = Decimal(str(summary.get("usdt_diff", "0") or "0"))
                    diffs_sample = report.get("diffs", [])[:5]
                    status = normalize_status("ok" if diff_count == 0 else "alert")
                    warn = None
                    if summary.get("has_negative_balance") or summary.get("has_equity_mismatch"):
                        warn = "unsafe_to_autofix"
                    within_tolerance = usdt_tolerance > 0 and usdt_diff.copy_abs() <= usdt_tolerance
                    if within_tolerance:
                        status = "ok"
                    status = apply_reconcile_policy_status(
                        summary=summary,
                        base_status=status,
                        policy=policy,
                    )
                    emit = False
                    now_mono = asyncio.get_running_loop().time()
                    if self._last_reconcile_action_at is None:
                        emit = True
                    else:
                        last_summary = self._last_reconcile_summary or {}
                        if (
                            last_summary.get("diff_count") != summary.get("diff_count")
                            or last_summary.get("has_negative_balance") != summary.get("has_negative_balance")
                            or last_summary.get("has_equity_mismatch") != summary.get("has_equity_mismatch")
                        ):
                            emit = True
                        else:
                            if diff_count > 0 and now_mono - self._last_reconcile_action_at >= 60:
                                emit = True
                            if diff_count == 0 and now_mono - self._last_reconcile_action_at >= 300:
                                emit = True
                    try:
                        accounts = await session.scalars(select(PaperAccount.id))
                        for account_id in accounts.all():
                            try:
                                data = await compute_equity(session, account_id, price_source="runner")
                                wrote = await write_equity_snapshot(
                                    session,
                                    account_id,
                                    data,
                                    source="runner",
                                    min_interval_seconds=60,
                                )
                                if wrote:
                                    await session.commit()
                                now_utc = datetime.now(timezone.utc)
                                risk_policy = await get_effective_risk_policy(session, account_id, None)
                                risk_result = await check_equity_risk(
                                    session,
                                    account_id=account_id,
                                    strategy_id=None,
                                    policy=risk_policy,
                                    now_utc=now_utc,
                                    return_details=True,
                                )
                                blocked_reason = None
                                details = None
                                if risk_result:
                                    blocked_reason = risk_result.get("code")
                                    details = risk_result.get("details")
                                blocked = bool(blocked_reason)
                                state_key = f"{blocked}:{blocked_reason or 'ok'}"
                                prev = self._equity_risk_state.get(account_id) or {}
                                prev_key = prev.get("state_key")
                                enabled = (
                                    getattr(risk_policy, "max_drawdown_usdt", None) is not None
                                    or getattr(risk_policy, "max_daily_loss_usdt", None) is not None
                                )
                                should_log = enabled or blocked
                                if state_key != prev_key and should_log:
                                    status = "blocked" if blocked else "ok"
                                    await write_admin_action_throttled(
                                        None,
                                        action_type="EQUITY_RISK_STATE",
                                        account_id=account_id,
                                        symbol=None,
                                        status=status,
                                        payload={
                                            "account_id": str(account_id),
                                            "blocked": blocked,
                                            "reason": blocked_reason or "ok",
                                            "details": details,
                                            "policy": {
                                                "max_daily_loss_usdt": str(risk_policy.max_daily_loss_usdt)
                                                if getattr(risk_policy, "max_daily_loss_usdt", None) is not None
                                                else None,
                                                "max_drawdown_usdt": str(risk_policy.max_drawdown_usdt)
                                                if getattr(risk_policy, "max_drawdown_usdt", None) is not None
                                                else None,
                                                "equity_lookback_hours": risk_policy.equity_lookback_hours,
                                            },
                                        },
                                        window_seconds=120,
                                        dedupe_key=f"EQUITY_RISK_STATE:{account_id}:{blocked_reason or 'ok'}",
                                    )
                                self._equity_risk_state[account_id] = {
                                    "risk_last_checked_at": now_utc,
                                    "equity_risk_blocked": blocked,
                                    "blocked_reason": blocked_reason,
                                    "last_details": details,
                                    "state_key": state_key,
                                }
                            except Exception as exc:
                                self.last_reconcile_error = str(exc)
                                await session.rollback()
                                await log_equity_snapshot_failed(account_id, exc)
                    except Exception as exc:
                        self.last_reconcile_error = str(exc)
                        logger.exception("Equity snapshot write failed")
                    if suppressed:
                        await asyncio.sleep(self.reconcile_interval_seconds)
                        continue
                    if emit and (emit_ok or diff_count > 0):
                        window_seconds = 600
                        if policy and policy.log_window_seconds:
                            window_seconds = int(policy.log_window_seconds)
                        await log_reconcile_report_throttled(
                            status=status,
                            account_id=account.id,
                            message="Auto reconcile report",
                            report_meta=to_jsonable({
                                "account_id": account.id,
                                "summary": summary,
                                "diff_count": diff_count,
                                "diffs_sample": diffs_sample,
                                "warning": warn,
                                "policy": policy_snapshot(policy),
                                "baseline_source": (report.get("derived") or {}).get("baseline_source"),
                            }),
                            window_seconds=window_seconds,
                        )
                        self._last_reconcile_summary = summary
                        self._last_reconcile_action_at = now_mono
                        self._last_reconcile_action_status = status

                    if diff_count > 0 and auto_apply:
                        if summary.get("has_negative_balance") or summary.get("has_equity_mismatch"):
                            continue
                        after = await reconcile_apply(session, account.id, apply_positions=True, apply_balances=True)
                        after_summary = after.get("after", {}).get("summary", {})
                        status_apply = normalize_status("fixed" if after_summary.get("diff_count", 0) == 0 else "alert")
                        session.add(
                            AdminAction(
                                action="RECONCILE_APPLY",
                                status=status_apply,
                                message="Auto reconcile apply",
                                meta=to_jsonable({
                                    "account_id": account.id,
                                    "summary_before": summary,
                                    "summary_after": after_summary,
                                    "diffs_sample_before": diffs_sample,
                                    "diffs_sample_after": after.get("after", {}).get("diffs", [])[:10],
                                }),
                            )
                        )
                        await session.commit()
                    self.reconcile_tick_count += 1
                    logger.info(
                        "reconcile tick",
                        extra={
                            "account_id": account.id,
                            "diff_count": diff_count,
                            "tick_count": self.reconcile_tick_count,
                        },
                    )
            except Exception as exc:
                self.last_reconcile_error = str(exc)
                async with AsyncSessionLocal() as session:
                    await log_reconcile_report_throttled(
                        account_id=0,
                        status=normalize_status("error"),
                        message="Auto reconcile error",
                        report_meta=to_jsonable({"account": account_name, "error": str(exc)}),
                    )
                logger.exception("Auto reconcile loop error")
            try:
                now_utc = datetime.now(timezone.utc)
                async with AsyncSessionLocal() as session:
                    session.info["reconcile_tick_count"] = self.reconcile_tick_count
                    await maybe_alert_strategy_stalls(session, now_utc=now_utc)
                logger.info("strategy_stalls wired tick executed")
            except Exception:
                logger.exception("Strategy stalls wired tick failed")
            await asyncio.sleep(self.reconcile_interval_seconds)

    async def _stall_monitor_loop(self) -> None:
        last_cycle_at: Optional[datetime] = None
        while self.is_running:
            try:
                engine = self._engine
                cycle_at = engine.last_cycle_at if engine else None
                if cycle_at and (last_cycle_at is None or cycle_at > last_cycle_at):
                    last_cycle_at = cycle_at
                    async with AsyncSessionLocal() as session:
                        await maybe_alert_strategy_stalls(session, now_utc=cycle_at)
            except Exception:
                logger.exception("Strategy stall monitor failed")
            await asyncio.sleep(1)

    async def _outbox_loop(self) -> None:
        poll_seconds = int(os.getenv("NOTIFICATIONS_OUTBOX_POLL_SECONDS", "10"))
        while self.is_running:
            started_at = datetime.now(timezone.utc)
            now_utc = started_at
            tick_bucket = now_utc.strftime("%Y%m%d%H%M")
            tick_dedupe_key = f"NOTIFICATIONS_OUTBOX_TICK:{tick_bucket}"
            async with AsyncSessionLocal() as session:
                try:
                    stats = await dispatch_outbox_once(session, now_utc=now_utc, limit=50)
                    duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
                    existing_tick = await session.scalar(
                        select(AdminAction)
                        .where(AdminAction.dedupe_key == tick_dedupe_key)
                        .limit(1)
                    )
                    def _stat(stats_obj, key, default=0):  # type: ignore[no-untyped-def]
                        if hasattr(stats_obj, key):
                            return getattr(stats_obj, key, default)
                        if isinstance(stats_obj, dict):
                            return stats_obj.get(key, default)
                        return default

                    def _stat_any(stats_obj, keys, default=0):  # type: ignore[no-untyped-def]
                        for key in keys:
                            value = _stat(stats_obj, key, None)
                            if value is not None:
                                return value
                        return default

                    meta = {
                        "now_utc": now_utc.isoformat(),
                        "picked_count": _stat_any(stats, ("picked_count", "picked")),
                        "sent_count": _stat_any(stats, ("sent_count", "sent")),
                        "failed_count": _stat_any(stats, ("failed_count", "failed")),
                        "pending_due_count": _stat_any(stats, ("pending_due_count", "pending_due")),
                        "pending_remaining": _stat_any(stats, ("pending_remaining",)),
                        "retried_count": _stat_any(stats, ("retried_count",), 0),
                        "duration_ms": duration_ms,
                        "limit": 50,
                    }
                    if existing_tick:
                        existing_meta = existing_tick.meta if isinstance(existing_tick.meta, dict) else {}
                        meta["picked_count"] += int(existing_meta.get("picked_count", 0) or 0)
                        meta["sent_count"] += int(existing_meta.get("sent_count", 0) or 0)
                        meta["failed_count"] += int(existing_meta.get("failed_count", 0) or 0)
                        meta["retried_count"] += int(existing_meta.get("retried_count", 0) or 0)
                        meta["pending_due_count"] = max(
                            meta["pending_due_count"],
                            int(existing_meta.get("pending_due_count", 0) or 0),
                        )
                        meta["pending_remaining"] = max(
                            meta["pending_remaining"],
                            int(existing_meta.get("pending_remaining", 0) or 0),
                        )
                        existing_tick.status = "ok"
                        existing_tick.message = "Outbox dispatcher tick"
                        existing_tick.meta = meta
                    else:
                        session.add(
                            AdminAction(
                                action="NOTIFICATIONS_OUTBOX_TICK",
                                status="ok",
                                message="Outbox dispatcher tick",
                                dedupe_key=tick_dedupe_key,
                                meta=meta,
                            )
                        )
                    await session.commit()
                except Exception as exc:
                    await session.rollback()
                    logger.exception("Notifications outbox dispatcher failed")
                    try:
                        duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
                        existing_tick = await session.scalar(
                            select(AdminAction)
                            .where(AdminAction.dedupe_key == tick_dedupe_key)
                            .limit(1)
                        )
                        error_meta = {
                            "now_utc": now_utc.isoformat(),
                            "error": str(exc),
                            "picked_count": 0,
                            "sent_count": 0,
                            "failed_count": 0,
                            "retried_count": 0,
                            "pending_due_count": 0,
                            "pending_remaining": 0,
                            "duration_ms": duration_ms,
                            "limit": 50,
                        }
                        if existing_tick:
                            existing_meta = existing_tick.meta if isinstance(existing_tick.meta, dict) else {}
                            for key in ("picked_count", "sent_count", "failed_count", "retried_count", "pending_due_count"):
                                error_meta[key] = int(existing_meta.get(key, 0) or 0)
                            error_meta["pending_remaining"] = int(existing_meta.get("pending_remaining", 0) or 0)
                            existing_tick.status = "error"
                            existing_tick.message = "Outbox dispatcher failed"
                            existing_tick.meta = error_meta
                        else:
                            session.add(
                                AdminAction(
                                    action="NOTIFICATIONS_OUTBOX_TICK",
                                    status="error",
                                    message="Outbox dispatcher failed",
                                    dedupe_key=tick_dedupe_key,
                                    meta=error_meta,
                                )
                            )
                        await session.commit()
                    except Exception:
                        logger.exception("Notifications outbox tick heartbeat write failed")
            await asyncio.sleep(max(poll_seconds, 1))


async def reset_paper_data(session: AsyncSession) -> Dict[str, object]:
    """
    Clear paper trading state for the configured paper account and re-seed the base balance.
    """
    config = PaperTraderConfig.from_env()
    account_name = f"paper-{config.exchange_name}"
    starting_balance_raw = os.getenv("PAPER_START_BALANCE")
    starting_balance = Decimal(starting_balance_raw) if starting_balance_raw else config.initial_balance

    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
    if account is None:
        account = PaperAccount(name=account_name, base_ccy=config.base_ccy, initial_cash_usd=starting_balance)
        session.add(account)
        await session.flush()
    else:
        if account.initial_cash_usd is None:
            account.initial_cash_usd = starting_balance
        starting_balance = account.initial_cash_usd

    account_id = account.id

    for model in (PaperOrder, PaperTrade, PaperPosition, PaperBalance, EquitySnapshot):
        await session.execute(delete(model).where(model.account_id == account_id))

    seed_balance = PaperBalance(
        account_id=account_id,
        ccy=config.base_ccy,
        available=starting_balance,
        updated_at=utc_now(),
    )
    session.add(seed_balance)

    await session.commit()

    logger.info(
        "Reset paper trading data",
        extra={"account": account_name, "base_ccy": config.base_ccy, "starting_balance": str(starting_balance)},
    )

    return {"ok": True, "account_name": account_name, "starting_balance": float(starting_balance)}
