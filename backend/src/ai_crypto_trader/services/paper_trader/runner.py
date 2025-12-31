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
from ai_crypto_trader.services.paper_trader.maintenance import reconcile_report, reconcile_apply, normalize_status, json_safe
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
        self.last_reconcile_at: Optional[datetime] = None
        self.reconcile_tick_count: int = 0
        self.reconcile_interval_seconds: int = 0
        self.last_reconcile_error: Optional[str] = None
        self._last_reconcile_summary: Optional[Dict[str, object]] = None
        self._last_reconcile_action_at: Optional[float] = None
        self._last_reconcile_action_status: Optional[str] = None

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
        self.last_reconcile_at = None
        self.reconcile_tick_count = 0
        self.last_reconcile_error = None
        self._last_reconcile_summary = None
        self._last_reconcile_action_at = None
        self._last_reconcile_action_status = None

    def status(self) -> Dict[str, object]:
        engine_cycle_at = self._engine.last_cycle_at.isoformat() if self._engine and self._engine.last_cycle_at else None
        engine_error = self._engine.last_error if self._engine else None
        reconcile_state = "none"
        reconcile_exc = None
        running = False
        if self._reconcile_task:
            if self._reconcile_task.cancelled():
                reconcile_state = "cancelled"
            elif self._reconcile_task.done():
                reconcile_state = "done"
                try:
                    reconcile_exc = self._reconcile_task.exception()
                except Exception:
                    reconcile_exc = None
            else:
                reconcile_state = "pending"
                running = True
        return {
            "running": self.is_running,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_cycle_at": engine_cycle_at,
            "last_error": engine_error or self.last_error,
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
        while self.is_running and self.reconcile_interval_seconds > 0:
            try:
                async with AsyncSessionLocal() as session:
                    account = await session.scalar(select(PaperAccount).where(PaperAccount.name == account_name))
                    if not account:
                        await asyncio.sleep(self.reconcile_interval_seconds)
                        continue
                    self.last_reconcile_at = datetime.now(timezone.utc)
                    report = await reconcile_report(session, account.id)
                    summary = report.get("summary") or {}
                    diff_count = summary.get("diff_count", 0) or 0
                    diffs_sample = report.get("diffs", [])[:5]
                    status = normalize_status("ok" if diff_count == 0 else "alert")
                    warn = None
                    if summary.get("has_negative_balance") or summary.get("has_equity_mismatch"):
                        warn = "unsafe_to_autofix"
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
                    if emit and (emit_ok or diff_count > 0):
                        session.add(
                            AdminAction(
                                action="RECONCILE_REPORT",
                                status=status,
                                message="Auto reconcile report",
                                meta=json_safe({
                                    "account_id": account.id,
                                    "summary": summary,
                                    "diff_count": diff_count,
                                    "diffs_sample": diffs_sample,
                                    "warning": warn,
                                }),
                            )
                        )
                        await session.commit()
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
                                meta=json_safe({
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
                    session.add(
                        AdminAction(
                            action="RECONCILE_REPORT",
                            status=normalize_status("error"),
                            message="Auto reconcile error",
                            meta=json_safe({"account": account_name, "error": str(exc)}),
                        )
                    )
                    await session.commit()
                logger.exception("Auto reconcile loop error")
            await asyncio.sleep(self.reconcile_interval_seconds)


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
