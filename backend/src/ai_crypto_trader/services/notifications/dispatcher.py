from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, NotificationOutbox, utc_now

logger = logging.getLogger(__name__)


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _resolve_channel(row: NotificationOutbox) -> str:
    return (row.channel or "").strip().lower()


async def _fetch_due_outbox(
    session: AsyncSession,
    *,
    now_utc: datetime,
    limit: int,
) -> list[NotificationOutbox]:
    stmt = (
        select(NotificationOutbox)
        .where(
            NotificationOutbox.status == "pending",
            NotificationOutbox.next_attempt_at <= now_utc,
        )
        .order_by(NotificationOutbox.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def _count_pending(session: AsyncSession) -> int:
    result = await session.execute(select(func.count()).select_from(NotificationOutbox).where(NotificationOutbox.status == "pending"))
    return int(result.scalar() or 0)


async def dispatch_outbox_once(
    session: AsyncSession,
    *,
    now_utc: datetime | None = None,
    limit: int = 50,
) -> dict[str, int]:
    now = now_utc or utc_now()
    webhook_url = (os.getenv("NOTIFICATIONS_WEBHOOK_URL") or "").strip()
    webhook_timeout = _env_float("NOTIFICATIONS_WEBHOOK_TIMEOUT_SECONDS", 5.0)
    retry_seconds = 60
    sent = 0
    failed = 0
    due_total_result = await session.execute(
        select(func.count())
        .select_from(NotificationOutbox)
        .where(
            NotificationOutbox.status == "pending",
            NotificationOutbox.next_attempt_at <= now,
        )
    )
    due_count = int(due_total_result.scalar() or 0)
    rows = await _fetch_due_outbox(session, now_utc=now, limit=limit)
    processed = 0

    client: httpx.AsyncClient | None = None
    if webhook_url:
        try:
            timeout = httpx.Timeout(webhook_timeout, connect=webhook_timeout, read=webhook_timeout)
            client = httpx.AsyncClient(timeout=timeout)
        except Exception:
            logger.exception("outbox webhook client init failed")
            webhook_url = ""
    try:
        for row in rows:
            processed += 1
            row.attempt_count = (row.attempt_count or 0) + 1
            row.updated_at = now
            try:
                resolved_channel = _resolve_channel(row)
                channel = resolved_channel.lower()
                payload_message = None
                if isinstance(row.payload, dict):
                    payload_message = row.payload.get("message")
                    if payload_message is None:
                        meta_message = row.payload.get("meta")
                        if isinstance(meta_message, dict):
                            payload_message = meta_message.get("message")
                if channel in {"noop", "log"}:
                    if channel == "log":
                        logger.info(
                            "outbox dispatch log",
                            extra={
                                "outbox_id": row.id,
                                "admin_action_id": row.admin_action_id,
                                "channel": resolved_channel,
                                "dedupe_key": row.dedupe_key,
                                "message": payload_message,
                            },
                        )
                    else:
                        logger.info(
                            "outbox dispatch noop",
                            extra={
                                "outbox_id": row.id,
                                "admin_action_id": row.admin_action_id,
                                "channel": resolved_channel,
                                "dedupe_key": row.dedupe_key,
                            },
                        )
                    row.status = "sent"
                    row.last_error = None
                    row.next_attempt_at = now
                    if channel == "log":
                        session.add(
                            AdminAction(
                                action="NOTIFICATION_SENT",
                                status="ok",
                                message="Notification dispatched to log channel",
                                dedupe_key=f"NOTIFICATION_SENT:{row.id}",
                                meta={
                                    "outbox_id": row.id,
                                    "admin_action_id": row.admin_action_id,
                                    "channel": resolved_channel,
                                    "dedupe_key": row.dedupe_key,
                                    "message": payload_message,
                                },
                            )
                        )
                    sent += 1
                elif channel == "webhook":
                    if not webhook_url:
                        row.status = "pending"
                        row.last_error = "Missing NOTIFICATIONS_WEBHOOK_URL"
                        row.next_attempt_at = now + timedelta(seconds=retry_seconds)
                        logger.info(
                            "outbox dispatch webhook missing url",
                            extra={
                                "outbox_id": row.id,
                                "admin_action_id": row.admin_action_id,
                                "dedupe_key": row.dedupe_key,
                                "next_attempt_at": row.next_attempt_at.isoformat(),
                            },
                        )
                    else:
                        payload_data = row.payload if isinstance(row.payload, dict) else {}
                        webhook_body = {
                            "outbox_id": row.id,
                            "admin_action_id": row.admin_action_id,
                            "dedupe_key": row.dedupe_key,
                            "channel": resolved_channel,
                            "status": row.status,
                            "payload": payload_data,
                        }
                        try:
                            response = await client.post(webhook_url, json=webhook_body) if client else None
                            error = None
                        except Exception as exc:
                            response = None
                            error = str(exc)
                        if response is not None and 200 <= response.status_code < 300:
                            row.status = "sent"
                            row.last_error = None
                            row.next_attempt_at = now
                            sent += 1
                            logger.info(
                                "outbox dispatch webhook ok",
                                extra={
                                    "outbox_id": row.id,
                                    "admin_action_id": row.admin_action_id,
                                    "dedupe_key": row.dedupe_key,
                                    "url": webhook_url,
                                    "message": payload_message,
                                    "status_code": response.status_code,
                                },
                            )
                        else:
                            if error is None and response is not None:
                                error = f"HTTP {response.status_code}"
                            row.last_error = error or "Webhook dispatch failed"
                            row.status = "pending"
                            row.next_attempt_at = now + timedelta(seconds=retry_seconds)
                            logger.info(
                                "outbox dispatch webhook failed",
                                extra={
                                    "outbox_id": row.id,
                                    "admin_action_id": row.admin_action_id,
                                    "dedupe_key": row.dedupe_key,
                                    "url": webhook_url,
                                    "message": payload_message,
                                    "error": row.last_error,
                                    "attempt_count": row.attempt_count,
                                    "next_attempt_at": row.next_attempt_at.isoformat(),
                                },
                            )
                else:
                    row.status = "failed"
                    row.last_error = f"Unsupported channel: {resolved_channel}"
                    row.next_attempt_at = now
                    failed += 1
            except Exception as exc:
                row.last_error = str(exc)
                row.status = "pending"
                row.next_attempt_at = now + timedelta(seconds=retry_seconds)
    finally:
        if client is not None:
            await client.aclose()

    await session.commit()
    pending_remaining = await _count_pending(session)
    return {
        "picked": processed,
        "sent": sent,
        "failed": failed,
        "pending_due": due_count,
        "pending_remaining": pending_remaining,
    }


async def dispatch_outbox(
    session: AsyncSession,
    *,
    now_utc: datetime | None = None,
    limit: int = 50,
) -> int:
    stats = await dispatch_outbox_once(session, now_utc=now_utc, limit=limit)
    return stats["sent"]
