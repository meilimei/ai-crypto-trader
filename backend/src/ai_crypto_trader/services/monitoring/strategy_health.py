from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Integer, String, bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.utils.json_safe import json_safe

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _bounded_limit(value: int) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if limit < 1:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


def _clean_str(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


STRATEGY_HEALTH_SQL = text(
    """
WITH base AS (
  SELECT DISTINCT
    (meta->>'account_id')::bigint AS account_id,
    NULLIF(meta->>'strategy_id','') AS strategy_id,
    upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) AS symbol
  FROM admin_actions
  WHERE meta ? 'account_id'
    AND NULLIF(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in'), '') IS NOT NULL
    AND (:account_id IS NULL OR (meta->>'account_id')::bigint = :account_id)
    AND (:strategy_id IS NULL OR NULLIF(meta->>'strategy_id','') = :strategy_id)
    AND (:symbol IS NULL OR upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) = :symbol)
),
last_trade AS (
  SELECT b.account_id, b.strategy_id, b.symbol, a.created_at AS last_trade_at
  FROM base b
  LEFT JOIN LATERAL (
    SELECT created_at
    FROM admin_actions
    WHERE action='SMOKE_TRADE' AND status='ok'
      AND (meta->>'account_id')::bigint = b.account_id
      AND NULLIF(meta->>'strategy_id','') IS NOT DISTINCT FROM b.strategy_id
      AND upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) = b.symbol
    ORDER BY created_at DESC
    LIMIT 1
  ) a ON true
),
last_order_event AS (
  SELECT b.account_id, b.strategy_id, b.symbol, a.created_at AS last_order_event_at
  FROM base b
  LEFT JOIN LATERAL (
    SELECT created_at
    FROM admin_actions
    WHERE action IN ('ORDER_REJECTED','ORDER_SKIPPED')
      AND (meta->>'account_id')::bigint = b.account_id
      AND NULLIF(meta->>'strategy_id','') IS NOT DISTINCT FROM b.strategy_id
      AND upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) = b.symbol
    ORDER BY created_at DESC
    LIMIT 1
  ) a ON true
),
last_alert AS (
  SELECT b.account_id, b.strategy_id, b.symbol,
         a.created_at AS last_alert_at,
         a.status AS last_alert_status,
         a.message AS last_alert_message,
         a.meta AS last_alert_meta
  FROM base b
  LEFT JOIN LATERAL (
    SELECT created_at, status, message, meta
    FROM admin_actions
    WHERE action='STRATEGY_ALERT'
      AND (meta->>'account_id')::bigint = b.account_id
      AND NULLIF(meta->>'strategy_id','') IS NOT DISTINCT FROM b.strategy_id
      AND upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) = b.symbol
    ORDER BY created_at DESC
    LIMIT 1
  ) a ON true
),
reject_counter AS (
  SELECT b.account_id, b.strategy_id, b.symbol,
         a.created_at AS reject_counter_at,
         a.meta AS reject_counter_meta
  FROM base b
  LEFT JOIN LATERAL (
    SELECT created_at, meta
    FROM admin_actions
    WHERE action='STRATEGY_REJECT_COUNTER'
      AND (meta->>'account_id')::bigint = b.account_id
      AND NULLIF(meta->>'strategy_id','') IS NOT DISTINCT FROM b.strategy_id
      AND upper(coalesce(meta->>'symbol', meta->>'symbol_normalized', meta->>'symbol_in')) = b.symbol
    ORDER BY created_at DESC
    LIMIT 1
  ) a ON true
),
equity_state AS (
  SELECT (meta->>'account_id')::bigint AS account_id, created_at, status, meta
  FROM admin_actions
  WHERE action='EQUITY_RISK_STATE' AND meta ? 'account_id'
),
last_equity_state AS (
  SELECT e1.*
  FROM equity_state e1
  JOIN (
    SELECT account_id, max(created_at) AS mx
    FROM equity_state
    GROUP BY account_id
  ) m ON m.account_id = e1.account_id AND m.mx = e1.created_at
)
SELECT
  b.account_id,
  b.strategy_id,
  b.symbol,
  lt.last_trade_at,
  lo.last_order_event_at,
  NULLIF(
    GREATEST(
      COALESCE(lt.last_trade_at, '-infinity'::timestamptz),
      COALESCE(lo.last_order_event_at, '-infinity'::timestamptz)
    ),
    '-infinity'::timestamptz
  ) AS last_activity_at,
  la.last_alert_at,
  la.last_alert_status,
  la.last_alert_message,
  la.last_alert_meta,
  rc.reject_counter_at,
  rc.reject_counter_meta,
  les.status AS equity_risk_status,
  les.meta AS equity_risk_meta
FROM base b
LEFT JOIN last_trade lt USING(account_id, strategy_id, symbol)
LEFT JOIN last_order_event lo USING(account_id, strategy_id, symbol)
LEFT JOIN last_alert la USING(account_id, strategy_id, symbol)
LEFT JOIN reject_counter rc USING(account_id, strategy_id, symbol)
LEFT JOIN last_equity_state les ON les.account_id = b.account_id
ORDER BY la.last_alert_at DESC NULLS LAST, lt.last_trade_at DESC NULLS LAST, b.account_id, b.symbol
LIMIT :limit
"""
).bindparams(
    bindparam("account_id", type_=BigInteger),
    bindparam("strategy_id", type_=String),
    bindparam("symbol", type_=String),
    bindparam("limit", type_=Integer),
)


async def get_strategy_health(
    session: AsyncSession,
    *,
    account_id: int | None = None,
    strategy_id: str | None = None,
    symbol: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    strategy_id_key = _clean_str(strategy_id)
    symbol_key = _clean_str(symbol)
    if symbol_key:
        symbol_key = symbol_key.upper()
    bounded_limit = _bounded_limit(limit)

    params = {
        "account_id": account_id,
        "strategy_id": strategy_id_key,
        "symbol": symbol_key,
        "limit": bounded_limit,
    }
    result = await session.execute(STRATEGY_HEALTH_SQL, params)
    rows = result.mappings().all()

    items: list[dict[str, Any]] = []
    for row in rows:
        account_val = row.get("account_id")
        try:
            account_id_int = int(account_val) if account_val is not None else None
        except (TypeError, ValueError):
            account_id_int = None

        last_alert_meta = row.get("last_alert_meta")
        reject_counter_meta = row.get("reject_counter_meta")
        equity_risk_meta = row.get("equity_risk_meta")

        reject_counter: dict[str, Any] | None = None
        if isinstance(reject_counter_meta, dict) and reject_counter_meta:
            reject_counter = {
                "count": reject_counter_meta.get("count"),
                "by_code": reject_counter_meta.get("by_code"),
                "first_seen_at": reject_counter_meta.get("first_seen_at"),
                "last_seen_at": reject_counter_meta.get("last_seen_at"),
                "window_seconds": reject_counter_meta.get("window_seconds"),
                "threshold": reject_counter_meta.get("threshold"),
                "last_reject_code": reject_counter_meta.get("last_reject_code"),
                "at": _iso(row.get("reject_counter_at")),
            }

        equity_risk_state: dict[str, Any] | None = None
        if row.get("equity_risk_status") is not None:
            equity_risk_state = {
                "status": row.get("equity_risk_status"),
                "meta": json_safe(equity_risk_meta) if equity_risk_meta is not None else {},
            }

        items.append(
            {
                "account_id": account_id_int,
                "strategy_id": row.get("strategy_id"),
                "symbol": row.get("symbol"),
                "last_trade_at": _iso(row.get("last_trade_at")),
                "last_order_event_at": _iso(row.get("last_order_event_at")),
                "last_activity_at": _iso(row.get("last_activity_at")),
                "last_alert_at": _iso(row.get("last_alert_at")),
                "last_alert_status": row.get("last_alert_status"),
                "last_alert_message": row.get("last_alert_message"),
                "last_alert_meta": json_safe(last_alert_meta) if last_alert_meta is not None else {},
                "reject_counter": reject_counter,
                "equity_risk_state": equity_risk_state,
            }
        )

    return items
