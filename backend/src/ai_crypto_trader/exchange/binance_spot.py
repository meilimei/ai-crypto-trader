from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import re
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any
from urllib.parse import urlencode

import httpx

from ai_crypto_trader.exchange.base import (
    ExchangeError,
    ExchangeOrderRequest,
    ExchangePlacementResult,
    RetriableExchangeError,
    SevereExchangeError,
)


def _to_decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _retry_after_seconds(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value.strip())
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def _used_weight_headers(headers: httpx.Headers) -> dict[str, str]:
    values: dict[str, str] = {}
    for key, value in headers.items():
        if key.upper().startswith("X-MBX-USED-WEIGHT"):
            values[key] = value
    return values


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _extract_filter_type(message: str) -> str | None:
    match = re.search(r"Filter failure:\s*([A-Z_]+)", message or "")
    if match:
        return match.group(1).strip().upper()
    return None


def _is_step_aligned(value: Decimal, step: Decimal) -> bool:
    if step <= 0:
        return True
    units = value / step
    return units == units.to_integral_value()


def _round_up_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return units * step


def quantize_qty_up(
    *,
    raw_suggested_qty: Decimal,
    min_qty: Decimal | None,
    step_size: Decimal | None,
) -> Decimal:
    qty = raw_suggested_qty.copy_abs()
    min_value = (min_qty or Decimal("0")).copy_abs()
    step_value = (step_size or Decimal("0")).copy_abs()
    if min_value > 0 and qty < min_value:
        qty = min_value
    if step_value > 0:
        qty = _round_up_to_step(qty, step_value)
    return qty


def validate_notional_bounds(
    *,
    qty: Decimal,
    reference_price: Decimal,
    min_notional: Decimal | None,
    max_notional: Decimal | None,
    step_size: Decimal | None = None,
    min_qty: Decimal | None = None,
) -> dict[str, Any] | None:
    if reference_price <= 0:
        return None
    notional = qty.copy_abs() * reference_price
    min_value = (min_notional or Decimal("0")).copy_abs()
    max_value = (max_notional or Decimal("0")).copy_abs()
    step = (step_size or Decimal("0")).copy_abs()
    if min_value > 0 and notional < min_value:
        raw_min_qty = min_value / reference_price
        suggested = quantize_qty_up(
            raw_suggested_qty=raw_min_qty,
            min_qty=min_qty,
            step_size=step_size,
        )
        return {
            "violated": "min",
            "notional": notional,
            "min_notional": min_value,
            "max_notional": max_value if max_value > 0 else None,
            "suggested_min_qty_raw": raw_min_qty,
            "suggested_min_qty": suggested,
        }
    if max_value > 0 and notional > max_value:
        return {
            "violated": "max",
            "notional": notional,
            "min_notional": min_value if min_value > 0 else None,
            "max_notional": max_value,
            "suggested_min_qty": None,
        }
    return None


def _parse_symbol_filters(raw_filters: dict[str, dict[str, Any]]) -> dict[str, Any]:
    notional_raw = raw_filters.get("NOTIONAL") if isinstance(raw_filters, dict) else None
    min_notional_raw = raw_filters.get("MIN_NOTIONAL") if isinstance(raw_filters, dict) else None
    lot_size_raw = raw_filters.get("LOT_SIZE") if isinstance(raw_filters, dict) else None
    market_lot_size_raw = raw_filters.get("MARKET_LOT_SIZE") if isinstance(raw_filters, dict) else None
    return {
        "notional": (
            {
                "min_notional": _to_decimal(notional_raw.get("minNotional")) if isinstance(notional_raw, dict) else None,
                "max_notional": _to_decimal(notional_raw.get("maxNotional")) if isinstance(notional_raw, dict) else None,
                "apply_min_to_market": _to_bool(notional_raw.get("applyMinToMarket", True)) if isinstance(notional_raw, dict) else True,
                "apply_max_to_market": _to_bool(notional_raw.get("applyMaxToMarket", True)) if isinstance(notional_raw, dict) else True,
                "avg_price_mins": int(notional_raw.get("avgPriceMins") or 0) if isinstance(notional_raw, dict) else 0,
            }
            if isinstance(notional_raw, dict)
            else None
        ),
        "min_notional": (
            {
                "min_notional": _to_decimal(min_notional_raw.get("minNotional")) if isinstance(min_notional_raw, dict) else None,
                "apply_to_market": _to_bool(min_notional_raw.get("applyToMarket", True)) if isinstance(min_notional_raw, dict) else True,
                "avg_price_mins": int(min_notional_raw.get("avgPriceMins") or 0) if isinstance(min_notional_raw, dict) else 0,
            }
            if isinstance(min_notional_raw, dict)
            else None
        ),
        "lot_size": (
            {
                "min_qty": _to_decimal(lot_size_raw.get("minQty")) if isinstance(lot_size_raw, dict) else None,
                "max_qty": _to_decimal(lot_size_raw.get("maxQty")) if isinstance(lot_size_raw, dict) else None,
                "step_size": _to_decimal(lot_size_raw.get("stepSize")) if isinstance(lot_size_raw, dict) else None,
            }
            if isinstance(lot_size_raw, dict)
            else None
        ),
        "market_lot_size": (
            {
                "min_qty": _to_decimal(market_lot_size_raw.get("minQty")) if isinstance(market_lot_size_raw, dict) else None,
                "max_qty": _to_decimal(market_lot_size_raw.get("maxQty")) if isinstance(market_lot_size_raw, dict) else None,
                "step_size": _to_decimal(market_lot_size_raw.get("stepSize")) if isinstance(market_lot_size_raw, dict) else None,
            }
            if isinstance(market_lot_size_raw, dict)
            else None
        ),
        "raw": raw_filters,
    }


@dataclass
class BinanceSpotClient:
    api_key: str
    api_secret: str
    base_url: str
    timeout_seconds: float = 10.0
    recv_window_ms: int = 10000
    time_sync_interval_seconds: int = 300
    exchange_info_ttl_seconds: int = 300
    _time_offset_ms: int = 0
    _time_synced_at_monotonic: float = 0.0
    _time_sync_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _exchange_info_cache: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict, init=False, repr=False)
    _exchange_info_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    @classmethod
    def from_env(cls) -> "BinanceSpotClient":
        return cls(
            api_key=(os.getenv("BINANCE_API_KEY") or "").strip(),
            api_secret=(os.getenv("BINANCE_API_SECRET") or "").strip(),
            base_url=(os.getenv("BINANCE_SPOT_BASE_URL") or "https://testnet.binance.vision").strip(),
            timeout_seconds=float(os.getenv("LIVE_EXCHANGE_TIMEOUT_SECONDS", "10") or 10),
            recv_window_ms=int(os.getenv("BINANCE_RECV_WINDOW_MS", "10000") or 10000),
            time_sync_interval_seconds=int(os.getenv("BINANCE_TIME_SYNC_INTERVAL_SECONDS", "300") or 300),
            exchange_info_ttl_seconds=int(os.getenv("BINANCE_EXCHANGE_INFO_TTL_SECONDS", "300") or 300),
        )

    def _sign(self, payload: dict[str, Any]) -> str:
        query = urlencode(payload, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _build_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.timeout_seconds, connect=self.timeout_seconds, read=self.timeout_seconds)

    def _is_sync_stale(self) -> bool:
        now_monotonic = time.monotonic()
        return (now_monotonic - self._time_synced_at_monotonic) >= max(30, int(self.time_sync_interval_seconds))

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms or 0)

    async def _sync_server_time(self, *, force: bool = False) -> None:
        if not force and not self._is_sync_stale():
            return
        async with self._time_sync_lock:
            if not force and not self._is_sync_stale():
                return
            timeout = self._build_timeout()
            try:
                async with httpx.AsyncClient(base_url=self.base_url, timeout=timeout) as client:
                    response = await client.get("/api/v3/time")
            except Exception as exc:
                raise RetriableExchangeError(f"Binance time sync failed: {exc.__class__.__name__}: {exc}") from exc

            if not (200 <= response.status_code < 300):
                raise RetriableExchangeError(
                    f"Binance time sync failed: HTTP {response.status_code}",
                    status_code=response.status_code,
                    retry_after_seconds=_retry_after_seconds(response.headers.get("Retry-After")),
                    used_weight_headers=_used_weight_headers(response.headers),
                )

            payload: dict[str, Any] = {}
            try:
                maybe_payload = response.json()
                if isinstance(maybe_payload, dict):
                    payload = maybe_payload
            except Exception:
                payload = {}

            server_time_raw = payload.get("serverTime")
            try:
                server_time_ms = int(server_time_raw)
            except Exception as exc:
                raise RetriableExchangeError("Binance time sync failed: invalid serverTime payload") from exc

            local_time_ms = int(time.time() * 1000)
            self._time_offset_ms = int(server_time_ms - local_time_ms)
            self._time_synced_at_monotonic = time.monotonic()

    async def _send_order_request(self, params: dict[str, Any]) -> tuple[httpx.Response, dict[str, Any], dict[str, str], int | None]:
        timeout = self._build_timeout()
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=timeout) as client:
                response = await client.post("/api/v3/order", data=params, headers=headers)
        except Exception as exc:
            raise RetriableExchangeError(f"Binance request failed: {exc.__class__.__name__}: {exc}") from exc

        used_weight = _used_weight_headers(response.headers)
        retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
        payload: dict[str, Any] = {}
        try:
            maybe_payload = response.json()
            if isinstance(maybe_payload, dict):
                payload = maybe_payload
        except Exception:
            payload = {}
        return response, payload, used_weight, retry_after

    async def _send_public_get(
        self, path: str, *, params: dict[str, Any] | None = None
    ) -> tuple[httpx.Response, dict[str, Any], dict[str, str], int | None]:
        timeout = self._build_timeout()
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=timeout) as client:
                response = await client.get(path, params=params)
        except Exception as exc:
            raise RetriableExchangeError(f"Binance request failed: {exc.__class__.__name__}: {exc}") from exc

        used_weight = _used_weight_headers(response.headers)
        retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
        payload: dict[str, Any] = {}
        try:
            maybe_payload = response.json()
            if isinstance(maybe_payload, dict):
                payload = maybe_payload
        except Exception:
            payload = {}
        return response, payload, used_weight, retry_after

    async def _get_symbol_filters(self, symbol: str) -> dict[str, Any]:
        symbol_key = symbol.strip().upper()
        now_mono = time.monotonic()
        cached = self._exchange_info_cache.get(symbol_key)
        if cached is not None:
            cached_at, filters = cached
            if (now_mono - cached_at) < max(30, int(self.exchange_info_ttl_seconds)):
                return filters

        async with self._exchange_info_lock:
            cached = self._exchange_info_cache.get(symbol_key)
            if cached is not None:
                cached_at, filters = cached
                if (time.monotonic() - cached_at) < max(30, int(self.exchange_info_ttl_seconds)):
                    return filters

            response, payload, used_weight, retry_after = await self._send_public_get(
                "/api/v3/exchangeInfo",
                params={"symbol": symbol_key},
            )
            if not (200 <= response.status_code < 300):
                if response.status_code in {408, 418, 429} or response.status_code >= 500:
                    raise RetriableExchangeError(
                        f"Binance exchangeInfo failed: HTTP {response.status_code}",
                        status_code=response.status_code,
                        retry_after_seconds=retry_after,
                        used_weight_headers=used_weight,
                    )
                raise ExchangeError(
                    f"Binance exchangeInfo failed: HTTP {response.status_code}",
                    retriable=False,
                    status_code=response.status_code,
                    retry_after_seconds=retry_after,
                    used_weight_headers=used_weight,
                )

            symbols_payload = payload.get("symbols")
            if not isinstance(symbols_payload, list) or not symbols_payload:
                raise RetriableExchangeError(
                    f"Binance exchangeInfo missing symbol filters: {symbol_key}",
                    details={"kind": "EXCHANGE_INFO_UNAVAILABLE", "symbol": symbol_key},
                )
            symbol_entry = symbols_payload[0] if isinstance(symbols_payload[0], dict) else {}
            raw_filters = symbol_entry.get("filters") if isinstance(symbol_entry, dict) else None
            mapped_filters: dict[str, dict[str, Any]] = {}
            if isinstance(raw_filters, list):
                for item in raw_filters:
                    if not isinstance(item, dict):
                        continue
                    filter_type = str(item.get("filterType") or "").strip().upper()
                    if filter_type:
                        mapped_filters[filter_type] = item
            parsed_filters = _parse_symbol_filters(mapped_filters)
            has_notional = isinstance(parsed_filters.get("notional"), dict) or isinstance(parsed_filters.get("min_notional"), dict)
            has_lot = isinstance(parsed_filters.get("lot_size"), dict) or isinstance(parsed_filters.get("market_lot_size"), dict)
            if not has_notional and not has_lot:
                raise RetriableExchangeError(
                    f"Binance exchangeInfo missing required filters for {symbol_key}",
                    details={
                        "kind": "EXCHANGE_INFO_UNAVAILABLE",
                        "symbol": symbol_key,
                    },
                )
            self._exchange_info_cache[symbol_key] = (time.monotonic(), parsed_filters)
            return parsed_filters

    async def _fetch_symbol_price(self, symbol: str) -> Decimal | None:
        response, payload, _, retry_after = await self._send_public_get(
            "/api/v3/ticker/price",
            params={"symbol": symbol.strip().upper()},
        )
        if not (200 <= response.status_code < 300):
            if response.status_code in {408, 418, 429} or response.status_code >= 500:
                raise RetriableExchangeError(
                    f"Binance ticker price failed: HTTP {response.status_code}",
                    status_code=response.status_code,
                    retry_after_seconds=retry_after,
                )
            return None
        return _to_decimal(payload.get("price"))

    async def _resolve_reference_price(self, request: ExchangeOrderRequest) -> Decimal | None:
        if request.price is not None and request.price > 0:
            return request.price
        if request.market_price is not None and request.market_price > 0:
            return request.market_price
        latest_price = await self._fetch_symbol_price(request.symbol)
        if latest_price is not None and latest_price > 0:
            return latest_price
        return None

    def _raise_filter_failure(self, filter_type: str, details: dict[str, Any]) -> None:
        raise ExchangeError(
            f"Filter failure: {filter_type}",
            retriable=False,
            error_code=-4000,
            details={
                "kind": "FILTER_FAILURE",
                "filter_type": filter_type,
                "retriable": False,
                "source": "local_precheck",
                **details,
            },
        )

    async def _preflight_validate_order(self, request: ExchangeOrderRequest) -> None:
        symbol = request.symbol.strip().upper()
        order_type = request.order_type.strip().upper()
        qty = request.qty.copy_abs()

        filters = await self._get_symbol_filters(symbol)
        lot_size_filter = filters.get("lot_size") if isinstance(filters.get("lot_size"), dict) else {}
        market_lot_size_filter = (
            filters.get("market_lot_size") if isinstance(filters.get("market_lot_size"), dict) else {}
        )
        lot_filter = None
        if order_type == "MARKET":
            lot_filter = market_lot_size_filter or lot_size_filter
        else:
            lot_filter = lot_size_filter

        if isinstance(lot_filter, dict):
            min_qty = _to_decimal(lot_filter.get("min_qty")) or _to_decimal(lot_size_filter.get("min_qty")) or Decimal("0")
            max_qty = _to_decimal(lot_filter.get("max_qty")) or _to_decimal(lot_size_filter.get("max_qty")) or Decimal("0")
            step_size = _to_decimal(lot_filter.get("step_size")) or _to_decimal(lot_size_filter.get("step_size")) or Decimal("0")
            if min_qty > 0 and qty < min_qty:
                suggested_rounded = quantize_qty_up(
                    raw_suggested_qty=min_qty,
                    min_qty=min_qty,
                    step_size=step_size if step_size > 0 else None,
                )
                self._raise_filter_failure(
                    "LOT_SIZE",
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "min_qty": str(min_qty),
                        "max_qty": str(max_qty) if max_qty > 0 else None,
                        "step_size": str(step_size) if step_size > 0 else None,
                        "suggested_min_qty_raw": str(min_qty),
                        "suggested_min_qty": str(suggested_rounded),
                    },
                )
            if max_qty > 0 and qty > max_qty:
                self._raise_filter_failure(
                    "LOT_SIZE",
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "min_qty": str(min_qty) if min_qty > 0 else None,
                        "max_qty": str(max_qty),
                        "step_size": str(step_size) if step_size > 0 else None,
                    },
                )
            if step_size > 0 and not _is_step_aligned(qty, step_size):
                suggested_rounded = quantize_qty_up(
                    raw_suggested_qty=qty,
                    min_qty=min_qty if min_qty > 0 else None,
                    step_size=step_size,
                )
                self._raise_filter_failure(
                    "LOT_SIZE",
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "min_qty": str(min_qty) if min_qty > 0 else None,
                        "step_size": str(step_size),
                        "suggested_min_qty_raw": str(qty),
                        "suggested_min_qty": str(suggested_rounded),
                    },
                )

        reference_price = await self._resolve_reference_price(request)
        if reference_price is None or reference_price <= 0:
            raise RetriableExchangeError(
                "Binance precheck failed: reference price unavailable",
                details={
                    "kind": "EXCHANGE_INFO_UNAVAILABLE",
                    "symbol": symbol,
                },
            )
        estimated_notional = qty * reference_price
        notional_filter = filters.get("notional")
        if isinstance(notional_filter, dict):
            min_notional = _to_decimal(notional_filter.get("min_notional")) or Decimal("0")
            max_notional = _to_decimal(notional_filter.get("max_notional")) or Decimal("0")
            # Binance MARKET orders are effectively subject to NOTIONAL constraints;
            # enforce regardless of optional flags to avoid exchange-side -1013 rejects.
            apply_min = True
            apply_max = True
            step_size = _to_decimal((lot_filter or {}).get("step_size")) if isinstance(lot_filter, dict) else None
            if (step_size is None or step_size <= 0) and isinstance(lot_size_filter, dict):
                step_size = _to_decimal(lot_size_filter.get("step_size"))
            min_qty = _to_decimal((lot_filter or {}).get("min_qty")) if isinstance(lot_filter, dict) else None
            if (min_qty is None or min_qty <= 0) and isinstance(lot_size_filter, dict):
                min_qty = _to_decimal(lot_size_filter.get("min_qty"))
            violation = validate_notional_bounds(
                qty=qty,
                reference_price=reference_price,
                min_notional=min_notional if apply_min else None,
                max_notional=max_notional if apply_max else None,
                step_size=step_size,
                min_qty=min_qty,
            )
            if violation is not None:
                self._raise_filter_failure(
                    "NOTIONAL",
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "reference_price": str(reference_price),
                        "estimated_notional": str(estimated_notional),
                        "min_notional": str(violation.get("min_notional")) if violation.get("min_notional") is not None else None,
                        "max_notional": str(violation.get("max_notional")) if violation.get("max_notional") is not None else None,
                        "min_qty": str(min_qty) if min_qty is not None and min_qty > 0 else None,
                        "step_size": str(step_size) if step_size is not None and step_size > 0 else None,
                        "suggested_min_qty_raw": (
                            str(violation.get("suggested_min_qty_raw"))
                            if violation.get("suggested_min_qty_raw") is not None
                            else None
                        ),
                        "suggested_min_qty": (
                            str(violation.get("suggested_min_qty"))
                            if violation.get("suggested_min_qty") is not None
                            else None
                        ),
                    },
                )

        min_notional_filter = filters.get("min_notional")
        if isinstance(min_notional_filter, dict):
            min_notional = _to_decimal(min_notional_filter.get("min_notional")) or Decimal("0")
            apply_to_market = True if order_type != "MARKET" else _to_bool(min_notional_filter.get("apply_to_market", True))
            step_size = _to_decimal((lot_filter or {}).get("step_size")) if isinstance(lot_filter, dict) else None
            if (step_size is None or step_size <= 0) and isinstance(lot_size_filter, dict):
                step_size = _to_decimal(lot_size_filter.get("step_size"))
            min_qty = _to_decimal((lot_filter or {}).get("min_qty")) if isinstance(lot_filter, dict) else None
            if (min_qty is None or min_qty <= 0) and isinstance(lot_size_filter, dict):
                min_qty = _to_decimal(lot_size_filter.get("min_qty"))
            violation = (
                validate_notional_bounds(
                    qty=qty,
                    reference_price=reference_price,
                    min_notional=min_notional if apply_to_market else None,
                    max_notional=None,
                    step_size=step_size,
                    min_qty=min_qty,
                )
                if apply_to_market and min_notional > 0
                else None
            )
            if violation is not None:
                self._raise_filter_failure(
                    "MIN_NOTIONAL",
                    {
                        "symbol": symbol,
                        "qty": str(qty),
                        "reference_price": str(reference_price),
                        "estimated_notional": str(estimated_notional),
                        "min_notional": str(violation.get("min_notional")) if violation.get("min_notional") is not None else None,
                        "min_qty": str(min_qty) if min_qty is not None and min_qty > 0 else None,
                        "step_size": str(step_size) if step_size is not None and step_size > 0 else None,
                        "suggested_min_qty_raw": (
                            str(violation.get("suggested_min_qty_raw"))
                            if violation.get("suggested_min_qty_raw") is not None
                            else None
                        ),
                        "suggested_min_qty": (
                            str(violation.get("suggested_min_qty"))
                            if violation.get("suggested_min_qty") is not None
                            else None
                        ),
                    },
                )

    def _raise_exchange_error(
        self,
        *,
        response: httpx.Response,
        payload: dict[str, Any],
        retry_after_seconds: int | None,
        used_weight_headers: dict[str, str],
        context_details: dict[str, Any] | None = None,
    ) -> None:
        error_code = payload.get("code")
        try:
            error_code_int = int(error_code)
        except Exception:
            error_code_int = None
        message = str(payload.get("msg") or payload.get("message") or response.text or f"HTTP {response.status_code}")
        message_lower = message.lower()
        filter_type = _extract_filter_type(message)

        if error_code_int in {-2014, -2015, -1022, -1002}:
            raise SevereExchangeError(
                f"Binance order failed: {message}",
                status_code=response.status_code,
                retry_after_seconds=retry_after_seconds,
                error_code=error_code_int,
                used_weight_headers=used_weight_headers,
            )
        if error_code_int == -1013 and filter_type is None:
            filter_type = "UNKNOWN"
        if filter_type:
            details: dict[str, Any] = {
                "kind": "FILTER_FAILURE",
                "filter_type": filter_type,
                "retriable": False,
                "source": "exchange_rejection",
            }
            if isinstance(context_details, dict):
                details.update(context_details)
            raise ExchangeError(
                f"Binance order failed: {message}",
                retriable=False,
                status_code=response.status_code,
                retry_after_seconds=retry_after_seconds,
                error_code=error_code_int,
                used_weight_headers=used_weight_headers,
                details=details,
            )
        if error_code_int == -1021 or "outside of the recvwindow" in message_lower or "invalid timestamp" in message_lower:
            raise RetriableExchangeError(
                f"Binance order failed: {message}",
                status_code=response.status_code,
                retry_after_seconds=retry_after_seconds,
                error_code=error_code_int,
                used_weight_headers=used_weight_headers,
            )
        if response.status_code in {408, 418, 429} or response.status_code >= 500 or error_code_int in {-1003, -1015}:
            raise RetriableExchangeError(
                f"Binance order failed: {message}",
                status_code=response.status_code,
                retry_after_seconds=retry_after_seconds,
                error_code=error_code_int,
                used_weight_headers=used_weight_headers,
            )

        raise ExchangeError(
            f"Binance order failed: {message}",
            retriable=False,
            status_code=response.status_code,
            retry_after_seconds=retry_after_seconds,
            error_code=error_code_int,
            used_weight_headers=used_weight_headers,
        )

    async def place_order(self, request: ExchangeOrderRequest) -> ExchangePlacementResult:
        if not self.api_key or not self.api_secret:
            raise ExchangeError(
                "Missing BINANCE_API_KEY/BINANCE_API_SECRET",
                retriable=False,
            )

        await self._preflight_validate_order(request)
        await self._sync_server_time(force=False)

        side = request.side.strip().upper()
        order_type = request.order_type.strip().upper()
        params: dict[str, Any] = {
            "symbol": request.symbol,
            "side": side,
            "type": order_type,
            "quantity": _to_decimal_text(request.qty.copy_abs()),
            "newClientOrderId": request.client_order_id,
            "recvWindow": self.recv_window_ms,
            "timestamp": self._timestamp_ms(),
        }
        if request.price is not None and order_type != "MARKET":
            params["price"] = _to_decimal_text(request.price)
            params["timeInForce"] = "GTC"

        params["signature"] = self._sign(params)
        response, payload, used_weight, retry_after = await self._send_order_request(params)
        if 200 <= response.status_code < 300:
            status = str(payload.get("status") or "accepted").lower()
            exchange_order_id = payload.get("orderId")
            return ExchangePlacementResult(
                status=status,
                exchange_order_id=str(exchange_order_id) if exchange_order_id is not None else None,
                response_payload=payload,
                used_weight_headers=used_weight,
            )

        error_code = payload.get("code")
        try:
            error_code_int = int(error_code)
        except Exception:
            error_code_int = None
        message = str(payload.get("msg") or payload.get("message") or response.text or "")
        message_lower = message.lower()
        if error_code_int == -1021 or "outside of the recvwindow" in message_lower or "invalid timestamp" in message_lower:
            await self._sync_server_time(force=True)
            params["timestamp"] = self._timestamp_ms()
            params["signature"] = self._sign(params)
            response, payload, used_weight, retry_after = await self._send_order_request(params)
            if 200 <= response.status_code < 300:
                status = str(payload.get("status") or "accepted").lower()
                exchange_order_id = payload.get("orderId")
                return ExchangePlacementResult(
                    status=status,
                    exchange_order_id=str(exchange_order_id) if exchange_order_id is not None else None,
                    response_payload=payload,
                    used_weight_headers=used_weight,
                )

        filter_context: dict[str, Any] = {
            "symbol": request.symbol,
            "qty": str(request.qty.copy_abs()),
        }
        reference_price = request.price if request.price is not None and request.price > 0 else request.market_price
        if reference_price is not None and reference_price > 0:
            estimated_notional = request.qty.copy_abs() * reference_price
            filter_context["reference_price"] = str(reference_price)
            filter_context["estimated_notional"] = str(estimated_notional)
            try:
                filter_snapshot = await self._get_symbol_filters(request.symbol)
                notional = filter_snapshot.get("notional") if isinstance(filter_snapshot, dict) else None
                min_notional = filter_snapshot.get("min_notional") if isinstance(filter_snapshot, dict) else None
                if isinstance(notional, dict):
                    filter_context["min_notional"] = (
                        str(notional.get("min_notional")) if notional.get("min_notional") is not None else None
                    )
                    filter_context["max_notional"] = (
                        str(notional.get("max_notional")) if notional.get("max_notional") is not None else None
                    )
                elif isinstance(min_notional, dict):
                    filter_context["min_notional"] = (
                        str(min_notional.get("min_notional")) if min_notional.get("min_notional") is not None else None
                    )
            except Exception:
                pass

        self._raise_exchange_error(
            response=response,
            payload=payload,
            retry_after_seconds=retry_after,
            used_weight_headers=used_weight,
            context_details=filter_context,
        )

    async def get_market_price(self, symbol: str) -> Decimal | None:
        return await self._fetch_symbol_price(symbol)

    async def precheck_order(self, request: ExchangeOrderRequest) -> None:
        await self._preflight_validate_order(request)
