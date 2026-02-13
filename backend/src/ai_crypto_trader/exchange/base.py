from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol


@dataclass(frozen=True)
class ExchangeOrderRequest:
    symbol: str
    side: str
    order_type: str
    qty: Decimal
    price: Decimal | None
    market_price: Decimal | None
    client_order_id: str
    recv_window_ms: int | None = None


@dataclass(frozen=True)
class ExchangePlacementResult:
    status: str
    exchange_order_id: str | None
    response_payload: dict[str, Any]
    used_weight_headers: dict[str, str]


class ExchangeError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retriable: bool,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
        error_code: int | None = None,
        used_weight_headers: dict[str, str] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.retriable = retriable
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.error_code = error_code
        self.used_weight_headers = used_weight_headers or {}
        self.details = details or {}

    @property
    def is_auth_error(self) -> bool:
        return self.error_code in {-2014, -2015, -1022, -1002}


class RetriableExchangeError(ExchangeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
        error_code: int | None = None,
        used_weight_headers: dict[str, str] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            retriable=True,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
            error_code=error_code,
            used_weight_headers=used_weight_headers,
            details=details,
        )


class SevereExchangeError(ExchangeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
        error_code: int | None = None,
        used_weight_headers: dict[str, str] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            retriable=False,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
            error_code=error_code,
            used_weight_headers=used_weight_headers,
            details=details,
        )


class BaseExchangeClient(Protocol):
    async def place_order(self, request: ExchangeOrderRequest) -> ExchangePlacementResult:
        ...

    async def get_market_price(self, symbol: str) -> Decimal | None:
        ...

    async def precheck_order(self, request: ExchangeOrderRequest) -> None:
        ...
