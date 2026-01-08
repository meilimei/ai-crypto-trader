from __future__ import annotations

import ipaddress
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi.encoders import jsonable_encoder


def to_jsonable(obj: Any) -> Any:
    return jsonable_encoder(
        obj,
        custom_encoder={
            Decimal: lambda value: str(value),
            datetime: lambda value: value.isoformat(),
            date: lambda value: value.isoformat(),
            UUID: lambda value: str(value),
            ipaddress.IPv4Address: lambda value: str(value),
            ipaddress.IPv6Address: lambda value: str(value),
        },
    )
