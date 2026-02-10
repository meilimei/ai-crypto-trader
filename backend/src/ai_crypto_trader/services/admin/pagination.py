from __future__ import annotations

from typing import TypeVar

from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")


def clamp_limit_offset(
    *,
    limit: int | None,
    offset: int | None,
    default_limit: int = 50,
    max_limit: int = 200,
) -> tuple[int, int]:
    limit_value = default_limit if limit is None else int(limit)
    offset_value = 0 if offset is None else int(offset)
    if limit_value < 1:
        limit_value = 1
    if limit_value > max_limit:
        limit_value = max_limit
    if offset_value < 0:
        offset_value = 0
    return limit_value, offset_value


async def fetch_page(
    session: AsyncSession,
    *,
    count_stmt: Select,
    data_stmt: Select,
    limit: int,
    offset: int,
) -> tuple[int, list[T]]:
    total = int((await session.execute(count_stmt)).scalar_one() or 0)
    rows = await session.execute(data_stmt.limit(limit).offset(offset))
    items = list(rows.scalars().all())
    return total, items

