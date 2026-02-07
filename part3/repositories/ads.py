from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import asyncpg


@dataclass(frozen=True, slots=True)
class Ad:
    id: int
    seller_id: int
    name: str
    description: str
    category: int
    images_qty: int
    created_at: datetime


def _row_to_ad(row: asyncpg.Record) -> Ad:
    return Ad(
        id=int(row["id"]),
        seller_id=int(row["seller_id"]),
        name=str(row["name"]),
        description=str(row["description"]),
        category=int(row["category"]),
        images_qty=int(row["images_qty"]),
        created_at=row["created_at"],
    )


async def create_ad(
    conn: asyncpg.Connection,
    *,
    seller_id: int,
    name: str,
    description: str,
    category: int,
    images_qty: int,
) -> Ad:
    row = await conn.fetchrow(
        """
        INSERT INTO public.ads (seller_id, name, description, category, images_qty)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, seller_id, name, description, category, images_qty, created_at
        """,
        int(seller_id),
        name,
        description,
        int(category),
        int(images_qty),
    )
    assert row is not None
    return _row_to_ad(row)


async def get_ad_by_id(conn: asyncpg.Connection, ad_id: int) -> Ad | None:
    row = await conn.fetchrow(
        """
        SELECT id, seller_id, name, description, category, images_qty, created_at
        FROM public.ads
        WHERE id = $1
        """,
        int(ad_id),
    )
    return _row_to_ad(row) if row else None


async def list_ads(
    conn: asyncpg.Connection,
    *,
    seller_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[Ad]:
    if seller_id is None:
        rows: Iterable[asyncpg.Record] = await conn.fetch(
            """
            SELECT id, seller_id, name, description, category, images_qty, created_at
            FROM public.ads
            ORDER BY id
            LIMIT $1 OFFSET $2
            """,
            int(limit),
            int(offset),
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, seller_id, name, description, category, images_qty, created_at
            FROM public.ads
            WHERE seller_id = $1
            ORDER BY id
            LIMIT $2 OFFSET $3
            """,
            int(seller_id),
            int(limit),
            int(offset),
        )
    return [_row_to_ad(r) for r in rows]


async def delete_ad(conn: asyncpg.Connection, ad_id: int) -> bool:
    result = await conn.execute(
        """
        DELETE FROM public.ads
        WHERE id = $1
        """,
        int(ad_id),
    )
    return result.split()[-1] != "0"