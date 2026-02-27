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
    is_closed: bool
    created_at: datetime


def _row_to_ad(row: asyncpg.Record) -> Ad:
    return Ad(
        id=int(row["id"]),
        seller_id=int(row["seller_id"]),
        name=str(row["name"]),
        description=str(row["description"]),
        category=int(row["category"]),
        images_qty=int(row["images_qty"]),
        is_closed=bool(row["is_closed"]),
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
        RETURNING id, seller_id, name, description, category, images_qty, is_closed, created_at
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
        SELECT id, seller_id, name, description, category, images_qty, is_closed, created_at
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
            SELECT id, seller_id, name, description, category, images_qty, is_closed, created_at
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
            SELECT id, seller_id, name, description, category, images_qty, is_closed, created_at
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


@dataclass(frozen=True, slots=True)
class AdWithSeller:
    ad_id: int
    seller_id: int
    name: str
    description: str
    category: int
    images_qty: int
    is_verified_seller: bool


async def get_ad_with_seller(
    conn: asyncpg.Connection, ad_id: int
) -> AdWithSeller | None:
    """Получить объявление вместе с данными продавца одним запросом."""
    row = await conn.fetchrow(
        """
        SELECT
            a.id            AS ad_id,
            a.seller_id,
            a.name,
            a.description,
            a.category,
            a.images_qty,
            u.is_verified   AS is_verified_seller
        FROM public.ads a
        INNER JOIN public.users u ON a.seller_id = u.id
        WHERE a.id = $1
        """,
        int(ad_id),
    )
    if row is None:
        return None
    return AdWithSeller(
        ad_id=int(row["ad_id"]),
        seller_id=int(row["seller_id"]),
        name=str(row["name"]),
        description=str(row["description"]),
        category=int(row["category"]),
        images_qty=int(row["images_qty"]),
        is_verified_seller=bool(row["is_verified_seller"]),
    )


async def delete_ad(conn: asyncpg.Connection, ad_id: int) -> bool:
    result = await conn.execute(
        """
        DELETE FROM public.ads
        WHERE id = $1
        """,
        int(ad_id),
    )
    return result.split()[-1] != "0"


async def close_ad(conn: asyncpg.Connection, ad_id: int) -> Ad | None:
    """закрытие объявления (is_closed = TRUE)"""
    row = await conn.fetchrow(
        """
        UPDATE public.ads
        SET is_closed = TRUE
        WHERE id = $1 AND is_closed = FALSE
        RETURNING id, seller_id, name, description, category, images_qty, is_closed, created_at
        """,
        int(ad_id),
    )
    return _row_to_ad(row) if row else None