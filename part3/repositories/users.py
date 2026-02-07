from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
import asyncpg


@dataclass(frozen=True, slots=True)
class User:
    id: int
    is_verified: bool
    created_at: datetime


def _row_to_user(row: asyncpg.Record) -> User:
    return User(
        id=int(row["id"]),
        is_verified=bool(row["is_verified"]),
        created_at=row["created_at"],
    )


async def create_user(conn: asyncpg.Connection, *, is_verified: bool = False) -> User:
    row = await conn.fetchrow(
        """
        INSERT INTO public.users (is_verified)
        VALUES ($1)
        RETURNING id, is_verified, created_at
        """,
        is_verified,
    )
    assert row is not None
    return _row_to_user(row)


async def get_user_by_id(conn: asyncpg.Connection, user_id: int) -> User | None:
    row = await conn.fetchrow(
        """
        SELECT id, is_verified, created_at
        FROM public.users
        WHERE id = $1
        """,
        int(user_id),
    )
    return _row_to_user(row) if row else None


async def list_users(
    conn: asyncpg.Connection, *, limit: int = 100, offset: int = 0
) -> list[User]:
    rows: Iterable[asyncpg.Record] = await conn.fetch(
        """
        SELECT id, is_verified, created_at
        FROM public.users
        ORDER BY id
        LIMIT $1 OFFSET $2
        """,
        int(limit),
        int(offset),
    )
    return [_row_to_user(r) for r in rows]


async def set_user_verified(
    conn: asyncpg.Connection, *, user_id: int, is_verified: bool
) -> User | None:
    row = await conn.fetchrow(
        """
        UPDATE public.users
        SET is_verified = $2
        WHERE id = $1
        RETURNING id, is_verified, created_at
        """,
        int(user_id),
        bool(is_verified),
    )
    return _row_to_user(row) if row else None


async def delete_user(conn: asyncpg.Connection, user_id: int) -> bool:
    # ON DELETE CASCADE удалит и объявления пользователя
    result = await conn.execute(
        """
        DELETE FROM public.users
        WHERE id = $1
        """,
        int(user_id),
    )
    # "DELETE <n>"
    return result.split()[-1] != "0"

