from __future__ import annotations

import hashlib
from dataclasses import dataclass

import asyncpg

from clients.postgres import get_pg_connection, track_db_query


def _hash_password(password: str) -> str:
    return hashlib.md5(password.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Account:
    id: int
    login: str
    password: str
    is_blocked: bool


def _row_to_account(row: asyncpg.Record) -> Account:
    return Account(
        id=int(row["id"]),
        login=row["login"],
        password=row["password"],
        is_blocked=bool(row["is_blocked"]),
    )


# --- raw SQL functions (принимают conn для интеграционных тестов) ---

async def create_account(
    conn: asyncpg.Connection, *, login: str, password: str,
) -> Account:
    with track_db_query("insert"):
        row = await conn.fetchrow(
            """
            INSERT INTO public.account (login, password)
            VALUES ($1, $2)
            RETURNING id, login, password, is_blocked
            """,
            login,
            _hash_password(password),
        )
    assert row is not None
    return _row_to_account(row)


async def get_account_by_id(
    conn: asyncpg.Connection, account_id: int,
) -> Account | None:
    with track_db_query("select"):
        row = await conn.fetchrow(
            """
            SELECT id, login, password, is_blocked
            FROM public.account
            WHERE id = $1
            """,
            int(account_id),
        )
    return _row_to_account(row) if row else None


async def find_account_by_credentials(
    conn: asyncpg.Connection, *, login: str, password: str,
) -> Account | None:
    with track_db_query("select"):
        row = await conn.fetchrow(
            """
            SELECT id, login, password, is_blocked
            FROM public.account
            WHERE login = $1 AND password = $2
            """,
            login,
            _hash_password(password),
        )
    return _row_to_account(row) if row else None


async def block_account(
    conn: asyncpg.Connection, account_id: int,
) -> Account | None:
    with track_db_query("update"):
        row = await conn.fetchrow(
            """
            UPDATE public.account
            SET is_blocked = TRUE
            WHERE id = $1 AND is_blocked = FALSE
            RETURNING id, login, password, is_blocked
            """,
            int(account_id),
        )
    return _row_to_account(row) if row else None


async def delete_account(
    conn: asyncpg.Connection, account_id: int,
) -> bool:
    with track_db_query("delete"):
        result = await conn.execute(
            """
            DELETE FROM public.account
            WHERE id = $1
            """,
            int(account_id),
        )
    return result.split()[-1] != "0"


# --- класс-обёртка, инкапсулирует get_pg_connection ---

class AccountPgStorage:
    """PG-хранилище аккаунтов. Сам управляет соединениями."""

    async def create(self, *, login: str, password: str) -> Account:
        async with get_pg_connection() as conn:
            return await create_account(conn, login=login, password=password)

    async def get_by_id(self, account_id: int) -> Account | None:
        async with get_pg_connection() as conn:
            return await get_account_by_id(conn, account_id)

    async def find_by_credentials(
        self, *, login: str, password: str,
    ) -> Account | None:
        async with get_pg_connection() as conn:
            return await find_account_by_credentials(
                conn, login=login, password=password,
            )

    async def block(self, account_id: int) -> Account | None:
        async with get_pg_connection() as conn:
            return await block_account(conn, account_id)

    async def delete(self, account_id: int) -> bool:
        async with get_pg_connection() as conn:
            return await delete_account(conn, account_id)
