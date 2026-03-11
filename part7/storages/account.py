from __future__ import annotations

from clients.postgres import get_pg_connection
from repositories.account import (
    Account,
    create_account,
    get_account_by_id,
    find_account_by_credentials,
    block_account,
    delete_account,
)


class AccountStorage:
    """хранилище аккаунтов поверх postgre sql."""

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


account_storage = AccountStorage()
