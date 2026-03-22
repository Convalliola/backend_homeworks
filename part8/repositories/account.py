from __future__ import annotations

import logging

from clients.redis import redis_client
from storages.account import Account, AccountPgStorage

# re-export для обратной совместимости импортов
__all__ = ["Account", "AccountRepository", "account_repository"]

logger = logging.getLogger(__name__)

ACCOUNT_CACHE_TTL = 60 * 5  # 5 минут


def _account_cache_key(account_id: int) -> str:
    return f"account:{account_id}"


def _account_to_dict(account: Account) -> dict:
    return {
        "id": account.id,
        "login": account.login,
        "password": account.password,
        "is_blocked": account.is_blocked,
    }


def _dict_to_account(data: dict) -> Account:
    return Account(
        id=data["id"],
        login=data["login"],
        password=data["password"],
        is_blocked=data["is_blocked"],
    )


class AccountRepository:
    """Репозиторий аккаунтов: абстракция поверх PG-стораджа + Redis-кэш."""

    def __init__(self, pg_storage: AccountPgStorage | None = None) -> None:
        self._pg = pg_storage or AccountPgStorage()

    async def create(self, *, login: str, password: str) -> Account:
        return await self._pg.create(login=login, password=password)

    async def get_by_id(self, account_id: int) -> Account | None:
        key = _account_cache_key(account_id)

        cached = await redis_client.get(key)
        if cached is not None:
            logger.debug("Cache HIT %s", key)
            return _dict_to_account(cached)

        account = await self._pg.get_by_id(account_id)

        if account is not None:
            await redis_client.set(key, _account_to_dict(account), ttl=ACCOUNT_CACHE_TTL)
            logger.debug("Cache SET %s", key)

        return account

    async def find_by_credentials(
        self, *, login: str, password: str,
    ) -> Account | None:
        return await self._pg.find_by_credentials(login=login, password=password)

    async def block(self, account_id: int) -> Account | None:
        result = await self._pg.block(account_id)
        await self._invalidate(account_id)
        return result

    async def delete(self, account_id: int) -> bool:
        result = await self._pg.delete(account_id)
        await self._invalidate(account_id)
        return result

    async def _invalidate(self, account_id: int) -> None:
        key = _account_cache_key(account_id)
        await redis_client.delete(key)
        logger.debug("Cache DEL %s", key)


account_repository = AccountRepository()
