"""
Интеграционные тесты хранилища аккаунтов
Каждый тест работает с postgre sql через фикстуру pg_conn,
которая оборачивает тест в транзакцию с откатом, так что тесты полностью изолированы.

Запуск тестов: pytest tests/test_account_storage.py -v -m integration
Пропускаются автоматически, если postgre sql недоступен.
"""

from __future__ import annotations

import pytest

from repositories.account import (
    create_account,
    get_account_by_id,
    find_account_by_credentials,
    block_account,
    delete_account,
)


# создание


@pytest.mark.integration
class TestAccountCreate:

    @pytest.mark.asyncio
    async def test_create_account(self, pg_conn):
        acc = await create_account(pg_conn, login="alice", password="secret")

        assert acc.id is not None
        assert acc.login == "alice"
        assert acc.password != "secret", "password must be stored as hash"
        assert len(acc.password) == 32  # MD5 hex digest length
        assert acc.is_blocked is False

    @pytest.mark.asyncio
    async def test_create_two_accounts(self, pg_conn):
        a1 = await create_account(pg_conn, login="alice", password="pw1")
        a2 = await create_account(pg_conn, login="bob", password="pw2")

        assert a1.id != a2.id
        assert a1.login != a2.login


# поиск по ID


@pytest.mark.integration
class TestAccountGetById:

    @pytest.mark.asyncio
    async def test_get_existing(self, pg_conn):
        created = await create_account(pg_conn, login="alice", password="pw")
        fetched = await get_account_by_id(pg_conn, created.id)

        assert fetched is not None
        assert fetched.id == created.id
        assert fetched.login == "alice"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, pg_conn):
        result = await get_account_by_id(pg_conn, 999999)
        assert result is None


# поиск по логину и паролю

@pytest.mark.integration
class TestAccountFindByCredentials:

    @pytest.mark.asyncio
    async def test_find_existing(self, pg_conn):
        created = await create_account(pg_conn, login="alice", password="pw")
        found = await find_account_by_credentials(
            pg_conn, login="alice", password="pw",
        )

        assert found is not None
        assert found.id == created.id

    @pytest.mark.asyncio
    async def test_wrong_password(self, pg_conn):
        await create_account(pg_conn, login="alice", password="pw")
        found = await find_account_by_credentials(
            pg_conn, login="alice", password="wrong",
        )

        assert found is None

    @pytest.mark.asyncio
    async def test_wrong_login(self, pg_conn):
        await create_account(pg_conn, login="alice", password="pw")
        found = await find_account_by_credentials(
            pg_conn, login="nobody", password="pw",
        )

        assert found is None


# блокировка

@pytest.mark.integration
class TestAccountBlock:

    @pytest.mark.asyncio
    async def test_block_account(self, pg_conn):
        acc = await create_account(pg_conn, login="alice", password="pw")
        assert acc.is_blocked is False

        blocked = await block_account(pg_conn, acc.id)

        assert blocked is not None
        assert blocked.is_blocked is True
        assert blocked.id == acc.id

    @pytest.mark.asyncio
    async def test_block_already_blocked(self, pg_conn):
        acc = await create_account(pg_conn, login="alice", password="pw")
        await block_account(pg_conn, acc.id)

        result = await block_account(pg_conn, acc.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_block_nonexistent(self, pg_conn):
        result = await block_account(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_blocked_account_persists(self, pg_conn):
        acc = await create_account(pg_conn, login="alice", password="pw")
        await block_account(pg_conn, acc.id)

        fetched = await get_account_by_id(pg_conn, acc.id)
        assert fetched is not None
        assert fetched.is_blocked is True


# удаление

@pytest.mark.integration
class TestAccountDelete:

    @pytest.mark.asyncio
    async def test_delete_account(self, pg_conn):
        acc = await create_account(pg_conn, login="alice", password="pw")

        deleted = await delete_account(pg_conn, acc.id)
        assert deleted is True

        fetched = await get_account_by_id(pg_conn, acc.id)
        assert fetched is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, pg_conn):
        deleted = await delete_account(pg_conn, 999999)
        assert deleted is False


# полный жизненный цикл


@pytest.mark.integration
class TestAccountLifecycle:

    @pytest.mark.asyncio
    async def test_create_find_block_delete(self, pg_conn):
        """Полный цикл: создание → поиск → блокировка → удаление."""
        acc = await create_account(pg_conn, login="alice", password="pw")
        assert acc.is_blocked is False

        found = await find_account_by_credentials(
            pg_conn, login="alice", password="pw",
        )
        assert found is not None
        assert found.id == acc.id

        blocked = await block_account(pg_conn, acc.id)
        assert blocked.is_blocked is True

        fetched = await get_account_by_id(pg_conn, acc.id)
        assert fetched.is_blocked is True

        deleted = await delete_account(pg_conn, acc.id)
        assert deleted is True

        assert await get_account_by_id(pg_conn, acc.id) is None
