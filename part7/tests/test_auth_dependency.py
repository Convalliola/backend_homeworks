"""
Юнит тесты dependency get_current_account
PostgreSQL мокается, тесты запускаются без внешних зависимостей
Запуск pytest tests/test_auth_dependency.py -v
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from errors import InvalidTokenError, AccountBlockedError
from repositories.account import Account
from services.auth_service import create_token
from routes.auth import get_current_account


@asynccontextmanager
async def _fake_pg_connection():
    yield AsyncMock()


def _make_account(*, account_id=1, login="alice", password="pw", is_blocked=False):
    return Account(id=account_id, login=login, password=password, is_blocked=is_blocked)


# нет куки

class TestNoCookie:

    @pytest.mark.asyncio
    async def test_no_cookie_raises(self):
        with pytest.raises(InvalidTokenError, match="Not authenticated"):
            await get_current_account(access_token=None)


# невалидный токен

class TestInvalidToken:

    @pytest.mark.asyncio
    async def test_garbage_token_raises(self):
        with pytest.raises(InvalidTokenError):
            await get_current_account(access_token="garbage.token.here")

    @pytest.mark.asyncio
    async def test_tampered_token_raises(self):
        token = create_token(account_id=1, login="alice")
        tampered = token[:-4] + "XXXX"

        with pytest.raises(InvalidTokenError):
            await get_current_account(access_token=tampered)


# аккаунт не найден

class TestAccountNotFound:

    @pytest.mark.asyncio
    async def test_deleted_account_raises(self):
        token = create_token(account_id=999, login="ghost")

        async def fake_get_account(_conn, _id):
            return None

        with (
            patch("routes.auth.get_pg_connection", _fake_pg_connection),
            patch("routes.auth.get_account_by_id", fake_get_account),
        ):
            with pytest.raises(InvalidTokenError, match="Account not found"):
                await get_current_account(access_token=token)


# заблокированный аккаунт

class TestBlockedAccount:

    @pytest.mark.asyncio
    async def test_blocked_account_raises(self):
        account = _make_account(is_blocked=True)
        token = create_token(account_id=account.id, login=account.login)

        async def fake_get_account(_conn, _id):
            return account

        with (
            patch("routes.auth.get_pg_connection", _fake_pg_connection),
            patch("routes.auth.get_account_by_id", fake_get_account),
        ):
            with pytest.raises(AccountBlockedError, match="blocked"):
                await get_current_account(access_token=token)


# успешная авторизация

class TestSuccess:

    @pytest.mark.asyncio
    async def test_returns_account(self):
        account = _make_account(account_id=42, login="bob")
        token = create_token(account_id=account.id, login=account.login)

        async def fake_get_account(_conn, account_id):
            assert account_id == 42
            return account

        with (
            patch("routes.auth.get_pg_connection", _fake_pg_connection),
            patch("routes.auth.get_account_by_id", fake_get_account),
        ):
            result = await get_current_account(access_token=token)

        assert isinstance(result, Account)
        assert result.id == 42
        assert result.login == "bob"
        assert result.is_blocked is False

    @pytest.mark.asyncio
    async def test_calls_repo_with_account_id_from_token(self):
        account = _make_account(account_id=7, login="carol")
        token = create_token(account_id=7, login="carol")

        called_with = {}

        async def fake_get_account(_conn, account_id):
            called_with["account_id"] = account_id
            return account

        with (
            patch("routes.auth.get_pg_connection", _fake_pg_connection),
            patch("routes.auth.get_account_by_id", fake_get_account),
        ):
            await get_current_account(access_token=token)

        assert called_with["account_id"] == 7
