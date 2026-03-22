"""
Юнит тесты dependency get_current_account
PostgreSQL мокается через account_repository, тесты запускаются без внешних зависимостей
Запуск pytest tests/test_auth_dependency.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from errors import InvalidTokenError, AccountBlockedError
from repositories.account import Account
from services.auth_service import create_token
from routes.auth import get_current_account


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

        with patch(
            "routes.auth.account_repository",
        ) as mock_repo:
            mock_repo.get_by_id = AsyncMock(return_value=None)
            with pytest.raises(InvalidTokenError, match="Account not found"):
                await get_current_account(access_token=token)


# заблокированный аккаунт

class TestBlockedAccount:

    @pytest.mark.asyncio
    async def test_blocked_account_raises(self):
        account = _make_account(is_blocked=True)
        token = create_token(account_id=account.id, login=account.login)

        with patch(
            "routes.auth.account_repository",
        ) as mock_repo:
            mock_repo.get_by_id = AsyncMock(return_value=account)
            with pytest.raises(AccountBlockedError, match="blocked"):
                await get_current_account(access_token=token)


# успешная авторизация

class TestSuccess:

    @pytest.mark.asyncio
    async def test_returns_account(self):
        account = _make_account(account_id=42, login="bob")
        token = create_token(account_id=account.id, login=account.login)

        with patch(
            "routes.auth.account_repository",
        ) as mock_repo:
            mock_repo.get_by_id = AsyncMock(return_value=account)
            result = await get_current_account(access_token=token)

        assert isinstance(result, Account)
        assert result.id == 42
        assert result.login == "bob"
        assert result.is_blocked is False

    @pytest.mark.asyncio
    async def test_calls_repo_with_account_id_from_token(self):
        account = _make_account(account_id=7, login="carol")
        token = create_token(account_id=7, login="carol")

        with patch(
            "routes.auth.account_repository",
        ) as mock_repo:
            mock_repo.get_by_id = AsyncMock(return_value=account)
            await get_current_account(access_token=token)

        mock_repo.get_by_id.assert_awaited_once_with(7)
