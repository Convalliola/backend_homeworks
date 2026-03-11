"""
Юнит тесты бизнес-логики авторизации
Тесты не требуют postgre sql, репозиторий мокается
Запуск тестов: pytest tests/test_auth_service.py -v
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from errors import AuthenticationError, AccountBlockedError, InvalidTokenError
from repositories.account import Account
from services.auth_service import (
    create_token,
    verify_token,
    login,
    JWT_ALGORITHM,
    TokenPayload,
)


# create_token/verify_token

class TestCreateToken:

    def test_creates_decodable_token(self):
        token = create_token(account_id=1, login="alice")
        assert isinstance(token, str)
        assert len(token) > 0

    def test_token_contains_correct_payload(self):
        token = create_token(account_id=42, login="bob")
        payload = verify_token(token)

        assert payload.account_id == 42
        assert payload.login == "bob"

    def test_different_accounts_produce_different_tokens(self):
        t1 = create_token(account_id=1, login="alice")
        t2 = create_token(account_id=2, login="bob")
        assert t1 != t2


class TestVerifyToken:

    def test_valid_token(self):
        token = create_token(account_id=1, login="alice")
        payload = verify_token(token)

        assert isinstance(payload, TokenPayload)
        assert payload.account_id == 1
        assert payload.login == "alice"

    def test_expired_token_raises(self):
        with patch("services.auth_service.JWT_EXPIRATION_SECONDS", 0):
            token = create_token(account_id=1, login="alice")

        time.sleep(1)

        with pytest.raises(InvalidTokenError, match="expired"):
            verify_token(token)

    def test_garbage_token_raises(self):
        with pytest.raises(InvalidTokenError, match="Invalid"):
            verify_token("not.a.valid.token")

    def test_tampered_token_raises(self):
        token = create_token(account_id=1, login="alice")
        tampered = token[:-4] + "XXXX"

        with pytest.raises(InvalidTokenError):
            verify_token(tampered)

    def test_wrong_secret_raises(self):
        import jwt as pyjwt

        payload = {
            "sub": 1,
            "login": "alice",
            "iat": time.time(),
            "exp": time.time() + 3600,
        }
        token = pyjwt.encode(payload, "wrong-secret", algorithm=JWT_ALGORITHM)

        with pytest.raises(InvalidTokenError):
            verify_token(token)


# login

def _make_account(*, account_id=1, login="alice", password="pw", is_blocked=False):
    return Account(id=account_id, login=login, password=password, is_blocked=is_blocked)


class TestLogin:

    @pytest.mark.asyncio
    async def test_successful_login_returns_token(self):
        conn = AsyncMock()
        account = _make_account()

        with patch(
            "services.auth_service.find_account_by_credentials",
            new_callable=AsyncMock,
            return_value=account,
        ):
            token = await login(conn, login="alice", password="pw")

        assert isinstance(token, str)
        payload = verify_token(token)
        assert payload.account_id == account.id
        assert payload.login == account.login

    @pytest.mark.asyncio
    async def test_wrong_credentials_raises(self):
        conn = AsyncMock()

        with patch(
            "services.auth_service.find_account_by_credentials",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with pytest.raises(AuthenticationError, match="Invalid login or password"):
                await login(conn, login="alice", password="wrong")

    @pytest.mark.asyncio
    async def test_blocked_account_raises(self):
        conn = AsyncMock()
        blocked = _make_account(is_blocked=True)

        with patch(
            "services.auth_service.find_account_by_credentials",
            new_callable=AsyncMock,
            return_value=blocked,
        ):
            with pytest.raises(AccountBlockedError, match="blocked"):
                await login(conn, login="alice", password="pw")

    @pytest.mark.asyncio
    async def test_login_calls_repo_with_correct_args(self):
        conn = AsyncMock()
        account = _make_account()
        mock_find = AsyncMock(return_value=account)

        with patch(
            "services.auth_service.find_account_by_credentials",
            mock_find,
        ):
            await login(conn, login="alice", password="pw")

        mock_find.assert_awaited_once_with(conn, login="alice", password="pw")
