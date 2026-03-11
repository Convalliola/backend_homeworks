"""
юнит тесты обработчика /login.
postgre SQL, кафка и Redis мокаются, тесты запускаются без внешних зависимостей
Запуск тестов: pytest tests/test_login.py -v
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from errors import AuthenticationError, AccountBlockedError
from repositories.account import Account
from services.auth_service import verify_token


@asynccontextmanager
async def _fake_pg_connection():
    yield AsyncMock()


def _patch_infra(monkeypatch):
    """мокаем lifespan зависимости: модель, kafka, redis"""
    import main
    from clients.kafka import kafka_producer
    from clients.redis import redis_client

    monkeypatch.setattr(main, "load_or_train_model", lambda *a, **kw: object())
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(redis_client, "start", AsyncMock())
    monkeypatch.setattr(redis_client, "stop", AsyncMock())


def _make_account(*, account_id=1, login="alice", password="pw", is_blocked=False):
    return Account(id=account_id, login=login, password=password, is_blocked=is_blocked)


# успешный логин

def test_login_success_sets_cookie(monkeypatch):
    _patch_infra(monkeypatch)

    import routes.auth as auth_route

    account = _make_account()

    async def fake_auth_login(_conn, *, login, password):
        from services.auth_service import create_token
        return create_token(account.id, account.login)

    monkeypatch.setattr(auth_route, "get_pg_connection", _fake_pg_connection)
    monkeypatch.setattr(auth_route, "auth_login", fake_auth_login)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "alice", "password": "pw"})

    assert resp.status_code == 200
    assert resp.json() == {"message": "Logged in successfully"}
    assert "access_token" in resp.cookies


def test_login_success_cookie_contains_valid_jwt(monkeypatch):
    _patch_infra(monkeypatch)

    import routes.auth as auth_route

    account = _make_account(account_id=42, login="bob")

    async def fake_auth_login(_conn, *, login, password):
        from services.auth_service import create_token
        return create_token(account.id, account.login)

    monkeypatch.setattr(auth_route, "get_pg_connection", _fake_pg_connection)
    monkeypatch.setattr(auth_route, "auth_login", fake_auth_login)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "bob", "password": "pw"})

    token = resp.cookies["access_token"]
    payload = verify_token(token)
    assert payload.account_id == 42
    assert payload.login == "bob"


# неверные креды

def test_login_wrong_credentials_returns_401(monkeypatch):
    _patch_infra(monkeypatch)

    import routes.auth as auth_route

    async def fake_auth_login(_conn, *, login, password):
        raise AuthenticationError("Invalid login or password")

    monkeypatch.setattr(auth_route, "get_pg_connection", _fake_pg_connection)
    monkeypatch.setattr(auth_route, "auth_login", fake_auth_login)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "alice", "password": "wrong"})

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid login or password"
    assert "access_token" not in resp.cookies


# заблокированный аккаунт

def test_login_blocked_account_returns_403(monkeypatch):
    _patch_infra(monkeypatch)

    import routes.auth as auth_route

    async def fake_auth_login(_conn, *, login, password):
        raise AccountBlockedError("Account is blocked")

    monkeypatch.setattr(auth_route, "get_pg_connection", _fake_pg_connection)
    monkeypatch.setattr(auth_route, "auth_login", fake_auth_login)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "alice", "password": "pw"})

    assert resp.status_code == 403
    assert resp.json()["detail"] == "Account is blocked"
    assert "access_token" not in resp.cookies


# валидация тела запроса

def test_login_missing_login_returns_422(monkeypatch):
    _patch_infra(monkeypatch)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"password": "pw"})

    assert resp.status_code == 422


def test_login_missing_password_returns_422(monkeypatch):
    _patch_infra(monkeypatch)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "alice"})

    assert resp.status_code == 422


def test_login_empty_body_returns_422(monkeypatch):
    _patch_infra(monkeypatch)

    with TestClient(main_app()) as client:
        resp = client.post("/login")

    assert resp.status_code == 422


# куки -атрибуты

def test_login_cookie_is_httponly(monkeypatch):
    _patch_infra(monkeypatch)

    import routes.auth as auth_route

    account = _make_account()

    async def fake_auth_login(_conn, *, login, password):
        from services.auth_service import create_token
        return create_token(account.id, account.login)

    monkeypatch.setattr(auth_route, "get_pg_connection", _fake_pg_connection)
    monkeypatch.setattr(auth_route, "auth_login", fake_auth_login)

    with TestClient(main_app()) as client:
        resp = client.post("/login", json={"login": "alice", "password": "pw"})

    cookie_header = resp.headers.get("set-cookie", "")
    assert "httponly" in cookie_header.lower()
    assert "samesite=lax" in cookie_header.lower()


# хелпер

def main_app():
    import main
    return main.app
