from __future__ import annotations

import os
import time
from dataclasses import dataclass

import jwt

from errors import AuthenticationError, AccountBlockedError, InvalidTokenError
from repositories.account import account_repository

JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-key-change-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_SECONDS = int(os.environ.get("JWT_EXPIRATION_SECONDS", "3600"))


@dataclass(frozen=True, slots=True)
class TokenPayload:
    account_id: int
    login: str


def create_token(account_id: int, login: str) -> str:
    now = time.time()
    payload = {
        "sub": str(account_id),
        "login": login,
        "iat": now,
        "exp": now + JWT_EXPIRATION_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(token: str) -> TokenPayload:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise InvalidTokenError("Token has expired")
    except jwt.InvalidTokenError:
        raise InvalidTokenError("Invalid token")

    return TokenPayload(account_id=int(payload["sub"]), login=payload["login"])


async def login(*, login: str, password: str) -> str:
    account = await account_repository.find_by_credentials(
        login=login, password=password,
    )

    if account is None:
        raise AuthenticationError("Invalid login or password")

    if account.is_blocked:
        raise AccountBlockedError("Account is blocked")

    return create_token(account.id, account.login)
