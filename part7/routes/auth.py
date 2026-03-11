from __future__ import annotations

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from clients.postgres import get_pg_connection
from errors import InvalidTokenError, AccountBlockedError
from repositories.account import Account, get_account_by_id
from services.auth_service import login as auth_login, verify_token

router = APIRouter()

COOKIE_KEY = "access_token"
COOKIE_MAX_AGE = 3600


async def get_current_account(
    access_token: str | None = Cookie(default=None),
) -> Account:
    if access_token is None:
        raise InvalidTokenError("Not authenticated")

    payload = verify_token(access_token)

    async with get_pg_connection() as conn:
        account = await get_account_by_id(conn, payload.account_id)

    if account is None:
        raise InvalidTokenError("Account not found")

    if account.is_blocked:
        raise AccountBlockedError("Account is blocked")

    return account


class LoginRequest(BaseModel):
    login: str
    password: str


@router.post("/login")
async def login_handler(body: LoginRequest) -> JSONResponse:
    async with get_pg_connection() as conn:
        token = await auth_login(conn, login=body.login, password=body.password)

    response = JSONResponse(content={"message": "Logged in successfully"})
    response.set_cookie(
        key=COOKIE_KEY,
        value=token,
        httponly=True,
        max_age=COOKIE_MAX_AGE,
        samesite="lax",
    )
    return response
