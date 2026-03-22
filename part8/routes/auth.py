from __future__ import annotations

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from errors import InvalidTokenError, AccountBlockedError
from repositories.account import Account, account_repository
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

    account = await account_repository.get_by_id(payload.account_id)

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
    token = await auth_login(login=body.login, password=body.password)

    response = JSONResponse(content={"message": "Logged in successfully"})
    response.set_cookie(
        key=COOKIE_KEY,
        value=token,
        httponly=True,
        max_age=COOKIE_MAX_AGE,
        samesite="lax",
    )
    return response
