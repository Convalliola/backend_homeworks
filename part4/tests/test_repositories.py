from __future__ import annotations

import asyncio
from datetime import datetime, timezone


class FakeConn:
    def __init__(self, *, fetchrow_result):
        self.fetchrow_result = fetchrow_result
        self.last_fetchrow_query = None
        self.last_fetchrow_args = None

    async def fetchrow(self, query: str, *args):
        self.last_fetchrow_query = query
        self.last_fetchrow_args = args
        return self.fetchrow_result


def test_create_user_calls_insert_and_maps_result():
    from repositories.users import create_user

    now = datetime.now(tz=timezone.utc)
    conn = FakeConn(fetchrow_result={"id": 1, "is_verified": True, "created_at": now})

    asyncio.run(create_user(conn, is_verified=True))

    assert "INSERT INTO public.users" in conn.last_fetchrow_query
    assert conn.last_fetchrow_args == (True,)


def test_create_ad_calls_insert_and_maps_result():
    from repositories.ads import create_ad

    now = datetime.now(tz=timezone.utc)
    conn = FakeConn(
        fetchrow_result={
            "id": 10,
            "seller_id": 1,
            "name": "Item",
            "description": "Some description",
            "category": 5,
            "images_qty": 2,
            "created_at": now,
        }
    )

    asyncio.run(
        create_ad(
            conn,
            seller_id=1,
            name="Item",
            description="Some description",
            category=5,
            images_qty=2,
        )
    )

    assert "INSERT INTO public.ads" in conn.last_fetchrow_query
    assert conn.last_fetchrow_args == (1, "Item", "Some description", 5, 2)

