from __future__ import annotations

import os

import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncGenerator

@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[asyncpg.Connection, None]:

    connection: asyncpg.Connection = await asyncpg.connect(
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
        database=os.environ.get("PG_DATABASE", "homework3"),
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=int(os.environ.get("PG_PORT", "5432")),
    )

    yield connection

    await connection.close()