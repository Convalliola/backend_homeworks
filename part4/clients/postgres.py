from __future__ import annotations
import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncGenerator

@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[asyncpg.Connection, None]:

    connection: asyncpg.Connection = await asyncpg.connect(
        user='radilkhanova',
        password='postgres',
        database='homework3',
        host='127.0.0.1',
        port=5432
    )

    yield connection

    await connection.close()