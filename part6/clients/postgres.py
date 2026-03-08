from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator

import asyncpg

from metrics import DB_QUERY_DURATION


@contextmanager
def track_db_query(query_type: str):
    """Context manager that records DB query duration into the histogram."""
    start = time.perf_counter()
    try:
        yield
    finally:
        DB_QUERY_DURATION.labels(query_type=query_type).observe(
            time.perf_counter() - start
        )


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