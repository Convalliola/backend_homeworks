import pathlib
import sys
import os

import pytest
import pytest_asyncio

PART2_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PART2_DIR) not in sys.path:
    sys.path.insert(0, str(PART2_DIR))


# маркеры

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: integration tests (Redis/fakeredis, PostgreSQL, etc.)",
    )


def pytest_collection_modifyitems(config, items):
    """пропускаются integration-тесты, требующие PostgreSQL, если БД недоступна """
    skip_pg = pytest.mark.skip(
        reason="PostgreSQL not available (set PG_TEST_DSN or ensure local DB)",
    )
    for item in items:
        if "pg_conn" not in getattr(item, "fixturenames", ()):
            continue
        dsn = os.environ.get("PG_TEST_DSN")
        if dsn:
            continue
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            item.add_marker(skip_pg)
            continue
        import asyncio

        async def _check():
            conn = await asyncpg.connect(
                user=os.environ.get("PG_TEST_USER", os.environ.get("PG_USER", "postgres")),
                password=os.environ.get("PG_TEST_PASSWORD", os.environ.get("PG_PASSWORD", "postgres")),
                database=os.environ.get("PG_TEST_DB", "homework3"),
                host=os.environ.get("PG_TEST_HOST", "127.0.0.1"),
                port=int(os.environ.get("PG_TEST_PORT", "5432")),
                timeout=2,
            )
            await conn.close()

        try:
            asyncio.run(_check())
        except Exception:
            item.add_marker(skip_pg)


# фикстуры для интеграционных PG тестов

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.users (
    id BIGSERIAL PRIMARY KEY,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.ads (
    id BIGSERIAL PRIMARY KEY,
    seller_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    category INTEGER NOT NULL,
    images_qty INTEGER NOT NULL CHECK (images_qty >= 0 AND images_qty <= 10),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.moderation_results (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT REFERENCES ads(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'failed')),
    is_violation BOOLEAN NULL,
    probability FLOAT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP NULL
);

-- V002
ALTER TABLE public.ads ADD COLUMN IF NOT EXISTS is_closed BOOLEAN NOT NULL DEFAULT FALSE;
"""


@pytest_asyncio.fixture
async def pg_conn():
    import asyncpg

    dsn = os.environ.get("PG_TEST_DSN")
    if dsn:
        conn = await asyncpg.connect(dsn)
    else:
        conn = await asyncpg.connect(
            user=os.environ.get("PG_TEST_USER", os.environ.get("PG_USER", "postgres")),
            password=os.environ.get("PG_TEST_PASSWORD", os.environ.get("PG_PASSWORD", "postgres")),
            database=os.environ.get("PG_TEST_DB", "homework3"),
            host=os.environ.get("PG_TEST_HOST", "127.0.0.1"),
            port=int(os.environ.get("PG_TEST_PORT", "5432")),
        )

    await conn.execute(SCHEMA_SQL)

    tx = conn.transaction()
    await tx.start()

    yield conn

    await tx.rollback()
    await conn.close()
