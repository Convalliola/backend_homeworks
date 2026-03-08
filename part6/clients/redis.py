from __future__ import annotations

import json
import logging
import os
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
DEFAULT_TTL_SECONDS = 300


class RedisClient:
    """ асинхронный клиент Redis для кэширования"""

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        default_ttl: int = DEFAULT_TTL_SECONDS,
    ) -> None:
        self._host = host
        self._port = port
        self._default_ttl = default_ttl
        self._redis: Redis | None = None

    async def start(self) -> None:
        self._redis = Redis(
            host=self._host,
            port=self._port,
            decode_responses=True,
        )
        await self._redis.ping()
        logger.info("Redis connected (%s:%s)", self._host, self._port)

    async def stop(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            logger.info("Redis connection closed")

    @property
    def client(self) -> Redis:
        if self._redis is None:
            raise RuntimeError("Redis client is not started")
        return self._redis

    async def get(self, key: str) -> Any | None:
        raw = await self.client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(
        self, key: str, value: Any, ttl: int | None = None,
    ) -> None:
        await self.client.set(
            key,
            json.dumps(value, default=str),
            ex=ttl or self._default_ttl,
        )

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self.client.exists(key))


redis_client = RedisClient()
