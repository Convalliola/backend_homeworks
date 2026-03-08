"""
Интеграционные тесты кэширования.

Проверка, что методы PredictCacheStorage корректно работают с Redis
- данные записываются и читаются
- TTL устанавливается
- invalidate удаляет запись
- pending результаты модерации не кэшируются
- разные ключи не конфликтуют

Используем fakeredis как in memory замену Redis, совместим с redis.asyncio, но без внешнего сервера.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
import fakeredis.aioredis

from clients.redis import RedisClient
from storages.predict_cache import (
    PredictCacheStorage,
    PREDICT_BY_ITEM_TTL,
    PREDICT_BY_FEATURES_TTL,
    MODERATION_RESULT_TTL,
)


#   фикстуры

@pytest_asyncio.fixture
async def redis_client_fake(monkeypatch):
    """RedisClient  работающий на fakeredis вместо настоящего сервера """
    client = RedisClient()
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client._redis = fake_redis

    import storages.predict_cache as pc
    monkeypatch.setattr(pc, "redis_client", client)

    yield client

    await fake_redis.aclose()


@pytest_asyncio.fixture
async def cache(redis_client_fake) -> PredictCacheStorage:
    return PredictCacheStorage()


# кэш предсказаний по item_id

@pytest.mark.integration
class TestItemCache:

    @pytest.mark.asyncio
    async def test_get_returns_none_when_empty(self, cache):
        result = await cache.get_by_item(999)
        assert result is None

    @pytest.mark.asyncio
    async def test_set_then_get_roundtrip(self, cache):
        await cache.set_by_item(10, is_valid=True, probability=0.85)
        result = await cache.get_by_item(10)

        assert result is not None
        assert result.is_valid is True
        assert result.probability == 0.85

    @pytest.mark.asyncio
    async def test_different_items_do_not_collide(self, cache):
        await cache.set_by_item(1, is_valid=True, probability=0.9)
        await cache.set_by_item(2, is_valid=False, probability=0.2)

        r1 = await cache.get_by_item(1)
        r2 = await cache.get_by_item(2)

        assert r1.is_valid is True and r1.probability == 0.9
        assert r2.is_valid is False and r2.probability == 0.2

    @pytest.mark.asyncio
    async def test_overwrite_replaces_value(self, cache):
        await cache.set_by_item(10, is_valid=True, probability=0.9)
        await cache.set_by_item(10, is_valid=False, probability=0.1)

        result = await cache.get_by_item(10)
        assert result.is_valid is False
        assert result.probability == 0.1

    @pytest.mark.asyncio
    async def test_invalidate_removes_entry(self, cache):
        await cache.set_by_item(10, is_valid=True, probability=0.8)
        assert await cache.get_by_item(10) is not None

        await cache.invalidate_by_item(10)
        assert await cache.get_by_item(10) is None

    @pytest.mark.asyncio
    async def test_invalidate_nonexistent_key_is_safe(self, cache):
        await cache.invalidate_by_item(12345)

    @pytest.mark.asyncio
    async def test_ttl_is_set(self, cache, redis_client_fake):
        await cache.set_by_item(10, is_valid=True, probability=0.8)
        ttl = await redis_client_fake.client.ttl("predict:item:10")
        assert 0 < ttl <= PREDICT_BY_ITEM_TTL


# Кэш предсказаний по фичам

@pytest.mark.integration
class TestFeaturesCache:

    @pytest.mark.asyncio
    async def test_get_returns_none_when_empty(self, cache):
        result = await cache.get_by_features(
            is_verified_seller=True, images_qty=3,
            description="test", category=1,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_set_then_get_roundtrip(self, cache):
        await cache.set_by_features(
            is_valid=True, probability=0.92,
            is_verified_seller=True, images_qty=5,
            description="hello world", category=7,
        )
        result = await cache.get_by_features(
            is_verified_seller=True, images_qty=5,
            description="hello world", category=7,
        )

        assert result is not None
        assert result.is_valid is True
        assert result.probability == 0.92

    @pytest.mark.asyncio
    async def test_different_features_produce_different_keys(self, cache):
        await cache.set_by_features(
            is_valid=True, probability=0.9,
            is_verified_seller=True, images_qty=5,
            description="aaa", category=1,
        )
        await cache.set_by_features(
            is_valid=False, probability=0.1,
            is_verified_seller=False, images_qty=2,
            description="bbb", category=2,
        )

        r1 = await cache.get_by_features(
            is_verified_seller=True, images_qty=5,
            description="aaa", category=1,
        )
        r2 = await cache.get_by_features(
            is_verified_seller=False, images_qty=2,
            description="bbb", category=2,
        )

        assert r1.is_valid is True
        assert r2.is_valid is False

    @pytest.mark.asyncio
    async def test_description_length_is_key_component(self, cache):
        """Две строки одинаковой длины дают одинаковый ключ """
        await cache.set_by_features(
            is_valid=True, probability=0.7,
            is_verified_seller=True, images_qty=1,
            description="abc", category=1,
        )
        result = await cache.get_by_features(
            is_verified_seller=True, images_qty=1,
            description="xyz", category=1,
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_ttl_is_set(self, cache, redis_client_fake):
        await cache.set_by_features(
            is_valid=True, probability=0.8,
            is_verified_seller=False, images_qty=2,
            description="test", category=3,
        )
        key = "predict:features:0:2:4:3"
        ttl = await redis_client_fake.client.ttl(key)
        assert 0 < ttl <= PREDICT_BY_FEATURES_TTL



# Кэш результатов модерации

@pytest.mark.integration
class TestModerationCache:

    @pytest.mark.asyncio
    async def test_get_returns_none_when_empty(self, cache):
        result = await cache.get_moderation(999)
        assert result is None

    @pytest.mark.asyncio
    async def test_completed_result_roundtrip(self, cache):
        await cache.set_moderation(
            42, status="completed", is_violation=True, probability=0.87,
        )
        result = await cache.get_moderation(42)

        assert result is not None
        assert result["task_id"] == 42
        assert result["status"] == "completed"
        assert result["is_violation"] is True
        assert result["probability"] == 0.87

    @pytest.mark.asyncio
    async def test_failed_result_is_cached(self, cache):
        await cache.set_moderation(
            43, status="failed", is_violation=None, probability=None,
        )
        result = await cache.get_moderation(43)

        assert result is not None
        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_pending_result_is_not_cached(self, cache):
        """Pending статус не должен попадать в кэшм"""
        await cache.set_moderation(
            44, status="pending", is_violation=None, probability=None,
        )
        result = await cache.get_moderation(44)
        assert result is None

    @pytest.mark.asyncio
    async def test_invalidate_removes_entry(self, cache):
        await cache.set_moderation(
            42, status="completed", is_violation=False, probability=0.6,
        )
        assert await cache.get_moderation(42) is not None

        await cache.invalidate_moderation(42)
        assert await cache.get_moderation(42) is None

    @pytest.mark.asyncio
    async def test_different_tasks_do_not_collide(self, cache):
        await cache.set_moderation(
            1, status="completed", is_violation=True, probability=0.9,
        )
        await cache.set_moderation(
            2, status="completed", is_violation=False, probability=0.1,
        )

        r1 = await cache.get_moderation(1)
        r2 = await cache.get_moderation(2)

        assert r1["is_violation"] is True
        assert r2["is_violation"] is False

    @pytest.mark.asyncio
    async def test_ttl_is_set(self, cache, redis_client_fake):
        await cache.set_moderation(
            42, status="completed", is_violation=True, probability=0.9,
        )
        ttl = await redis_client_fake.client.ttl("moderation:result:42")
        assert 0 < ttl <= MODERATION_RESULT_TTL


# RedisClient, низкоуровневые операции

@pytest.mark.integration
class TestRedisClientOperations:

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self, redis_client_fake):
        result = await redis_client_fake.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_get_json_roundtrip(self, redis_client_fake):
        await redis_client_fake.set("k1", {"a": 1, "b": [2, 3]})
        result = await redis_client_fake.get("k1")
        assert result == {"a": 1, "b": [2, 3]}

    @pytest.mark.asyncio
    async def test_delete_removes_key(self, redis_client_fake):
        await redis_client_fake.set("k2", "value")
        assert await redis_client_fake.exists("k2")

        await redis_client_fake.delete("k2")
        assert not await redis_client_fake.exists("k2")

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_missing(self, redis_client_fake):
        assert not await redis_client_fake.exists("missing_key")

    @pytest.mark.asyncio
    async def test_set_with_custom_ttl(self, redis_client_fake):
        await redis_client_fake.set("k3", "val", ttl=120)
        ttl = await redis_client_fake.client.ttl("k3")
        assert 0 < ttl <= 120
