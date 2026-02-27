from __future__ import annotations

import logging
from dataclasses import dataclass

from clients.redis import redis_client

logger = logging.getLogger(__name__)


# TTL для кэша предсказаний по item_id  (simple_predict, async_predict)

# Результат предсказания зависит от данных объявления (description, images_qty, category) 
# и статуса продавца (is_verified), но эти данные могут меняться, например
# продавец может пройти верификацию, объявление может быть отредактировано итд.
#  TTL = 10 минут достаточно короткий, чтобы подхватить изменения в данных без
# заметной задержки для пользователя но и достаточно длинный, чтобы загасить волну одинаковых
# запросов (например, при открытии карточки объявления несколькими пользователями) и снизить нагрузку на БД + ML-модель.

PREDICT_BY_ITEM_TTL = 60 * 10  # 10 минут


# TTL для кэша предсказаний по фичам  (predict)

# Результат предсказания инвалидируется только при переобучении модели
# (т.е при новом деплое), поэтому TTL может быть сильно
# больше(например, 1 час).  Если модель переобучена и задеплоена, 
# в худшем случае пользователь получит устаревший ответ максимум на час.

PREDICT_BY_FEATURES_TTL = 60 * 60  # 1 час

# TTL для кэша результатов модерации  (moderation_result)

# Результат со статусом completed или failed неизменяется, так что
# его безопасно кэшировать надолго.  TTL = 30 минут выбран, чтобы
# не занимать память Redis записями, которые после просмотра
# пользователем вряд ли будут запрашиваться повторно.
# Результаты со статусом pending не кэшируются, потому что они
# перейдут в финальное состояние в ближайшие секунды/минуты.

MODERATION_RESULT_TTL = 60 * 30  # 30 минут


def _item_predict_key(item_id: int) -> str:
    return f"predict:item:{item_id}"


def _features_predict_key(
    is_verified_seller: bool,
    images_qty: int,
    description_length: int,
    category: int,
) -> str:
    return (
        f"predict:features:"
        f"{int(is_verified_seller)}:{images_qty}:{description_length}:{category}"
    )


def _moderation_key(task_id: int) -> str:
    return f"moderation:result:{task_id}"


@dataclass(frozen=True, slots=True)
class CachedPrediction:
    is_valid: bool
    probability: float


class PredictCacheStorage:
    """кэш хранилище результатов предсказаний поверх redis"""

    # кэш по item_id (simple_predict / worker)

    async def get_by_item(self, item_id: int) -> CachedPrediction | None:
        data = await redis_client.get(_item_predict_key(item_id))
        if data is None:
            return None
        logger.debug("Cache HIT predict:item:%s", item_id)
        return CachedPrediction(**data)

    async def set_by_item(
        self, item_id: int, is_valid: bool, probability: float,
    ) -> None:
        await redis_client.set(
            _item_predict_key(item_id),
            {"is_valid": is_valid, "probability": probability},
            ttl=PREDICT_BY_ITEM_TTL,
        )
        logger.debug("Cache SET predict:item:%s", item_id)

    async def invalidate_by_item(self, item_id: int) -> None:
        await redis_client.delete(_item_predict_key(item_id))
        logger.debug("Cache DEL predict:item:%s", item_id)

    # кэш по фичам (predict)

    async def get_by_features(
        self,
        *,
        is_verified_seller: bool,
        images_qty: int,
        description: str,
        category: int,
    ) -> CachedPrediction | None:
        key = _features_predict_key(
            is_verified_seller, images_qty, len(description), category,
        )
        data = await redis_client.get(key)
        if data is None:
            return None
        logger.debug("Cache HIT %s", key)
        return CachedPrediction(**data)

    async def set_by_features(
        self,
        *,
        is_valid: bool,
        probability: float,
        is_verified_seller: bool,
        images_qty: int,
        description: str,
        category: int,
    ) -> None:
        key = _features_predict_key(
            is_verified_seller, images_qty, len(description), category,
        )
        await redis_client.set(
            key,
            {"is_valid": is_valid, "probability": probability},
            ttl=PREDICT_BY_FEATURES_TTL,
        )
        logger.debug("Cache SET %s", key)

    # кэш результатов модерации

    async def get_moderation(self, task_id: int) -> dict | None:
        data = await redis_client.get(_moderation_key(task_id))
        if data is None:
            return None
        logger.debug("Cache HIT moderation:result:%s", task_id)
        return data

    async def set_moderation(
        self,
        task_id: int,
        *,
        status: str,
        is_violation: bool | None,
        probability: float | None,
    ) -> None:
        if status == "pending":
            return
        await redis_client.set(
            _moderation_key(task_id),
            {
                "task_id": task_id,
                "status": status,
                "is_violation": is_violation,
                "probability": probability,
            },
            ttl=MODERATION_RESULT_TTL,
        )
        logger.debug("Cache SET moderation:result:%s", task_id)

    async def invalidate_moderation(self, task_id: int) -> None:
        await redis_client.delete(_moderation_key(task_id))
        logger.debug("Cache DEL moderation:result:%s", task_id)


predict_cache = PredictCacheStorage()
