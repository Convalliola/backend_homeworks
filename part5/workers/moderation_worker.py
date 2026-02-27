"""
Kafka Consumer 
Запуск python -m workers.moderation_worker
"""

from __future__ import annotations

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from clients.kafka import KafkaProducerClient, KAFKA_BOOTSTRAP_SERVERS, MODERATION_TOPIC
from clients.postgres import get_pg_connection
from model import load_or_train_model, DEFAULT_MODEL_PATH
from repositories.ads import get_ad_with_seller
from repositories.moderation import update_moderation_completed, update_moderation_failed
from services.predict_service import predict_validity
from storages.predict_cache import predict_cache
from clients.redis import redis_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("worker.moderation")

CONSUMER_GROUP = "moderation-worker"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5  # растёт экспоненциально


async def process_message(
    model: object,
    message_value: dict,
    producer: KafkaProducerClient,
) -> None:
    """обработка сообщения из кафки с retry-логикой """
    task_id: int = message_value["task_id"]
    item_id: int = message_value["item_id"]
    retry_count: int = message_value.get("retry_count", 0)

    logger.info(
        "Processing task_id=%s item_id=%s (attempt %s/%s)",
        task_id, item_id, retry_count + 1, MAX_RETRIES,
    )

    async with get_pg_connection() as conn:
        # получение данныъ объявления и продавца из БД
        row = await get_ad_with_seller(conn, item_id)
        if row is None:
            # ретраить бессмысленно, сразу в DLQ, тк постоянная ошибка
            error_msg = f"Ad with id={item_id} not found"
            logger.error("Ad not found: item_id=%s, marking task as failed", item_id)
            await update_moderation_failed(
                conn,
                moderation_id=task_id,
                error_message=error_msg,
            )
            await producer.send_to_dlq(
                original_message=message_value,
                error=error_msg,
                retry_count=retry_count + 1,
            )
            return

        # вызов ML-модели для предсказания
        try:
            is_valid, proba = predict_validity(
                model,
                seller_id=row.seller_id,
                item_id=row.ad_id,
                is_verified_seller=row.is_verified_seller,
                images_qty=row.images_qty,
                description=row.description,
                category=row.category,
            )
        except Exception as e:
            error_msg = str(e)
            next_retry = retry_count + 1

            if next_retry < MAX_RETRIES:
                delay = RETRY_DELAY_SECONDS * (2 ** retry_count)
                logger.warning(
                    "Temporary error for task_id=%s (attempt %s/%s), "
                    "retrying in %ss: %s",
                    task_id, next_retry, MAX_RETRIES, delay, error_msg,
                )
                await asyncio.sleep(delay)
                # повторный вызов с увеличенным retry_count
                message_value["retry_count"] = next_retry
                await process_message(model, message_value, producer)
            else:
                logger.error(
                    "Max retries (%s) exceeded for task_id=%s, sending to DLQ",
                    MAX_RETRIES, task_id,
                )
                await update_moderation_failed(
                    conn,
                    moderation_id=task_id,
                    error_message=error_msg,
                )
                await producer.send_to_dlq(
                    original_message=message_value,
                    error=error_msg,
                    retry_count=next_retry,
                )
            return

        # обновление записи в moderation_results при успехе
        is_violation = not is_valid
        await update_moderation_completed(
            conn,
            moderation_id=task_id,
            is_violation=is_violation,
            probability=proba,
        )

    await predict_cache.set_by_item(item_id, is_valid, proba)
    await predict_cache.set_moderation(
        task_id,
        status="completed",
        is_violation=is_violation,
        probability=proba,
    )

    logger.info(
        "Completed task_id=%s: is_violation=%s probability=%.4f",
        task_id, is_violation, proba,
    )


async def main() -> None:
    # загрузка ML-модели
    logger.info("Loading ML model...")
    model = load_or_train_model(DEFAULT_MODEL_PATH)
    logger.info("ML model loaded")

    await redis_client.start()

    # продюсер для отправки в DLQ
    producer = KafkaProducerClient()
    await producer.start()

    # kafka consumer
    consumer = AIOKafkaConsumer(
        MODERATION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(
        "Consumer started (topic=%s, group=%s)",
        MODERATION_TOPIC,
        CONSUMER_GROUP,
    )

    try:
        async for msg in consumer:
            try:
                await process_message(model, msg.value, producer)
            except Exception:
                logger.exception("Unhandled error processing message: %s", msg.value)
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.stop()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
