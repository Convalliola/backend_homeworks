from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
MODERATION_TOPIC = "moderation"
MODERATION_DLQ_TOPIC = "moderation_dlq"


class KafkaProducerClient:
    """асинхронный кафка-продюсер для отправки сообщений"""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """запуск продюсера(вызов при старте приложения)"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()
        logger.info("Kafka producer started (servers=%s)", self._bootstrap_servers)

    async def stop(self) -> None:
        """остановка продюсера(при завершении)"""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def send_moderation_request(self, item_id: int, task_id: int) -> None:
        """отправка запроса на модерацию в топик moderation"""
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        message = {
            "task_id": task_id,
            "item_id": item_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._producer.send_and_wait(MODERATION_TOPIC, value=message)
        logger.info(
            "Sent moderation request: task_id=%s item_id=%s to topic=%s",
            task_id,
            item_id,
            MODERATION_TOPIC,
        )


    async def send_to_dlq(
        self,
        original_message: dict,
        error: str,
        retry_count: int = 1,
    ) -> None:
        """Отправка сообщение в DLQ """
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        dlq_message = {
            "original_message": original_message,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retry_count": retry_count,
        }
        await self._producer.send_and_wait(MODERATION_DLQ_TOPIC, value=dlq_message)
        logger.info(
            "Sent message to DLQ: topic=%s error=%s",
            MODERATION_DLQ_TOPIC,
            error,
        )


kafka_producer = KafkaProducerClient()
