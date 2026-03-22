"""
Скрипт для обучения модели и регистрации её в MLflow Model Registry.

Использование:
    python register_model.py

Переменные окружения:
    MLFLOW_TRACKING_URI  — URI трекинг-сервера (по умолчанию sqlite:///mlflow.db)
    MLFLOW_MODEL_NAME    — имя модели в реестре  (по умолчанию moderation-model)
"""

import logging

from model import train_model, register_model_in_mlflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Обучаем модель...")
    model = train_model()

    logger.info("Регистрируем модель в MLflow...")
    register_model_in_mlflow(model=model)

    logger.info("Готово. Модель зарегистрирована и готова к использованию в production")


if __name__ == "__main__":
    main()
