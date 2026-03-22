from __future__ import annotations

import os
import pickle
from pathlib import Path
import random
from typing import Any

import logging
logger = logging.getLogger(__name__)
DEFAULT_MODEL_PATH = Path(__file__).with_name("model.pkl")

_default_mlflow_db = Path(__file__).with_name("mlflow.db").resolve()
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{_default_mlflow_db.as_posix()}")
MLFLOW_MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "moderation-model")
MLFLOW_MODEL_STAGE = os.environ.get("MLFLOW_MODEL_STAGE", "Production")


def _use_mlflow() -> bool:
    return os.environ.get("USE_MLFLOW", "false").lower() in ("true", "1", "yes")


def train_model():
    # Признаки is_verified_seller, images_qty, description_length, category
    random.seed(42)
    X = [[random.random() for _ in range(4)] for _ in range(1000)]

    # Целевая переменная: 1 валидное объявление, 0 невалидное
    y_violation = [(row[0] < 0.3) and (row[1] < 0.2) for row in X]
    y = [0 if is_bad else 1 for is_bad in y_violation]

    from sklearn.linear_model import LogisticRegression  # type: ignore[import-not-found]

    model = LogisticRegression()
    model.fit(X, y)
    return model


def save_model(model: Any, path: Path = DEFAULT_MODEL_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)


def load_model(path: Path = DEFAULT_MODEL_PATH):
    with open(path, "rb") as f:
        return pickle.load(f)


def register_model_in_mlflow(
    model: Any | None = None,
    model_name: str = MLFLOW_MODEL_NAME,
    tracking_uri: str = MLFLOW_TRACKING_URI,
) -> None:
    """Обучает (если модель не передана) и регистрирует модель в MLflow Model Registry"""
    import mlflow
    from mlflow.sklearn import log_model

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("moderation-model")

    if model is None:
        model = train_model()

    with mlflow.start_run():
        log_model(model, "model", registered_model_name=model_name)
        logger.info("Model registered in MLflow as '%s'", model_name)


def load_model_from_mlflow(
    model_name: str = MLFLOW_MODEL_NAME,
    stage: str = MLFLOW_MODEL_STAGE,
    tracking_uri: str = MLFLOW_TRACKING_URI,
):
    """Загружает модель из MLflow Model Registry по имени и стадии"""
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    model_uri = f"models:/{model_name}/{stage}"
    logger.info("Loading model from MLflow: %s", model_uri)
    return mlflow.sklearn.load_model(model_uri)


def load_or_train_model(path: Path = DEFAULT_MODEL_PATH):
    """загружает модель из model.pkl, если файла нет обучает и сохраняет"""
    if path.exists():
        model = load_model(path)
        logger.info("Loaded model from %s", path)
        return model
    else:
        model = train_model()
        save_model(model, path)
        logger.info("Trained and saved model to %s", path)
        return model


def get_model(path: Path = DEFAULT_MODEL_PATH):
    """
    Загружает модель в зависимости от USE_MLFLOW:
      - true  -> из MLflow Model Registry
      - false -> из локального файла model.pkl (если файла нет, то обучает)
    """
    if _use_mlflow():
        return load_model_from_mlflow()
    return load_or_train_model(path)
