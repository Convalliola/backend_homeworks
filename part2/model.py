from __future__ import annotations

import pickle
from pathlib import Path
import random
from typing import Any

import logging
logger = logging.getLogger(__name__)
DEFAULT_MODEL_PATH = Path(__file__).with_name("model.pkl")


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