from __future__ import annotations

import pickle
from pathlib import Path
import numpy as np
from sklearn.linear_model import LogisticRegression

import logging
logger = logging.getLogger(__name__)
DEFAULT_MODEL_PATH = Path(__file__).with_name("model.pkl")


def train_model() -> LogisticRegression:
    np.random.seed(42)

    # Признаки is_verified_seller, images_qty, description_length, category
    X = np.random.rand(1000, 4)

    # Целевая переменная: 1 нарушение, 0 нет нарушения
    y = (X[:, 0] < 0.3) & (X[:, 1] < 0.2)
    y = y.astype(int)

    model = LogisticRegression()
    model.fit(X, y)
    return model


def save_model(model: LogisticRegression, path: Path = DEFAULT_MODEL_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)


def load_model(path: Path = DEFAULT_MODEL_PATH) -> LogisticRegression:
    with open(path, "rb") as f:
        return pickle.load(f)


def load_or_train_model(path: Path = DEFAULT_MODEL_PATH) -> LogisticRegression:
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