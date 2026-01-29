from __future__ import annotations

import logging

from errors import PredictionError

logger = logging.getLogger("app.predict")


def to_features(*, is_verified_seller: bool, images_qty: int, description: str, category: int) -> list[list[float]]:
    x0 = 1.0 if is_verified_seller else 0.0
    x1 = float(images_qty) / 10.0
    x2 = float(len(description)) / 1000.0
    x3 = float(category) / 100.0
    return [[x0, x1, x2, x3]]


def predict_validity(
    model,
    *,
    seller_id: int,
    item_id: int,
    is_verified_seller: bool,
    images_qty: int,
    description: str,
    category: int,
) -> tuple[bool, float]:
    X = to_features(
        is_verified_seller=is_verified_seller,
        images_qty=images_qty,
        description=description,
        category=category,
    )

    features = X[0]
    logger.info(
        "predict_request seller_id=%s item_id=%s features=%s",
        seller_id, item_id, features
    )

    try:
        proba = float(model.predict_proba(X)[0][1])
        is_valid = bool(proba >= 0.5)
    except Exception as e:
        logger.exception(
            "predict_failed seller_id=%s item_id=%s",
            seller_id,
            item_id,
        )
        raise PredictionError("Prediction failed") from e

    logger.info(
        "predict_result seller_id=%s item_id=%s is_valid=%s probability=%.6f",
        seller_id, item_id, is_valid, proba
    )

    return is_valid, proba