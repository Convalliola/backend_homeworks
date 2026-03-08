from __future__ import annotations
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient


def _patch_infra(monkeypatch):
    """мок redis и kafka в lifespan и кэш в роутах """
    from clients.kafka import kafka_producer
    from clients.redis import redis_client
    import routes.predict as rp

    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(redis_client, "start", AsyncMock())
    monkeypatch.setattr(redis_client, "stop", AsyncMock())
    monkeypatch.setattr(rp.predict_cache, "get_by_features", AsyncMock(return_value=None))
    monkeypatch.setattr(rp.predict_cache, "set_by_features", AsyncMock())


class FakeModel:
    def __init__(self, proba: float):
        self._proba = float(proba)

    def predict_proba(self, X):
        return [[1.0 - self._proba, self._proba]]


def make_payload(**overrides):
    payload = {
        "seller_id": 1,
        "is_verified_seller": False,
        "item_id": 10,
        "name": "Item",
        "description": "Some description",
        "category": 5,
        "images_qty": 2,
    }
    payload.update(overrides)
    return payload


def test_predict_success_is_valid_true(monkeypatch):
    import main
    import routes.predict as rp

    _patch_infra(monkeypatch)
    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.9))

    with TestClient(main.app) as client:
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 200
    assert resp.json()["is_valid"] is True
    assert abs(resp.json()["probability"] - 0.9) < 1e-9
    rp.predict_cache.set_by_features.assert_awaited_once()


def test_predict_success_is_valid_false(monkeypatch):
    import main

    _patch_infra(monkeypatch)
    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.1))

    with TestClient(main.app) as client:
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 200
    assert resp.json()["is_valid"] is False
    assert abs(resp.json()["probability"] - 0.1) < 1e-9


def test_predict_cache_hit_returns_cached(monkeypatch):
    """При cache hit модель не вызывается, ответ из кэша"""
    import main
    import routes.predict as rp
    from storages.predict_cache import CachedPrediction

    _patch_infra(monkeypatch)
    monkeypatch.setattr(
        rp.predict_cache, "get_by_features",
        AsyncMock(return_value=CachedPrediction(is_valid=True, probability=0.95)),
    )

    predict_called = False
    original_predict = rp.predict_validity

    def spy_predict(*a, **kw):
        nonlocal predict_called
        predict_called = True
        return original_predict(*a, **kw)

    monkeypatch.setattr(rp, "predict_validity", spy_predict)

    with TestClient(main.app) as client:
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 200
    assert resp.json() == {"is_valid": True, "probability": 0.95}
    assert not predict_called, "model should NOT be called on cache hit"


def test_predict_validation_invalid_types(monkeypatch):
    import main

    _patch_infra(monkeypatch)
    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.1))

    with TestClient(main.app) as client:
        resp = client.post(
            "/predict",
            json=make_payload(
                seller_id="not-an-int",
                images_qty="not-an-int",
                is_verified_seller="not-a-bool",
            ),
        )

    assert resp.status_code == 422


def test_predict_model_unavailable_returns_503(monkeypatch):
    import main

    _patch_infra(monkeypatch)
    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.1))

    with TestClient(main.app) as client:
        if hasattr(client.app.state, "model"):
            delattr(client.app.state, "model")
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 503
    assert "detail" in resp.json()