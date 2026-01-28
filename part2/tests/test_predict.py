from __future__ import annotations
from fastapi.testclient import TestClient


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


def test_predict_success_is_violation_true(monkeypatch):
    import main  

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.9))

    with TestClient(main.app) as client:
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 200
    assert resp.json()["is_violation"] is True
    assert abs(resp.json()["probability"] - 0.9) < 1e-9


def test_predict_success_is_violation_false(monkeypatch):
    import main

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.1))

    with TestClient(main.app) as client:
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 200
    assert resp.json()["is_violation"] is False
    assert abs(resp.json()["probability"] - 0.1) < 1e-9


def test_predict_validation_invalid_types(monkeypatch):
    import main

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

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: FakeModel(0.1))

    with TestClient(main.app) as client:
        if hasattr(client.app.state, "model"):
            delattr(client.app.state, "model")
        resp = client.post("/predict", json=make_payload())

    assert resp.status_code == 503
    assert "detail" in resp.json()

