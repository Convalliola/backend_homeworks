from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient


@asynccontextmanager
async def fake_get_pg_connection():
    yield object()


def _patch_lifespan(monkeypatch):
    """Мокаем загрузку модели и kafka producer для lifespan."""
    import main
    from clients.kafka import kafka_producer

    monkeypatch.setattr(main, "load_or_train_model", lambda *a, **kw: object())
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())


def test_simple_predict_success_passes_db_fields(monkeypatch):
    import main
    import routes.predict as predict_routes
    from repositories.ads import AdWithSeller

    _patch_lifespan(monkeypatch)

    called = {}

    def fake_predict_validity(
        _model,
        *,
        seller_id: int,
        item_id: int,
        is_verified_seller: bool,
        images_qty: int,
        description: str,
        category: int,
    ):
        called.update(
            dict(
                seller_id=seller_id,
                item_id=item_id,
                is_verified_seller=is_verified_seller,
                images_qty=images_qty,
                description=description,
                category=category,
            )
        )
        return True, 0.7

    fake_row = AdWithSeller(
        ad_id=10, seller_id=1, name="Item",
        description="Some description", category=5,
        images_qty=2, is_verified_seller=True,
    )

    async def fake_get_ad_with_seller(_conn, _id):
        return fake_row

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_with_seller", fake_get_ad_with_seller)
    monkeypatch.setattr(predict_routes, "predict_validity", fake_predict_validity)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 10})

    assert resp.status_code == 200
    assert resp.json() == {"is_valid": True, "probability": 0.7}

    assert called == {
        "seller_id": 1,
        "item_id": 10,
        "is_verified_seller": True,
        "images_qty": 2,
        "description": "Some description",
        "category": 5,
    }


def test_simple_predict_ad_not_found_404(monkeypatch):
    import main
    import routes.predict as predict_routes

    _patch_lifespan(monkeypatch)

    async def fake_get_ad_with_seller(_conn, _id):
        return None

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_with_seller", fake_get_ad_with_seller)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 999})

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Ad not found"


def test_simple_predict_negative_result(monkeypatch):
    import main
    import routes.predict as predict_routes
    from repositories.ads import AdWithSeller

    _patch_lifespan(monkeypatch)

    def fake_predict_validity(
        _model,
        *,
        seller_id: int,
        item_id: int,
        is_verified_seller: bool,
        images_qty: int,
        description: str,
        category: int,
    ):
        return False, 0.1

    fake_row = AdWithSeller(
        ad_id=11, seller_id=2, name="Item",
        description="Some description", category=5,
        images_qty=2, is_verified_seller=False,
    )

    async def fake_get_ad_with_seller(_conn, _id):
        return fake_row

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_with_seller", fake_get_ad_with_seller)
    monkeypatch.setattr(predict_routes, "predict_validity", fake_predict_validity)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 11})

    assert resp.status_code == 200
    assert resp.json() == {"is_valid": False, "probability": 0.1}


def test_simple_predict_validation(monkeypatch):
    import main

    _patch_lifespan(monkeypatch)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 0})
    assert resp.status_code == 422
