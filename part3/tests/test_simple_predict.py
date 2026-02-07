from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi.testclient import TestClient


def test_simple_predict_success_passes_db_fields(monkeypatch):
    import main
    import routes.predict as predict_routes
    from repositories.ads import Ad
    from repositories.users import User

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: object())

    @asynccontextmanager
    async def fake_get_pg_connection():
        yield object()

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

    fake_ad = Ad(
        id=10,
        seller_id=1,
        name="Item",
        description="Some description",
        category=5,
        images_qty=2,
        created_at=datetime.now(tz=timezone.utc),
    )
    fake_user = User(
        id=1,
        is_verified=True,
        created_at=datetime.now(tz=timezone.utc),
    )

    async def fake_get_ad_by_id(_conn, _id):
        return fake_ad

    async def fake_get_user_by_id(_conn, _id):
        return fake_user

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_by_id", fake_get_ad_by_id)
    monkeypatch.setattr(predict_routes, "get_user_by_id", fake_get_user_by_id)
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

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: object())

    @asynccontextmanager
    async def fake_get_pg_connection():
        yield object()

    async def fake_get_ad_by_id(_conn, _id):
        return None

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_by_id", fake_get_ad_by_id)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 999})

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Ad not found"


def test_simple_predict_negative_result(monkeypatch):
    import main
    import routes.predict as predict_routes
    from repositories.ads import Ad
    from repositories.users import User

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: object())

    @asynccontextmanager
    async def fake_get_pg_connection():
        yield object()

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

    fake_ad = Ad(
        id=11,
        seller_id=2,
        name="Item",
        description="Some description",
        category=5,
        images_qty=2,
        created_at=datetime.now(tz=timezone.utc),
    )
    fake_user = User(
        id=2,
        is_verified=False,
        created_at=datetime.now(tz=timezone.utc),
    )

    async def fake_get_ad_by_id(_conn, _id):
        return fake_ad

    async def fake_get_user_by_id(_conn, _id):
        return fake_user

    monkeypatch.setattr(predict_routes, "get_pg_connection", fake_get_pg_connection)
    monkeypatch.setattr(predict_routes, "get_ad_by_id", fake_get_ad_by_id)
    monkeypatch.setattr(predict_routes, "get_user_by_id", fake_get_user_by_id)
    monkeypatch.setattr(predict_routes, "predict_validity", fake_predict_validity)

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 11})

    assert resp.status_code == 200
    assert resp.json() == {"is_valid": False, "probability": 0.1}


def test_simple_predict_validation(monkeypatch):
    import main

    monkeypatch.setattr(main, "load_or_train_model", lambda *_args, **_kwargs: object())

    with TestClient(main.app) as client:
        resp = client.post("/simple_predict", params={"item_id": 0})
    assert resp.status_code == 422

