"""
Юнит-тесты для POST /close
Мокаем БД и кэш, проверяем правильность вызовов
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient


# хэлперы

@asynccontextmanager
async def fake_pg_connection():
    yield object()


def _fake_ad(*, is_closed=False):
    from repositories.ads import Ad
    return Ad(
        id=10, seller_id=1, name="Item",
        description="Some description", category=5,
        images_qty=2, is_closed=is_closed,
        created_at=datetime.now(tz=timezone.utc),
    )


def _patch_lifespan(monkeypatch):
    import main
    from clients.kafka import kafka_producer
    from clients.redis import redis_client

    monkeypatch.setattr(main, "load_or_train_model", lambda *a, **kw: object())
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(redis_client, "start", AsyncMock())
    monkeypatch.setattr(redis_client, "stop", AsyncMock())



# Юнит-тесты /close

class TestCloseAdUnit:

    def test_close_success_invalidates_caches(self, monkeypatch):
        """Успешное закрытие 
        БД обновлена, кэш предсказаний и модерации удалён"""
        import main
        import routes.predict as rp

        _patch_lifespan(monkeypatch)
        monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

        async def fake_close(_conn, _id):
            return _fake_ad(is_closed=True)

        async def fake_delete_mod(_conn, _item_id):
            return [42, 43]

        monkeypatch.setattr(rp, "close_ad", fake_close)
        monkeypatch.setattr(rp, "delete_moderation_by_item", fake_delete_mod)

        mock_inv_item = AsyncMock()
        mock_inv_mod = AsyncMock()
        monkeypatch.setattr(rp, "predict_cache", MagicMock(
            invalidate_by_item=mock_inv_item,
            invalidate_moderation=mock_inv_mod,
        ))

        with TestClient(main.app) as client:
            resp = client.post("/close", params={"item_id": 10})

        assert resp.status_code == 200
        assert resp.json() == {"item_id": 10, "message": "Ad closed"}

        mock_inv_item.assert_awaited_once_with(10)
        assert mock_inv_mod.await_count == 2
        mock_inv_mod.assert_any_await(42)
        mock_inv_mod.assert_any_await(43)

    def test_close_ad_not_found_returns_404(self, monkeypatch):
        """Объявление не найдено или уже закрыто, 404"""
        import main
        import routes.predict as rp

        _patch_lifespan(monkeypatch)
        monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

        async def fake_close(_conn, _id):
            return None

        monkeypatch.setattr(rp, "close_ad", fake_close)

        mock_inv_item = AsyncMock()
        mock_inv_mod = AsyncMock()
        monkeypatch.setattr(rp, "predict_cache", MagicMock(
            invalidate_by_item=mock_inv_item,
            invalidate_moderation=mock_inv_mod,
        ))

        with TestClient(main.app) as client:
            resp = client.post("/close", params={"item_id": 999})

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Ad not found or already closed"

        mock_inv_item.assert_not_awaited()
        mock_inv_mod.assert_not_awaited()

    def test_close_no_moderation_results(self, monkeypatch):
        """закрытие объявления без результатов модерации (invalidate_moderation не вызывается)"""
        import main
        import routes.predict as rp

        _patch_lifespan(monkeypatch)
        monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

        async def fake_close(_conn, _id):
            return _fake_ad(is_closed=True)

        async def fake_delete_mod(_conn, _item_id):
            return []

        monkeypatch.setattr(rp, "close_ad", fake_close)
        monkeypatch.setattr(rp, "delete_moderation_by_item", fake_delete_mod)

        mock_inv_item = AsyncMock()
        mock_inv_mod = AsyncMock()
        monkeypatch.setattr(rp, "predict_cache", MagicMock(
            invalidate_by_item=mock_inv_item,
            invalidate_moderation=mock_inv_mod,
        ))

        with TestClient(main.app) as client:
            resp = client.post("/close", params={"item_id": 10})

        assert resp.status_code == 200
        mock_inv_item.assert_awaited_once_with(10)
        mock_inv_mod.assert_not_awaited()

    def test_close_validation_item_id_zero(self, monkeypatch):
        """item_id < 1  ошибка валидации 422 """
        import main

        _patch_lifespan(monkeypatch)

        with TestClient(main.app) as client:
            resp = client.post("/close", params={"item_id": 0})

        assert resp.status_code == 422
