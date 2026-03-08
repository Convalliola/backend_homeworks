from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient


#helpers


@asynccontextmanager
async def fake_pg_connection():
    yield object()


def _fake_ad():
    from repositories.ads import Ad

    return Ad(
        id=10, seller_id=1, name="Item",
        description="Some description", category=5,
        images_qty=2, is_closed=False,
        created_at=datetime.now(tz=timezone.utc),
    )


def _fake_ad_with_seller():
    from repositories.ads import AdWithSeller

    return AdWithSeller(
        ad_id=10, seller_id=1, name="Item",
        description="Some description", category=5,
        images_qty=2, is_verified_seller=True,
    )


def _fake_moderation(
    *, task_id=42, status="pending", is_violation=None, probability=None
):
    from repositories.moderation import ModerationResult

    return ModerationResult(
        id=task_id, item_id=10, status=status,
        is_violation=is_violation, probability=probability,
        error_message=None,
        created_at=datetime.now(tz=timezone.utc),
        processed_at=datetime.now(tz=timezone.utc) if status != "pending" else None,
    )


def _patch_lifespan(monkeypatch):
    """ моки для запуска TestClient (модель, kafka producer, redis) """
    import main
    from clients.kafka import kafka_producer
    from clients.redis import redis_client
    import routes.predict as rp

    monkeypatch.setattr(main, "load_or_train_model", lambda *a, **kw: object())
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(redis_client, "start", AsyncMock())
    monkeypatch.setattr(redis_client, "stop", AsyncMock())
    monkeypatch.setattr(rp.predict_cache, "get_moderation", AsyncMock(return_value=None))
    monkeypatch.setattr(rp.predict_cache, "set_moderation", AsyncMock())
    monkeypatch.setattr(rp.predict_cache, "invalidate_by_item", AsyncMock())
    monkeypatch.setattr(rp.predict_cache, "invalidate_moderation", AsyncMock())


# POST /async_predict
def test_async_predict_creates_task(monkeypatch):
    """успешное создание задачи модерации"""
    import main
    import routes.predict as rp
    from clients.kafka import kafka_producer

    _patch_lifespan(monkeypatch)
    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

    async def fake_get_ad(_conn, _id):
        return _fake_ad()

    async def fake_create_mod(_conn, *, item_id):
        return _fake_moderation()

    monkeypatch.setattr(rp, "get_ad_by_id", fake_get_ad)
    monkeypatch.setattr(rp, "create_moderation_request", fake_create_mod)

    mock_send = AsyncMock()
    monkeypatch.setattr(kafka_producer, "send_moderation_request", mock_send)

    with TestClient(main.app) as client:
        resp = client.post("/async_predict", params={"item_id": 10})

    assert resp.status_code == 200
    assert resp.json() == {
        "task_id": 42,
        "status": "pending",
        "message": "Moderation request accepted",
    }
    mock_send.assert_awaited_once_with(item_id=10, task_id=42)


def test_async_predict_ad_not_found_404(monkeypatch):
    """объявление не найдено (ошибка404) """
    import main
    import routes.predict as rp

    _patch_lifespan(monkeypatch)
    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

    async def fake_get_ad(_conn, _id):
        return None

    monkeypatch.setattr(rp, "get_ad_by_id", fake_get_ad)

    with TestClient(main.app) as client:
        resp = client.post("/async_predict", params={"item_id": 999})

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Ad not found"


# GET /moderation_result/{task_id}
def test_moderation_result_pending(monkeypatch):
    """статус задачи pending """
    import main
    import routes.predict as rp

    _patch_lifespan(monkeypatch)
    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

    async def fake_get_mod(_conn, _id):
        return _fake_moderation(status="pending")

    monkeypatch.setattr(rp, "get_moderation_by_id", fake_get_mod)

    with TestClient(main.app) as client:
        resp = client.get("/moderation_result/42")

    assert resp.status_code == 200
    assert resp.json() == {
        "task_id": 42,
        "status": "pending",
        "is_violation": None,
        "probability": None,
    }


def test_moderation_result_completed(monkeypatch):
    """Задача завершена, есть is_violation и probability """
    import main
    import routes.predict as rp

    _patch_lifespan(monkeypatch)
    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

    async def fake_get_mod(_conn, _id):
        return _fake_moderation(
            status="completed", is_violation=True, probability=0.87,
        )

    monkeypatch.setattr(rp, "get_moderation_by_id", fake_get_mod)

    with TestClient(main.app) as client:
        resp = client.get("/moderation_result/42")

    assert resp.status_code == 200
    assert resp.json() == {
        "task_id": 42,
        "status": "completed",
        "is_violation": True,
        "probability": 0.87,
    }
    rp.predict_cache.set_moderation.assert_awaited_once()


def test_moderation_result_not_found_404(monkeypatch):
    """Задача не найдена (404)"""
    import main
    import routes.predict as rp

    _patch_lifespan(monkeypatch)
    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)

    async def fake_get_mod(_conn, _id):
        return None

    monkeypatch.setattr(rp, "get_moderation_by_id", fake_get_mod)

    with TestClient(main.app) as client:
        resp = client.get("/moderation_result/999")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Task not found"
    rp.predict_cache.set_moderation.assert_not_awaited()


def test_moderation_result_cache_hit_skips_db(monkeypatch):
    """при cache hit не идём в БД """
    import main
    import routes.predict as rp

    _patch_lifespan(monkeypatch)

    cached_data = {
        "task_id": 42, "status": "completed",
        "is_violation": True, "probability": 0.87,
    }
    monkeypatch.setattr(
        rp.predict_cache, "get_moderation",
        AsyncMock(return_value=cached_data),
    )

    db_called = False

    async def spy_get_mod(_conn, _id):
        nonlocal db_called
        db_called = True
        return _fake_moderation()

    monkeypatch.setattr(rp, "get_pg_connection", fake_pg_connection)
    monkeypatch.setattr(rp, "get_moderation_by_id", spy_get_mod)

    with TestClient(main.app) as client:
        resp = client.get("/moderation_result/42")

    assert resp.status_code == 200
    assert resp.json() == cached_data
    assert not db_called, "DB should NOT be called on cache hit"


# worker успешная обработка сообщения

def _patch_worker_cache(monkeypatch, wm):
    """Мокаем predict_cache в worker-модуле """
    monkeypatch.setattr(wm.predict_cache, "set_by_item", AsyncMock())
    monkeypatch.setattr(wm.predict_cache, "set_moderation", AsyncMock())


def test_worker_process_message_success(monkeypatch):
    import workers.moderation_worker as wm

    monkeypatch.setattr(wm, "get_pg_connection", fake_pg_connection)
    _patch_worker_cache(monkeypatch, wm)

    async def fake_get_ad(_conn, _id):
        return _fake_ad_with_seller()

    monkeypatch.setattr(wm, "get_ad_with_seller", fake_get_ad)
    monkeypatch.setattr(wm, "predict_validity", lambda _m, **kw: (True, 0.85))

    completed = {}

    async def fake_update_completed(
        _conn, *, moderation_id, is_violation, probability
    ):
        completed.update(
            moderation_id=moderation_id,
            is_violation=is_violation,
            probability=probability,
        )
        return _fake_moderation(
            status="completed", is_violation=is_violation, probability=probability,
        )

    monkeypatch.setattr(wm, "update_moderation_completed", fake_update_completed)

    mock_producer = MagicMock()
    mock_producer.send_to_dlq = AsyncMock()

    msg = {"task_id": 42, "item_id": 10}
    asyncio.run(wm.process_message(object(), msg, mock_producer))

    assert completed == {
        "moderation_id": 42,
        "is_violation": False,
        "probability": 0.85,
    }
    mock_producer.send_to_dlq.assert_not_awaited()
    wm.predict_cache.set_by_item.assert_awaited_once()
    wm.predict_cache.set_moderation.assert_awaited_once()


# Worker DLQ при постоянной ошибке (объявление не найдено)

def test_worker_dlq_on_ad_not_found(monkeypatch):
    """сразу failed и DLQ без retry"""
    import workers.moderation_worker as wm

    monkeypatch.setattr(wm, "get_pg_connection", fake_pg_connection)
    _patch_worker_cache(monkeypatch, wm)

    async def fake_get_ad(_conn, _id):
        return None

    monkeypatch.setattr(wm, "get_ad_with_seller", fake_get_ad)

    failed = {}

    async def fake_update_failed(_conn, *, moderation_id, error_message):
        failed.update(moderation_id=moderation_id, error_message=error_message)
        return None

    monkeypatch.setattr(wm, "update_moderation_failed", fake_update_failed)

    mock_producer = MagicMock()
    mock_producer.send_to_dlq = AsyncMock()

    msg = {"task_id": 42, "item_id": 999}
    asyncio.run(wm.process_message(object(), msg, mock_producer))

    # статус обновлен на failed
    assert failed["moderation_id"] == 42
    assert "999" in failed["error_message"]

    # отправка сообщения в DLQ
    mock_producer.send_to_dlq.assert_awaited_once()
    dlq_kwargs = mock_producer.send_to_dlq.call_args.kwargs
    assert dlq_kwargs["retry_count"] == 1
    assert dlq_kwargs["original_message"] == msg
    wm.predict_cache.set_by_item.assert_not_awaited()
    wm.predict_cache.set_moderation.assert_not_awaited()


# worker retry и DLQ при временной ошибке модели

def test_worker_retries_then_dlq_on_prediction_error(monkeypatch):
    """
    если модель недоступна, делаем 3 попытки (0, 1, 2) и отправляем в DLQ с retry_count=3
    """
    import workers.moderation_worker as wm

    monkeypatch.setattr(wm, "get_pg_connection", fake_pg_connection)
    _patch_worker_cache(monkeypatch, wm)
    # убираем задержку, чтобы тест не ждал
    monkeypatch.setattr(wm, "RETRY_DELAY_SECONDS", 0)

    async def fake_get_ad(_conn, _id):
        return _fake_ad_with_seller()

    monkeypatch.setattr(wm, "get_ad_with_seller", fake_get_ad)

    attempt_count = 0

    def failing_predict(_m, **kw):
        nonlocal attempt_count
        attempt_count += 1
        raise RuntimeError("ML model unavailable")

    monkeypatch.setattr(wm, "predict_validity", failing_predict)

    failed = {}

    async def fake_update_failed(_conn, *, moderation_id, error_message):
        failed.update(moderation_id=moderation_id, error_message=error_message)
        return None

    monkeypatch.setattr(wm, "update_moderation_failed", fake_update_failed)

    mock_producer = MagicMock()
    mock_producer.send_to_dlq = AsyncMock()

    msg = {"task_id": 42, "item_id": 10}
    asyncio.run(wm.process_message(object(), msg, mock_producer))

    # 3 попытки предсказания
    assert attempt_count == 3

    # если больше нет попыток, то статус failed
    assert failed["moderation_id"] == 42
    assert "ML model unavailable" in failed["error_message"]

    # одно сообщение в DLQ с retry_count=3
    mock_producer.send_to_dlq.assert_awaited_once()
    dlq_kwargs = mock_producer.send_to_dlq.call_args.kwargs
    assert dlq_kwargs["retry_count"] == 3
    assert "ML model unavailable" in dlq_kwargs["error"]
