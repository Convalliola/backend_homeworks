"""
Интеграционные тесты репозиториев.

Каждый тест работает с postgreSQL через фикстуру pg_conn,
оборачивающую тест в транзакцию с откатом, тесты полностью изолированы друг от друга.

Запускается: pytest tests/test_pg_repositories.py -v -m integration
Пропускаются автоматически, если postgreSQL недоступен
"""

from __future__ import annotations

import pytest

from repositories.users import (
    create_user,
    get_user_by_id,
    list_users,
    set_user_verified,
    delete_user,
)
from repositories.ads import (
    Ad,
    create_ad,
    get_ad_by_id,
    list_ads,
    get_ad_with_seller,
    close_ad,
    delete_ad,
)
from repositories.moderation import (
    create_moderation_request,
    get_moderation_by_id,
    update_moderation_completed,
    update_moderation_failed,
    delete_moderation_by_item,
)


# хэлперы

async def _make_user(conn, *, is_verified=False):
    return await create_user(conn, is_verified=is_verified)


async def _make_ad(conn, *, seller_id=None, **kw):
    if seller_id is None:
        user = await _make_user(conn)
        seller_id = user.id
    defaults = dict(
        seller_id=seller_id,
        name="Test Ad",
        description="Test description for ad",
        category=3,
        images_qty=2,
    )
    defaults.update(kw)
    return await create_ad(conn, **defaults)


# тесты для Users

@pytest.mark.integration
class TestUsersRepository:

    @pytest.mark.asyncio
    async def test_create_user_default_not_verified(self, pg_conn):
        user = await create_user(pg_conn)
        assert user.id is not None
        assert user.is_verified is False
        assert user.created_at is not None

    @pytest.mark.asyncio
    async def test_create_user_verified(self, pg_conn):
        user = await create_user(pg_conn, is_verified=True)
        assert user.is_verified is True

    @pytest.mark.asyncio
    async def test_get_user_by_id(self, pg_conn):
        created = await create_user(pg_conn, is_verified=True)
        fetched = await get_user_by_id(pg_conn, created.id)

        assert fetched is not None
        assert fetched.id == created.id
        assert fetched.is_verified is True

    @pytest.mark.asyncio
    async def test_get_user_by_id_not_found(self, pg_conn):
        result = await get_user_by_id(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_list_users(self, pg_conn):
        await create_user(pg_conn)
        await create_user(pg_conn, is_verified=True)

        users = await list_users(pg_conn)
        assert len(users) >= 2

    @pytest.mark.asyncio
    async def test_list_users_pagination(self, pg_conn):
        for _ in range(5):
            await create_user(pg_conn)

        page1 = await list_users(pg_conn, limit=2, offset=0)
        page2 = await list_users(pg_conn, limit=2, offset=2)

        assert len(page1) == 2
        assert len(page2) == 2
        assert page1[0].id != page2[0].id

    @pytest.mark.asyncio
    async def test_set_user_verified(self, pg_conn):
        user = await create_user(pg_conn, is_verified=False)
        assert user.is_verified is False

        updated = await set_user_verified(pg_conn, user_id=user.id, is_verified=True)
        assert updated is not None
        assert updated.is_verified is True

    @pytest.mark.asyncio
    async def test_set_user_verified_nonexistent(self, pg_conn):
        result = await set_user_verified(pg_conn, user_id=999999, is_verified=True)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_user(self, pg_conn):
        user = await create_user(pg_conn)

        deleted = await delete_user(pg_conn, user.id)
        assert deleted is True

        fetched = await get_user_by_id(pg_conn, user.id)
        assert fetched is None

    @pytest.mark.asyncio
    async def test_delete_user_cascades_to_ads(self, pg_conn):
        user = await create_user(pg_conn)
        ad = await _make_ad(pg_conn, seller_id=user.id)

        await delete_user(pg_conn, user.id)

        fetched_ad = await get_ad_by_id(pg_conn, ad.id)
        assert fetched_ad is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_user(self, pg_conn):
        deleted = await delete_user(pg_conn, 999999)
        assert deleted is False


# тесты для Ads

@pytest.mark.integration
class TestAdsRepository:

    @pytest.mark.asyncio
    async def test_create_ad(self, pg_conn):
        ad = await _make_ad(pg_conn, name="My Ad", description="Desc", category=5, images_qty=3)

        assert ad.id is not None
        assert ad.name == "My Ad"
        assert ad.description == "Desc"
        assert ad.category == 5
        assert ad.images_qty == 3
        assert ad.is_closed is False
        assert ad.created_at is not None

    @pytest.mark.asyncio
    async def test_get_ad_by_id(self, pg_conn):
        ad = await _make_ad(pg_conn)
        fetched = await get_ad_by_id(pg_conn, ad.id)

        assert fetched is not None
        assert fetched.id == ad.id
        assert fetched.name == ad.name

    @pytest.mark.asyncio
    async def test_get_ad_by_id_not_found(self, pg_conn):
        result = await get_ad_by_id(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_list_ads_all(self, pg_conn):
        user = await _make_user(pg_conn)
        await _make_ad(pg_conn, seller_id=user.id)
        await _make_ad(pg_conn, seller_id=user.id)

        ads = await list_ads(pg_conn)
        assert len(ads) >= 2

    @pytest.mark.asyncio
    async def test_list_ads_by_seller(self, pg_conn):
        user1 = await _make_user(pg_conn)
        user2 = await _make_user(pg_conn)
        await _make_ad(pg_conn, seller_id=user1.id)
        await _make_ad(pg_conn, seller_id=user1.id)
        await _make_ad(pg_conn, seller_id=user2.id)

        ads_user1 = await list_ads(pg_conn, seller_id=user1.id)
        ads_user2 = await list_ads(pg_conn, seller_id=user2.id)

        assert len(ads_user1) == 2
        assert len(ads_user2) == 1

    @pytest.mark.asyncio
    async def test_list_ads_pagination(self, pg_conn):
        user = await _make_user(pg_conn)
        for _ in range(4):
            await _make_ad(pg_conn, seller_id=user.id)

        page = await list_ads(pg_conn, limit=2, offset=0)
        assert len(page) == 2

    @pytest.mark.asyncio
    async def test_get_ad_with_seller(self, pg_conn):
        user = await _make_user(pg_conn, is_verified=True)
        ad = await _make_ad(pg_conn, seller_id=user.id)

        row = await get_ad_with_seller(pg_conn, ad.id)

        assert row is not None
        assert row.ad_id == ad.id
        assert row.seller_id == user.id
        assert row.is_verified_seller is True
        assert row.name == ad.name

    @pytest.mark.asyncio
    async def test_get_ad_with_seller_not_found(self, pg_conn):
        result = await get_ad_with_seller(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_close_ad(self, pg_conn):
        ad = await _make_ad(pg_conn)
        assert ad.is_closed is False

        closed = await close_ad(pg_conn, ad.id)
        assert closed is not None
        assert closed.is_closed is True
        assert closed.id == ad.id

        fetched = await get_ad_by_id(pg_conn, ad.id)
        assert fetched.is_closed is True

    @pytest.mark.asyncio
    async def test_close_ad_already_closed(self, pg_conn):
        ad = await _make_ad(pg_conn)
        await close_ad(pg_conn, ad.id)

        result = await close_ad(pg_conn, ad.id)
        assert result is None

    @pytest.mark.asyncio
    async def test_close_ad_not_found(self, pg_conn):
        result = await close_ad(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_ad(self, pg_conn):
        ad = await _make_ad(pg_conn)

        deleted = await delete_ad(pg_conn, ad.id)
        assert deleted is True

        fetched = await get_ad_by_id(pg_conn, ad.id)
        assert fetched is None

    @pytest.mark.asyncio
    async def test_delete_ad_cascades_to_moderation(self, pg_conn):
        ad = await _make_ad(pg_conn)
        mod = await create_moderation_request(pg_conn, item_id=ad.id)

        await delete_ad(pg_conn, ad.id)

        fetched_mod = await get_moderation_by_id(pg_conn, mod.id)
        assert fetched_mod is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_ad(self, pg_conn):
        deleted = await delete_ad(pg_conn, 999999)
        assert deleted is False


# тесты для Moderation

@pytest.mark.integration
class TestModerationRepository:

    @pytest.mark.asyncio
    async def test_create_moderation_request(self, pg_conn):
        ad = await _make_ad(pg_conn)
        mod = await create_moderation_request(pg_conn, item_id=ad.id)

        assert mod.id is not None
        assert mod.item_id == ad.id
        assert mod.status == "pending"
        assert mod.is_violation is None
        assert mod.probability is None
        assert mod.error_message is None
        assert mod.processed_at is None

    @pytest.mark.asyncio
    async def test_get_moderation_by_id(self, pg_conn):
        ad = await _make_ad(pg_conn)
        mod = await create_moderation_request(pg_conn, item_id=ad.id)

        fetched = await get_moderation_by_id(pg_conn, mod.id)
        assert fetched is not None
        assert fetched.id == mod.id
        assert fetched.status == "pending"

    @pytest.mark.asyncio
    async def test_get_moderation_by_id_not_found(self, pg_conn):
        result = await get_moderation_by_id(pg_conn, 999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_update_moderation_completed(self, pg_conn):
        ad = await _make_ad(pg_conn)
        mod = await create_moderation_request(pg_conn, item_id=ad.id)

        updated = await update_moderation_completed(
            pg_conn,
            moderation_id=mod.id,
            is_violation=True,
            probability=0.87,
        )

        assert updated is not None
        assert updated.status == "completed"
        assert updated.is_violation is True
        assert abs(updated.probability - 0.87) < 1e-6
        assert updated.processed_at is not None

    @pytest.mark.asyncio
    async def test_update_moderation_completed_nonexistent(self, pg_conn):
        result = await update_moderation_completed(
            pg_conn,
            moderation_id=999999,
            is_violation=False,
            probability=0.5,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_update_moderation_failed(self, pg_conn):
        ad = await _make_ad(pg_conn)
        mod = await create_moderation_request(pg_conn, item_id=ad.id)

        updated = await update_moderation_failed(
            pg_conn,
            moderation_id=mod.id,
            error_message="ML model error",
        )

        assert updated is not None
        assert updated.status == "failed"
        assert updated.error_message == "ML model error"
        assert updated.processed_at is not None
        assert updated.is_violation is None

    @pytest.mark.asyncio
    async def test_update_moderation_failed_nonexistent(self, pg_conn):
        result = await update_moderation_failed(
            pg_conn,
            moderation_id=999999,
            error_message="err",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_moderation_by_item(self, pg_conn):
        ad = await _make_ad(pg_conn)
        m1 = await create_moderation_request(pg_conn, item_id=ad.id)
        m2 = await create_moderation_request(pg_conn, item_id=ad.id)

        deleted_ids = await delete_moderation_by_item(pg_conn, ad.id)

        assert set(deleted_ids) == {m1.id, m2.id}

        assert await get_moderation_by_id(pg_conn, m1.id) is None
        assert await get_moderation_by_id(pg_conn, m2.id) is None

    @pytest.mark.asyncio
    async def test_delete_moderation_by_item_empty(self, pg_conn):
        ad = await _make_ad(pg_conn)

        deleted_ids = await delete_moderation_by_item(pg_conn, ad.id)
        assert deleted_ids == []

    @pytest.mark.asyncio
    async def test_delete_moderation_does_not_affect_other_items(self, pg_conn):
        user = await _make_user(pg_conn)
        ad1 = await _make_ad(pg_conn, seller_id=user.id)
        ad2 = await _make_ad(pg_conn, seller_id=user.id)

        m1 = await create_moderation_request(pg_conn, item_id=ad1.id)
        m2 = await create_moderation_request(pg_conn, item_id=ad2.id)

        await delete_moderation_by_item(pg_conn, ad1.id)

        assert await get_moderation_by_id(pg_conn, m1.id) is None
        assert await get_moderation_by_id(pg_conn, m2.id) is not None

    @pytest.mark.asyncio
    async def test_full_moderation_lifecycle(self, pg_conn):
        """pending → completed: полный цикл жизни записи модерации."""
        ad = await _make_ad(pg_conn)

        mod = await create_moderation_request(pg_conn, item_id=ad.id)
        assert mod.status == "pending"

        completed = await update_moderation_completed(
            pg_conn,
            moderation_id=mod.id,
            is_violation=False,
            probability=0.95,
        )
        assert completed.status == "completed"
        assert completed.is_violation is False
        assert abs(completed.probability - 0.95) < 1e-6

        fetched = await get_moderation_by_id(pg_conn, mod.id)
        assert fetched.status == "completed"



# Кросс-репозиторные сценарии (close_ad + moderation)

@pytest.mark.integration
class TestCloseAdWithModeration:

    @pytest.mark.asyncio
    async def test_close_ad_then_delete_moderation(self, pg_conn):
        """Сценарий /close: закрываем объявление, удаляем результаты модерации."""
        ad = await _make_ad(pg_conn)
        m1 = await create_moderation_request(pg_conn, item_id=ad.id)
        m2 = await create_moderation_request(pg_conn, item_id=ad.id)

        closed = await close_ad(pg_conn, ad.id)
        assert closed.is_closed is True

        deleted_ids = await delete_moderation_by_item(pg_conn, ad.id)
        assert set(deleted_ids) == {m1.id, m2.id}

        fetched_ad = await get_ad_by_id(pg_conn, ad.id)
        assert fetched_ad.is_closed is True

        assert await get_moderation_by_id(pg_conn, m1.id) is None
        assert await get_moderation_by_id(pg_conn, m2.id) is None

    @pytest.mark.asyncio
    async def test_close_ad_does_not_affect_other_ads(self, pg_conn):
        """Закрытие одного объявления не затрагивает другие."""
        user = await _make_user(pg_conn)
        ad1 = await _make_ad(pg_conn, seller_id=user.id)
        ad2 = await _make_ad(pg_conn, seller_id=user.id)
        m_other = await create_moderation_request(pg_conn, item_id=ad2.id)

        await close_ad(pg_conn, ad1.id)
        await delete_moderation_by_item(pg_conn, ad1.id)

        fetched_ad2 = await get_ad_by_id(pg_conn, ad2.id)
        assert fetched_ad2.is_closed is False

        fetched_m = await get_moderation_by_id(pg_conn, m_other.id)
        assert fetched_m is not None
