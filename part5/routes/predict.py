from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, conint, confloat
from errors import ModelNotLoadedError
from services.predict_service import predict_validity
from clients.postgres import get_pg_connection
from repositories.ads import get_ad_by_id, get_ad_with_seller, close_ad
from repositories.moderation import create_moderation_request, get_moderation_by_id, delete_moderation_by_item
from clients.kafka import kafka_producer
from storages.predict_cache import predict_cache

router = APIRouter()


class PredictRequest(BaseModel):
    seller_id: int
    is_verified_seller: bool
    item_id: int
    name: str
    description: str
    category: int
    images_qty: conint(ge=0, le=10) = Field(..., description="0..10")


class PredictResponse(BaseModel):
    is_valid: bool
    probability: confloat(ge=0.0, le=1.0)


class AsyncPredictResponse(BaseModel):
    task_id: int
    status: str
    message: str


class ModerationResultResponse(BaseModel):
    task_id: int
    status: str
    is_violation: bool | None
    probability: float | None


class CloseAdResponse(BaseModel):
    item_id: int
    message: str


def get_model(request: Request):
    model = getattr(request.app.state, "model", None)
    if model is None:
        raise ModelNotLoadedError("ML model is not loaded")
    return model


@router.post("/predict", response_model=PredictResponse)
async def predict_handler(req: PredictRequest, model=Depends(get_model)) -> PredictResponse:
    cached = await predict_cache.get_by_features(
        is_verified_seller=req.is_verified_seller,
        images_qty=req.images_qty,
        description=req.description,
        category=req.category,
    )
    if cached is not None:
        return PredictResponse(is_valid=cached.is_valid, probability=cached.probability)

    is_valid, proba = predict_validity(
        model,
        seller_id=req.seller_id,
        item_id=req.item_id,
        is_verified_seller=req.is_verified_seller,
        images_qty=req.images_qty,
        description=req.description,
        category=req.category,
    )

    await predict_cache.set_by_features(
        is_valid=is_valid,
        probability=proba,
        is_verified_seller=req.is_verified_seller,
        images_qty=req.images_qty,
        description=req.description,
        category=req.category,
    )

    return PredictResponse(is_valid=is_valid, probability=proba)


@router.get("/moderation_result/{task_id}", response_model=ModerationResultResponse)
async def moderation_result(task_id: int) -> ModerationResultResponse:
    cached = await predict_cache.get_moderation(task_id)
    if cached is not None:
        return ModerationResultResponse(**cached)

    async with get_pg_connection() as conn:
        moderation = await get_moderation_by_id(conn, task_id)
        if moderation is None:
            raise HTTPException(status_code=404, detail="Task not found")

    await predict_cache.set_moderation(
        moderation.id,
        status=moderation.status,
        is_violation=moderation.is_violation,
        probability=moderation.probability,
    )

    return ModerationResultResponse(
        task_id=moderation.id,
        status=moderation.status,
        is_violation=moderation.is_violation,
        probability=moderation.probability,
    )


@router.post("/async_predict", response_model=AsyncPredictResponse)
async def async_predict(
    item_id: int = Query(..., ge=1, description="Идентификатор объявления (ads.id), >= 1"),
) -> AsyncPredictResponse:
    async with get_pg_connection() as conn:
        ad = await get_ad_by_id(conn, item_id)
        if ad is None:
            raise HTTPException(status_code=404, detail="Ad not found")

        moderation = await create_moderation_request(conn, item_id=item_id)

    await kafka_producer.send_moderation_request(
        item_id=item_id,
        task_id=moderation.id,
    )

    return AsyncPredictResponse(
        task_id=moderation.id,
        status="pending",
        message="Moderation request accepted",
    )


@router.post("/simple_predict", response_model=PredictResponse)
async def simple_predict(
    item_id: int = Query(..., ge=1, description="Идентификатор объявления (ads.id), >= 1"),
    model=Depends(get_model),
) -> PredictResponse:
    cached = await predict_cache.get_by_item(item_id)
    if cached is not None:
        return PredictResponse(is_valid=cached.is_valid, probability=cached.probability)

    async with get_pg_connection() as conn:
        row = await get_ad_with_seller(conn, item_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Ad not found")

    is_valid, proba = predict_validity(
        model,
        seller_id=row.seller_id,
        item_id=row.ad_id,
        is_verified_seller=row.is_verified_seller,
        images_qty=row.images_qty,
        description=row.description,
        category=row.category,
    )

    await predict_cache.set_by_item(item_id, is_valid, proba)

    return PredictResponse(is_valid=is_valid, probability=proba)


@router.post("/close", response_model=CloseAdResponse)
async def close_ad_handler(
    item_id: int = Query(..., ge=1, description="Идентификатор объявления (ads.id), >= 1"),
) -> CloseAdResponse:
    async with get_pg_connection() as conn:
        ad = await close_ad(conn, item_id)
        if ad is None:
            raise HTTPException(
                status_code=404,
                detail="Ad not found or already closed",
            )

        deleted_task_ids = await delete_moderation_by_item(conn, item_id)

    await predict_cache.invalidate_by_item(item_id)
    for task_id in deleted_task_ids:
        await predict_cache.invalidate_moderation(task_id)

    return CloseAdResponse(item_id=item_id, message="Ad closed")