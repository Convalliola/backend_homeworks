from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, conint, confloat
from errors import ModelNotLoadedError
from services.predict_service import predict_validity
from clients.postgres import get_pg_connection
from repositories.ads import get_ad_by_id, get_ad_with_seller
from repositories.moderation import create_moderation_request, get_moderation_by_id
from clients.kafka import kafka_producer

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


def get_model(request: Request):
    model = getattr(request.app.state, "model", None)
    if model is None:
        raise ModelNotLoadedError("ML model is not loaded")
    return model


@router.post("/predict", response_model=PredictResponse)
def predict_handler(req: PredictRequest, model=Depends(get_model)) -> PredictResponse:
    is_valid, proba = predict_validity(
        model,
        seller_id=req.seller_id,
        item_id=req.item_id,
        is_verified_seller=req.is_verified_seller,
        images_qty=req.images_qty,
        description=req.description,
        category=req.category,
    )
    return PredictResponse(is_valid=is_valid, probability=proba)


@router.get("/moderation_result/{task_id}", response_model=ModerationResultResponse)
async def moderation_result(task_id: int) -> ModerationResultResponse:
    async with get_pg_connection() as conn:
        moderation = await get_moderation_by_id(conn, task_id)
        if moderation is None:
            raise HTTPException(status_code=404, detail="Task not found")

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
    return PredictResponse(is_valid=is_valid, probability=proba)