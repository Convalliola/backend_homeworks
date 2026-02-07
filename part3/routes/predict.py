from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, conint, confloat
from errors import ModelNotLoadedError
from services.predict_service import predict_validity
from clients.postgres import get_pg_connection
from repositories.ads import get_ad_by_id
from repositories.users import get_user_by_id

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


@router.post("/simple_predict", response_model=PredictResponse)
async def simple_predict(
    item_id: int = Query(..., ge=1, description="Идентификатор объявления (ads.id), >= 1"),
    model=Depends(get_model),
) -> PredictResponse:
    async with get_pg_connection() as conn:
        ad = await get_ad_by_id(conn, item_id)
        if ad is None:
            raise HTTPException(status_code=404, detail="Ad not found")

        seller = await get_user_by_id(conn, ad.seller_id)
        if seller is None:
            # из-за FK это не должно случаться но пусть будет явная ошибка
            raise HTTPException(status_code=500, detail="Seller not found")

    is_valid, proba = predict_validity(
        model,
        seller_id=ad.seller_id,
        item_id=ad.id,
        is_verified_seller=seller.is_verified,
        images_qty=ad.images_qty,
        description=ad.description,
        category=ad.category,
    )
    return PredictResponse(is_valid=is_valid, probability=proba)