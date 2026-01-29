from __future__ import annotations
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, conint, confloat
from errors import ModelNotLoadedError
from services.predict_service import predict_validity

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