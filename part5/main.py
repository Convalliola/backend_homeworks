from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from model import load_or_train_model, DEFAULT_MODEL_PATH
from routes.predict import router as predict_router
from clients.kafka import kafka_producer
from clients.redis import redis_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    try:
        app.state.model = load_or_train_model(DEFAULT_MODEL_PATH)
        logger.info("ML model is ready: %s", DEFAULT_MODEL_PATH)
    except Exception:
        logger.exception("Failed to initialize ML model")
        raise

    await kafka_producer.start()
    await redis_client.start()

    yield

    await redis_client.stop()
    await kafka_producer.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(predict_router)

from fastapi import Request
from fastapi.responses import JSONResponse
from errors import ModelNotLoadedError, PredictionError

@app.exception_handler(ModelNotLoadedError)
async def model_not_loaded_handler(request: Request, exc: ModelNotLoadedError):
    return JSONResponse(status_code=503, content={"detail": str(exc)})

@app.exception_handler(PredictionError)
async def prediction_error_handler(request: Request, exc: PredictionError):
    return JSONResponse(status_code=500, content={"detail": str(exc)})