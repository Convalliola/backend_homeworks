import os
import time
from contextlib import asynccontextmanager
import logging

import sentry_sdk
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
#from prometheus_fastapi_instrumentator import Instrumentator

from model import load_or_train_model, DEFAULT_MODEL_PATH
from routes.predict import router as predict_router
from clients.kafka import kafka_producer
from clients.redis import redis_client
from errors import ModelNotLoadedError, PredictionError, AdvertisementNotFoundError


sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN", ""),
    traces_sample_rate=1.0,
    environment=os.environ.get("SENTRY_ENVIRONMENT", "development"),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("app")

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        method = request.method
        endpoint = request.url.path
        start_time = time.time()

        response = await call_next(request)

        duration = time.time() - start_time
        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=response.status_code).inc()
        REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.model = load_or_train_model(DEFAULT_MODEL_PATH)
        logger.info("ML model is ready: %s", DEFAULT_MODEL_PATH)
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        logger.exception("Failed to initialize ML model")
        raise

    await kafka_producer.start()
    await redis_client.start()

    yield

    await redis_client.stop()
    await kafka_producer.stop()


app = FastAPI(lifespan=lifespan)

app.add_middleware(PrometheusMiddleware)

#Instrumentator().instrument(app).expose(app)

app.include_router(predict_router)


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.exception_handler(ModelNotLoadedError)
async def model_not_loaded_handler(request: Request, exc: ModelNotLoadedError):
    sentry_sdk.capture_exception(exc)
    return JSONResponse(status_code=503, content={"detail": str(exc)})


@app.exception_handler(PredictionError)
async def prediction_error_handler(request: Request, exc: PredictionError):
    sentry_sdk.capture_exception(exc)
    return JSONResponse(status_code=500, content={"detail": str(exc)})

@app.exception_handler(AdvertisementNotFoundError)
async def advertisement_not_found_handler(request: Request, exc: AdvertisementNotFoundError):
    sentry_sdk.capture_exception(exc)
    return JSONResponse(status_code=404, content={"detail": str(exc)})


# тестовый эндпоинт для проверки Sentry интеграции
@app.get("/sentry-debug")
async def trigger_error():
    raise PredictionError("Test Sentry integration")