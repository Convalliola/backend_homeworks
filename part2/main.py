from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI

from model import load_or_train_model, DEFAULT_MODEL_PATH
from mlflow_utils import load_model_from_mlflow, parse_bool_env, default_tracking_uri
from routes.predict import router as predict_router


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    try:
        use_mlflow = parse_bool_env("USE_MLFLOW", default=False)
        if use_mlflow:
            model_name = os.getenv("MLFLOW_MODEL_NAME", "moderation-model")
            stage = os.getenv("MLFLOW_MODEL_STAGE", "Production")
            tracking_uri = os.getenv("MLFLOW_TRACKING_URI") or default_tracking_uri()
            app.state.model = load_model_from_mlflow(
                model_name=model_name,
                stage=stage,
                tracking_uri=tracking_uri,
            )
            logger.info(
                "ML model is ready (mlflow): name=%s stage=%s tracking_uri=%s",
                model_name,
                stage,
                tracking_uri,
            )
        else:
            app.state.model = load_or_train_model(DEFAULT_MODEL_PATH)
            logger.info("ML model is ready (local): %s", DEFAULT_MODEL_PATH)
    except Exception:
        logger.exception("Failed to initialize ML model")
        raise

    yield

    #logger.info("Service shutdown")


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