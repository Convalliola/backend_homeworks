from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def default_tracking_uri() -> str:
    db_path = Path(__file__).with_name("mlflow.db").resolve()
    return f"sqlite:///{db_path.as_posix()}"


def load_model_from_mlflow(model_name: str, stage: str = "Production", tracking_uri: str | None = None) -> Any:
    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(tracking_uri or default_tracking_uri())
    model_uri = f"models:/{model_name}/{stage}"
    return mlflow.sklearn.load_model(model_uri)

