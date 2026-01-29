from __future__ import annotations

import os

import mlflow
from mlflow.sklearn import log_model

from model import train_model
from mlflow_utils import default_tracking_uri


def main() -> None:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI") or default_tracking_uri()
    experiment = os.getenv("MLFLOW_EXPERIMENT_NAME", "moderation-model")
    registered_model_name = os.getenv("MLFLOW_MODEL_NAME", "moderation-model")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run():
        model = train_model()
        log_model(model, "model", registered_model_name=registered_model_name)
        print(
            f"Registered model '{registered_model_name}' to MLflow. "
            f"Tracking URI: {tracking_uri}. "
            "Open MLflow UI and transition the latest version to Production."
        )


if __name__ == "__main__":
    main()

