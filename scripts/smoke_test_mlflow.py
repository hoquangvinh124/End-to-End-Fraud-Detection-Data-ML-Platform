"""
MLflow infrastructure smoke test.

Run:  uv run python scripts/smoke_test_mlflow.py
Requires: MLflow server running at http://localhost:5000
          (docker compose -f src/lakehouse/docker-compose.lakehouse.yml
                          -f src/mlflow/docker-compose.mlflow.yml up -d)

DELETE THIS FILE after the training pipeline is wired up and adds a real model.
"""

import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.linear_model import LogisticRegression

TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "smoke-test"
MODEL_NAME = "smoke-test-model"


def smoke_test() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Log a run with params, metrics, and a model artifact
    with mlflow.start_run() as run:
        mlflow.log_params({"n_features": 4, "solver": "lbfgs"})
        mlflow.log_metric("accuracy", 0.95)

        rng = np.random.default_rng(seed=42)
        X = rng.standard_normal((100, 4))
        y = (X[:, 0] > 0).astype(int)
        model = LogisticRegression(solver="lbfgs").fit(X, y)

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        run_id = run.info.run_id

    print(f"✓ Tracking OK  (run_id={run_id[:8]}…)")

    # Verify model appears in registry
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    assert len(versions) > 0, "Model was not registered!"
    print(f"✓ Registry OK  (version {versions[0].version})")

    # Cleanup — remove model and experiment so the registry stays clean
    client.delete_registered_model(MODEL_NAME)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    mlflow.delete_experiment(experiment.experiment_id)
    print("✓ Cleaned up.")
    print()
    print("Stack is healthy. Delete scripts/smoke_test_mlflow.py when done.")


if __name__ == "__main__":
    smoke_test()
