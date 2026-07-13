from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import clickhouse_connect
import mlflow.sklearn
import numpy as np
import pandas as pd
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import mlflow
from api.features import FEATURE_COLUMNS

LABEL_COLUMN = "TX_FRAUD"
DEFAULT_MODEL_NAME = "fraud-detection"
CLICKHOUSE_COLUMN_ALIASES = {
    "TX_AMOUNT": "tx_amount AS TX_AMOUNT",
    "CUSTOMER_AVG_AMOUNT_WINDOW_1D": (
        "customer_avg_amount_window_1d AS CUSTOMER_AVG_AMOUNT_WINDOW_1D"
    ),
    "CUSTOMER_AVG_AMOUNT_WINDOW_7D": (
        "customer_avg_amount_window_7d AS CUSTOMER_AVG_AMOUNT_WINDOW_7D"
    ),
    "CUSTOMER_AVG_AMOUNT_WINDOW_30D": (
        "customer_avg_amount_window_30d AS CUSTOMER_AVG_AMOUNT_WINDOW_30D"
    ),
    "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D": (
        "customer_number_of_transactions_window_1d AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"
    ),
    "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D": (
        "customer_number_of_transactions_window_7d AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D"
    ),
    "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D": (
        "customer_number_of_transactions_window_30d AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D"
    ),
    "TERMINAL_RISK_1DAY_WINDOW": (
        "terminal_risk_1day_window AS TERMINAL_RISK_1DAY_WINDOW"
    ),
    "TERMINAL_RISK_7DAY_WINDOW": (
        "terminal_risk_7day_window AS TERMINAL_RISK_7DAY_WINDOW"
    ),
    "TERMINAL_RISK_30DAY_WINDOW": (
        "terminal_risk_30day_window AS TERMINAL_RISK_30DAY_WINDOW"
    ),
    "TERMINAL_NB_TX_1DAY_WINDOW": (
        "terminal_nb_tx_1day_window AS TERMINAL_NB_TX_1DAY_WINDOW"
    ),
    "TERMINAL_NB_TX_7DAY_WINDOW": (
        "terminal_nb_tx_7day_window AS TERMINAL_NB_TX_7DAY_WINDOW"
    ),
    "TERMINAL_NB_TX_30DAY_WINDOW": (
        "terminal_nb_tx_30day_window AS TERMINAL_NB_TX_30DAY_WINDOW"
    ),
    "IS_WEEKEND": "is_weekend AS IS_WEEKEND",
    "IS_NIGHT": "is_night AS IS_NIGHT",
    LABEL_COLUMN: "tx_fraud AS TX_FRAUD",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train fraud model from the ClickHouse-backed feature mart and register it in MLflow."
    )
    parser.add_argument("--clickhouse-host", default="localhost")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-user", default="abcbank")
    parser.add_argument("--clickhouse-password", default="abcbank")
    parser.add_argument("--feature-table", default="gold.mart_fraud_ml_features")
    parser.add_argument("--tracking-uri", default="http://localhost:5000")
    parser.add_argument("--experiment-name", default="fraud-detection")
    parser.add_argument("--model-name", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--model-alias", default="champion")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--limit", type=int, default=250_000)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def build_query(args: argparse.Namespace) -> tuple[str, dict[str, object]]:
    selected_columns = [
        "transaction_id",
        "event_timestamp",
        *(CLICKHOUSE_COLUMN_ALIASES[column] for column in FEATURE_COLUMNS),
        CLICKHOUSE_COLUMN_ALIASES[LABEL_COLUMN],
    ]
    query = f"SELECT {', '.join(selected_columns)} FROM {args.feature_table}"
    filters = []
    params: dict[str, object] = {}
    if args.start_date:
        filters.append("feature_date >= {start_date:Date}")
        params["start_date"] = args.start_date
    if args.end_date:
        filters.append("feature_date <= {end_date:Date}")
        params["end_date"] = args.end_date
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY event_timestamp"
    if args.limit and args.limit > 0:
        query += f" LIMIT {args.limit}"
    return query, params


def load_training_frame(args: argparse.Namespace) -> pd.DataFrame:
    client = clickhouse_connect.get_client(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    try:
        query, params = build_query(args)
        frame = client.query_df(query, parameters=params)
    finally:
        client.close()

    if frame.empty:
        raise RuntimeError(f"No training rows returned from {args.feature_table}")
    if frame[LABEL_COLUMN].nunique() < 2:
        raise RuntimeError("Training data must contain both fraud and non-fraud labels")
    return frame


def prepare_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    features = frame.loc[:, FEATURE_COLUMNS].copy()
    for bool_col in ["IS_WEEKEND", "IS_NIGHT"]:
        features[bool_col] = features[bool_col].astype(int)
    features = features.astype(np.float32)
    labels = frame[LABEL_COLUMN].astype(int)
    return features, labels


def train_model(features: pd.DataFrame, labels: pd.Series, args: argparse.Namespace) -> tuple[Pipeline, dict[str, float]]:
    x_train, x_test, y_train, y_test = train_test_split(
        features,
        labels,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=labels,
    )
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=500,
                    random_state=args.random_state,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    metrics = {
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "pr_auc": float(average_precision_score(y_test, probabilities)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "train_rows": float(len(x_train)),
        "test_rows": float(len(x_test)),
        "positive_rate": float(labels.mean()),
    }
    return model, metrics


def export_onnx(model: Pipeline, output_path: Path) -> None:
    initial_types = [("float_input", FloatTensorType([None, len(FEATURE_COLUMNS)]))]
    onnx_model = convert_sklearn(
        model,
        initial_types=initial_types,
        options={id(model.steps[-1][1]): {"zipmap": False}},
        target_opset=17,
    )
    output_path.write_bytes(onnx_model.SerializeToString())


def register_model_version(client: mlflow.tracking.MlflowClient, model_name: str, run_id: str, alias: str) -> str:
    versions = client.search_model_versions(f"name='{model_name}'")
    matching_versions = [version for version in versions if version.run_id == run_id]
    if not matching_versions:
        raise RuntimeError(f"No registered MLflow model version found for run_id={run_id}")
    version = max(matching_versions, key=lambda item: int(item.version))
    client.set_registered_model_alias(model_name, alias, version.version)
    return str(version.version)


def main() -> None:
    args = parse_args()
    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)

    frame = load_training_frame(args)
    features, labels = prepare_features(frame)
    model, metrics = train_model(features, labels, args)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "feature_table": args.feature_table,
                "feature_columns": ",".join(FEATURE_COLUMNS),
                "model_type": "LogisticRegression",
                "onnx_runtime": "required",
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="sklearn_model",
            registered_model_name=args.model_name,
            input_example=features.head(5),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            onnx_path = Path(tmp_dir) / "model.onnx"
            export_onnx(model, onnx_path)
            mlflow.log_artifact(str(onnx_path), artifact_path="onnx")

        client = mlflow.tracking.MlflowClient()
        version = register_model_version(client, args.model_name, run.info.run_id, args.model_alias)

    print(
        f"Registered {args.model_name} version {version} "
        f"with alias {args.model_alias}; run_id={run.info.run_id}"
    )


if __name__ == "__main__":
    main()
