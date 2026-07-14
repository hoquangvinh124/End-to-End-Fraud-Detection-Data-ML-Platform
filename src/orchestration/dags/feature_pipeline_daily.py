"""feature_pipeline_daily — daily batch pipeline: Silver → dbt → Redis → MLflow.

Dependency graph:

  cdc_freshness_gate ──► Spark Bronze→Silver ──► dbt staging/intermediate/marts
                                                               └──► Redis ──► MLflow

dbt_staging reads the normalized Silver Delta tables through Trino.
dbt_intermediate writes customer + terminal window features to ClickHouse.
dbt_marts writes the flat ML feature table to ClickHouse.
materialize_online_features reads ClickHouse Gold features and pushes them to Redis.
train_register_model trains from the Gold mart, exports ONNX, and updates the
MLflow Registry ``champion`` alias.

CDC ingestion is a long-running service and is deliberately not scheduled here.
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, TaskGroup
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles.trino.base import TrinoBaseProfileMapping
from pipeline_readiness import cdc_freshness_ready

_SPARK_IMAGE = os.environ.get("SPARK_BATCH_IMAGE", "mlops-batch:latest")
_DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "mlops_default")
_PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
_OTEL_ENDPOINT = os.environ.get(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318"
)
_ML_RUNTIME_IMAGE = os.environ.get(
    "ML_RUNTIME_IMAGE", "mlops-fraud-detection-api:latest"
)
_MLFLOW_TRACKING_URI = os.environ.get(
    "MLFLOW_TRACKING_URI", "http://mlflow-server:5000"
)
_DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spark_task(
    task_id: str,
    script: str,
    doc_md: str = "",
    extra_conf: dict[str, str] | None = None,
) -> DockerOperator:
    """Return a DockerOperator running spark-submit inside the batch image."""
    conf_flags = " ".join(f"--conf {k}={v}" for k, v in (extra_conf or {}).items())
    cmd = f"spark-submit {conf_flags} {script}".strip()

    op = DockerOperator(
        task_id=task_id,
        image=_SPARK_IMAGE,
        command=cmd,
        network_mode=_DOCKER_NETWORK,
        environment={
            "OTEL_EXPORTER_OTLP_ENDPOINT": _OTEL_ENDPOINT,
            "OTEL_METRIC_EXPORT_INTERVAL": "15000",
            "DEPLOYMENT_ENVIRONMENT": "local",
        },
        auto_remove="success",
        mount_tmp_dir=False,
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=2),
    )
    if doc_md:
        op.doc_md = doc_md
    return op


def _dbt_task_group(
    group_id: str,
    select: str,
) -> DbtTaskGroup:
    """Return a cosmos DbtTaskGroup running dbt models for the given selector.

    feature_date is passed as a dbt var so models filter to the Airflow logical date.
    Trino connection is read from TRINO_HOST/TRINO_PORT env vars (set in docker-compose).
    """
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=_DBT_PROJECT_DIR,
            project_name="fraud_detection",
        ),
        profile_config=ProfileConfig(
            profile_name="fraud_detection",
            target_name="dev",
            profile_mapping=TrinoBaseProfileMapping(
                conn_id="trino_default",
                profile_args={
                    "database": "lakehouse",
                    "schema": "staging",
                    "http_scheme": "http",
                    "threads": 4,
                },
            ),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
        ),
        render_config=RenderConfig(select=[select]),
        operator_args={
            "vars": {"feature_date": "{{ ds }}"},
        },
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="feature_pipeline_daily",
    description="Freshness-gated Silver/Gold, feature materialization, and model lifecycle",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "features", "dbt", "clickhouse"],
    doc_md=__doc__,
):
    cdc_freshness_gate = PythonSensor(
        task_id="cdc_freshness_gate",
        python_callable=cdc_freshness_ready,
        op_kwargs={"prometheus_url": _PROMETHEUS_URL},
        poke_interval=30,
        timeout=30 * 60,
        mode="reschedule",
        doc_md="Wait for both CDC services to be healthy and at most five minutes behind Kafka.",
    )

    # ── Bronze → Silver normalisation (Spark) ────────────────────────────
    with TaskGroup("silver"):
        normalize_transactions = _spark_task(
            task_id="normalize_transactions",
            script="/opt/bronze_to_silver/cdc_transactions_normalize_merge_silver.py",
            doc_md="Reads Bronze Parquet `s3a://bronze/cdc/transactions` → casts types, deduplicates by LSN, MERGEs into Silver Delta `s3a://silver/pg_banking/transactions`.",
        )

        normalize_fraud_cases = _spark_task(
            task_id="normalize_fraud_cases",
            script="/opt/bronze_to_silver/cdc_fraud_cases_normalize_merge_silver.py",
            doc_md="Reads Bronze Parquet `s3a://bronze/cdc/fraud_cases` → casts types, derives `is_fraud`, deduplicates by LSN, MERGEs into Silver Delta `s3a://silver/pg_banking/fraud_cases`.",
        )

    # ── dbt transform layers ──────────────────────────────────────────────
    dbt_staging = _dbt_task_group(
        group_id="dbt_staging",
        select="staging",
    )

    dbt_intermediate = _dbt_task_group(
        group_id="dbt_intermediate",
        select="intermediate",
    )

    dbt_marts = _dbt_task_group(
        group_id="dbt_marts",
        select="marts",
    )

    # ── Feast → Redis materialization ─────────────────────────────────────
    materialize_online_features = DockerOperator(
        task_id="materialize_online_features",
        image=_ML_RUNTIME_IMAGE,
        command=(
            "bash -c 'cd /app/feature_store && PYTHONPATH=/app /app/.venv/bin/feast apply "
            "&& cd /app && python feature_store/materialize_to_redis.py "
            "--feature-date {{ ds }}'"
        ),
        network_mode=_DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_PORT": "8123",
            "CLICKHOUSE_USER": "abcbank",
            "CLICKHOUSE_PASSWORD": "abcbank",
            "OTEL_EXPORTER_OTLP_ENDPOINT": _OTEL_ENDPOINT,
            "OTEL_METRIC_EXPORT_INTERVAL": "15000",
            "DEPLOYMENT_ENVIRONMENT": "local",
            "PYTHONPATH": "/app",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Reads customer + terminal features from the ClickHouse Gold mart "
            "for `{{ ds }}` and pushes to Redis via `feast write_to_online_store`."
        ),
    )

    train_register_model = DockerOperator(
        task_id="train_register_model",
        image=_ML_RUNTIME_IMAGE,
        command=(
            "python -m training.train_from_feature_store "
            "--clickhouse-host clickhouse "
            "--clickhouse-port 8123 "
            f"--tracking-uri {_MLFLOW_TRACKING_URI} "
            "--start-date {{ macros.ds_add(ds, -90) }} "
            "--end-date {{ ds }}"
        ),
        network_mode=_DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "OTEL_EXPORTER_OTLP_ENDPOINT": _OTEL_ENDPOINT,
            "OTEL_METRIC_EXPORT_INTERVAL": "15000",
            "DEPLOYMENT_ENVIRONMENT": "local",
        },
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
        doc_md=(
            "Trains an ONNX-compatible model from the ClickHouse Gold mart, "
            "logs metrics and artifacts to MLflow, and updates `champion`."
        ),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    cdc_freshness_gate >> [normalize_transactions, normalize_fraud_cases]
    [normalize_transactions, normalize_fraud_cases] >> dbt_staging
    dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_marts
    dbt_marts >> materialize_online_features >> train_register_model
