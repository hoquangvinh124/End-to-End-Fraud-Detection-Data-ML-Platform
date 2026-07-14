"""feature_pipeline_daily — daily batch pipeline: Silver → dbt → Redis.

Dependency graph:

  cdc_freshness_gate ──► Spark Bronze→Silver ──► dbt staging/intermediate/marts ──► Redis

dbt_staging reads the normalized Silver Delta tables through Trino.
dbt_intermediate writes customer + terminal window features to ClickHouse.
dbt_marts writes the flat ML feature table to ClickHouse.
materialize_online_features reads ClickHouse intermediate tables → pushes to Redis.

CDC ingestion is a long-running service and is deliberately not scheduled here.
"""
from __future__ import annotations

import os
import pathlib
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
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
_MATERIALIZE_SCRIPT = os.environ.get(
    "MATERIALIZE_SCRIPT_PATH",
    str(pathlib.Path(__file__).resolve().parents[2] / "feature_store" / "materialize_to_redis.py"),
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
    description="Daily batch pipeline: Bronze (Spark) → dbt staging/intermediate/marts → Redis",
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
    materialize_online_features = BashOperator(
        task_id="materialize_online_features",
        bash_command=f"uv run python {_MATERIALIZE_SCRIPT} --feature-date {{{{ ds }}}}",
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Reads customer + terminal features from ClickHouse intermediate tables "
            "for `{{ ds }}` and pushes to Redis via `feast write_to_online_store`."
        ),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    cdc_freshness_gate >> [normalize_transactions, normalize_fraud_cases]
    [normalize_transactions, normalize_fraud_cases] >> dbt_staging
    dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_marts
    dbt_marts >> materialize_online_features
