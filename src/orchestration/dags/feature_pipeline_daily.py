"""feature_pipeline_daily — daily Spark batch pipeline to build ML features.

Runs every day at 02:00 UTC and processes CDC data for the previous calendar day
(Airflow's ``{{ ds }}``) through three medallion layers:

  Bronze   → Silver   → Gold

Dependency graph:

  bronze.ingest_transactions ──► silver.normalize_transactions ──► gold.aggregate_customer_features ──┐
                                                                ──► gold.aggregate_terminal_features ──► gold.assemble_ml_features ──► feast.materialize_online_features
  bronze.ingest_fraud_cases  ──► silver.normalize_fraud_cases  ──►/

Each task runs a Docker container from the batch image on the shared infrastructure
network. Configure via environment variables in docker-compose.airflow.yml:

  SPARK_BATCH_IMAGE   Docker image built from src/batch_processing/Dockerfile
                      Default: mlops-batch:latest
  DOCKER_NETWORK      Docker network shared with Kafka + MinIO
                      Default: mlops_default
                      Set to the output of:
                        docker network ls --filter name=default --format '{{.Name}}'
"""
from __future__ import annotations

import os
import pathlib
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

# Resolved at import time from environment — injected by docker-compose so the
# scheduler never hits the DB during DAG parsing.
_SPARK_IMAGE = os.environ.get("SPARK_BATCH_IMAGE", "mlops-batch:latest")
_DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "mlops_default")

# Absolute path to the materialize script — works in any deployment as long as
# the repo layout (src/orchestration/dags/ and src/feature_store/) is preserved.
_MATERIALIZE_SCRIPT = (
    pathlib.Path(__file__).resolve().parents[2] / "feature_store" / "materialize_to_redis.py"
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _spark_task(
    task_id: str,
    script: str,
    extra_conf: dict[str, str] | None = None,
) -> DockerOperator:
    """Return a DockerOperator that runs spark-submit inside the batch image.

    ``extra_conf`` entries are appended as ``--conf key=value`` flags so
    Airflow can inject runtime values (e.g. feature_date) without rebuilding
    the image or editing spark-defaults.conf.
    """
    conf_flags = " ".join(f"--conf {k}={v}" for k, v in (extra_conf or {}).items())
    cmd = f"spark-submit {conf_flags} {script}".strip()

    return DockerOperator(
        task_id=task_id,
        image=_SPARK_IMAGE,
        command=cmd,
        network_mode=_DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="feature_pipeline_daily",
    description="Daily medallion batch pipeline: Bronze → Silver → Gold",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "features", "spark"],
    doc_md=__doc__,
):
    # ── CDC Ingestion ─────────────────────────────────────────────────────
    with TaskGroup("cdc_ingestion"):
        ingest_transactions = _spark_task(
            task_id="ingest_transactions",
            script="/opt/cdc_ingestion/cdc_transactions_to_bronze.py",
        )

        ingest_fraud_cases = _spark_task(
            task_id="ingest_fraud_cases",
            script="/opt/cdc_ingestion/cdc_fraud_cases_to_bronze.py",
        )

    # ── Silver ────────────────────────────────────────────────────────────
    with TaskGroup("silver"):
        normalize_transactions = _spark_task(
            task_id="normalize_transactions",
            script="/opt/silver/cdc_transactions_normalize_merge_silver.py",
        )

        normalize_fraud_cases = _spark_task(
            task_id="normalize_fraud_cases",
            script="/opt/silver/cdc_fraud_cases_normalize_merge_silver.py",
        )

    # ── Gold ──────────────────────────────────────────────────────────────
    with TaskGroup("gold"):
        # feature_date = Airflow logical date (yesterday's data)
        aggregate_customer_features = _spark_task(
            task_id="aggregate_customer_features",
            script="/opt/gold/silver_transactions_window_aggregate_customer_gold.py",
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

        aggregate_terminal_features = _spark_task(
            task_id="aggregate_terminal_features",
            script="/opt/gold/silver_transactions_window_aggregate_terminal_gold.py",
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

        assemble_ml_features = _spark_task(
            task_id="assemble_ml_features",
            script="/opt/gold/silver_transactions_ml_features_gold.py",
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

    # ── Feast ─────────────────────────────────────────────────────────────
    materialize_online_features = BashOperator(
        task_id="materialize_online_features",
        bash_command=f"uv run python {_MATERIALIZE_SCRIPT} ",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    ingest_transactions >> normalize_transactions
    ingest_fraud_cases >> normalize_fraud_cases

    # Customer features only need transaction Silver
    normalize_transactions >> aggregate_customer_features

    # Terminal features needs BOTH Silver tables (transactions + fraud labels)
    [normalize_transactions, normalize_fraud_cases] >> aggregate_terminal_features

    # ML features table needs both Gold partitions to be ready
    [aggregate_customer_features, aggregate_terminal_features] >> assemble_ml_features

    assemble_ml_features >> materialize_online_features
