"""feature_pipeline_daily — daily Spark batch pipeline to build ML features.

Runs every day at 02:00 UTC and processes CDC data for the previous calendar day
(Airflow's ``{{ ds }}``) through three medallion layers:

  Bronze   → Silver   → Gold

Dependency graph:

  bronze.ingest_transactions ──► silver.normalize_transactions ──► gold.aggregate_customer_features
                                                                ──► gold.aggregate_terminal_features
  bronze.ingest_fraud_cases  ──► silver.normalize_fraud_cases  ──►/

Each task runs a Docker container from the batch image (``SPARK_BATCH_IMAGE``
Airflow Variable, default ``mlops-batch:latest``) on the shared infrastructure
network (``DOCKER_NETWORK`` Airflow Variable, default ``mlops_default``).

Airflow Variables (set in Admin → Variables or via CLI):
  SPARK_BATCH_IMAGE   Docker image built from src/batch_processing/Dockerfile
                      Default: mlops-batch:latest
  DOCKER_NETWORK      Docker network shared with Kafka + MinIO
                      Default: mlops_default
                      Set to the output of:
                        docker network ls --filter name=default --format '{{.Name}}'
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# Runtime configuration — set in Airflow UI: Admin → Variables
# ---------------------------------------------------------------------------

SPARK_IMAGE = Variable.get("SPARK_BATCH_IMAGE", default_var="mlops-batch:latest")
DOCKER_NETWORK = Variable.get("DOCKER_NETWORK", default_var="mlops_default")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def spark_task(
    task_id: str,
    script: str,
    dag: DAG,
    extra_conf: dict[str, str] | None = None,
) -> DockerOperator:
    """Return a DockerOperator that runs spark-submit inside the batch image.

    ``extra_conf`` entries are appended as ``--conf key=value`` flags so
    Airflow can inject runtime values (e.g. feature_date) without rebuilding
    the image or editing spark-defaults.conf.
    """
    conf_flags = " ".join(
        f"--conf {k}={v}" for k, v in (extra_conf or {}).items()
    )
    cmd = f"spark-submit {conf_flags} {script}".strip()

    return DockerOperator(
        task_id=task_id,
        dag=dag,
        image=SPARK_IMAGE,
        command=cmd,
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mount_tmp_dir=False,
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
        execution_timeout=pendulum.duration(hours=2),
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
) as dag:

    # ── CDC Ingestion ────────────────────────────────────────────────────────────
    with TaskGroup("cdc_ingestion") as cdc_ingestion_group:
        ingest_transactions = spark_task(
            task_id="ingest_transactions",
            script="/opt/cdc_ingestion/cdc_transactions_to_bronze.py",
            dag=dag,
        )

        ingest_fraud_cases = spark_task(
            task_id="ingest_fraud_cases",
            script="/opt/cdc_ingestion/cdc_fraud_cases_to_bronze.py",
            dag=dag,
        )

    # ── Silver ────────────────────────────────────────────────────────────
    with TaskGroup("silver") as silver_group:
        normalize_transactions = spark_task(
            task_id="normalize_transactions",
            script="/opt/silver/cdc_transactions_normalize_merge_silver.py",
            dag=dag,
        )

        normalize_fraud_cases = spark_task(
            task_id="normalize_fraud_cases",
            script="/opt/silver/cdc_fraud_cases_normalize_merge_silver.py",
            dag=dag,
        )

    # ── Gold ──────────────────────────────────────────────────────────────
    with TaskGroup("gold") as gold_group:
        # feature_date = Airflow logical date (yesterday's data)
        aggregate_customer_features = spark_task(
            task_id="aggregate_customer_features",
            script="/opt/gold/silver_transactions_window_aggregate_customer_gold.py",
            dag=dag,
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

        aggregate_terminal_features = spark_task(
            task_id="aggregate_terminal_features",
            script="/opt/gold/silver_transactions_window_aggregate_terminal_gold.py",
            dag=dag,
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

        assemble_ml_features = spark_task(
            task_id="assemble_ml_features",
            script="/opt/gold/silver_transactions_ml_features_gold.py",
            dag=dag,
            extra_conf={"spark.gold.feature.date": "{{ ds }}"},
        )

    # ── Dependencies ──────────────────────────────────────────────────────
    ingest_transactions >> normalize_transactions
    ingest_fraud_cases >> normalize_fraud_cases

    # Customer features only need transaction Silver
    normalize_transactions >> aggregate_customer_features

    # Terminal features need BOTH Silver tables (transactions + fraud labels)
    [normalize_transactions, normalize_fraud_cases] >> aggregate_terminal_features

    # ML features table needs both Gold partitions to be ready
    [aggregate_customer_features, aggregate_terminal_features] >> assemble_ml_features
