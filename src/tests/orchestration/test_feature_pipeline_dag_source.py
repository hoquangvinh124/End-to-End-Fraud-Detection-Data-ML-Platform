from __future__ import annotations

import ast
from pathlib import Path

DAG_PATH = (
    Path(__file__).resolve().parents[2]
    / "orchestration"
    / "dags"
    / "feature_pipeline_daily.py"
)
COMPOSE_PATH = DAG_PATH.parents[1] / "docker-compose.airflow.yml"


def test_dag_source_parses_and_has_no_streaming_ingestion_tasks():
    source = DAG_PATH.read_text(encoding="utf-8")
    ast.parse(source)
    assert "cdc_transactions_to_bronze.py" not in source
    assert "cdc_fraud_cases_to_bronze.py" not in source
    assert "ingest_transactions" not in source
    assert "ingest_fraud_cases" not in source


def test_dag_starts_with_bounded_reschedule_freshness_sensor():
    source = DAG_PATH.read_text(encoding="utf-8")
    assert 'task_id="cdc_freshness_gate"' in source
    assert "poke_interval=30" in source
    assert "timeout=30 * 60" in source
    assert 'mode="reschedule"' in source
    assert "cdc_freshness_gate >> [normalize_transactions, normalize_fraud_cases]" in source


def test_manual_runs_use_a_feature_date_without_legacy_ds_context():
    source = DAG_PATH.read_text(encoding="utf-8")

    assert "{{ ds }}" not in source
    assert "data_interval_end" in source
    assert "dag_run.run_after" in source


def test_airflow_compose_defines_the_trino_connection_used_by_cosmos():
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    assert "AIRFLOW_CONN_TRINO_DEFAULT" in compose
    assert "trino://airflow@trino:8080" in compose


def test_cosmos_uses_a_manifest_generated_once_during_airflow_init():
    source = DAG_PATH.read_text(encoding="utf-8")
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    assert "LoadMode.DBT_MANIFEST" in source
    assert "manifest_path=" in source
    assert "dbt parse" in compose
    assert "DBT_LOG_PATH: /tmp/dbt-logs" in compose
    assert "airflow-dbt-target-init:" in compose
    assert 'chown -R 50000:0 /target' in compose
    assert "airflow_dbt_target:/opt/airflow/dbt/target" in compose
