from __future__ import annotations

import ast
from pathlib import Path

DAG_PATH = (
    Path(__file__).resolve().parents[2]
    / "orchestration"
    / "dags"
    / "feature_pipeline_daily.py"
)


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
