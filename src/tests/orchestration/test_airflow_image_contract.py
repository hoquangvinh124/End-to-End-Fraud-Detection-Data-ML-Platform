from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


def test_airflow_dependencies_are_baked_into_a_reproducible_image():
    compose_path = ROOT / "src/orchestration/docker-compose.airflow.yml"
    compose_source = compose_path.read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_source)
    common = compose["x-airflow-common"]

    assert common["image"] == "mlops-airflow:3.1.0"
    assert common["build"]["dockerfile"] == "src/orchestration/Dockerfile"
    assert "_PIP_ADDITIONAL_REQUIREMENTS" not in common["environment"]
    assert "pip install" not in compose_source

    dockerfile = (ROOT / "src/orchestration/Dockerfile").read_text(encoding="utf-8")
    assert "FROM apache/airflow:3.1.0" in dockerfile
    assert "astronomer-cosmos==1.14.1" in dockerfile
    assert "dbt-trino==1.10.1" in dockerfile
    assert "'protobuf<6.32'" in dockerfile
