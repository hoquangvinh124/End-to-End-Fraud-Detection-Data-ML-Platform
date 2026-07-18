from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


def test_silver_jobs_wait_for_healthy_hive_metastore():
    lakehouse = yaml.safe_load(
        (ROOT / "src/lakehouse/docker-compose.lakehouse.yml").read_text(
            encoding="utf-8"
        )
    )
    root_compose = yaml.safe_load(
        (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )

    assert "healthcheck" in lakehouse["services"]["hive-metastore"]
    for service_name in ("silver-transactions", "silver-fraud-cases"):
        dependency = root_compose["services"][service_name]["depends_on"]
        assert dependency["hive-metastore"]["condition"] == "service_healthy"


def test_silver_jobs_publish_the_image_expected_by_airflow():
    batch = yaml.safe_load(
        (ROOT / "src/batch_processing/docker-compose.batch_processing.yml").read_text(
            encoding="utf-8"
        )
    )

    for service_name in ("silver-transactions", "silver-fraud-cases"):
        assert batch["services"][service_name]["image"] == "mlops-batch:latest"
        assert batch["services"][service_name]["profiles"] == ["batch"]
