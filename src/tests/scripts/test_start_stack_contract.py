from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


def test_start_script_preflights_waits_and_reports_failures():
    source = (ROOT / "scripts/start_stack.ps1").read_text(encoding="utf-8")

    for marker in (
        "docker info",
        "docker compose config --quiet",
        "--profile batch build silver-transactions",
        "docker compose up -d --build --remove-orphans",
        "TimeoutSeconds",
        "docker compose logs --tail",
        "Add-Type -AssemblyName System.Net.Http",
        "http://localhost:8000/health",
        "http://localhost:8092/api/v2/monitor/health",
        "http://localhost:9090/-/ready",
    ):
        assert marker in source


def test_mutable_runtime_images_are_pinned_by_digest():
    cdc = yaml.safe_load(
        (ROOT / "src/cdc/docker-compose.cdc.yml").read_text(encoding="utf-8")
    )
    lakehouse = yaml.safe_load(
        (ROOT / "src/lakehouse/docker-compose.lakehouse.yml").read_text(
            encoding="utf-8"
        )
    )

    assert "@sha256:" in cdc["services"]["kafka-ui"]["image"]
    assert "@sha256:" in cdc["services"]["debezium-ui"]["image"]
    assert "@sha256:" in lakehouse["services"]["clickhouse"]["image"]

    kafka = cdc["services"]["kafka"]
    assert kafka["environment"]["KAFKA_LOG_DIRS"] == "/var/lib/kafka/data"
    assert "kafka_data:/var/lib/kafka/data" in kafka["volumes"]


def test_cold_start_dependencies_gate_background_consumers():
    cdc_ingestion = yaml.safe_load(
        (ROOT / "src/cdc_ingestion/docker-compose.cdc_ingestion.yml").read_text(
            encoding="utf-8"
        )
    )
    monitoring = yaml.safe_load(
        (ROOT / "src/monitoring/docker-compose.observability.yml").read_text(
            encoding="utf-8"
        )
    )

    tx_dependencies = cdc_ingestion["services"]["cdc-transactions"]["depends_on"]
    assert tx_dependencies["connector-init-debezium"]["condition"] == "service_completed_successfully"
    assert tx_dependencies["minio-init"]["condition"] == "service_completed_successfully"

    for service in ("cdc-transactions", "cdc-fraud-cases"):
        healthcheck = cdc_ingestion["services"][service]["healthcheck"]
        assert "/tmp/cdc-stream-ready" in " ".join(healthcheck["test"])
        assert healthcheck["start_period"] == "120s"

    observer_dependencies = monitoring["services"]["pipeline-observer"]["depends_on"]
    for service in ("kafka-connect", "minio", "trino", "clickhouse", "redis"):
        assert observer_dependencies[service]["condition"] == "service_healthy"

    assert monitoring["services"]["kafka-exporter"]["depends_on"]["kafka"]["condition"] == "service_healthy"


def test_connector_registration_waits_for_running_tasks():
    for relative_path in (
        "src/cdc/connectors/transactions/register.sh",
        "src/cdc/connectors/fraud-cases/register.sh",
    ):
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "MAX_STATUS_ATTEMPTS=30" in source
        assert "Connector task is RUNNING" in source
        assert 'grep -q \'"tasks":\\[{"id":0,"state":"RUNNING"\'' in source
