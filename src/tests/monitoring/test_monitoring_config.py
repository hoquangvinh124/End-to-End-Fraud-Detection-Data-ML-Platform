from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
MONITORING = ROOT / "src" / "monitoring"


def test_lakehouse_dashboard_is_provisionable_json():
    dashboard = json.loads(
        (
            MONITORING
            / "grafana/provisioning/dashboards/json/lakehouse-pipeline-overview.json"
        ).read_text(encoding="utf-8")
    )
    assert dashboard["uid"] == "lakehouse-pipeline-overview"
    assert dashboard["title"] == "Lakehouse Pipeline Overview"
    assert len(dashboard["panels"]) >= 8


def test_collector_routes_otlp_and_scraped_metrics_to_prometheus():
    config = yaml.safe_load(
        (MONITORING / "otel/otel-collector-config.yaml").read_text(encoding="utf-8")
    )
    metrics = config["service"]["pipelines"]["metrics"]
    assert set(metrics["receivers"]) == {"otlp", "prometheus"}
    assert metrics["exporters"] == ["prometheus"]
    assert config["exporters"]["prometheus"]["metric_expiration"] == "48h"


def test_alerting_has_discord_receiver_and_required_sla_rules():
    alertmanager = yaml.safe_load(
        (MONITORING / "alertmanager/alertmanager.yml").read_text(encoding="utf-8")
    )
    assert alertmanager["route"]["receiver"] == "discord-pipeline-alerts"
    rules = yaml.safe_load(
        (MONITORING / "prometheus/rules/lakehouse-pipeline-alerts.yml").read_text(
            encoding="utf-8"
        )
    )
    names = {
        rule["alert"] for group in rules["groups"] for rule in group["rules"]
    }
    assert {
        "PipelineComponentDown",
        "CDCProcessingDelayHigh",
        "FeaturePipelineDailyDeadlineMissed",
        "OnlineFeaturesNotRefreshed",
    } <= names


def test_no_discord_webhook_is_embedded_in_tracked_configuration():
    candidates = [ROOT / ".env.example", ROOT / "docker-compose.yml"]
    for directory in (ROOT / "src", ROOT / "docs"):
        candidates.extend(
            path
            for path in directory.rglob("*")
            if path.is_file() and path.suffix in {".py", ".md", ".yml", ".yaml"}
        )
    webhook_marker = "discord.com/api/" + "webhooks"
    leaked = [
        str(path.relative_to(ROOT))
        for path in candidates
        if webhook_marker in path.read_text(encoding="utf-8")
    ]
    assert leaked == []
