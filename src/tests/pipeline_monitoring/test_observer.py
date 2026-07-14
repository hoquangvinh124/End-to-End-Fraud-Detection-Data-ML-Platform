from __future__ import annotations

from pipeline_monitoring.observer import (
    DatasetSnapshot,
    PipelineObserver,
    compute_processing_delay_seconds,
)

from .test_spark_listener import RecordingSink


def test_idle_topic_has_zero_processing_delay():
    assert compute_processing_delay_seconds(None, None, now_ms=10_000) == 0.0


def test_kafka_ahead_of_bronze_reports_timestamp_gap():
    assert compute_processing_delay_seconds(20_000, 12_000, now_ms=25_000) == 8.0


def test_missing_bronze_watermark_ages_from_kafka_record():
    assert compute_processing_delay_seconds(10_000, None, now_ms=25_000) == 15.0


class FakeProbe:
    def snapshot(self, dataset: str) -> DatasetSnapshot:
        return DatasetSnapshot(
            kafka_watermark_ms=20_000,
            bronze_watermark_ms=12_000,
            silver_watermark_s=15.0,
            gold_watermark_s=18.0,
        )

    def component_health(self) -> dict[tuple[str, str], bool]:
        return {
            ("kafka-connect-transactions", "cdc"): True,
            ("redis", "online"): False,
        }


def test_observer_emits_layer_watermarks_delay_and_component_health():
    sink = RecordingSink()
    observer = PipelineObserver(FakeProbe(), sink, clock=lambda: 25.0)

    observer.collect_once()

    transaction_labels = {
        "dataset": "transactions",
        "pipeline.stage": "bronze",
        "service.name": "pipeline-observer",
    }
    assert (
        "mlops.pipeline.processing.delay",
        8.0,
        transaction_labels,
    ) in sink.gauges
    assert (
        "mlops.pipeline.data.watermark.time",
        12.0,
        transaction_labels,
    ) in sink.gauges
    assert any(
        name == "mlops.pipeline.component.up"
        and value == 0.0
        and labels["service.name"] == "redis"
        for name, value, labels in sink.gauges
    )
