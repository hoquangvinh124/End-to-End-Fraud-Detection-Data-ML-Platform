from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from pipeline_monitoring.spark_listener import PipelineStreamingQueryListener


class RecordingSink:
    def __init__(self) -> None:
        self.gauges: list[tuple[str, float, dict[str, str]]] = []
        self.counters: list[tuple[str, float, dict[str, str]]] = []
        self.histograms: list[tuple[str, float, dict[str, str]]] = []

    def set_gauge(self, name: str, value: float, attributes: dict[str, str]) -> None:
        self.gauges.append((name, value, attributes))

    def add_counter(self, name: str, value: float, attributes: dict[str, str]) -> None:
        self.counters.append((name, value, attributes))

    def record_histogram(
        self, name: str, value: float, attributes: dict[str, str]
    ) -> None:
        self.histograms.append((name, value, attributes))


def test_progress_records_stream_health_and_batch_metrics():
    sink = RecordingSink()
    listener = PipelineStreamingQueryListener("transactions", sink=sink, clock=lambda: 1000.0)
    source = SimpleNamespace(metrics={"maxOffsetsBehindLatest": "7"})
    progress = SimpleNamespace(
        numInputRows=12,
        inputRowsPerSecond=2.5,
        processedRowsPerSecond=3.5,
        durationMs={"triggerExecution": 4000},
        sources=[source],
    )

    listener.onQueryProgress(SimpleNamespace(progress=progress))

    labels = {
        "dataset": "transactions",
        "pipeline.stage": "bronze",
        "service.name": "cdc-transactions",
    }
    assert ("mlops.pipeline.component.up", 1.0, labels) in sink.gauges
    assert ("mlops.pipeline.heartbeat.time", 1000.0, labels) in sink.gauges
    assert ("mlops.pipeline.offsets.behind", 7.0, labels) in sink.gauges
    assert ("mlops.pipeline.records.processed", 12.0, labels) in sink.counters
    assert ("mlops.pipeline.batch.duration", 4.0, labels) in sink.histograms
    assert ("mlops.pipeline.batch.duration.last", 4.0, labels) in sink.gauges


def test_heartbeat_refreshes_last_progress_gauges():
    sink = RecordingSink()
    listener = PipelineStreamingQueryListener(
        "transactions", sink=sink, clock=lambda: 1000.0
    )
    progress = SimpleNamespace(
        numInputRows=12,
        inputRowsPerSecond=2.5,
        processedRowsPerSecond=3.5,
        durationMs={"triggerExecution": 4000},
        sources=[SimpleNamespace(metrics={"maxOffsetsBehindLatest": "7"})],
    )
    listener.onQueryProgress(SimpleNamespace(progress=progress))
    sink.gauges.clear()

    listener.onQueryIdle(SimpleNamespace())

    refreshed = {(name, value) for name, value, _ in sink.gauges}
    assert ("mlops.pipeline.offsets.behind", 7.0) in refreshed
    assert ("mlops.pipeline.input.rate", 2.5) in refreshed
    assert ("mlops.pipeline.processing.rate", 3.5) in refreshed
    assert ("mlops.pipeline.batch.duration.last", 4.0) in refreshed


def test_termination_marks_stream_down_and_counts_failure():
    sink = RecordingSink()
    listener = PipelineStreamingQueryListener("fraud_cases", sink=sink, clock=lambda: 1000.0)

    listener.onQueryTerminated(SimpleNamespace(exception="lost Kafka connection"))

    labels = {
        "dataset": "fraud_cases",
        "pipeline.stage": "bronze",
        "service.name": "cdc-fraud-cases",
    }
    assert ("mlops.pipeline.component.up", 0.0, labels) in sink.gauges
    assert (
        "mlops.pipeline.failures",
        1.0,
        {**labels, "status": "failed"},
    ) in sink.counters


def test_idle_event_refreshes_heartbeat_without_counting_rows():
    sink = RecordingSink()
    listener = PipelineStreamingQueryListener("transactions", sink=sink, clock=lambda: 2000.0)

    listener.onQueryIdle(SimpleNamespace())

    assert any(name == "mlops.pipeline.heartbeat.time" for name, _, _ in sink.gauges)
    assert sink.counters == []


def test_query_start_launches_independent_heartbeat_loop():
    sink = RecordingSink()
    thread = MagicMock()
    thread_factory = MagicMock(return_value=thread)
    listener = PipelineStreamingQueryListener(
        "transactions", sink=sink, thread_factory=thread_factory
    )

    listener.onQueryStarted(SimpleNamespace())

    thread_factory.assert_called_once()
    assert thread_factory.call_args.kwargs["daemon"] is True
    thread.start.assert_called_once_with()
