from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from pipeline_monitoring.telemetry import MetricSink, OtelMetricSink


class PipelineStreamingQueryListener(StreamingQueryListener):
    """Publish low-cardinality Structured Streaming progress metrics."""

    def __init__(
        self,
        dataset: str,
        sink: MetricSink | None = None,
        clock: Callable[[], float] = time.time,
        thread_factory: Callable[..., threading.Thread] = threading.Thread,
        heartbeat_interval_seconds: int = 30,
    ) -> None:
        self.dataset = dataset
        self.service_name = f"cdc-{dataset.replace('_', '-')}"
        self.sink = sink or OtelMetricSink(self.service_name)
        self.clock = clock
        self.thread_factory = thread_factory
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self._stop_heartbeat = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    @property
    def attributes(self) -> dict[str, str]:
        return {
            "dataset": self.dataset,
            "pipeline.stage": "bronze",
            "service.name": self.service_name,
        }

    def _heartbeat(self) -> None:
        self.sink.set_gauge("mlops.pipeline.component.up", 1.0, self.attributes)
        self.sink.set_gauge(
            "mlops.pipeline.heartbeat.time", self.clock(), self.attributes
        )

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802
        self._heartbeat()
        self._heartbeat_thread = self.thread_factory(
            target=self._heartbeat_loop,
            name=f"{self.service_name}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._stop_heartbeat.wait(self.heartbeat_interval_seconds):
            self._heartbeat()

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        progress = event.progress
        self._heartbeat()
        offsets_behind = 0.0
        for source in getattr(progress, "sources", []):
            metrics = getattr(source, "metrics", {}) or {}
            offsets_behind = max(
                offsets_behind, float(metrics.get("maxOffsetsBehind", 0) or 0)
            )
        self.sink.set_gauge(
            "mlops.pipeline.offsets.behind", offsets_behind, self.attributes
        )
        self.sink.set_gauge(
            "mlops.pipeline.input.rate",
            float(getattr(progress, "inputRowsPerSecond", 0) or 0),
            self.attributes,
        )
        self.sink.set_gauge(
            "mlops.pipeline.processing.rate",
            float(getattr(progress, "processedRowsPerSecond", 0) or 0),
            self.attributes,
        )
        self.sink.add_counter(
            "mlops.pipeline.records.processed",
            float(getattr(progress, "numInputRows", 0) or 0),
            self.attributes,
        )
        duration_ms = float(
            (getattr(progress, "durationMs", {}) or {}).get("triggerExecution", 0)
            or 0
        )
        self.sink.record_histogram(
            "mlops.pipeline.batch.duration", duration_ms / 1000.0, self.attributes
        )

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        self._heartbeat()

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        self._stop_heartbeat.set()
        self.sink.set_gauge("mlops.pipeline.component.up", 0.0, self.attributes)
        if getattr(event, "exception", None):
            self.sink.add_counter(
                "mlops.pipeline.failures",
                1.0,
                {**self.attributes, "status": "failed"},
            )
