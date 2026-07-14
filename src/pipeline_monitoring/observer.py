from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from pipeline_monitoring.telemetry import MetricSink

DATASETS = ("transactions", "fraud_cases")


@dataclass(frozen=True)
class DatasetSnapshot:
    kafka_watermark_ms: int | None
    bronze_watermark_ms: int | None
    silver_watermark_s: float | None
    gold_watermark_s: float | None


class PipelineProbe(Protocol):
    def snapshot(self, dataset: str) -> DatasetSnapshot: ...

    def component_health(self) -> dict[tuple[str, str], bool]: ...


def compute_processing_delay_seconds(
    kafka_watermark_ms: int | None,
    bronze_watermark_ms: int | None,
    *,
    now_ms: int,
) -> float:
    if kafka_watermark_ms is None:
        return 0.0
    comparison_ms = bronze_watermark_ms
    if comparison_ms is None:
        comparison_ms = now_ms
        return max(0.0, (comparison_ms - kafka_watermark_ms) / 1000.0)
    return max(0.0, (kafka_watermark_ms - comparison_ms) / 1000.0)


class PipelineObserver:
    def __init__(
        self,
        probe: PipelineProbe,
        sink: MetricSink,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.probe = probe
        self.sink = sink
        self.clock = clock

    def _labels(self, dataset: str, stage: str) -> dict[str, str]:
        return {
            "dataset": dataset,
            "pipeline.stage": stage,
            "service.name": "pipeline-observer",
        }

    def collect_once(self) -> None:
        now_s = self.clock()
        for dataset in DATASETS:
            snapshot = self.probe.snapshot(dataset)
            bronze_labels = self._labels(dataset, "bronze")
            delay = compute_processing_delay_seconds(
                snapshot.kafka_watermark_ms,
                snapshot.bronze_watermark_ms,
                now_ms=int(now_s * 1000),
            )
            self.sink.set_gauge(
                "mlops.pipeline.processing.delay", delay, bronze_labels
            )
            watermarks = {
                "kafka": (
                    None
                    if snapshot.kafka_watermark_ms is None
                    else snapshot.kafka_watermark_ms / 1000.0
                ),
                "bronze": (
                    None
                    if snapshot.bronze_watermark_ms is None
                    else snapshot.bronze_watermark_ms / 1000.0
                ),
                "silver": snapshot.silver_watermark_s,
                "gold": snapshot.gold_watermark_s,
            }
            for stage, watermark in watermarks.items():
                if watermark is None:
                    continue
                labels = self._labels(dataset, stage)
                self.sink.set_gauge(
                    "mlops.pipeline.data.watermark.time", watermark, labels
                )
                self.sink.set_gauge(
                    "mlops.pipeline.freshness", max(0.0, now_s - watermark), labels
                )

        for (service_name, stage), healthy in self.probe.component_health().items():
            self.sink.set_gauge(
                "mlops.pipeline.component.up",
                float(healthy),
                {
                    "dataset": "all",
                    "pipeline.stage": stage,
                    "service.name": service_name,
                },
            )
