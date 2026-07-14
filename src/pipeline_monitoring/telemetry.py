from __future__ import annotations

import os
from typing import Protocol


class MetricSink(Protocol):
    def set_gauge(self, name: str, value: float, attributes: dict[str, str]) -> None: ...

    def add_counter(self, name: str, value: float, attributes: dict[str, str]) -> None: ...

    def record_histogram(
        self, name: str, value: float, attributes: dict[str, str]
    ) -> None: ...


class OtelMetricSink:
    """Small synchronous facade over the OTel Python metrics API."""

    def __init__(self, service_name: str) -> None:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create(
            {
                "service.name": service_name,
                "deployment.environment": os.environ.get(
                    "DEPLOYMENT_ENVIRONMENT", "local"
                ),
            }
        )
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=int(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "15000")),
        )
        self._provider = MeterProvider(resource=resource, metric_readers=[reader])
        self._meter = self._provider.get_meter("mlops.pipeline")
        self._gauges: dict[str, object] = {}
        self._counters: dict[str, object] = {}
        self._histograms: dict[str, object] = {}

    def set_gauge(self, name: str, value: float, attributes: dict[str, str]) -> None:
        gauge = self._gauges.get(name)
        if gauge is None:
            gauge = self._meter.create_gauge(name)
            self._gauges[name] = gauge
        gauge.set(value, attributes)

    def add_counter(self, name: str, value: float, attributes: dict[str, str]) -> None:
        counter = self._counters.get(name)
        if counter is None:
            counter = self._meter.create_counter(name)
            self._counters[name] = counter
        counter.add(value, attributes)

    def record_histogram(
        self, name: str, value: float, attributes: dict[str, str]
    ) -> None:
        histogram = self._histograms.get(name)
        if histogram is None:
            histogram = self._meter.create_histogram(name)
            self._histograms[name] = histogram
        histogram.record(value, attributes)

    def force_flush(self, timeout_millis: int = 5000) -> bool:
        return self._provider.force_flush(timeout_millis)
