from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable

from pipeline_monitoring.observer import PipelineObserver
from pipeline_monitoring.runtime_probe import create_runtime_probe
from pipeline_monitoring.telemetry import MetricSink, OtelMetricSink

_LOGGER = logging.getLogger("pipeline-observer")
_LABELS = {
    "dataset": "all",
    "pipeline.stage": "monitoring",
    "service.name": "pipeline-observer",
}


def collect_iteration(
    observer: PipelineObserver,
    sink: MetricSink,
    *,
    clock: Callable[[], float] = time.time,
) -> bool:
    sink.set_gauge("mlops.pipeline.component.up", 1.0, _LABELS)
    sink.set_gauge("mlops.pipeline.heartbeat.time", clock(), _LABELS)
    try:
        observer.collect_once()
        return True
    except Exception:
        _LOGGER.exception("pipeline observation failed")
        sink.set_gauge("mlops.pipeline.component.up", 0.0, _LABELS)
        sink.add_counter(
            "mlops.pipeline.failures", 1.0, {**_LABELS, "status": "failed"}
        )
        return False


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    interval = int(os.environ.get("PIPELINE_OBSERVER_INTERVAL_SECONDS", "30"))
    sink = OtelMetricSink("pipeline-observer")
    observer = PipelineObserver(create_runtime_probe(), sink)
    while True:
        collect_iteration(observer, sink)
        time.sleep(interval)


if __name__ == "__main__":
    main()
