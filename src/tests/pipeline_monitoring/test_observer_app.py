from unittest.mock import MagicMock

from pipeline_monitoring.observer_app import collect_iteration

from .test_spark_listener import RecordingSink


def test_observer_heartbeat_is_emitted_before_slow_probe_collection():
    sink = RecordingSink()
    observer = MagicMock()

    assert collect_iteration(observer, sink, clock=lambda: 1000.0) is True

    assert sink.gauges[0][0:2] == ("mlops.pipeline.component.up", 1.0)
    assert sink.gauges[1][0:2] == ("mlops.pipeline.heartbeat.time", 1000.0)
    observer.collect_once.assert_called_once_with()


def test_observer_iteration_marks_component_down_when_collection_raises():
    sink = RecordingSink()
    observer = MagicMock()
    observer.collect_once.side_effect = RuntimeError("probe failed")

    assert collect_iteration(observer, sink, clock=lambda: 1000.0) is False

    assert any(
        name == "mlops.pipeline.component.up" and value == 0.0
        for name, value, _ in sink.gauges
    )
