from unittest.mock import MagicMock

from pipeline_monitoring.telemetry import OtelMetricSink


def test_metric_instruments_are_created_once_per_name():
    sink = object.__new__(OtelMetricSink)
    sink._meter = MagicMock()
    sink._gauges = {}
    sink._counters = {}
    sink._histograms = {}

    sink.set_gauge("example.gauge", 1.0, {})
    sink.set_gauge("example.gauge", 2.0, {})
    sink.add_counter("example.counter", 1.0, {})
    sink.add_counter("example.counter", 1.0, {})
    sink.record_histogram("example.histogram", 1.0, {})
    sink.record_histogram("example.histogram", 2.0, {})

    sink._meter.create_gauge.assert_called_once_with("example.gauge")
    sink._meter.create_counter.assert_called_once_with("example.counter")
    sink._meter.create_histogram.assert_called_once_with("example.histogram")
