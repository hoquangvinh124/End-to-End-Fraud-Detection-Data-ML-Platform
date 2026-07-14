from pipeline_monitoring.telemetry import MetricSink


def record_quarantine_rows(
    sink: MetricSink, dataset: str, row_count: int
) -> None:
    if row_count <= 0:
        return
    sink.add_counter(
        "mlops.pipeline.quarantine.rows",
        float(row_count),
        {
            "dataset": dataset,
            "pipeline.stage": "silver",
            "service.name": f"silver-{dataset.replace('_', '-')}",
        },
    )
