from unittest.mock import MagicMock

from bronze_to_silver import cdc_fraud_cases_normalize_merge_silver as fraud_job
from bronze_to_silver import cdc_transactions_normalize_merge_silver as transaction_job

from pipeline_monitoring.batch_metrics import record_quarantine_rows

from .test_spark_listener import RecordingSink


def test_record_quarantine_rows_emits_silver_counter():
    sink = RecordingSink()

    record_quarantine_rows(sink, "transactions", 4)

    assert sink.counters == [
        (
            "mlops.pipeline.quarantine.rows",
            4.0,
            {
                "dataset": "transactions",
                "pipeline.stage": "silver",
                "service.name": "silver-transactions",
            },
        )
    ]


def test_zero_quarantine_rows_do_not_emit_counter():
    sink = RecordingSink()

    record_quarantine_rows(sink, "transactions", 0)

    assert sink.counters == []


def test_quarantine_writer_returns_persisted_row_count_for_both_jobs():
    for job in (transaction_job, fraud_job):
        frame = MagicMock()
        frame.isEmpty.return_value = False
        frame.count.return_value = 4

        assert job.write_quarantine("s3a://silver/quarantine", frame) == 4
        frame.write.format.assert_called_once_with("delta")


def test_quarantine_writer_returns_zero_without_writing_empty_frame():
    frame = MagicMock()
    frame.isEmpty.return_value = True

    assert transaction_job.write_quarantine("unused", frame) == 0
    frame.write.format.assert_not_called()
