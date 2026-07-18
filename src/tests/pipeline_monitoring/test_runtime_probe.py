from __future__ import annotations

from datetime import datetime, timezone

from pipeline_monitoring.runtime_probe import (
    RuntimePipelineProbe,
    latest_bronze_object_timestamp_ms,
)


class _FakeS3Client:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.calls = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs)
        return next(self.pages)


def test_latest_bronze_object_timestamp_uses_latest_parquet_across_pages():
    older = datetime(2026, 7, 17, 14, 5, tzinfo=timezone.utc)
    newer = datetime(2026, 7, 17, 14, 10, tzinfo=timezone.utc)
    client = _FakeS3Client(
        [
            {
                "Contents": [
                    {"Key": "cdc/transactions/_spark_metadata/3", "LastModified": newer},
                    {"Key": "cdc/transactions/part-old.snappy.parquet", "LastModified": older},
                ],
                "IsTruncated": True,
                "NextContinuationToken": "next-page",
            },
            {
                "Contents": [
                    {"Key": "cdc/transactions/part-new.snappy.parquet", "LastModified": newer}
                ],
                "IsTruncated": False,
            },
        ]
    )

    result = latest_bronze_object_timestamp_ms(client, "transactions")

    assert result == int(newer.timestamp() * 1000)
    assert client.calls[0] == {"Bucket": "bronze", "Prefix": "cdc/transactions/"}
    assert client.calls[1]["ContinuationToken"] == "next-page"


def test_latest_bronze_object_timestamp_returns_none_without_parquet():
    client = _FakeS3Client([{"Contents": [], "IsTruncated": False}])

    assert latest_bronze_object_timestamp_ms(client, "transactions") is None


def test_snapshot_uses_dataset_topic_and_layer_queries():
    queries: list[str] = []
    clickhouse_queries: list[str] = []

    def query_scalar(sql: str):
        queries.append(sql)
        if "silver.transactions" in sql:
            return 15.0
        if "mart_fraud_ml_features" in sql:
            return 18.0
        raise AssertionError(sql)

    def clickhouse_scalar(sql: str):
        clickhouse_queries.append(sql)
        return 18.0

    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: 25_000 if topic == "cdc.transactions" else None,
        bronze_latest=lambda dataset: 20_000 if dataset == "transactions" else None,
        trino_scalar=query_scalar,
        clickhouse_scalar=clickhouse_scalar,
        connector_healthy=lambda connector: True,
        redis_healthy=lambda: True,
    )

    snapshot = probe.snapshot("transactions")

    assert snapshot.kafka_watermark_ms == 25_000
    assert snapshot.bronze_watermark_ms == 20_000
    assert snapshot.silver_watermark_s == 15.0
    assert snapshot.gold_watermark_s == 18.0
    assert all("lakehouse.bronze" not in query for query in queries)
    assert any("silver.transactions" in query for query in queries)
    assert any("max(event_timestamp)" in query for query in queries)
    assert any("max(event_timestamp)" in query for query in clickhouse_queries)


def test_fraud_cases_silver_watermark_uses_reported_at():
    queries: list[str] = []

    def query_scalar(sql: str):
        queries.append(sql)
        return 15.0

    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: 25_000,
        bronze_latest=lambda dataset: 20_000,
        trino_scalar=query_scalar,
        clickhouse_scalar=lambda sql: 18.0,
        connector_healthy=lambda connector: True,
        redis_healthy=lambda: True,
    )

    probe.snapshot("fraud_cases")

    assert any("max(reported_at)" in query for query in queries)
    assert all("_silver_updated_at" not in query for query in queries)


def test_component_health_includes_both_connectors_and_redis():
    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: None,
        bronze_latest=lambda dataset: None,
        trino_scalar=lambda sql: None,
        clickhouse_scalar=lambda sql: None,
        connector_healthy=lambda connector: connector.startswith("transactions"),
        redis_healthy=lambda: False,
    )

    health = probe.component_health()

    assert health[("kafka-connect-transactions", "cdc")] is True
    assert health[("kafka-connect-fraud-cases", "cdc")] is False
    assert health[("redis", "online")] is False


def test_missing_downstream_table_does_not_hide_kafka_and_bronze_state():
    def trino_scalar(sql: str):
        raise RuntimeError("silver table does not exist yet")

    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: 25_000,
        bronze_latest=lambda dataset: 20_000,
        trino_scalar=trino_scalar,
        clickhouse_scalar=lambda sql: (_ for _ in ()).throw(
            RuntimeError("gold table does not exist yet")
        ),
        connector_healthy=lambda connector: True,
        redis_healthy=lambda: True,
    )

    snapshot = probe.snapshot("transactions")

    assert snapshot.kafka_watermark_ms == 25_000
    assert snapshot.bronze_watermark_ms == 20_000
    assert snapshot.silver_watermark_s is None
    assert snapshot.gold_watermark_s is None
