from __future__ import annotations

from pipeline_monitoring.runtime_probe import RuntimePipelineProbe


def test_snapshot_uses_dataset_topic_and_layer_queries():
    queries: list[str] = []

    def query_scalar(sql: str):
        queries.append(sql)
        if "bronze.transactions" in sql:
            return 20_000
        if "silver.transactions" in sql:
            return 15.0
        if "mart_fraud_ml_features" in sql:
            return 18.0
        raise AssertionError(sql)

    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: 25_000 if topic == "cdc.transactions" else None,
        trino_scalar=query_scalar,
        clickhouse_scalar=query_scalar,
        connector_healthy=lambda connector: True,
        redis_healthy=lambda: True,
    )

    snapshot = probe.snapshot("transactions")

    assert snapshot.kafka_watermark_ms == 25_000
    assert snapshot.bronze_watermark_ms == 20_000
    assert snapshot.silver_watermark_s == 15.0
    assert snapshot.gold_watermark_s == 18.0
    assert any("bronze.transactions" in query for query in queries)
    assert any("silver.transactions" in query for query in queries)


def test_component_health_includes_both_connectors_and_redis():
    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: None,
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
        if "bronze.transactions" in sql:
            return 20_000
        raise RuntimeError("silver table does not exist yet")

    probe = RuntimePipelineProbe(
        kafka_latest=lambda topic: 25_000,
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
