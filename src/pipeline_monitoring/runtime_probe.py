from __future__ import annotations

import json
import logging
import os
import urllib.request
from collections.abc import Callable
from typing import Any

from pipeline_monitoring.observer import DatasetSnapshot

ScalarQuery = Callable[[str], Any]
_LOGGER = logging.getLogger(__name__)
_SILVER_WATERMARK_COLUMNS = {
    "transactions": "event_timestamp",
    "fraud_cases": "reported_at",
}


def _safe_call(operation: Callable[[], Any], description: str) -> Any:
    try:
        return operation()
    except Exception as exc:
        _LOGGER.warning("%s unavailable: %s", description, exc)
        return None


def latest_bronze_object_timestamp_ms(client: Any, dataset: str) -> int | None:
    """Return the newest Bronze Parquet object's modification time."""
    request: dict[str, Any] = {
        "Bucket": os.environ.get("BRONZE_BUCKET", "bronze"),
        "Prefix": f"cdc/{dataset}/",
    }
    latest_timestamp_ms: int | None = None
    while True:
        response = client.list_objects_v2(**request)
        for item in response.get("Contents", []):
            if not item["Key"].endswith(".parquet"):
                continue
            timestamp_ms = int(item["LastModified"].timestamp() * 1000)
            latest_timestamp_ms = max(latest_timestamp_ms or timestamp_ms, timestamp_ms)
        if not response.get("IsTruncated"):
            return latest_timestamp_ms
        request["ContinuationToken"] = response["NextContinuationToken"]


class RuntimePipelineProbe:
    def __init__(
        self,
        *,
        kafka_latest: Callable[[str], int | None],
        bronze_latest: Callable[[str], int | None],
        trino_scalar: ScalarQuery,
        clickhouse_scalar: ScalarQuery,
        connector_healthy: Callable[[str], bool],
        redis_healthy: Callable[[], bool],
    ) -> None:
        self.kafka_latest = kafka_latest
        self.bronze_latest = bronze_latest
        self.trino_scalar = trino_scalar
        self.clickhouse_scalar = clickhouse_scalar
        self.connector_healthy = connector_healthy
        self.redis_healthy = redis_healthy

    def snapshot(self, dataset: str) -> DatasetSnapshot:
        if dataset not in {"transactions", "fraud_cases"}:
            raise ValueError(f"unsupported dataset: {dataset}")
        return DatasetSnapshot(
            kafka_watermark_ms=_safe_call(
                lambda: self.kafka_latest(f"cdc.{dataset}"), f"{dataset} Kafka"
            ),
            bronze_watermark_ms=_safe_call(
                lambda: self.bronze_latest(dataset),
                f"{dataset} Bronze",
            ),
            silver_watermark_s=_safe_call(
                lambda: self.trino_scalar(
                    f"SELECT to_unixtime(max({_SILVER_WATERMARK_COLUMNS[dataset]})) "
                    f"FROM lakehouse.silver.{dataset}"
                ),
                f"{dataset} Silver",
            ),
            gold_watermark_s=_safe_call(
                lambda: self.clickhouse_scalar(
                    "SELECT toUnixTimestamp(max(event_timestamp)) "
                    "FROM gold.mart_fraud_ml_features"
                ),
                "Gold features",
            ),
        )

    def component_health(self) -> dict[tuple[str, str], bool]:
        return {
            ("kafka-connect-transactions", "cdc"): self.connector_healthy(
                "transactions-cdc-connector"
            ),
            ("kafka-connect-fraud-cases", "cdc"): self.connector_healthy(
                "fraud-cases-cdc-connector"
            ),
            ("redis", "online"): self.redis_healthy(),
        }


def _trino_scalar(query: str) -> Any:
    from trino.dbapi import connect

    connection = connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="pipeline-observer",
        catalog="lakehouse",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        return None if not row else row[0]
    finally:
        connection.close()


def _clickhouse_scalar(query: str) -> Any:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "abcbank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "abcbank"),
    )
    try:
        result = client.query(query).first_row
        return None if not result else result[0]
    finally:
        client.close()


def _bronze_latest_timestamp(dataset: str) -> int | None:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minio12345"),
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
    )
    return latest_bronze_object_timestamp_ms(client, dataset)


def _connector_healthy(connector: str) -> bool:
    base_url = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
    try:
        with urllib.request.urlopen(
            f"{base_url}/connectors/{connector}/status", timeout=5
        ) as response:
            status = json.load(response)
    except Exception:
        return False
    connector_running = status.get("connector", {}).get("state") == "RUNNING"
    tasks = status.get("tasks", [])
    return connector_running and bool(tasks) and all(
        task.get("state") == "RUNNING" for task in tasks
    )


def _redis_healthy() -> bool:
    import redis

    try:
        client = redis.Redis(
            host=os.environ.get("REDIS_HOST", "redis"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            socket_timeout=5,
        )
        return bool(client.ping())
    except Exception:
        return False


def _kafka_latest_timestamp(topic: str) -> int | None:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
            ),
            "group.id": "pipeline-observer",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    latest_timestamp: int | None = None
    try:
        metadata = consumer.list_topics(topic, timeout=5)
        for partition_id in metadata.topics[topic].partitions:
            partition = TopicPartition(topic, partition_id)
            _, high = consumer.get_watermark_offsets(partition, timeout=5)
            if high == 0:
                continue
            consumer.assign([TopicPartition(topic, partition_id, high - 1)])
            message = consumer.poll(5)
            if message is None or message.error():
                continue
            _, timestamp_ms = message.timestamp()
            if timestamp_ms is not None:
                latest_timestamp = max(latest_timestamp or timestamp_ms, timestamp_ms)
    finally:
        consumer.close()
    return latest_timestamp


def create_runtime_probe() -> RuntimePipelineProbe:
    return RuntimePipelineProbe(
        kafka_latest=_kafka_latest_timestamp,
        bronze_latest=_bronze_latest_timestamp,
        trino_scalar=_trino_scalar,
        clickhouse_scalar=_clickhouse_scalar,
        connector_healthy=_connector_healthy,
        redis_healthy=_redis_healthy,
    )
