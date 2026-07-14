"""Spark Structured Streaming job: Kafka CDC -> Parquet Bronze (MinIO).

Reads Debezium Avro-encoded messages from the ``cdc.transactions`` Kafka topic,
lightly unwraps the payload, adds CDC metadata columns, and writes plain
Parquet files to the MinIO bronze bucket on a configurable micro-batch trigger.

All configuration is loaded from spark-defaults.conf (baked into the image at
$SPARK_HOME/conf/spark-defaults.conf). Job parameters live under the
``spark.bronze.*`` namespace.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from utils.schema_registry_helpers import fetch_avro_schema

from pipeline_monitoring.spark_listener import PipelineStreamingQueryListener


def build_spark_session() -> SparkSession:
    """Create a plain SparkSession without Hive support (not needed for Parquet bronze)."""
    return (
        SparkSession.builder.appName("cdc-transactions-to-bronze")
        .getOrCreate()
    )


def build_bronze_rows(raw_df: DataFrame, avro_schema_str: str) -> DataFrame:
    """Unwrap Debezium Avro envelope and add bronze metadata columns.

    Confluent wire format prefixes each Avro message with 5 bytes:
    0x00 (magic byte) + 4-byte big-endian schema ID. Those bytes are stripped
    before passing the payload to ``from_avro``.
    """
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    event_col = from_avro(avro_payload, avro_schema_str).alias("event")
    event_df = raw_df.select(event_col)

    payload = F.col("event.after")

    return event_df.select(
        payload.transaction_id.alias("transaction_id"),
        payload.event_timestamp.alias("event_timestamp"),
        payload.customer_id.alias("customer_id"),
        payload.account_id.alias("account_id"),
        payload.card_id.alias("card_id"),
        payload.terminal_id.alias("terminal_id"),
        payload.amount.alias("amount"),
        payload.currency_code.alias("currency_code"),
        payload.transaction_type.alias("transaction_type"),
        payload.channel_type.alias("channel_type"),
        payload.auth_status.alias("auth_status"),
        payload.tx_time_seconds.alias("tx_time_seconds"),
        payload.tx_time_days.alias("tx_time_days"),
        payload.is_weekend.alias("is_weekend"),
        payload.is_night.alias("is_night"),
        payload.created_at.alias("created_at"),
        F.col("event.op").alias("_op"),
        F.concat_ws(
            ".", F.col("event.source.schema"), F.col("event.source.table")
        ).alias("_source_table"),
        F.col("event.source.ts_ms").alias("_source_ts_ms"),
        F.col("event.ts_ms").alias("_cdc_ts_ms"),
        F.col("event.source.snapshot").alias("_snapshot"),
        F.col("event.source.lsn").alias("_lsn"),
        F.current_timestamp().alias("_ingested_at"),
    )


def main() -> None:
    spark = build_spark_session()
    spark.streams.addListener(PipelineStreamingQueryListener("transactions"))

    topic = spark.conf.get("spark.bronze.topic")
    bootstrap_servers = spark.conf.get("spark.bronze.bootstrap.servers")
    output_path = spark.conf.get("spark.bronze.output.path")
    checkpoint_path = spark.conf.get("spark.bronze.checkpoint.path")
    trigger_interval = spark.conf.get("spark.bronze.trigger.interval")
    sr_url = spark.conf.get("spark.bronze.schema.registry.url")

    avro_schema_str = fetch_avro_schema(sr_url, f"{topic}-value")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = build_bronze_rows(kafka_df, avro_schema_str)

    query = (
        bronze_df.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start(output_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
