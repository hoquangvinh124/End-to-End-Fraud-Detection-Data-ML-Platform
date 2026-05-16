"""Spark Structured Streaming job: Kafka CDC -> Parquet Bronze (MinIO).

Reads Debezium Avro-encoded messages from the ``cdc.fraud_cases`` Kafka topic,
lightly unwraps the payload, adds CDC metadata columns, and writes plain
Parquet files to the MinIO bronze bucket on a configurable micro-batch trigger.

Source table schema (banking.fraud_cases):
  case_id           text
  transaction_id    int8
  customer_id       text
  card_id           text
  fraud_scenario    int4
  case_status       text        -- 'open' | 'confirmed' | 'dismissed'
  resolution_source text
  reported_at       timestamp
  resolved_at       timestamp   -- NULL until investigation closes
  loss_amount       numeric
  created_at        timestamp

CDC lifecycle: rows are INSERTed when reported (resolved_at NULL),
then UPDATEd when the investigation closes (resolved_at set, case_status updated).

All configuration is loaded from spark-defaults.conf.
Job parameters live under the ``spark.bronze.fraud_cases.*`` namespace.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from utils.schema_registry_helpers import fetch_avro_schema


def build_spark_session() -> SparkSession:
    """Create a plain SparkSession without Hive support (not needed for Parquet bronze)."""
    return (
        SparkSession.builder.appName("cdc-fraud-cases-to-bronze")
        .getOrCreate()
    )


def build_bronze_rows(raw_df: DataFrame, avro_schema_str: str) -> DataFrame:
    """Unwrap Debezium Avro envelope and add bronze metadata columns.

    Confluent wire format: 1-byte magic (0x00) + 4-byte schema ID = 5 bytes stripped.
    fraud_cases lifecycle is INSERT + UPDATE only; event.after carries current state.
    """
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    event_col = from_avro(avro_payload, avro_schema_str).alias("event")
    event_df = raw_df.select(event_col)

    payload = F.col("event.after")

    return event_df.select(
        payload.case_id.alias("case_id"),
        payload.transaction_id.alias("transaction_id"),
        payload.customer_id.alias("customer_id"),
        payload.card_id.alias("card_id"),
        payload.fraud_scenario.alias("fraud_scenario"),
        payload.case_status.alias("case_status"),
        payload.resolution_source.alias("resolution_source"),
        payload.reported_at.alias("reported_at"),
        payload.resolved_at.alias("resolved_at"),
        payload.loss_amount.alias("loss_amount"),
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

    topic = spark.conf.get("spark.bronze.fraud_cases.topic")
    bootstrap_servers = spark.conf.get("spark.bronze.bootstrap.servers")
    output_path = spark.conf.get("spark.bronze.fraud_cases.output.path")
    checkpoint_path = spark.conf.get("spark.bronze.fraud_cases.checkpoint.path")
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
