"""Spark Structured Streaming job: Kafka CDC -> Parquet Bronze (MinIO).

Reads Debezium envelope messages from the ``cdc.transactions`` Kafka topic,
lightly unwraps the payload, adds CDC metadata columns, and writes plain
Parquet files to the MinIO bronze bucket on a configurable micro-batch trigger.

All configuration is loaded from spark-defaults.conf (baked into the image at
$SPARK_HOME/conf/spark-defaults.conf). Job parameters live under the
``spark.bronze.*`` namespace — change the conf file to tune the job.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark_session() -> SparkSession:
    # Spark auto-loads $SPARK_HOME/conf/spark-defaults.conf — no manual config needed.
    return SparkSession.builder.appName("cdc-transactions-to-bronze").getOrCreate()


# ---------------------------------------------------------------------------
# Debezium envelope schema
# ---------------------------------------------------------------------------

_TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.LongType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("account_id", T.StringType()),
        T.StructField("card_id", T.StringType()),
        T.StructField("terminal_id", T.StringType()),
        T.StructField("amount", T.StringType()),
        T.StructField("currency_code", T.StringType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("channel_type", T.StringType()),
        T.StructField("auth_status", T.StringType()),
        T.StructField("tx_time_seconds", T.IntegerType()),
        T.StructField("tx_time_days", T.IntegerType()),
        T.StructField("is_weekend", T.BooleanType()),
        T.StructField("is_night", T.BooleanType()),
        T.StructField("created_at", T.StringType()),
    ]
)

_SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("schema", T.StringType()),
        T.StructField("table", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
        T.StructField("snapshot", T.StringType()),
        T.StructField("lsn", T.LongType()),
    ]
)

ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("before", _TRANSACTION_SCHEMA),
        T.StructField("after", _TRANSACTION_SCHEMA),
        T.StructField("op", T.StringType()),
        T.StructField("source", _SOURCE_SCHEMA),
        T.StructField("ts_ms", T.LongType()),
    ]
)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def build_bronze_rows(raw_df):
    """Unwrap Debezium envelope and add bronze metadata columns."""
    event_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("event")
    )

    # For deletes the relevant payload is ``before``; for all others use ``after``.
    payload = F.when(
        F.col("event.op") == F.lit("d"), F.col("event.before")
    ).otherwise(F.col("event.after"))

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
        # Bronze CDC metadata columns
        F.col("event.op").alias("_op"),
        F.concat_ws(
            ".", F.col("event.source.schema"), F.col("event.source.table")
        ).alias("_source_table"),
        F.col("event.source.ts_ms").alias("_source_ts_ms"),
        F.col("event.ts_ms").alias("_cdc_ts_ms"),
        F.col("event.source.snapshot").alias("_snapshot"),
        F.col("event.source.lsn").alias("_lsn"),
        (F.col("event.op") == F.lit("d")).alias("_deleted"),
        F.current_timestamp().alias("_ingested_at"),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    topic = spark.conf.get("spark.bronze.topic")
    bootstrap_servers = spark.conf.get("spark.bronze.bootstrap.servers")
    output_path = spark.conf.get("spark.bronze.output.path")
    checkpoint_path = spark.conf.get("spark.bronze.checkpoint.path")
    trigger_interval = spark.conf.get("spark.bronze.trigger.interval")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = build_bronze_rows(kafka_df)

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