"""Spark Structured Streaming job: Kafka CDC -> Parquet Bronze (MinIO).

Reads Debezium Avro-encoded messages from the ``cdc.transactions`` Kafka topic,
lightly unwraps the payload, adds CDC metadata columns, and writes plain
Parquet files to the MinIO bronze bucket on a configurable micro-batch trigger.

All configuration is loaded from spark-defaults.conf (baked into the image at
$SPARK_HOME/conf/spark-defaults.conf). Job parameters live under the
``spark.bronze.*`` namespace — change the conf file to tune the job.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


def build_spark_session() -> SparkSession:
    # Spark auto-loads $SPARK_HOME/conf/spark-defaults.conf — no manual config needed.
    return SparkSession.builder.appName("cdc-transactions-to-bronze").getOrCreate()


# ---------------------------------------------------------------------------
# Schema Registry helpers
# ---------------------------------------------------------------------------


def fetch_avro_schema(
    sr_url: str, subject: str, retries: int = 30, delay: int = 5
) -> str:
    """Fetch latest Avro schema string from Confluent Schema Registry.

    Retries up to ``retries`` times with ``delay`` seconds between attempts
    to handle the window between connector registration and Debezium's first
    snapshot message (which triggers schema registration).
    """
    url = f"{sr_url}/subjects/{subject}/versions/latest"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url) as resp:
                return json.loads(resp.read())["schema"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                print(
                    f"Schema not yet registered for {subject!r}, "
                    f"retrying ({attempt + 1}/{retries})…"
                )
                time.sleep(delay)
            else:
                raise
        except urllib.error.URLError as exc:
            print(
                f"Schema Registry unreachable: {exc}, "
                f"retrying ({attempt + 1}/{retries})…"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not fetch Avro schema for subject {subject!r} after {retries} attempts"
    )


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def build_bronze_rows(raw_df, avro_schema_str: str):
    """Unwrap Debezium Avro envelope and add bronze metadata columns.

    Confluent wire format prefixes each Avro message with 5 bytes:
    0x00 (magic byte) + 4-byte big-endian schema ID. Those bytes are stripped
    before passing the payload to ``from_avro``.
    """
    event_df = raw_df.select(
        from_avro(
            # Skip the 5-byte Confluent wire format prefix (1-indexed in Spark SQL).
            F.expr("substring(value, 6, length(value) - 5)"),
            avro_schema_str,
        ).alias("event")
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