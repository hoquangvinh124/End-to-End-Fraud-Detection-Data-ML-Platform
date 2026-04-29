"""Silver transactions batch job: Bronze Parquet → Silver Delta.

Runs as a single-pass batch (trigger=availableNow) that:
  1. Reads new Bronze CDC Parquet files incrementally via checkpoint.
  2. Casts types, adds ``event_date`` partition column, normalises CDC timestamps.
  3. Validates rows — invalid records go to a quarantine Delta table.
  4. Deduplicates valid rows by LSN (latest CDC event per ``transaction_id``).
  5. MERGEs into Silver Delta table (handles inserts, updates, logical deletes).

Run:
    spark-submit /opt/silver/silver_transactions_batch.py

All configuration is loaded from spark-defaults.conf (``spark.silver.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

# ---------------------------------------------------------------------------
# Bronze schema — must match the Parquet written by cdc_transactions_to_bronze
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = T.StructType(
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
        T.StructField("_op", T.StringType()),
        T.StructField("_source_table", T.StringType()),
        T.StructField("_source_ts_ms", T.LongType()),
        T.StructField("_cdc_ts_ms", T.LongType()),
        T.StructField("_snapshot", T.StringType()),
        T.StructField("_lsn", T.LongType()),
        T.StructField("_deleted", T.BooleanType()),
        T.StructField("_ingested_at", T.TimestampType()),
    ]
)


def build_spark_session() -> SparkSession:
    # Spark auto-loads $SPARK_HOME/conf/spark-defaults.conf — Delta extensions
    # (DeltaSparkSessionExtension + DeltaCatalog) are configured there.
    return SparkSession.builder.appName("silver-transactions-batch").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types.

    Both ``_lsn`` and ``_source_ts`` are kept in the output:
    ``_lsn`` is preserved in Silver to guard cross-batch MERGE ordering;
    ``_source_ts`` serves as a tie-breaker when two events share the same LSN.
    """
    return (
        df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumnRenamed("_op", "_cdc_op")
        .withColumn(
            "_source_ts", (F.col("_source_ts_ms") / 1000).cast(T.TimestampType())
        )
        .withColumn("_cdc_ts", (F.col("_cdc_ts_ms") / 1000).cast(T.TimestampType()))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .drop(
            "_source_table",
            "_snapshot",
            "_ingested_at",
            "_source_ts_ms",
            "_cdc_ts_ms",
            "_deleted",
        )
    )


# ---------------------------------------------------------------------------
# Validation and quarantine
# ---------------------------------------------------------------------------


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid_df, quarantine_df).

    Quarantine rows keep all columns plus ``_error_reason`` (first failed
    rule) and ``_quarantine_ts`` (wall-clock time of quarantine write).
    """
    null_id = F.col("transaction_id").isNull()
    null_ts = F.col("event_timestamp").isNull()
    bad_amount = F.col("amount").isNull() | (F.col("amount") <= 0)

    error_reason = (
        F.when(null_id, F.lit("transaction_id is null"))
        .when(null_ts, F.lit("event_timestamp is null"))
        .when(bad_amount, F.lit("amount must be > 0"))
    )

    flagged = df.withColumn("_error_reason", error_reason)
    valid_df = flagged.filter(F.col("_error_reason").isNull()).drop("_error_reason")
    quarantine_df = (
        flagged.filter(F.col("_error_reason").isNotNull())
        .withColumn("_quarantine_ts", F.current_timestamp())
    )
    return valid_df, quarantine_df


def write_quarantine(quarantine_path: str, quarantine_df: DataFrame) -> None:
    """Append invalid rows to the quarantine Delta table.

    Quarantine is an audit log — every bad row from every run is preserved.
    MERGE-based dedup is intentionally avoided here because rows with a NULL
    ``transaction_id`` and the same ``_error_reason`` would collapse into a
    single record and hide the true volume of data quality failures.
    """
    if quarantine_df.isEmpty():
        return

    bad_count = quarantine_df.count()
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[silver] quarantined {bad_count:,} bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN and MERGE into the Silver Delta table.

    PostgreSQL LSN (Log Sequence Number) is monotonically increasing within a
    Postgres instance, making it the correct primary ordering key when multiple
    CDC events for the same ``transaction_id`` arrive in the same micro-batch.
    ``_source_ts_ms`` is used as a tie-breaker for events sharing the same LSN
    (e.g. multiple changes committed in the same transaction).

    ``_lsn`` is **kept** in the Silver schema so the MERGE can guard against
    late or out-of-order Bronze files overwriting newer Silver rows across
    separate batch runs. The update condition ``bronze._lsn >= silver._lsn``
    ensures older events never regress the Silver row to a stale state.
    """
    window = W.Window.partitionBy("transaction_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")  # _lsn is retained in Silver for cross-batch LSN guard
    )

    good_count = dedup_df.count()

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.transaction_id = bronze.transaction_id",
            )
            # Delete only when the incoming event is as fresh or fresher than Silver.
            .whenMatchedDelete(
                condition="bronze._cdc_op = 'd'"
                " AND (silver._lsn IS NULL OR bronze._lsn >= silver._lsn)"
            )
            # Update only when the incoming event is strictly newer than Silver.
            .whenMatchedUpdateAll(
                condition="bronze._cdc_op != 'd'"
                " AND (silver._lsn IS NULL OR bronze._lsn >= silver._lsn)"
            )
            .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
            .execute()
        )
    else:
        # First run: no existing Silver Delta table — skip logical deletes
        # (nothing to delete in an empty table) and partition by event_date.
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .write.format("delta")
            .partitionBy("event_date")
            .save(silver_path)
        )

    print(f"[silver] merged {good_count:,} rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    checkpoint_path = spark.conf.get("spark.silver.checkpoint.path")
    quarantine_path = spark.conf.get("spark.silver.quarantine.path")

    bronze_stream = (
        spark.readStream.format("parquet")
        .schema(BRONZE_SCHEMA)
        .load(bronze_path)
    )

    def foreach_batch(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        typed_df = cast_types(batch_df)
        valid_df, quarantine_df = validate_and_split(typed_df)
        write_quarantine(quarantine_path, quarantine_df)
        merge_to_silver(spark, silver_path, valid_df)

    (
        bronze_stream.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
