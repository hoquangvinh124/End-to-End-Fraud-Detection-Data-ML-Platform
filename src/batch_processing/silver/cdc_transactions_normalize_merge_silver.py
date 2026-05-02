"""CDC transactions normalize-merge to Silver: Bronze Delta (CDF) → Silver Delta.

Reads new Bronze CDC rows incrementally via Delta Change Data Feed, normalises
them into canonical Silver rows, and MERGEs the result into a partitioned
Silver Delta table.

Incremental state is tracked by a Delta watermark table
(``spark.silver.watermark.path``): the last successfully processed Bronze
Delta version is persisted there and read at job start so each nightly run
only processes new Bronze commits.

Pipeline steps:
  1. Read watermark → last_bronze_version (None on first run → startingVersion=0).
  2. Read current Bronze Delta version from history.
  3. If no new commits, exit 0.
  4. Batch CDF read: startingVersion=last+1, endingVersion=current.
     Filter _change_type='insert', drop CDF metadata columns.
  5. cast_types → validate_and_split → write_quarantine → merge_to_silver.
  6. write_watermark(current_version) — only after successful MERGE.

Run:
    spark-submit /opt/silver/cdc_transactions_normalize_merge_silver.py

All configuration is loaded from spark-defaults.conf (``spark.silver.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W
from utils.watermark import read_watermark, write_watermark

# CDF metadata columns added by Delta — dropped before cast_types so the
# downstream functions see the same schema as before.
_CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

JOB_NAME = "silver-transactions"


def build_spark_session() -> SparkSession:
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
    print(f"[{JOB_NAME}] quarantined {bad_count:,} bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN and MERGE into the Silver Delta table.

    PostgreSQL LSN (Log Sequence Number) is monotonically increasing within a
    Postgres instance, making it the correct primary ordering key when multiple
    CDC events for the same ``transaction_id`` arrive in the same batch.
    ``_source_ts`` is used as a tie-breaker for events sharing the same LSN.

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
            # Update only when the incoming event is as fresh or fresher than Silver.
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

    print(f"[{JOB_NAME}] merged {good_count:,} rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    quarantine_path = spark.conf.get("spark.silver.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.watermark.path")

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        raise RuntimeError(
            f"Bronze path {bronze_path!r} is not a Delta table. "
            "Ensure the Bronze streaming job has been updated to write Delta "
            "format and at least one micro-batch has been committed."
        )

    last_version: int | None = read_watermark(spark, watermark_path, JOB_NAME)
    current_version = int(
        DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
    )

    if last_version is not None and last_version >= current_version:
        print(
            f"[{JOB_NAME}] no new data "
            f"(last_processed={last_version}, "
            f"bronze_current={current_version}), exiting."
        )
        return

    start_version = 0 if last_version is None else last_version + 1
    print(f"[{JOB_NAME}] reading Bronze CDF versions {start_version}–{current_version}")

    try:
        bronze_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start_version)
            .option("endingVersion", current_version)
            .load(bronze_path)
            .filter(F.col("_change_type") == "insert")
            .drop(*_CDF_META_COLS)
        )
    except Exception as exc:
        msg = str(exc)
        if "outside the range" in msg or "is not enabled" in msg:
            raise RuntimeError(
                f"Bronze CDF read failed for {JOB_NAME}: {msg}. "
                "If startingVersion is outside log retention, reset the "
                f"watermark by deleting the '{JOB_NAME}' row from "
                f"{watermark_path!r} and re-run."
            ) from exc
        raise

    if bronze_df.isEmpty():
        print(f"[{JOB_NAME}] CDF returned 0 insert rows, exiting.")
        return

    typed_df = cast_types(bronze_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    write_watermark(spark, watermark_path, JOB_NAME, current_version)
    print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


if __name__ == "__main__":
    main()
