"""CDC fraud cases normalize-merge to Silver: Bronze Delta (CDF) → Silver Delta.

Reads new Bronze fraud_cases CDC rows incrementally via Delta Change Data Feed,
normalises them into canonical Silver rows, and MERGEs the result into a
partitioned Silver Delta table.

``is_fraud`` is derived in Silver (not kept as a raw column) so all downstream
consumers — Gold terminal features, training dataset, reporting — can read a
single clean boolean without reimplementing the business rule:

  is_fraud = 1  iff  case_status = 'confirmed' AND resolved_at IS NOT NULL
  is_fraud = 0  otherwise (open, dismissed, or not yet resolved)

CDC lifecycle:
  - INSERT when investigation opens (resolved_at NULL, case_status='open')
  - UPDATE when investigation closes (resolved_at set,
    case_status='confirmed'/'dismissed')
  - Silver MERGE keeps the latest version per case_id (LSN-ordered)

Incremental state is tracked by the shared Delta watermark table
(``spark.silver.fraud_cases.watermark.path``).

Run:
    spark-submit /opt/silver/cdc_fraud_cases_normalize_merge_silver.py

All configuration is loaded from spark-defaults.conf
(``spark.silver.fraud_cases.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W
from utils.watermark import read_watermark, write_watermark

_CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

JOB_NAME = "silver-fraud-cases"


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("silver-fraud-cases-batch").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types and derive is_fraud.

    is_fraud encodes the business outcome so all consumers read a single
    authoritative column instead of re-evaluating case_status + resolved_at.

    _lsn is kept in Silver for cross-batch MERGE ordering (same pattern as
    the transactions Silver job).
    """
    return (
        df.withColumn("reported_at", F.to_timestamp("reported_at"))
        .withColumn("resolved_at", F.to_timestamp("resolved_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn(
            "loss_amount", F.col("loss_amount").cast(T.DecimalType(12, 2))
        )
        .withColumn(
            "is_fraud",
            (F.col("case_status") == F.lit("confirmed"))
            & F.col("resolved_at").isNotNull(),
        )
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

    Minimum validity rules:
      - case_id must not be null (primary key)
      - transaction_id must not be null (foreign key to transactions)
      - reported_at must not be null (investigation open timestamp)
    """
    null_case_id = F.col("case_id").isNull()
    null_txn_id = F.col("transaction_id").isNull()
    null_reported = F.col("reported_at").isNull()

    error_reason = (
        F.when(null_case_id, F.lit("case_id is null"))
        .when(null_txn_id, F.lit("transaction_id is null"))
        .when(null_reported, F.lit("reported_at is null"))
    )

    flagged = df.withColumn("_error_reason", error_reason)
    valid_df = flagged.filter(F.col("_error_reason").isNull()).drop("_error_reason")
    quarantine_df = (
        flagged.filter(F.col("_error_reason").isNotNull())
        .withColumn("_quarantine_ts", F.current_timestamp())
    )
    return valid_df, quarantine_df


def write_quarantine(quarantine_path: str, quarantine_df: DataFrame) -> None:
    """Append invalid rows to the quarantine Delta table (audit log, never MERGE)."""
    if quarantine_df.isEmpty():
        return
    bad_count = quarantine_df.count()
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(
        f"[silver-fraud_cases] quarantined {bad_count:,} bad rows → "
        f"{quarantine_path}"
    )


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN and MERGE into the Silver fraud_cases Delta table.

    Same LSN-guard pattern as the transactions Silver job: only update/delete
    when the incoming event is at least as fresh as the current Silver row,
    preventing late Bronze files from overwriting newer Silver state.
    """
    window = W.Window.partitionBy("case_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    good_count = dedup_df.count()

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.case_id = bronze.case_id",
            )
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
        # First run: partition by reported_at date
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .withColumn("reported_date", F.to_date("reported_at"))
            .write.format("delta")
            .partitionBy("reported_date")
            .save(silver_path)
        )

    print(f"[silver-fraud_cases] merged {good_count:,} rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.fraud_cases.watermark.path")

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
