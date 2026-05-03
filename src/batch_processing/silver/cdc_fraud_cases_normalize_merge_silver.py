"""Silver fraud_cases batch job: Bronze Delta (CDF) → Silver Delta.

Reads new CDC rows incrementally via Delta Change Data Feed, normalises them,
and MERGEs the result into a Silver Delta table.

``is_fraud`` is derived in Silver: case_status='confirmed' AND resolved_at IS NOT NULL.
All configuration from spark-defaults.conf (``spark.silver.fraud_cases.*``).
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
    return SparkSession.builder.appName(f"{JOB_NAME}-batch").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types and derive is_fraud.

    is_fraud = True iff case_status = 'confirmed' AND resolved_at IS NOT NULL.
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

    Rules: case_id, transaction_id, and reported_at must not be null.
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
    """Append invalid rows to the quarantine Delta table (audit log)."""
    if quarantine_df.isEmpty():
        return
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[{JOB_NAME}] quarantined bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN, then MERGE into Silver fraud_cases Delta table."""
    window = W.Window.partitionBy("case_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.case_id = bronze.case_id",
            )
            .whenMatchedUpdateAll(condition="bronze._cdc_op != 'd'")
            .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
            .execute()
        )
    else:
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .withColumn("reported_date", F.to_date("reported_at"))
            .write.format("delta")
            .partitionBy("reported_date")
            .save(silver_path)
        )

    print(f"[{JOB_NAME}] merged rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.fraud_cases.watermark.path")

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

    bronze_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", current_version)
        .load(bronze_path)
        .filter(F.col("_change_type") == "insert")
        .drop(*_CDF_META_COLS)
    )

    typed_df = cast_types(bronze_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    write_watermark(spark, watermark_path, JOB_NAME, current_version)
    print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


if __name__ == "__main__":
    main()
