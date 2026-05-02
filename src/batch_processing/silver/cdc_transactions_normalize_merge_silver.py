"""Silver transactions batch job: Bronze Delta (CDF) → Silver Delta.

Reads new CDC rows incrementally via Delta Change Data Feed, normalises them,
and MERGEs the result into a Silver Delta table.
All configuration from spark-defaults.conf (``spark.silver.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from utils.watermark import read_watermark, write_watermark

_CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

JOB_NAME = "silver-transactions"


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("silver-transactions-batch").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types."""
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
            "_lsn",
        )
    )


# ---------------------------------------------------------------------------
# Validation and quarantine
# ---------------------------------------------------------------------------


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid_df, quarantine_df) based on null/range checks."""
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
    """Append invalid rows to the quarantine Delta table (audit log)."""
    if quarantine_df.isEmpty():
        return
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[{JOB_NAME}] quarantined bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """MERGE new Bronze rows into the Silver transactions Delta table."""
    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                batch_df.alias("bronze"),
                "silver.transaction_id = bronze.transaction_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").partitionBy("event_date").save(silver_path)
    print(f"[{JOB_NAME}] merged rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    quarantine_path = spark.conf.get("spark.silver.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.watermark.path")

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
