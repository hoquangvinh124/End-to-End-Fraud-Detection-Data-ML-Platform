"""Silver fraud_cases batch job: Bronze Delta (CDF) → Silver Delta.

Reads new CDC rows incrementally via Delta Change Data Feed using Spark
Structured Streaming with trigger(availableNow=True), normalises them,
and MERGEs the result into a Silver Delta table.

``is_fraud`` is derived in Silver: case_status='confirmed' AND resolved_at IS NOT NULL.
All configuration from spark-defaults.conf (``spark.silver.fraud_cases.*``).
"""
from __future__ import annotations

from functools import partial
from textwrap import dedent

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

_CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

JOB_NAME = "silver-fraud-cases"


def build_spark_session() -> SparkSession:
    return SparkSession.builder \
                .appName(f"{JOB_NAME}-batch") \
                .enableHiveSupport() \
                .getOrCreate()


SILVER_DATABASE = "banking"
SILVER_TABLE = "fraud_cases"
SILVER_TABLE_FQN = f"{SILVER_DATABASE}.{SILVER_TABLE}"


def register_fraud_cases_silver_table(
    spark: SparkSession, silver_path: str, db_location: str
) -> None:
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE} LOCATION '{db_location}'"
    )
    spark.sql(
        dedent(
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_TABLE_FQN}
            USING DELTA
            LOCATION '{silver_path}'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """
        ).strip()
    )
    spark.sql(
        dedent(
            f"""
            ALTER TABLE {SILVER_TABLE_FQN}
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """
        ).strip()
    )

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
            .mode("overwrite")
            .partitionBy("reported_date")
            .save(silver_path)
        )

    print(f"[{JOB_NAME}] merged rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def process_batch(
    batch_df: DataFrame,
    batch_id: int,  # noqa: ARG001
    *,
    spark: SparkSession,
    silver_path: str,
    quarantine_path: str,
) -> None:
    """foreachBatch handler for the Silver fraud_cases stream."""
    clean_df = (
        batch_df
        .filter(F.col("_change_type") == "insert")
        .drop(*_CDF_META_COLS)
    )
    if clean_df.isEmpty():
        return
    typed_df = cast_types(clean_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    print(f"[{JOB_NAME}] batch {batch_id} processed.")


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
    checkpoint_path = spark.conf.get("spark.silver.fraud_cases.checkpoint.path")
    db_location = spark.conf.get("spark.banking.database.location")

    register_fraud_cases_silver_table(spark, silver_path, db_location)

    (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .load(bronze_path)
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(partial(process_batch, spark=spark, silver_path=silver_path, quarantine_path=quarantine_path))
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
