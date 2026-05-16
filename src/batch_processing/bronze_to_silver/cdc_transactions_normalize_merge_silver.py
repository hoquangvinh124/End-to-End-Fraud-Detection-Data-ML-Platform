"""Silver transactions batch job: Bronze Parquet → Silver Delta (pg_banking).

Reads new Parquet files from the Bronze bucket incrementally via Spark
Structured Streaming with trigger(availableNow=True), normalises them,
and MERGEs the result into a Silver Delta table registered in Hive as
pg_banking.transactions.

All configuration from spark-defaults.conf (``spark.silver.*`` namespace).
"""
from __future__ import annotations

from functools import partial
from textwrap import dedent

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

JOB_NAME = "silver-transactions"

SILVER_DATABASE = "pg_banking"
SILVER_TABLE = "transactions"
SILVER_TABLE_FQN = f"{SILVER_DATABASE}.{SILVER_TABLE}"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName(f"{JOB_NAME}-batch")
        .enableHiveSupport()
        .getOrCreate()
    )


def register_transactions_silver_table(
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
        )
    )


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


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN, then MERGE into Silver transactions Delta table."""
    window = W.Window.partitionBy("transaction_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_lsn")
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.transaction_id = bronze.transaction_id",
            )
            .whenMatchedUpdateAll(condition="bronze._cdc_op != 'd'")
            .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
            .execute()
        )
    else:
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .save(silver_path)
        )
    print(f"[{JOB_NAME}] merged rows → {silver_path}")


def process_batch(
    batch_df: DataFrame,
    batch_id: int,  # noqa: ARG001
    *,
    spark: SparkSession,
    silver_path: str,
    quarantine_path: str,
) -> None:
    """foreachBatch handler for the Silver transactions stream."""
    if batch_df.isEmpty():
        return
    typed_df = cast_types(batch_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    print(f"[{JOB_NAME}] batch {batch_id} processed.")


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    quarantine_path = spark.conf.get("spark.silver.quarantine.path")
    checkpoint_path = spark.conf.get("spark.silver.checkpoint.path")
    db_location = spark.conf.get("spark.pg_banking.database.location")

    register_transactions_silver_table(spark, silver_path, db_location)

    (
        spark.readStream.format("parquet")
        .load(bronze_path)
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(
            partial(
                process_batch,
                spark=spark,
                silver_path=silver_path,
                quarantine_path=quarantine_path,
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
