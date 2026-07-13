"""Silver transactions batch job: Bronze Parquet → Silver Delta (silver).

Reads new Parquet files from the Bronze bucket incrementally via Spark
Structured Streaming with trigger(availableNow=True), normalises them,
and MERGEs the result into a Silver Delta table registered in Hive as
silver.transactions.

Configuration priority (highest → lowest):
  1. CLI flags (--bronze-path, --silver-path, …)
  2. spark-defaults.conf keys (spark.silver.*)
  3. Environment variables (SILVER_BRONZE_PATH, SILVER_OUTPUT_PATH, …)

Run via Docker (production):
  docker compose -f src/batch_processing/docker-compose.batch_processing.yml \\
    run --rm silver-transactions

Run manually (local spark-submit):
  spark-submit cdc_transactions_normalize_merge_silver.py \\
    --bronze-path s3a://bronze/cdc/transactions \\
    --silver-path s3a://silver/transactions \\
    --quarantine-path s3a://silver/quarantine/transactions \\
    --checkpoint-path s3a://silver/_checkpoints/cdc_transactions_silver \\
    --db-location s3a://warehouse/silver.db
"""
from __future__ import annotations

import os
from functools import partial
from textwrap import dedent

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

JOB_NAME = "silver-transactions"

SILVER_DATABASE = "silver"
SILVER_TABLE = "transactions"
SILVER_TABLE_FQN = f"{SILVER_DATABASE}.{SILVER_TABLE}"



def _resolve(spark: SparkSession, spark_key: str, env_key: str, cli_value: str | None) -> str:
    """Return the first non-empty value from: CLI flag → Spark conf → env var."""
    if cli_value:
        return cli_value
    try:
        val = spark.conf.get(spark_key)
        if val:
            return val
    except Exception:  # noqa: BLE001
        pass
    val = os.environ.get(env_key, "")
    if not val:
        raise ValueError(
            f"Required config missing: pass --{spark_key.split('.')[-1].replace('_', '-')}, "
            f"set spark conf '{spark_key}', or set env var '{env_key}'"
        )
    return val


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName(f"{JOB_NAME}-batch")
        .enableHiveSupport()
        .getOrCreate()
    )


def ensure_silver_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")


def register_silver_table(spark: SparkSession, silver_path: str) -> None:
    """Register the Delta table in Hive after data has been written.

    CREATE TABLE stores the real path in TABLE_PARAMS but Spark's
    HiveExternalCatalog writes a placeholder to SDS.LOCATION.
    ALTER TABLE SET LOCATION fixes SDS.LOCATION via the Thrift metastore
    protocol so Trino's delta_lake connector can resolve it.
    """
    spark.sql(
        dedent(
            f"""
            CREATE TABLE IF NOT EXISTS {SILVER_TABLE_FQN}
            USING DELTA
            LOCATION '{silver_path}'
            """
        ).strip()
    )
    spark.sql(f"ALTER TABLE {SILVER_TABLE_FQN} SET LOCATION '{silver_path}'")


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types."""
    return (
        df.withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumnRenamed("_op", "_cdc_op")
        .withColumn(
            "_source_ts", (F.col("_source_ts_ms") / 1000).cast(T.TimestampType())
        )
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
        .drop("_rn", "_lsn", "_source_ts")
    )

    deletes_df = dedup_df.filter(F.col("_cdc_op") == "d").select("transaction_id")
    upserts_df = dedup_df.filter(F.col("_cdc_op") != "d").drop("_cdc_op")

    is_initialized = DeltaTable.isDeltaTable(spark, silver_path) and bool(
        DeltaTable.forPath(spark, silver_path).toDF().columns
    )

    if is_initialized:
        dt = DeltaTable.forPath(spark, silver_path)
        if not deletes_df.isEmpty():
            (
                dt.alias("silver")
                .merge(
                    deletes_df.alias("bronze"),
                    "silver.transaction_id = bronze.transaction_id",
                )
                .whenMatchedDelete()
                .execute()
            )
        if not upserts_df.isEmpty():
            (
                dt.alias("silver")
                .merge(
                    upserts_df.alias("bronze"),
                    "silver.transaction_id = bronze.transaction_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
    else:
        (
            upserts_df.write.format("delta")
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

    ensure_silver_database(spark)

    bronze_schema = spark.read.parquet(bronze_path).schema

    (
        spark.readStream.format("parquet")
        .schema(bronze_schema)
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
    register_silver_table(spark, silver_path)


if __name__ == "__main__":
    main()
