"""Silver fraud_cases batch job: Bronze Parquet → Silver Delta (silver).

Reads new Parquet files from the Bronze bucket incrementally via Spark
Structured Streaming with trigger(availableNow=True), normalises them,
and MERGEs the result into a Silver Delta table registered in Hive as
silver.fraud_cases.

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

from pipeline_monitoring.batch_metrics import record_quarantine_rows
from pipeline_monitoring.telemetry import MetricSink, OtelMetricSink

JOB_NAME = "silver-fraud-cases"

SILVER_DATABASE = "silver"
SILVER_TABLE = "fraud_cases"
SILVER_TABLE_FQN = f"{SILVER_DATABASE}.{SILVER_TABLE}"



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
    """Cast Bronze raw types to Silver canonical types and derive is_fraud.

    is_fraud = True iff case_status = 'confirmed' AND resolved_at IS NOT NULL.
    """
    return (
        df.withColumn(
            "loss_amount", F.col("loss_amount").cast(T.DecimalType(12, 2))
        )
        .withColumn(
            "is_fraud",
            (F.col("case_status") == F.lit("confirmed_fraud"))
            & F.col("resolved_at").isNotNull(),
        )
        .withColumn("reported_date", F.to_date("reported_at"))
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


def write_quarantine(quarantine_path: str, quarantine_df: DataFrame) -> int:
    """Append invalid rows to the quarantine Delta table (audit log)."""
    if quarantine_df.isEmpty():
        return 0
    row_count = quarantine_df.count()
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[{JOB_NAME}] quarantined bad rows → {quarantine_path}")
    return row_count


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN, then MERGE into Silver fraud_cases Delta table."""
    window = W.Window.partitionBy("case_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_lsn", "_source_ts")
    )

    deletes_df = dedup_df.filter(F.col("_cdc_op") == "d").select("case_id")
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
                    "silver.case_id = bronze.case_id",
                )
                .whenMatchedDelete()
                .execute()
            )
        if not upserts_df.isEmpty():
            (
                dt.alias("silver")
                .merge(
                    upserts_df.alias("bronze"),
                    "silver.case_id = bronze.case_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
    else:
        (
            upserts_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("reported_date")
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
    metric_sink: MetricSink | None = None,
) -> None:
    """foreachBatch handler for the Silver fraud_cases stream."""
    if batch_df.isEmpty():
        return
    typed_df = cast_types(batch_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    quarantine_rows = write_quarantine(quarantine_path, quarantine_df)
    if metric_sink:
        record_quarantine_rows(metric_sink, "fraud_cases", quarantine_rows)
    merge_to_silver(spark, silver_path, valid_df)
    print(f"[{JOB_NAME}] batch {batch_id} processed.")


def main() -> None:
    spark = build_spark_session()
    metric_sink = OtelMetricSink(JOB_NAME)

    bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
    checkpoint_path = spark.conf.get("spark.silver.fraud_cases.checkpoint.path")

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
                metric_sink=metric_sink,
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )
    register_silver_table(spark, silver_path)
    metric_sink.force_flush()


if __name__ == "__main__":
    main()
