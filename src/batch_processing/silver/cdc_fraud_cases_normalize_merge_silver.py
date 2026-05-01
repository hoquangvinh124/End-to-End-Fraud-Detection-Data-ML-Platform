"""CDC fraud cases normalize-merge to Silver: Bronze Parquet → Silver Delta.

Reads incremental Bronze fraud_cases CDC Parquet files, normalises them into
canonical Silver rows, and MERGEs the result into a partitioned Silver Delta table.

``is_fraud`` is derived in Silver (not kept as a raw column) so all downstream
consumers — Gold terminal features, training dataset, reporting — can read a
single clean boolean without reimplementing the business rule:

  is_fraud = 1  iff  case_status = 'confirmed' AND resolved_at IS NOT NULL
  is_fraud = 0  otherwise (open, dismissed, or not yet resolved)

CDC lifecycle:
  - INSERT when investigation opens (resolved_at NULL, case_status='open')
  - UPDATE when investigation closes (resolved_at set, case_status='confirmed'/'dismissed')
  - Silver MERGE keeps the latest version per case_id (LSN-ordered)

Pipeline steps:
  1. Read new Bronze CDC Parquet files incrementally (checkpoint-tracked).
  2. Cast types: reported_at/resolved_at/created_at → TIMESTAMP,
     loss_amount → DECIMAL(12,2); derive is_fraud (BooleanType).
  3. Validate rows — invalid records go to quarantine Delta table.
  4. Deduplicate valid rows by _lsn DESC, _source_ts DESC per case_id.
  5. MERGE into Silver Delta table (LSN guard prevents late events regressing state).

Run:
    spark-submit /opt/bronze/cdc_fraud_cases_normalize_merge_silver.py

All configuration is loaded from spark-defaults.conf (``spark.silver.fraud_cases.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

# ---------------------------------------------------------------------------
# Bronze schema — must match the Parquet written by cdc_fraud_cases_to_bronze
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("case_id", T.StringType()),
        T.StructField("transaction_id", T.LongType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("card_id", T.StringType()),
        T.StructField("fraud_scenario", T.IntegerType()),
        T.StructField("case_status", T.StringType()),
        T.StructField("resolution_source", T.StringType()),
        T.StructField("reported_at", T.StringType()),
        T.StructField("resolved_at", T.StringType()),   # nullable — NULL while open
        T.StructField("loss_amount", T.StringType()),
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
        .withColumn("resolved_at", F.to_timestamp("resolved_at"))   # stays NULL if not yet resolved
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("loss_amount", F.col("loss_amount").cast(T.DecimalType(12, 2)))
        # is_fraud = confirmed investigation that has a resolution timestamp
        .withColumn(
            "is_fraud",
            (F.col("case_status") == F.lit("confirmed")) & F.col("resolved_at").isNotNull(),
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
    print(f"[silver-fraud_cases] quarantined {bad_count:,} bad rows → {quarantine_path}")


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
    checkpoint_path = spark.conf.get("spark.silver.fraud_cases.checkpoint.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")

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
