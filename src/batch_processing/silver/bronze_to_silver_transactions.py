"""Spark Structured Streaming job: Bronze Parquet -> Silver Delta (transactions).

Reads new Bronze CDC Parquet files incrementally via ``foreachBatch``, applies
type casts, deduplicates within each micro-batch keeping the latest CDC event
per ``transaction_id`` by LSN (source sequence), and MERGEs into a Silver
Delta table — correctly handling inserts, updates, and deletes.

All configuration is loaded from spark-defaults.conf under ``spark.silver.*``.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import window as W

# ---------------------------------------------------------------------------
# Bronze schema (must match the schema written by cdc_transactions_to_bronze)
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
    return SparkSession.builder.appName("bronze-to-silver-transactions").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze string/epoch columns to Silver types.

    ``_lsn`` is retained here for deterministic deduplication in
    ``merge_to_silver`` and dropped just before the MERGE.
    """
    return (
        df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
        .withColumnRenamed("_op", "_cdc_op")
        .withColumn(
            "_source_ts", (F.col("_source_ts_ms") / 1000).cast(T.TimestampType())
        )
        .withColumn("_cdc_ts", (F.col("_cdc_ts_ms") / 1000).cast(T.TimestampType()))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .drop(
            "_source_table", "_snapshot", "_ingested_at",
            "_source_ts_ms", "_cdc_ts_ms", "_deleted",
        )
    )


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN and MERGE into Silver Delta table.

    Deduplication uses ``_lsn DESC`` (Postgres log sequence number) as the
    primary ordering key — it is monotonically increasing within a Postgres
    instance and gives the correct event ordering when a single transaction_id
    appears multiple times in the same micro-batch.
    """
    window = W.Window.partitionBy("transaction_id").orderBy(F.desc("_lsn"))
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_lsn")  # _lsn not needed in Silver after dedup
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.transaction_id = bronze.transaction_id",
            )
            .whenMatchedDelete(condition="bronze._cdc_op = 'd'")
            .whenMatchedUpdateAll(condition="bronze._cdc_op != 'd'")
            .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
            .execute()
        )
    else:
        # Initial write: filter out delete-only records (nothing to delete yet)
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .write.format("delta")
            .save(silver_path)
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    checkpoint_path = spark.conf.get("spark.silver.checkpoint.path")
    trigger_interval = spark.conf.get("spark.silver.trigger.interval")

    bronze_stream = (
        spark.readStream.format("parquet")
        .schema(BRONZE_SCHEMA)
        .load(bronze_path)
    )

    def foreach_batch(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        clean_df = cast_types(batch_df)
        merge_to_silver(spark, silver_path, clean_df)

    (
        bronze_stream.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
