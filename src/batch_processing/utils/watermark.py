"""Shared watermark utility for Silver batch jobs.

Persists the last successfully processed Bronze Delta version per job in a
small Delta table (``s3a://silver/_watermarks/``). Silver jobs call
``read_watermark`` before reading Bronze CDF and ``write_watermark`` only
after a successful Silver MERGE — guaranteeing idempotent re-runs.
"""
from __future__ import annotations

import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_SCHEMA = T.StructType(
    [
        T.StructField("job_name", T.StringType(), nullable=False),
        T.StructField("last_bronze_version", T.LongType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)


def read_watermark(
    spark: SparkSession, watermark_path: str, job_name: str
) -> int | None:
    """Return the last successfully processed Bronze Delta version for *job_name*.

    Returns ``None`` when no watermark table or no row for *job_name* exists
    (first run) — callers treat ``None`` as ``startingVersion=0``.
    """
    if not DeltaTable.isDeltaTable(spark, watermark_path):
        return None
    row = (
        spark.read.format("delta")
        .load(watermark_path)
        .filter(F.col("job_name") == job_name)
        .select("last_bronze_version")
        .first()
    )
    return int(row["last_bronze_version"]) if row else None


def write_watermark(
    spark: SparkSession, watermark_path: str, job_name: str, version: int
) -> None:
    """Upsert the watermark for *job_name* to *version*.

    Uses a Delta MERGE on ``job_name`` so the table stays at one row per job.
    Falls back to an initial Delta write when the table does not yet exist.
    """
    new_row = spark.createDataFrame(
        [(job_name, version, datetime.datetime.utcnow())],
        schema=_SCHEMA,
    )
    if DeltaTable.isDeltaTable(spark, watermark_path):
        (
            DeltaTable.forPath(spark, watermark_path)
            .alias("wm")
            .merge(new_row.alias("new"), "wm.job_name = new.job_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").save(watermark_path)
