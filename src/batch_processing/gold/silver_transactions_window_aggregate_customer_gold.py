"""Silver transactions window-aggregate to Gold customer features.

Reads Silver transactions Delta and computes rolling window aggregations
(1-day, 7-day, 30-day) for each customer as of ``feature_date``.

Output columns match ``api/models.py`` TransactionRequest exactly so the
features can be passed to the fraud model without renaming:

  customer_id, feature_date,
  CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D/7D/30D,
  CUSTOMER_AVG_AMOUNT_WINDOW_1D/7D/30D

Window semantics (no label needed — purely transaction-based):
  - 1D: transactions where event_date == feature_date
  - 7D: event_date in [feature_date - 6, feature_date]
  - 30D: event_date in [feature_date - 29, feature_date]

Missing-window values follow the training notebook (.fillna(0)):
  count → 0, avg_amount → 0.0

Pipeline steps:
  1. Resolve ``feature_date`` — from ``spark.gold.feature.date`` conf
     (Airflow passes ``YYYY-MM-DD``); defaults to yesterday when blank.
  2. Read Silver Delta filtered to [feature_date - 29, feature_date].
  3. Compute features in a single group-by pass using conditional aggregation.
  4. Write to Gold customer_features Delta, overwriting only the
     ``feature_date`` partition (idempotent reruns).

Run:
    spark-submit /opt/gold/silver_transactions_window_aggregate_customer_gold.py

All configuration is loaded from spark-defaults.conf (``spark.gold.*`` namespace).
"""
from __future__ import annotations

import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Window sizes (days) — must match the feature names in api/models.py
WINDOW_DAYS = [1, 7, 30]


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("gold-customer-window-features").getOrCreate()


def resolve_feature_date(spark: SparkSession) -> datetime.date:
    """Return the target feature date.

    Reads ``spark.gold.feature.date`` (Airflow passes ``YYYY-MM-DD``).
    Defaults to yesterday so the job runs standalone without Airflow.
    """
    raw = spark.conf.get("spark.gold.feature.date", "").strip()
    return (
        datetime.date.fromisoformat(raw)
        if raw
        else datetime.date.today() - datetime.timedelta(days=1)
    )


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------


def compute_customer_features(
    silver_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per customer with rolling window features as of ``feature_date``.

    Windows [feature_date - (N-1), feature_date] inclusive give exactly N days.
    A single group-by pass computes all window sizes via conditional aggregation.
    Null avg (customer with zero transactions in a window) is coalesced to 0.0
    to match the notebook's ``.fillna(0)`` treatment.
    """
    fd = F.lit(feature_date).cast(T.DateType())

    agg_exprs = []
    for days in WINDOW_DAYS:
        # date_sub(fd, days-1): first day of the N-day window
        # 1d → fd-0 = fd (today only)
        # 7d → fd-6  (last 7 days)
        # 30d → fd-29 (last 30 days)
        cutoff = F.date_sub(fd, days - 1)
        in_window = F.col("event_date") >= cutoff
        suffix = f"WINDOW_{days}D"
        agg_exprs += [
            F.count(F.when(in_window, F.lit(1))).alias(
                f"CUSTOMER_NUMBER_OF_TRANSACTIONS_{suffix}"
            ),
            F.coalesce(
                F.avg(F.when(in_window, F.col("amount"))).cast(T.DecimalType(18, 2)),
                F.lit(0).cast(T.DecimalType(18, 2)),
            ).alias(f"CUSTOMER_AVG_AMOUNT_{suffix}"),
        ]

    return silver_df.groupBy("customer_id").agg(*agg_exprs).withColumn("feature_date", fd)


# ---------------------------------------------------------------------------
# Write to Gold Delta
# ---------------------------------------------------------------------------


def write_gold_partition(
    spark: SparkSession,
    df: DataFrame,
    gold_path: str,
    feature_date: datetime.date,
    label: str,
) -> None:
    """Overwrite only the ``feature_date`` partition in the Gold Delta table.

    ``replaceWhere`` leaves every other historical partition untouched so
    reruns are fully idempotent — no duplicate feature rows accumulate.
    On first run the table does not yet exist: fall back to a plain
    partitioned write that creates the Delta table from scratch.
    """
    row_count = df.count()

    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, gold_path):
        writer = writer.option("replaceWhere", f"feature_date = '{feature_date}'")
    else:
        writer = writer.partitionBy("feature_date")

    writer.save(gold_path)
    print(f"[gold] {label}: {row_count:,} rows for feature_date={feature_date} → {gold_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    silver_path = spark.conf.get("spark.gold.silver.input.path")
    gold_path = spark.conf.get("spark.gold.customer.output.path")

    feature_date = resolve_feature_date(spark)
    # 30-day window needs fd-29 inclusive → scan from fd-29
    lookback_start = feature_date - datetime.timedelta(days=29)

    print(
        f"[gold] customer features: feature_date={feature_date}"
        f"  lookback_start={lookback_start}"
    )

    silver_df = (
        spark.read.format("delta")
        .load(silver_path)
        .filter(
            (F.col("event_date") >= F.lit(lookback_start))
            & (F.col("event_date") <= F.lit(feature_date))
        )
        .select("customer_id", "event_date", "amount")
    )

    customer_df = compute_customer_features(silver_df, feature_date)
    write_gold_partition(spark, customer_df, gold_path, feature_date, "customer_features")


if __name__ == "__main__":
    main()
