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
    spark: SparkSession, silver_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per customer with rolling window features as of ``feature_date``.

    Windows: 1D=[fd, fd], 7D=[fd-6, fd], 30D=[fd-29, fd].
    Null avg (no transactions in a window) is coalesced to 0.0 to match .fillna(0).

    Precondition: ``silver_df`` must already be filtered to
    ``event_date BETWEEN fd-29 AND fd``; future-dated rows will be
    miscounted otherwise.
    """
    silver_df.createOrReplaceTempView("silver_txn_customer")
    return spark.sql(f"""
        SELECT
            customer_id,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)
                THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
            COALESCE(
                AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)
                    THEN amount END),
                0
            )
                AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)
                THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
            COALESCE(
                AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)
                    THEN amount END),
                0
            )
                AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29)
                THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,
            COALESCE(
                AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29)
                    THEN amount END),
                0
            )
                AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
            DATE '{feature_date}' AS feature_date
        FROM silver_txn_customer
        GROUP BY customer_id
    """)


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
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, gold_path):
        writer = writer.option("replaceWhere", f"feature_date = '{feature_date}'")
    else:
        writer = writer.partitionBy("feature_date")
    writer.save(gold_path)
    print(f"[gold] {label}: feature_date={feature_date} → {gold_path}")


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

    customer_df = compute_customer_features(spark, silver_df, feature_date)
    write_gold_partition(
        spark, customer_df, gold_path, feature_date, "customer_features"
    )


if __name__ == "__main__":
    main()
