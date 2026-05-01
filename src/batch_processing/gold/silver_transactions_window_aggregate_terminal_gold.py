"""Silver transactions + fraud cases window-aggregate to Gold terminal features.

Reads Silver transactions joined with Silver fraud cases, then computes rolling
window aggregations (1-day, 7-day, 30-day) with a ``delay_period=7`` offset
for each terminal as of ``feature_date``.

The delay period mirrors ``get_count_risk_rolling_window(delay_period=7)`` in
the training notebook.  The most recent 7 days of fraud labels may not yet be
confirmed by the time the batch runs, so each window is offset by 7 days:

  - 1D  → transactions in [feature_date - 8,  feature_date - 7]
  - 7D  → transactions in [feature_date - 14, feature_date - 7]
  - 30D → transactions in [feature_date - 37, feature_date - 7]

Output columns match ``api/models.py`` TransactionRequest exactly:

  terminal_id, feature_date,
  TERMINAL_NB_TX_1DAY_WINDOW/7DAY_WINDOW/30DAY_WINDOW,
  TERMINAL_RISK_1DAY_WINDOW/7DAY_WINDOW/30DAY_WINDOW  (fraud rate 0.0–1.0)

TERMINAL_RISK = confirmed_fraud_count / total_tx_count for the window.
Terminals with zero transactions in a window get RISK = 0.0.

Dependencies:
  ``spark.gold.silver.input.path``  → s3a://silver/transactions   (exists)
  ``spark.gold.silver.fraud.path``  → s3a://silver/fraud_cases    (build separately)

Expected ``silver_fraud_cases`` schema (minimum):
  transaction_id : LongType   — FK to silver.transactions
  is_fraud       : IntegerType — 0 = legitimate, 1 = confirmed fraud

Transactions absent from fraud_cases are treated as non-fraud (is_fraud = 0),
which is correct: a missing fraud case record means the transaction was never
flagged.

Pipeline steps:
  1. Resolve ``feature_date`` — from ``spark.gold.feature.date`` conf; yesterday by default.
  2. Read Silver transactions filtered to the max look-back window
     [feature_date - 37, feature_date - 7] (delay + 30-day window).
  3. Left-join with Silver fraud_cases on ``transaction_id``; coalesce null → 0.
  4. Compute terminal features in one group-by pass using conditional aggregation.
  5. Derive TERMINAL_RISK from intermediate nb_fraud / nb_tx counts.
  6. Write to Gold terminal_features Delta, overwriting only the
     ``feature_date`` partition (idempotent reruns).

Run:
    spark-submit /opt/gold/silver_transactions_window_aggregate_terminal_gold.py

All configuration is loaded from spark-defaults.conf (``spark.gold.*`` namespace).
"""
from __future__ import annotations

import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DELAY_DAYS = 7          # label delay offset (days)
WINDOW_DAYS = [1, 7, 30]  # window sizes — must match api/models.py


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("gold-terminal-window-features").getOrCreate()


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
# Load
# ---------------------------------------------------------------------------


def load_labeled_transactions(
    spark: SparkSession,
    silver_txn_path: str,
    silver_fraud_path: str,
    lookback_start: datetime.date,
    delay_end: datetime.date,
) -> DataFrame:
    """Join Silver transactions with Silver fraud cases to attach is_fraud.

    Both tables are filtered to the delay-offset look-back window
    [lookback_start, delay_end] before the join to minimise shuffle size.

    Transactions with no matching fraud_case record are treated as non-fraud
    (is_fraud = 0): a missing entry means the transaction was never flagged.
    """
    txn_df = (
        spark.read.format("delta")
        .load(silver_txn_path)
        .filter(
            (F.col("event_date") >= F.lit(lookback_start))
            & (F.col("event_date") <= F.lit(delay_end))
        )
        .select("transaction_id", "terminal_id", "event_date")
    )

    fraud_df = (
        spark.read.format("delta")
        .load(silver_fraud_path)
        .select(
            "transaction_id",
            F.col("is_fraud").cast(T.IntegerType()),
        )
    )

    return (
        txn_df.join(fraud_df, on="transaction_id", how="left").withColumn(
            "is_fraud", F.coalesce(F.col("is_fraud"), F.lit(0))
        )
    )


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------


def compute_terminal_features(
    labeled_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per terminal with fraud-rate window features as of ``feature_date``.

    Each window W covers [feature_date - (DELAY_DAYS + W), feature_date - DELAY_DAYS]
    inclusive — the upper bound is already enforced by the Silver filter, so the
    conditional aggregation only needs to gate on the lower bound.

    The two-step approach (aggregate → derive risk) avoids using an aggregation
    Column expression inside F.when, which is not supported in a single agg() call.
    """
    fd = F.lit(feature_date).cast(T.DateType())

    # Step 1: compute raw nb_tx and nb_fraud per window in one group-by pass.
    agg_exprs = []
    for days in WINDOW_DAYS:
        # Lower bound of the W-day window:  fd - (DELAY + W)
        # Example (delay=7, W=1): fd-8 ≤ event_date ≤ fd-7  →  1 day
        cutoff = F.date_sub(fd, DELAY_DAYS + days)
        in_window = F.col("event_date") >= cutoff
        agg_exprs += [
            F.count(F.when(in_window, F.lit(1))).alias(f"_nb_tx_{days}d"),
            F.coalesce(
                F.sum(F.when(in_window, F.col("is_fraud"))), F.lit(0)
            )
            .cast(T.LongType())
            .alias(f"_nb_fraud_{days}d"),
        ]

    raw_df = labeled_df.groupBy("terminal_id").agg(*agg_exprs)

    # Step 2: derive TERMINAL_RISK = nb_fraud / nb_tx; 0.0 when no transactions.
    result = raw_df
    for days in WINDOW_DAYS:
        suffix = f"{days}DAY_WINDOW"
        nb_tx_col = F.col(f"_nb_tx_{days}d")
        nb_fraud_col = F.col(f"_nb_fraud_{days}d")
        result = (
            result.withColumn(f"TERMINAL_NB_TX_{suffix}", nb_tx_col)
            .withColumn(
                f"TERMINAL_RISK_{suffix}",
                F.when(nb_tx_col > 0, (nb_fraud_col / nb_tx_col).cast(T.DoubleType())).otherwise(
                    F.lit(0.0)
                ),
            )
            .drop(f"_nb_tx_{days}d", f"_nb_fraud_{days}d")
        )

    return result.withColumn("feature_date", fd)


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
    reruns are fully idempotent.  On first run (table absent) falls back to
    a plain partitioned write that creates the Delta table from scratch.
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

    silver_txn_path = spark.conf.get("spark.gold.silver.input.path")
    silver_fraud_path = spark.conf.get("spark.gold.silver.fraud.path")
    gold_path = spark.conf.get("spark.gold.terminal.output.path")

    feature_date = resolve_feature_date(spark)
    # Max look-back = delay + max_window = 7 + 30 = 37 days
    lookback_start = feature_date - datetime.timedelta(days=DELAY_DAYS + max(WINDOW_DAYS))
    delay_end = feature_date - datetime.timedelta(days=DELAY_DAYS)

    print(
        f"[gold] terminal features: feature_date={feature_date}"
        f"  window=[{lookback_start}, {delay_end}]"
    )

    labeled_df = load_labeled_transactions(
        spark, silver_txn_path, silver_fraud_path, lookback_start, delay_end
    )
    terminal_df = compute_terminal_features(labeled_df, feature_date)
    write_gold_partition(spark, terminal_df, gold_path, feature_date, "terminal_features")


if __name__ == "__main__":
    main()
