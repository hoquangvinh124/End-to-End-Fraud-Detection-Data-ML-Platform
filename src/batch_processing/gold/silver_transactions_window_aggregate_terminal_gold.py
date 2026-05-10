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
  1. Resolve ``feature_date`` from ``spark.gold.feature.date`` conf; else yesterday.
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
from textwrap import dedent

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DELAY_DAYS = 7  # label delay offset (days)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-terminal-window-features")
        .enableHiveSupport()
        .getOrCreate()
    )


GOLD_DATABASE = "banking"
GOLD_TERMINAL_TABLE = "terminal_features"
GOLD_TERMINAL_TABLE_FQN = f"{GOLD_DATABASE}.{GOLD_TERMINAL_TABLE}"


def register_terminal_gold_table(
    spark: SparkSession, gold_path: str, db_location: str
) -> None:
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE} LOCATION '{db_location}'"
    )
    spark.sql(
        dedent(
            f"""
            CREATE TABLE IF NOT EXISTS {GOLD_TERMINAL_TABLE_FQN}
            USING DELTA
            LOCATION '{gold_path}'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """
        ).strip()
    )
    spark.sql(
        dedent(
            f"""
            ALTER TABLE {GOLD_TERMINAL_TABLE_FQN}
            SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """
        ).strip()
    )


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


def _attach_fraud_label(txn_df: DataFrame, fraud_df: DataFrame) -> DataFrame:
    """Left-join transactions with fraud cases; missing entry → is_fraud = 0."""
    return txn_df.join(fraud_df, on="transaction_id", how="left").withColumn(
        "is_fraud", F.coalesce(F.col("is_fraud"), F.lit(0))
    )


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

    return _attach_fraud_label(txn_df, fraud_df)


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------


def compute_terminal_features(
    spark: SparkSession, labeled_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per terminal with fraud-rate window features as of ``feature_date``.

    Each window W covers [feature_date - (DELAY_DAYS + W), feature_date - DELAY_DAYS].
    The upper bound is enforced by the Silver filter in load_labeled_transactions.
    Zero-transaction windows get RISK = 0.0 via the CASE guard.

    Precondition: ``labeled_df`` must already be filtered to
    ``event_date BETWEEN fd-37 AND fd-7``; future-dated rows will be
    miscounted otherwise.
    """
    labeled_df.createOrReplaceTempView("labeled_txn_terminal")
    return spark.sql(f"""
        WITH counts AS (
            SELECT
                terminal_id,
                COUNT(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})
                    THEN 1 END
                ) AS nb_tx_1d,
                COUNT(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})
                    THEN 1 END
                ) AS nb_tx_7d,
                COUNT(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30})
                    THEN 1 END
                ) AS nb_tx_30d,
                SUM(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})
                    THEN is_fraud ELSE 0 END
                ) AS nb_fraud_1d,
                SUM(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})
                    THEN is_fraud ELSE 0 END
                ) AS nb_fraud_7d,
                SUM(
                    CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30})
                    THEN is_fraud ELSE 0 END
                ) AS nb_fraud_30d,
                DATE '{feature_date}' AS feature_date
            FROM labeled_txn_terminal
            GROUP BY terminal_id
        )
        SELECT
            terminal_id,
            nb_tx_1d AS TERMINAL_NB_TX_1DAY_WINDOW,
            nb_tx_7d AS TERMINAL_NB_TX_7DAY_WINDOW,
            nb_tx_30d AS TERMINAL_NB_TX_30DAY_WINDOW,
            CASE WHEN nb_tx_1d > 0 THEN nb_fraud_1d * 1.0 / nb_tx_1d ELSE 0.0 END AS TERMINAL_RISK_1DAY_WINDOW,
            CASE WHEN nb_tx_7d > 0 THEN nb_fraud_7d * 1.0 / nb_tx_7d ELSE 0.0 END AS TERMINAL_RISK_7DAY_WINDOW,
            CASE WHEN nb_tx_30d > 0 THEN nb_fraud_30d * 1.0 / nb_tx_30d ELSE 0.0 END AS TERMINAL_RISK_30DAY_WINDOW,
            feature_date
        FROM counts
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
    reruns are fully idempotent.  On first run (table absent) falls back to
    a plain partitioned write that creates the Delta table from scratch.
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

    silver_txn_path = spark.conf.get("spark.gold.silver.input.path")
    silver_fraud_path = spark.conf.get("spark.gold.silver.fraud.path")
    gold_path = spark.conf.get("spark.gold.terminal.output.path")
    db_location = spark.conf.get("spark.banking.database.location")

    register_terminal_gold_table(spark, gold_path, db_location)

    feature_date = resolve_feature_date(spark)
    # Max look-back = delay + max_window = 7 + 30 = 37 days
    lookback_start = feature_date - datetime.timedelta(days=DELAY_DAYS + 30)
    delay_end = feature_date - datetime.timedelta(days=DELAY_DAYS)

    print(
        f"[gold] terminal features: feature_date={feature_date}"
        f"  window=[{lookback_start}, {delay_end}]"
    )

    labeled_df = load_labeled_transactions(
        spark, silver_txn_path, silver_fraud_path, lookback_start, delay_end
    )
    terminal_df = compute_terminal_features(spark, labeled_df, feature_date)
    write_gold_partition(
        spark, terminal_df, gold_path, feature_date, "terminal_features"
    )


if __name__ == "__main__":
    main()
