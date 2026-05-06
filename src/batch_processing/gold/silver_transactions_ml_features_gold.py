"""Silver transactions + Gold customer/terminal → Gold ML features table.

Assembles one flat row per transaction with all 15 fraud-model features and a
TX_FRAUD label.  The table is partitioned by ``feature_date`` and written with
``replaceWhere`` so nightly re-runs are fully idempotent.

Output columns (match ``api/models.py`` TransactionRequest + TX_FRAUD label):

  transaction_id, event_timestamp,
  TX_AMOUNT, IS_WEEKEND, IS_NIGHT,
  CUSTOMER_AVG_AMOUNT_WINDOW_1D/7D/30D,
  CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D/7D/30D,
  TERMINAL_RISK_1DAY/7DAY/30DAY_WINDOW,
  TERMINAL_NB_TX_1DAY/7DAY/30DAY_WINDOW,
  TX_FRAUD,
  feature_date

``event_timestamp`` is the Feast point-in-time join key.  ML teams call
``feast.get_historical_features(entity_df)`` with a date-range entity_df to
retrieve any slice of history without re-running this job.

Sources:
  spark.gold.silver.input.path    → s3a://silver/transactions
  spark.gold.customer.output.path → s3a://gold/customer_features    (read as input)
  spark.gold.terminal.output.path → s3a://gold/terminal_features    (read as input)
  spark.gold.silver.fraud.path    → s3a://silver/fraud_cases
  spark.gold.ml.output.path       → s3a://gold/fraud_detection_ml_features

Dependencies: This job must run after both Gold customer and Gold terminal jobs
have completed for the same ``feature_date`` (enforced in Airflow DAG).

Run:
    spark-submit /opt/gold/silver_transactions_ml_features_gold.py

All configuration is loaded from spark-defaults.conf (``spark.gold.*`` namespace).
"""
from __future__ import annotations

import datetime
from textwrap import dedent

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-ml-features")
        .enableHiveSupport()
        .getOrCreate()
    )


GOLD_DATABASE = "banking"
GOLD_ML_TABLE = "fraud_detection_ml_features"
GOLD_ML_TABLE_FQN = f"{GOLD_DATABASE}.{GOLD_ML_TABLE}"


def register_ml_features_gold_table(
    spark: SparkSession, gold_path: str, db_location: str
) -> None:
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE} LOCATION '{db_location}'"
    )
    spark.sql(
        dedent(
            f"""
            CREATE TABLE IF NOT EXISTS {GOLD_ML_TABLE_FQN}
            USING DELTA
            LOCATION '{gold_path}'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """
        ).strip()
    )
    spark.sql(
        dedent(
            f"""
            ALTER TABLE {GOLD_ML_TABLE_FQN}
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
    if not raw:
        return datetime.date.today() - datetime.timedelta(days=1)
    try:
        return datetime.date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"spark.gold.feature.date must be YYYY-MM-DD, got: {raw!r}"
        ) from exc


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


def load_silver_transactions(
    spark: SparkSession, silver_path: str, feature_date: datetime.date
) -> DataFrame:
    """Read Silver transactions for exactly ``feature_date``.

    Selects only the columns needed for the ML features table so the shuffle
    in assemble_ml_features is as narrow as possible.
    """
    return (
        spark.read.format("delta")
        .load(silver_path)
        .filter(F.col("event_date") == F.lit(feature_date))
        .select(
            "transaction_id",
            "customer_id",
            "terminal_id",
            "event_timestamp",
            F.col("amount").cast(T.DoubleType()).alias("amount"),
            F.col("is_weekend").cast(T.BooleanType()).alias("is_weekend"),
            F.col("is_night").cast(T.BooleanType()).alias("is_night"),
        )
    )


def load_gold_features(
    spark: SparkSession, gold_path: str, feature_date: datetime.date
) -> DataFrame:
    """Read a single ``feature_date`` partition from a Gold Delta table.

    ``feature_date`` is dropped from the returned DataFrame because it is
    re-added by assemble_ml_features after the join.
    """
    return (
        spark.read.format("delta")
        .load(gold_path)
        .filter(F.col("feature_date") == F.lit(feature_date))
        .drop("feature_date")
    )


def load_fraud_labels(spark: SparkSession, fraud_path: str) -> DataFrame:
    """Read Silver fraud_cases and return (transaction_id, is_fraud IntegerType)."""
    return (
        spark.read.format("delta")
        .load(fraud_path)
        .select(
            "transaction_id",
            F.col("is_fraud").cast(T.IntegerType()).alias("is_fraud"),
        )
    )


# ---------------------------------------------------------------------------
# Feature assembly
# ---------------------------------------------------------------------------


def assemble_ml_features(
    spark: SparkSession,
    silver_txn_df: DataFrame,
    customer_df: DataFrame,
    terminal_df: DataFrame,
    fraud_df: DataFrame,
    feature_date: datetime.date,
) -> DataFrame:
    """Join all sources into one flat row per transaction.

    Left-joins ensure:
    - New customers/terminals not yet in Gold default to 0 (COALESCE).
    - Transactions absent from fraud_cases default to TX_FRAUD = 0 (legitimate).

    Precondition: ``silver_txn_df`` is already filtered to ``event_date = feature_date``.
    """
    silver_txn_df.createOrReplaceTempView("ml_silver_txn")
    customer_df.createOrReplaceTempView("ml_customer")
    terminal_df.createOrReplaceTempView("ml_terminal")
    fraud_df.createOrReplaceTempView("ml_fraud")

    return spark.sql(f"""
        SELECT
            t.transaction_id,
            t.event_timestamp,
            t.amount                                              AS TX_AMOUNT,
            t.is_weekend                                          AS IS_WEEKEND,
            t.is_night                                            AS IS_NIGHT,
            COALESCE(c.CUSTOMER_AVG_AMOUNT_WINDOW_1D,  0.0)      AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
            COALESCE(c.CUSTOMER_AVG_AMOUNT_WINDOW_7D,  0.0)      AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
            COALESCE(c.CUSTOMER_AVG_AMOUNT_WINDOW_30D, 0.0)      AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
            COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,  0) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
            COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,  0) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
            COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D, 0) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,
            COALESCE(tm.TERMINAL_RISK_1DAY_WINDOW,  0.0)          AS TERMINAL_RISK_1DAY_WINDOW,
            COALESCE(tm.TERMINAL_RISK_7DAY_WINDOW,  0.0)          AS TERMINAL_RISK_7DAY_WINDOW,
            COALESCE(tm.TERMINAL_RISK_30DAY_WINDOW, 0.0)          AS TERMINAL_RISK_30DAY_WINDOW,
            COALESCE(tm.TERMINAL_NB_TX_1DAY_WINDOW,  0)           AS TERMINAL_NB_TX_1DAY_WINDOW,
            COALESCE(tm.TERMINAL_NB_TX_7DAY_WINDOW,  0)           AS TERMINAL_NB_TX_7DAY_WINDOW,
            COALESCE(tm.TERMINAL_NB_TX_30DAY_WINDOW, 0)           AS TERMINAL_NB_TX_30DAY_WINDOW,
            COALESCE(f.is_fraud, 0)                               AS TX_FRAUD,
            DATE '{feature_date}'                                 AS feature_date
        FROM ml_silver_txn t
        LEFT JOIN ml_customer c   ON t.customer_id   = c.customer_id
        LEFT JOIN ml_terminal tm  ON t.terminal_id   = tm.terminal_id
        LEFT JOIN ml_fraud f      ON t.transaction_id = f.transaction_id
    """)


# ---------------------------------------------------------------------------
# Write
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

    silver_txn_path   = spark.conf.get("spark.gold.silver.input.path")
    customer_path     = spark.conf.get("spark.gold.customer.output.path")
    terminal_path     = spark.conf.get("spark.gold.terminal.output.path")
    silver_fraud_path = spark.conf.get("spark.gold.silver.fraud.path")
    gold_path         = spark.conf.get("spark.gold.ml.output.path")
    db_location       = spark.conf.get("spark.banking.database.location")

    register_ml_features_gold_table(spark, gold_path, db_location)

    feature_date = resolve_feature_date(spark)
    print(f"[gold] ml_features: feature_date={feature_date}")

    silver_txn_df = load_silver_transactions(spark, silver_txn_path, feature_date)
    customer_df   = load_gold_features(spark, customer_path, feature_date)
    terminal_df   = load_gold_features(spark, terminal_path, feature_date)
    fraud_df      = load_fraud_labels(spark, silver_fraud_path)

    ml_df = assemble_ml_features(
        spark, silver_txn_df, customer_df, terminal_df, fraud_df, feature_date
    )
    write_gold_partition(spark, ml_df, gold_path, feature_date, "ml_features")


if __name__ == "__main__":
    main()
