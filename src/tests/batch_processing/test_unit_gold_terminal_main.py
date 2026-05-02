"""Unit tests for Silver → Gold terminal window-features batch job.

Tests: resolve_feature_date (conf/default), _attach_fraud_label (join logic),
compute_terminal_features (SQL window counts and risk with delay offset),
write_gold_partition (first-run vs replaceWhere).
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

import batch_processing.gold.silver_transactions_window_aggregate_terminal_gold as mod

MODULE = "batch_processing.gold.silver_transactions_window_aggregate_terminal_gold"

FEATURE_DATE = datetime.date(2024, 1, 15)
# DELAY_DAYS = 7 → delay_end = fd - 7 = 2024-01-08

_TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("terminal_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
])
_FRAUD_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("is_fraud", T.IntegerType()),
])
_LABELED_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("terminal_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("is_fraud", T.IntegerType()),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-terminal")
        .getOrCreate()
    )


class TestResolveFeatureDate:
    def test_conf_set(self, spark):
        spark.conf.set("spark.gold.feature.date", "2024-01-15")
        assert mod.resolve_feature_date(spark) == datetime.date(2024, 1, 15)

    def test_conf_blank_defaults_to_yesterday(self, spark):
        spark.conf.set("spark.gold.feature.date", "")
        expected = datetime.date.today() - datetime.timedelta(days=1)
        assert mod.resolve_feature_date(spark) == expected


class TestAttachFraudLabel:
    """Tests for the private _attach_fraud_label helper.

    Join logic in load_labeled_transactions.
    """

    def test_join_attaches_is_fraud(self, spark):
        txn_df = spark.createDataFrame(
            [(1, "T001", datetime.date(2024, 1, 8))], schema=_TXN_SCHEMA
        )
        fraud_df = spark.createDataFrame([(1, 1)], schema=_FRAUD_SCHEMA)

        result = mod._attach_fraud_label(txn_df, fraud_df)

        assert result.first()["is_fraud"] == 1

    def test_null_fraud_becomes_zero(self, spark):
        txn_df = spark.createDataFrame(
            [(2, "T001", datetime.date(2024, 1, 8))], schema=_TXN_SCHEMA
        )
        fraud_df = spark.createDataFrame([], schema=_FRAUD_SCHEMA)

        result = mod._attach_fraud_label(txn_df, fraud_df)

        assert result.first()["is_fraud"] == 0


class TestComputeTerminalFeatures:
    def test_nb_tx_counts_per_window(self, spark):
        """Verify NB_TX counts for all 3 windows with DELAY_DAYS=7.

        Window cutoffs for FEATURE_DATE=2024-01-15:
          1D → event_date >= date_sub(fd, 8)  = 2024-01-07
          7D → event_date >= date_sub(fd, 14) = 2024-01-01
         30D → event_date >= date_sub(fd, 37) = 2023-12-09
        Upper bound (fd-7 = 2024-01-08) already enforced by load_labeled_transactions.
        """
        rows = [
            (1, "T001", datetime.date(2024, 1, 8), 0),   # 1D, 7D, 30D
            (2, "T001", datetime.date(2024, 1, 7), 0),   # 1D, 7D, 30D
            (3, "T001", datetime.date(2024, 1, 1), 0),   # 7D, 30D (not 1D)
            (
                4,
                "T001",
                datetime.date(2023, 12, 15),
                0,
            ),  # 30D only
        ]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T001'").first()

        assert row["TERMINAL_NB_TX_1DAY_WINDOW"] == 2
        assert row["TERMINAL_NB_TX_7DAY_WINDOW"] == 3
        assert row["TERMINAL_NB_TX_30DAY_WINDOW"] == 4

    def test_risk_calculation(self, spark):
        """4 transactions in 1D window, 1 fraud → RISK_1D = 0.25."""
        rows = [
            (1, "T001", datetime.date(2024, 1, 8), 1),  # fraud
            (2, "T001", datetime.date(2024, 1, 8), 0),
            (3, "T001", datetime.date(2024, 1, 7), 0),
            (4, "T001", datetime.date(2024, 1, 7), 0),
        ]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T001'").first()

        assert row["TERMINAL_RISK_1DAY_WINDOW"] == pytest.approx(0.25)

    def test_zero_tx_gives_zero_risk(self, spark):
        """Terminal with no 1D transactions → RISK_1D = 0.0 (not division-by-zero)."""
        rows = [(1, "T002", datetime.date(2023, 12, 15), 0)]  # 30D only
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T002'").first()

        assert row["TERMINAL_RISK_1DAY_WINDOW"] == 0.0
        assert row["TERMINAL_NB_TX_1DAY_WINDOW"] == 0
        assert row["TERMINAL_NB_TX_30DAY_WINDOW"] == 1      # row IS in 30D window
        assert row["TERMINAL_RISK_30DAY_WINDOW"] == 0.0     # no fraud → risk = 0.0

    def test_feature_date_column_set(self, spark):
        rows = [(1, "T003", datetime.date(2024, 1, 8), 0)]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T003'").first()
        assert row["feature_date"] == FEATURE_DATE


class TestWriteGoldPartition:
    def test_first_run_creates_table(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=False)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.partitionBy.assert_called_once_with("feature_date")
        mock_writer.option.assert_not_called()

    def test_existing_table_uses_replace_where(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=True)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.option.assert_called_once_with(
            "replaceWhere", f"feature_date = '{FEATURE_DATE}'"
        )
        mock_writer.partitionBy.assert_not_called()
