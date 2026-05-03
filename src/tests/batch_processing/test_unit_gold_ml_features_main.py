"""Unit tests for Silver + Gold → Gold ML features batch job.

Tests: resolve_feature_date (conf/default), assemble_ml_features (join logic,
column renaming, null defaults), write_gold_partition (first-run vs replaceWhere).
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

import batch_processing.gold.silver_transactions_ml_features_gold as mod

MODULE = "batch_processing.gold.silver_transactions_ml_features_gold"

FEATURE_DATE = datetime.date(2024, 1, 15)

_TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("terminal_id", T.StringType()),
    T.StructField("event_timestamp", T.TimestampType()),
    T.StructField("amount", T.DecimalType(18, 2)),
    T.StructField("is_weekend", T.BooleanType()),
    T.StructField("is_night", T.BooleanType()),
])

_CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("CUSTOMER_AVG_AMOUNT_WINDOW_1D", T.DoubleType()),
    T.StructField("CUSTOMER_AVG_AMOUNT_WINDOW_7D", T.DoubleType()),
    T.StructField("CUSTOMER_AVG_AMOUNT_WINDOW_30D", T.DoubleType()),
    T.StructField("CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D", T.LongType()),
    T.StructField("CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D", T.LongType()),
    T.StructField("CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D", T.LongType()),
])

_TERMINAL_SCHEMA = T.StructType([
    T.StructField("terminal_id", T.StringType()),
    T.StructField("TERMINAL_NB_TX_1DAY_WINDOW", T.LongType()),
    T.StructField("TERMINAL_NB_TX_7DAY_WINDOW", T.LongType()),
    T.StructField("TERMINAL_NB_TX_30DAY_WINDOW", T.LongType()),
    T.StructField("TERMINAL_RISK_1DAY_WINDOW", T.DoubleType()),
    T.StructField("TERMINAL_RISK_7DAY_WINDOW", T.DoubleType()),
    T.StructField("TERMINAL_RISK_30DAY_WINDOW", T.DoubleType()),
])

_FRAUD_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("is_fraud", T.IntegerType()),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-ml-features")
        .getOrCreate()
    )


def _make_txn(spark, txn_id=1, customer_id="C001", terminal_id="T001",
              amount="100.00", is_weekend=False, is_night=False):
    ts = datetime.datetime(2024, 1, 15, 10, 0, 0)
    return spark.createDataFrame(
        [(txn_id, customer_id, terminal_id, ts, Decimal(amount), is_weekend, is_night)],
        schema=_TXN_SCHEMA,
    )


def _make_customer(spark, customer_id="C001", avg1=100.0, avg7=90.0, avg30=80.0,
                   nb1=2, nb7=10, nb30=30):
    return spark.createDataFrame(
        [(customer_id, avg1, avg7, avg30, nb1, nb7, nb30)],
        schema=_CUSTOMER_SCHEMA,
    )


def _make_terminal(spark, terminal_id="T001", nb1=5, nb7=30, nb30=100,
                   risk1=0.1, risk7=0.05, risk30=0.02):
    return spark.createDataFrame(
        [(terminal_id, nb1, nb7, nb30, risk1, risk7, risk30)],
        schema=_TERMINAL_SCHEMA,
    )


def _make_fraud(spark, txn_id=1, is_fraud=1):
    return spark.createDataFrame([(txn_id, is_fraud)], schema=_FRAUD_SCHEMA)


def _empty_customer(spark):
    return spark.createDataFrame([], schema=_CUSTOMER_SCHEMA)


def _empty_terminal(spark):
    return spark.createDataFrame([], schema=_TERMINAL_SCHEMA)


def _empty_fraud(spark):
    return spark.createDataFrame([], schema=_FRAUD_SCHEMA)


class TestResolveFeatureDate:
    def test_conf_set(self, spark):
        spark.conf.set("spark.gold.feature.date", "2024-01-15")
        assert mod.resolve_feature_date(spark) == datetime.date(2024, 1, 15)

    def test_conf_blank_defaults_to_yesterday(self, spark):
        spark.conf.set("spark.gold.feature.date", "")
        expected = datetime.date.today() - datetime.timedelta(days=1)
        assert mod.resolve_feature_date(spark) == expected


class TestAssembleMlFeatures:
    def test_all_api_columns_present(self, spark):
        """Output must contain all 15 API feature columns plus TX_FRAUD and feature_date."""
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark),
            _make_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        cols = set(result.columns)
        expected = {
            "transaction_id", "event_timestamp",
            "TX_AMOUNT", "IS_WEEKEND", "IS_NIGHT",
            "CUSTOMER_AVG_AMOUNT_WINDOW_1D", "CUSTOMER_AVG_AMOUNT_WINDOW_7D",
            "CUSTOMER_AVG_AMOUNT_WINDOW_30D",
            "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D",
            "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D",
            "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D",
            "TERMINAL_RISK_1DAY_WINDOW", "TERMINAL_RISK_7DAY_WINDOW",
            "TERMINAL_RISK_30DAY_WINDOW",
            "TERMINAL_NB_TX_1DAY_WINDOW", "TERMINAL_NB_TX_7DAY_WINDOW",
            "TERMINAL_NB_TX_30DAY_WINDOW",
            "TX_FRAUD", "feature_date",
        }
        assert expected.issubset(cols)

    def test_tx_amount_renamed_from_amount(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, amount="150.00"),
            _make_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        assert float(result.first()["TX_AMOUNT"]) == pytest.approx(150.0)

    def test_is_weekend_is_night_passed_through(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, is_weekend=True, is_night=True),
            _make_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        row = result.first()
        assert row["IS_WEEKEND"] is True
        assert row["IS_NIGHT"] is True

    def test_customer_features_joined(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark),
            _make_customer(spark, avg1=123.0, nb7=42),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        row = result.first()
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == pytest.approx(123.0)
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D"] == 42

    def test_terminal_features_joined(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark),
            _make_customer(spark),
            _make_terminal(spark, risk1=0.25, nb30=999),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        row = result.first()
        assert float(row["TERMINAL_RISK_1DAY_WINDOW"]) == pytest.approx(0.25)
        assert row["TERMINAL_NB_TX_30DAY_WINDOW"] == 999

    def test_fraud_label_attached(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, txn_id=1),
            _make_customer(spark),
            _make_terminal(spark),
            _make_fraud(spark, txn_id=1, is_fraud=1),
            FEATURE_DATE,
        )
        assert result.first()["TX_FRAUD"] == 1

    def test_no_fraud_case_defaults_to_zero(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, txn_id=99),
            _make_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        assert result.first()["TX_FRAUD"] == 0

    def test_no_customer_features_defaults_to_zero(self, spark):
        """New customer not yet in Gold → all customer features = 0."""
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, customer_id="NEW_C"),
            _empty_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        row = result.first()
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == 0.0
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 0

    def test_no_terminal_features_defaults_to_zero(self, spark):
        """New terminal not yet in Gold → all terminal features = 0."""
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark, terminal_id="NEW_T"),
            _make_customer(spark),
            _empty_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        row = result.first()
        assert float(row["TERMINAL_RISK_1DAY_WINDOW"]) == 0.0
        assert row["TERMINAL_NB_TX_1DAY_WINDOW"] == 0

    def test_one_row_per_transaction_no_fanout(self, spark):
        """Join must not multiply rows even with multiple customers on same terminal."""
        txn_df = spark.createDataFrame(
            [
                (1, "C001", "T001", datetime.datetime(2024, 1, 15), Decimal("100.00"), False, False),
                (2, "C002", "T001", datetime.datetime(2024, 1, 15), Decimal("200.00"), False, False),
            ],
            schema=_TXN_SCHEMA,
        )
        customer_df = spark.createDataFrame(
            [
                ("C001", 100.0, 90.0, 80.0, 2, 10, 30),
                ("C002", 50.0, 45.0, 40.0, 1, 5, 15),
            ],
            schema=_CUSTOMER_SCHEMA,
        )
        result = mod.assemble_ml_features(
            spark, txn_df, customer_df, _make_terminal(spark),
            _empty_fraud(spark), FEATURE_DATE,
        )
        assert result.count() == 2

    def test_feature_date_column_set(self, spark):
        result = mod.assemble_ml_features(
            spark,
            _make_txn(spark),
            _make_customer(spark),
            _make_terminal(spark),
            _empty_fraud(spark),
            FEATURE_DATE,
        )
        assert result.first()["feature_date"] == FEATURE_DATE


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
