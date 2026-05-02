"""Unit tests for Silver → Gold customer window-features batch job.

Tests: resolve_feature_date (conf/default), compute_customer_features (SQL window
logic for all 3 windows), write_gold_partition (first-run vs replaceWhere).
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

import batch_processing.gold.silver_transactions_window_aggregate_customer_gold as mod

MODULE = "batch_processing.gold.silver_transactions_window_aggregate_customer_gold"

FEATURE_DATE = datetime.date(2024, 1, 15)

_TXN_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("amount", T.DecimalType(18, 2)),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-customer")
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


class TestComputeCustomerFeatures:
    def test_count_and_avg_per_window(self, spark):
        """Verify all 3 window counts and avgs for multi-day customer row."""
        # Window cutoffs for FEATURE_DATE=2024-01-15:
        #   1D → event_date >= 2024-01-15  (offset 0)
        #   7D → event_date >= 2024-01-09  (offset 6)
        #  30D → event_date >= 2023-12-17  (offset 29)
        rows = [
            ("C001", datetime.date(2024, 1, 15), Decimal("100.00")),  # 1D, 7D, 30D
            ("C001", datetime.date(2024, 1, 15), Decimal("200.00")),  # 1D, 7D, 30D
            ("C001", datetime.date(2024, 1, 10), Decimal("50.00")),   # 7D, 30D only
            ("C001", datetime.date(2023, 12, 20), Decimal("300.00")), # 30D only
            ("C999", datetime.date(2024, 1, 15), Decimal("999.00")),  # 1D, 7D, 30D
        ]
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C001'").first()

        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 2
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D"] == 3
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D"] == 4
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == pytest.approx(150.0)
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_7D"]) == pytest.approx(
            116.67, abs=0.01
        )
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_30D"]) == pytest.approx(162.5)

        row_c999 = result.filter("customer_id = 'C999'").first()
        assert row_c999["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 1
        assert result.count() == 2  # exactly one row per customer, no fan-out

    def test_zero_transactions_in_window_gives_zero_avg(self, spark):
        """Customer with no 1D transactions gets AVG_1D = 0, not NULL."""
        rows = [("C002", datetime.date(2023, 12, 20), Decimal("100.00"))]  # 30D only
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C002'").first()

        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 0
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == 0.0

    def test_feature_date_column_set(self, spark):
        rows = [("C003", datetime.date(2024, 1, 15), Decimal("50.00"))]
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C003'").first()
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
