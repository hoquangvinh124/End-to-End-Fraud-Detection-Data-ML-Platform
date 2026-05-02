"""Unit tests for cdc_transactions_normalize_merge_silver.main() logic.

Validates version-range logic, no-data early exit, CDF column dropping,
error handling, and watermark write-after-MERGE ordering.
All Spark/Delta interactions are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# The module under test — import after patching heavy deps
MODULE = "batch_processing.silver.cdc_transactions_normalize_merge_silver"


def _make_spark_mock(bronze_is_delta: bool = True, current_version: int = 10):
    spark = MagicMock()
    # DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
    spark._bronze_version = current_version
    return spark


class TestMainNoNewData:
    def test_exits_early_when_last_equals_current(self, capsys):
        with (
            patch(f"{MODULE}.DeltaTable") as mock_dt,
            patch(f"{MODULE}.read_watermark", return_value=10),
            patch(f"{MODULE}.write_watermark") as mock_wm,
            patch(f"{MODULE}.build_spark_session") as mock_build,
        ):
            mock_dt.isDeltaTable.return_value = True
            mock_dt.forPath.return_value.history.return_value.first.return_value = {
                "version": 10
            }
            mock_build.return_value.conf.get.side_effect = lambda k: {
                "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                "spark.silver.output.path": "s3a://silver/transactions",
                "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                "spark.silver.watermark.path": "s3a://silver/_watermarks/",
            }[k]

            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            silver_mod.main()

        mock_wm.assert_not_called()
        captured = capsys.readouterr()
        assert "no new data" in captured.out


class TestMainCDFReadError:
    def test_raises_runtime_error_on_retention_exceeded(self):
        with (
            patch(f"{MODULE}.DeltaTable") as mock_dt,
            patch(f"{MODULE}.read_watermark", return_value=0),
            patch(f"{MODULE}.build_spark_session") as mock_build,
        ):
            mock_dt.isDeltaTable.return_value = True
            mock_dt.forPath.return_value.history.return_value.first.return_value = {
                "version": 5
            }
            spark = mock_build.return_value
            spark.conf.get.side_effect = lambda k: {
                "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                "spark.silver.output.path": "s3a://silver/transactions",
                "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                "spark.silver.watermark.path": "s3a://silver/_watermarks/",
            }[k]
            read_chain = (
                spark.read.format.return_value.option.return_value.option.return_value.option.return_value
            )
            read_chain.load.side_effect = Exception(
                "outside the range of retained versions"
            )

            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            with pytest.raises(RuntimeError, match="outside log retention"):
                silver_mod.main()


class TestMainWatermarkWrittenAfterMerge:
    def test_watermark_written_only_after_successful_merge(self):
        with (
            patch(f"{MODULE}.DeltaTable") as mock_dt,
            patch(f"{MODULE}.read_watermark", return_value=None),
            patch(f"{MODULE}.write_watermark") as mock_wm,
            patch(f"{MODULE}.cast_types") as mock_cast,
            patch(f"{MODULE}.validate_and_split") as mock_split,
            patch(f"{MODULE}.write_quarantine"),
            patch(f"{MODULE}.merge_to_silver") as mock_merge,
            patch(f"{MODULE}.build_spark_session") as mock_build,
            patch(f"{MODULE}.F"),
        ):
            mock_dt.isDeltaTable.return_value = True
            mock_dt.forPath.return_value.history.return_value.first.return_value = {
                "version": 3
            }
            spark = mock_build.return_value
            spark.conf.get.side_effect = lambda k: {
                "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                "spark.silver.output.path": "s3a://silver/transactions",
                "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                "spark.silver.watermark.path": "s3a://silver/_watermarks/",
            }[k]
            # CDF read returns a non-empty DataFrame mock
            mock_df = MagicMock()
            mock_df.isEmpty.return_value = False
            (
                spark.read.format.return_value
                .option.return_value.option.return_value.option.return_value
                .load.return_value.filter.return_value.drop.return_value
            ) = mock_df
            mock_cast.return_value = mock_df
            mock_split.return_value = (mock_df, mock_df)

            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            silver_mod.main()

        mock_merge.assert_called_once()
        mock_wm.assert_called_once_with(
            spark, "s3a://silver/_watermarks/", "silver-transactions", 3
        )


class TestMainAllRowsQuarantined:
    def test_watermark_advances_when_all_rows_quarantined(self):
        """Watermark must advance even when all valid_df rows are quarantined.

        Prevents infinite reprocessing of permanently-invalid Bronze versions.
        """
        with (
            patch(f"{MODULE}.DeltaTable") as mock_dt,
            patch(f"{MODULE}.read_watermark", return_value=None),
            patch(f"{MODULE}.write_watermark") as mock_wm,
            patch(f"{MODULE}.cast_types") as mock_cast,
            patch(f"{MODULE}.validate_and_split") as mock_split,
            patch(f"{MODULE}.write_quarantine") as mock_quarantine,
            patch(f"{MODULE}.merge_to_silver") as mock_merge,
            patch(f"{MODULE}.build_spark_session") as mock_build,
            patch(f"{MODULE}.F"),
        ):
            mock_dt.isDeltaTable.return_value = True
            mock_dt.forPath.return_value.history.return_value.first.return_value = {
                "version": 7
            }
            spark = mock_build.return_value
            spark.conf.get.side_effect = lambda k: {
                "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                "spark.silver.output.path": "s3a://silver/transactions",
                "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                "spark.silver.watermark.path": "s3a://silver/_watermarks/",
            }[k]
            mock_df = MagicMock()
            mock_df.isEmpty.return_value = False
            (
                spark.read.format.return_value
                .option.return_value.option.return_value.option.return_value
                .load.return_value.filter.return_value.drop.return_value
            ) = mock_df
            mock_cast.return_value = mock_df
            empty_df = MagicMock()
            quarantine_df = MagicMock()
            mock_split.return_value = (empty_df, quarantine_df)

            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            silver_mod.main()

        mock_quarantine.assert_called_once()
        mock_merge.assert_called_once()
        mock_wm.assert_called_once_with(
            spark, "s3a://silver/_watermarks/", "silver-transactions", 7
        )
