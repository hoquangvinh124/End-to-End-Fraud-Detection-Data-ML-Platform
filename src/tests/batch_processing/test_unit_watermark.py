"""Unit tests for batch_processing.utils.watermark.

Uses unittest.mock throughout — no real SparkSession or Delta table needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from batch_processing.utils.watermark import read_watermark, write_watermark

# ---------------------------------------------------------------------------
# read_watermark
# ---------------------------------------------------------------------------


class TestReadWatermark:
    def test_returns_none_when_no_delta_table(self):
        spark = MagicMock()
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = False
            result = read_watermark(
                spark, "s3a://silver/_watermarks/", "silver-transactions"
            )
        assert result is None
        mock_dt.isDeltaTable.assert_called_once_with(
            spark, "s3a://silver/_watermarks/"
        )

    def test_returns_none_when_no_row_for_job_name(self):
        spark = MagicMock()
        (
            spark.read.format.return_value
            .load.return_value
            .filter.return_value
            .select.return_value
            .first.return_value
        ) = None
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            with patch("batch_processing.utils.watermark.F") as mock_f:
                mock_dt.isDeltaTable.return_value = True
                mock_f.col.return_value = MagicMock()
                result = read_watermark(
                    spark, "s3a://silver/_watermarks/", "silver-transactions"
                )
        assert result is None

    def test_returns_version_int_when_row_exists(self):
        spark = MagicMock()
        mock_row = {"last_bronze_version": 42}
        (
            spark.read.format.return_value
            .load.return_value
            .filter.return_value
            .select.return_value
            .first.return_value
        ) = mock_row
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            with patch("batch_processing.utils.watermark.F") as mock_f:
                mock_dt.isDeltaTable.return_value = True
                mock_f.col.return_value = MagicMock()
                result = read_watermark(
                    spark, "s3a://silver/_watermarks/", "silver-transactions"
                )
        assert result == 42
        assert isinstance(result, int)


# ---------------------------------------------------------------------------
# write_watermark
# ---------------------------------------------------------------------------


class TestWriteWatermark:
    def test_initial_write_when_no_table(self):
        spark = MagicMock()
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = False
            write_watermark(
                spark, "s3a://silver/_watermarks/", "silver-transactions", 5
            )
        (
            spark.createDataFrame.return_value
            .write.format.return_value
            .mode.return_value
            .save.assert_called_once_with("s3a://silver/_watermarks/")
        )

    def test_merge_when_table_exists(self):
        spark = MagicMock()
        mock_merge_builder = MagicMock()
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = True
            mock_delta = MagicMock()
            mock_dt.forPath.return_value = mock_delta
            mock_delta.alias.return_value.merge.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdateAll.return_value = (
                mock_merge_builder
            )
            mock_merge_builder.whenNotMatchedInsertAll.return_value = (
                mock_merge_builder
            )
            write_watermark(
                spark, "s3a://silver/_watermarks/", "silver-transactions", 5
            )
        mock_dt.forPath.assert_called_once_with(
            spark, "s3a://silver/_watermarks/"
        )
        mock_merge_builder.execute.assert_called_once()

    def test_falls_back_to_merge_when_concurrent_create_race(self):
        spark = MagicMock()
        mock_merge_builder = MagicMock()
        with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = False
            # Simulate race: initial save fails
            (
                spark.createDataFrame.return_value
                .write.format.return_value
                .mode.return_value
                .save.side_effect
            ) = Exception("path already exists")
            mock_delta = MagicMock()
            mock_dt.forPath.return_value = mock_delta
            mock_delta.alias.return_value.merge.return_value = mock_merge_builder
            mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
            mock_merge_builder.whenNotMatchedInsertAll.return_value = (
                mock_merge_builder
            )
            write_watermark(
                spark, "s3a://silver/_watermarks/", "silver-transactions", 5
            )
        mock_merge_builder.execute.assert_called_once()
