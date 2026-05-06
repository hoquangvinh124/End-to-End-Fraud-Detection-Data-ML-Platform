"""Unit tests for cdc_transactions_normalize_merge_silver — streaming pattern.

Validates:
  - register_transactions_silver_table issues correct SQL (CREATE DATABASE with
    LOCATION, CREATE TABLE, ALTER TABLE).
  - make_process_batch handler filters _change_type, drops CDF cols, calls
    cast_types / validate_and_split / write_quarantine / merge_to_silver.
  - Empty batch is a no-op.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

MODULE = "batch_processing.silver.cdc_transactions_normalize_merge_silver"


class TestRegisterTransactionsSilverTable:
    def test_creates_database_with_location_and_table(self):
        spark = MagicMock()
        with patch(f"{MODULE}.build_spark_session"):
            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )
            silver_mod.register_transactions_silver_table(
                spark,
                "s3a://silver/transactions",
                "s3a://warehouse/banking.db",
            )

        sql_calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any("CREATE DATABASE IF NOT EXISTS banking" in s for s in sql_calls)
        assert any("s3a://warehouse/banking.db" in s for s in sql_calls)
        assert any("CREATE TABLE IF NOT EXISTS banking.transactions" in s for s in sql_calls)
        assert any("ALTER TABLE banking.transactions" in s for s in sql_calls)
        assert any("delta.enableChangeDataFeed" in s for s in sql_calls)


class TestMakeProcessBatch:
    def test_filters_inserts_and_calls_pipeline(self):
        spark = MagicMock()
        silver_path = "s3a://silver/transactions"
        quarantine_path = "s3a://silver/quarantine/transactions"

        mock_clean = MagicMock()
        mock_clean.isEmpty.return_value = False

        mock_typed = MagicMock()
        mock_valid = MagicMock()
        mock_quarantine_df = MagicMock()

        with (
            patch(f"{MODULE}.cast_types", return_value=mock_typed) as mock_cast,
            patch(f"{MODULE}.validate_and_split", return_value=(mock_valid, mock_quarantine_df)) as mock_split,
            patch(f"{MODULE}.write_quarantine") as mock_wq,
            patch(f"{MODULE}.merge_to_silver") as mock_merge,
        ):
            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            batch_df = MagicMock()
            batch_df.filter.return_value.drop.return_value = mock_clean

            handler = silver_mod.make_process_batch(spark, silver_path, quarantine_path)
            handler(batch_df, 0)

        batch_df.filter.assert_called_once()
        mock_cast.assert_called_once_with(mock_clean)
        mock_split.assert_called_once_with(mock_typed)
        mock_wq.assert_called_once_with(quarantine_path, mock_quarantine_df)
        mock_merge.assert_called_once_with(spark, silver_path, mock_valid)

    def test_empty_batch_is_noop(self):
        spark = MagicMock()

        with (
            patch(f"{MODULE}.cast_types") as mock_cast,
            patch(f"{MODULE}.merge_to_silver") as mock_merge,
        ):
            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )

            mock_clean = MagicMock()
            mock_clean.isEmpty.return_value = True

            batch_df = MagicMock()
            batch_df.filter.return_value.drop.return_value = mock_clean

            handler = silver_mod.make_process_batch(spark, "s3a://silver/transactions", "s3a://quarantine")
            handler(batch_df, 0)

        mock_cast.assert_not_called()
        mock_merge.assert_not_called()


class TestMain:
    def test_registers_table_and_starts_stream(self):
        with (
            patch(f"{MODULE}.build_spark_session") as mock_build,
            patch(f"{MODULE}.register_transactions_silver_table") as mock_reg,
            patch(f"{MODULE}.make_process_batch"),
        ):
            spark = mock_build.return_value
            spark.conf.get.side_effect = lambda k: {
                "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                "spark.silver.output.path": "s3a://silver/transactions",
                "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                "spark.silver.checkpoint.path": "s3a://silver/_checkpoints/cdc_transactions_silver",
                "spark.banking.database.location": "s3a://warehouse/banking.db",
            }[k]

            stream_mock = (
                spark.readStream.format.return_value
                .option.return_value
                .load.return_value
                .writeStream
                .trigger.return_value
                .foreachBatch.return_value
                .option.return_value
                .start.return_value
            )

            from batch_processing.silver import (
                cdc_transactions_normalize_merge_silver as silver_mod,
            )
            silver_mod.main()

        mock_reg.assert_called_once_with(
            spark, "s3a://silver/transactions", "s3a://warehouse/banking.db"
        )
        stream_mock.awaitTermination.assert_called_once()