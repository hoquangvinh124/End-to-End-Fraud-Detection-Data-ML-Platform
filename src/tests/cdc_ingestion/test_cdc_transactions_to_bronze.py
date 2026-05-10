"""Unit tests for cdc_transactions_to_bronze.py."""

from __future__ import annotations

import pathlib
import sys
from unittest.mock import MagicMock, patch

MODULE_DIR = pathlib.Path(__file__).resolve().parents[2] / "cdc_ingestion"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

import cdc_transactions_to_bronze as mod  # noqa: E402


class TestBuildSparkSession:
    def test_enables_hive_support_without_hard_coded_metastore(self):
        builder = MagicMock()
        builder.appName.return_value = builder
        builder.enableHiveSupport.return_value = builder
        spark = MagicMock()
        builder.getOrCreate.return_value = spark

        spark_session = MagicMock()
        spark_session.builder = builder

        with patch.object(mod, "SparkSession", spark_session):
            result = mod.build_spark_session()

        assert result is spark
        builder.appName.assert_called_once_with("cdc-transactions-to-bronze")
        builder.enableHiveSupport.assert_called_once_with()
        builder.config.assert_not_called()
        builder.getOrCreate.assert_called_once_with()


class TestRegisterTransactionsExternalTable:
    def test_registers_hive_database_with_location_and_cdf_table(self):
        spark = MagicMock()

        mod.register_transactions_external_table(
            spark, "s3a://bronze/cdc/transactions", "s3a://warehouse/banking.db"
        )

        sql_calls = [c.args[0] for c in spark.sql.call_args_list]
        assert any(
            "CREATE DATABASE IF NOT EXISTS banking" in s and "s3a://warehouse/banking.db" in s
            for s in sql_calls
        )
        assert any("CREATE TABLE IF NOT EXISTS banking.transactions_bronze" in s for s in sql_calls)
        assert any("ALTER TABLE banking.transactions_bronze" in s for s in sql_calls)
        assert any("delta.enableChangeDataFeed" in s for s in sql_calls)


class TestMain:
    def test_bootstraps_catalog_before_stream_start(self):
        spark = MagicMock()
        raw_df = MagicMock()
        bronze_df = MagicMock()
        query = MagicMock()
        order: list[str] = []

        spark.conf.get.side_effect = lambda key: {
            "spark.bronze.topic": "cdc.transactions",
            "spark.bronze.bootstrap.servers": "kafka:9092",
            "spark.bronze.output.path": "s3a://bronze/cdc/transactions",
            "spark.bronze.checkpoint.path": (
                "s3a://bronze/_checkpoints/cdc_transactions_bronze"
            ),
            "spark.bronze.trigger.interval": "5 minutes",
            "spark.bronze.schema.registry.url": "http://schema-registry:8081",
            "spark.banking.database.location": "s3a://warehouse/banking.db",
        }[key]

        reader = spark.readStream
        reader.format.return_value.option.return_value.option.return_value.load.return_value = raw_df

        writer = bronze_df.writeStream
        writer.format.return_value = writer
        writer.outputMode.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.side_effect = lambda *_args, **_kwargs: order.append("start") or query

        with (
            patch.object(mod, "build_spark_session", return_value=spark),
            patch.object(mod, "fetch_avro_schema", return_value='{"type":"record"}'),
            patch.object(mod, "build_bronze_rows", return_value=bronze_df),
            patch.object(
                mod,
                "register_transactions_external_table",
                side_effect=lambda *_args, **_kwargs: order.append("table"),
            ),
        ):
            mod.main()

        assert order == ["table", "start"]
        query.awaitTermination.assert_called_once_with()