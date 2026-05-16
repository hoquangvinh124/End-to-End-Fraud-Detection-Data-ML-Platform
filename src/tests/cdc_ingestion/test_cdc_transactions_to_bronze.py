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
    def test_does_not_enable_hive_support(self):
        builder = MagicMock()
        builder.appName.return_value = builder
        spark = MagicMock()
        builder.getOrCreate.return_value = spark

        spark_session = MagicMock()
        spark_session.builder = builder

        with patch.object(mod, "SparkSession", spark_session):
            result = mod.build_spark_session()

        assert result is spark
        builder.appName.assert_called_once_with("cdc-transactions-to-bronze")
        builder.enableHiveSupport.assert_not_called()
        builder.getOrCreate.assert_called_once_with()


class TestMain:
    def test_writes_parquet_not_delta(self):
        spark = MagicMock()
        raw_df = MagicMock()
        bronze_df = MagicMock()
        query = MagicMock()

        spark.conf.get.side_effect = lambda key: {
            "spark.bronze.topic": "cdc.transactions",
            "spark.bronze.bootstrap.servers": "kafka:9092",
            "spark.bronze.output.path": "s3a://bronze/cdc/transactions",
            "spark.bronze.checkpoint.path": (
                "s3a://bronze/_checkpoints/cdc_transactions_bronze"
            ),
            "spark.bronze.trigger.interval": "5 minutes",
            "spark.bronze.schema.registry.url": "http://schema-registry:8081",
        }[key]

        reader = spark.readStream
        reader.format.return_value.option.return_value.option.return_value.load.return_value = raw_df

        writer = bronze_df.writeStream
        writer.format.return_value = writer
        writer.outputMode.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = query

        with (
            patch.object(mod, "build_spark_session", return_value=spark),
            patch.object(mod, "fetch_avro_schema", return_value='{"type":"record"}'),
            patch.object(mod, "build_bronze_rows", return_value=bronze_df),
        ):
            mod.main()

        writer.format.assert_called_once_with("parquet")
        query.awaitTermination.assert_called_once_with()

    def test_no_hive_registration_in_main(self):
        """main() must not call spark.sql() — no Hive DDL."""
        spark = MagicMock()
        raw_df = MagicMock()
        bronze_df = MagicMock()
        query = MagicMock()

        spark.conf.get.side_effect = lambda key: {
            "spark.bronze.topic": "cdc.transactions",
            "spark.bronze.bootstrap.servers": "kafka:9092",
            "spark.bronze.output.path": "s3a://bronze/cdc/transactions",
            "spark.bronze.checkpoint.path": (
                "s3a://bronze/_checkpoints/cdc_transactions_bronze"
            ),
            "spark.bronze.trigger.interval": "5 minutes",
            "spark.bronze.schema.registry.url": "http://schema-registry:8081",
        }[key]

        reader = spark.readStream
        reader.format.return_value.option.return_value.option.return_value.load.return_value = raw_df

        writer = bronze_df.writeStream
        writer.format.return_value = writer
        writer.outputMode.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = query

        with (
            patch.object(mod, "build_spark_session", return_value=spark),
            patch.object(mod, "fetch_avro_schema", return_value='{"type":"record"}'),
            patch.object(mod, "build_bronze_rows", return_value=bronze_df),
        ):
            mod.main()

        spark.sql.assert_not_called()
