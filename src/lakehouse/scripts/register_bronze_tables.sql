-- Run once after Spark Bronze ingestion has written at least one batch.
-- Registers the existing Bronze Delta tables into the Hive Metastore so
-- Trino's lakehouse catalog can query them as dbt sources.
--
-- Safe to re-run: existing registrations are removed first.

CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse.staging;

-- Unregister first so this script is idempotent (no-ops if table not registered)
CALL lakehouse.system.unregister_table(schema_name => 'bronze', table_name => 'transactions');
CALL lakehouse.system.unregister_table(schema_name => 'bronze', table_name => 'fraud_cases');

-- Register Bronze transactions Delta table (auto-infers schema from Delta log)
CALL lakehouse.system.register_table(
    schema_name    => 'bronze',
    table_name     => 'transactions',
    table_location => 's3://bronze/cdc/transactions'
);

-- Register Bronze fraud_cases Delta table
CALL lakehouse.system.register_table(
    schema_name    => 'bronze',
    table_name     => 'fraud_cases',
    table_location => 's3://bronze/cdc/fraud_cases'
);
