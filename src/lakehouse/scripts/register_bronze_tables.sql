-- Registers Bronze Delta tables into the Hive Metastore.
-- Run via register_bronze_tables.sh (which handles idempotency).
-- Requires: Bronze Delta tables already written by Spark (at least one batch).

CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse.staging;

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
