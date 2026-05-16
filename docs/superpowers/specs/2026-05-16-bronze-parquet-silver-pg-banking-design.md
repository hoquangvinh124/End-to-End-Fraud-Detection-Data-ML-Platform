# Design: Bronze → Parquet, Silver pg_banking namespace, Trino allow-drop-table fix

**Date:** 2026-05-16
**Status:** Approved
**Scope:** Fix Trino ClickHouse connector error; refactor Bronze jobs to plain Parquet; refactor Silver jobs to read Parquet (not Delta CDF); align Silver Hive namespace to `pg_banking`; update dbt sources to read from Silver.

---

## 1. Problem Statement

Three issues surfaced when running the dbt+Trino+ClickHouse pipeline:

1. **`clickhouse.allow-drop-table` not used** — Trino 481 removed this property from the ClickHouse connector. The catalog file still declares it, causing a startup error.

2. **Bronze writes Delta Lake** — Bronze jobs register a Hive metastore table and enable CDF. Bronze is the raw landing zone and should be as simple as possible: plain Parquet, no table format overhead, no CDF.

3. **Silver reads Bronze via CDF** — Silver jobs use `readStream.format("delta").option("readChangeFeed", "true")`, which couples Silver to Delta-specific Bronze. Since Bronze is moving to plain Parquet, Silver must be updated accordingly.

4. **Silver Hive namespace is `banking`** — too generic, does not communicate source system. `pg_banking` (PostgreSQL OLTP banking domain) is more precise.

5. **dbt sources point to Bronze** — `sources.yml` references `lakehouse.bronze.*`. With Silver now owning normalized data, dbt staging models should read from `lakehouse.pg_banking.*` (Silver).

---

## 2. Architecture After This Change

```
Kafka (Debezium Avro)
        ↓
Spark Structured Streaming [Bronze job]
        ↓  format=parquet, append, no Hive registration
MinIO: s3a://bronze/transactions/   (plain Parquet, partitioned by _ingested_at date)
MinIO: s3a://bronze/fraud_cases/

        ↓
Spark Structured Streaming [Silver job]
  readStream.format("parquet")
  foreachBatch: dedup → cast → validate → MERGE into Delta
        ↓
MinIO: s3a://silver/pg_banking/transactions/   (Delta Lake, CDF enabled)
MinIO: s3a://silver/pg_banking/fraud_cases/    (Delta Lake, CDF enabled)
Hive Metastore: pg_banking.transactions, pg_banking.fraud_cases
Trino: lakehouse.pg_banking.transactions, lakehouse.pg_banking.fraud_cases

        ↓
dbt staging (reads lakehouse.pg_banking.*)
        ↓  stg_transactions, stg_fraud_cases
dbt intermediate (ClickHouse)
        ↓  int_customer_window_features, int_terminal_window_features
dbt marts (ClickHouse)
        ↓  mart_fraud_ml_features
```

---

## 3. Changes Per Component

### 3.1 `src/lakehouse/trino/catalog/clickhouse.properties`

Remove the unsupported property:

```diff
- clickhouse.allow-drop-table=true
```

### 3.2 Bronze Jobs (2 files)

**Files:** `src/cdc_ingestion/cdc_transactions_to_bronze.py`, `src/cdc_ingestion/cdc_fraud_cases_to_bronze.py`

- Remove `register_*_external_table()` function entirely
- Remove `.enableHiveSupport()` from `build_spark_session()`
- Remove all `spark.sql(...)` Hive DDL calls from `main()`
- Change `writeStream.format("delta")` → `format("parquet")`
- Remove `TBLPROPERTIES delta.enableChangeDataFeed` (no longer written)
- Keep all other logic (Avro schema fetch, field extraction, `_ingested_at` column)

Bronze output: plain Parquet files, append mode, checkpoint-tracked.

### 3.3 Silver Jobs (2 files)

**Files:** `src/batch_processing/bronze_to_silver/cdc_transactions_normalize_merge_silver.py`, `src/batch_processing/bronze_to_silver/cdc_fraud_cases_normalize_merge_silver.py`

- Change `SILVER_DATABASE = "banking"` → `"pg_banking"` in both files
- Change `readStream.format("delta").option("readChangeFeed", "true")` → `readStream.format("parquet")`
- Remove `_CDF_META_COLS` constant
- Remove `.filter(F.col("_change_type") == "insert")` and `.drop(*_CDF_META_COLS)` from `process_batch()`
- Silver still writes Delta Lake with CDF enabled (unchanged — dbt and downstream systems rely on this)
- Quarantine path, MERGE logic, validation, type casting: unchanged

### 3.4 dbt `sources.yml`

**File:** `src/dbt/models/staging/sources.yml`

```diff
  sources:
    - name: bronze
-     database: lakehouse
-     schema: bronze
+     database: lakehouse
+     schema: pg_banking
      tables:
        - name: transactions
-         description: "CDC Bronze table: banking.transactions via Debezium"
+         description: "Silver normalized table: pg_banking.transactions (Delta Lake)"
        - name: fraud_cases
-         description: "CDC Bronze table: banking.fraud_cases via Debezium"
+         description: "Silver normalized table: pg_banking.fraud_cases (Delta Lake)"
```

> The dbt source name remains `bronze` to avoid renaming all `{{ source('bronze', ...) }}` references in staging models. Only the physical schema changes.

---

## 4. What Does NOT Change

- `src/batch_processing/bronze_to_silver/` folder name — kept as-is
- Silver MERGE logic (transactions: `whenNotMatchedInsertAll`; fraud_cases: LSN-ordered dedup + `whenMatchedUpdateAll`)
- Silver CDF enabled (`delta.enableChangeDataFeed = true`)
- Quarantine write to Delta
- All dbt model SQL logic (`stg_transactions.sql`, `stg_fraud_cases.sql`, intermediate, marts)
- `dbt_project.yml` model configs
- ClickHouse intermediate and marts models
- Airflow DAG structure

---

## 5. Error Handling

| Failure point | Behavior |
|---|---|
| Bronze Parquet write failure | Kafka offset not committed → Spark auto-retries on restart from checkpoint |
| Silver reads incomplete Parquet file | Spark file source tracks committed files via checkpoint; partial files not committed |
| Silver MERGE failure | Delta transaction rolled back; checkpoint not advanced → retry on next trigger |
| Hive registration failure | Silver main() fails before streaming starts → surfaced in Airflow logs |

---

## 6. Testing Notes

- After Bronze change: verify Parquet files land in MinIO at expected path with correct schema
- After Silver change: verify `pg_banking.transactions` and `pg_banking.fraud_cases` are queryable via Trino (`lakehouse.pg_banking.*`)
- After dbt sources change: run `dbt ls --select staging` to verify source resolution, then `dbt run --select staging`
- Trino ClickHouse: verify `SHOW CATALOGS` in Trino no longer throws `allow-drop-table` error

---

## 7. Files Changed

| Action | Path |
|---|---|
| MODIFY | `src/lakehouse/trino/catalog/clickhouse.properties` |
| MODIFY | `src/cdc_ingestion/cdc_transactions_to_bronze.py` |
| MODIFY | `src/cdc_ingestion/cdc_fraud_cases_to_bronze.py` |
| MODIFY | `src/batch_processing/bronze_to_silver/cdc_transactions_normalize_merge_silver.py` |
| MODIFY | `src/batch_processing/bronze_to_silver/cdc_fraud_cases_normalize_merge_silver.py` |
| MODIFY | `src/dbt/models/staging/sources.yml` |
