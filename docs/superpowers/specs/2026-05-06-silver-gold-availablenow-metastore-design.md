# Silver `availableNow` Streaming + Silver/Gold Hive Metastore Registration

**Date:** 2026-05-06  
**Status:** Approved

---

## Problem

Silver batch jobs (`cdc_transactions_normalize_merge_silver.py`, `cdc_fraud_cases_normalize_merge_silver.py`) use a custom watermark utility (`utils/watermark.py`) to track the last processed Bronze Delta version. This is brittle: it requires a separate Delta table for state, manual version arithmetic, and two separate read/write calls around the merge. Spark Structured Streaming's built-in checkpoint mechanism handles exactly-once incremental reads automatically—no custom state needed.

Additionally, Silver and Gold Delta tables are not registered in Hive Metastore, making them invisible to Spark SQL, Trino, and other catalog-aware clients. The Bronze job already demonstrates the correct registration pattern.

---

## Approach

### Silver Jobs — Replace Watermark with `availableNow` Streaming

Replace `spark.read` CDF + manual version tracking with `spark.readStream` + `trigger(availableNow=True)` + `foreachBatch`. Spark manages offsets in a checkpoint directory automatically.

**Before:**
```
read_watermark(spark, path, job) → startVersion
spark.read.format("delta").option("readChangeFeed", True)
    .option("startingVersion", startVersion).option("endingVersion", currentVersion)
merge(batch) → write_watermark(spark, path, job, currentVersion)
```

**After:**
```
spark.readStream.format("delta").option("readChangeFeed", True).load(bronze_path)
    .writeStream.trigger(availableNow=True)
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .start()
    .awaitTermination()
```

The `process_batch(batch_df, batch_id)` closure:
1. Filter `_change_type == "insert"` and drop CDF meta columns
2. `cast_types(batch_df)`
3. `validate_and_split(typed_df)` → `write_quarantine` + `merge_to_silver`

### Hive Metastore Registration — Silver + Gold

Each job gets a `register_*_table(spark, path)` function following the Bronze pattern exactly:
1. `CREATE DATABASE IF NOT EXISTS banking`
2. `CREATE TABLE IF NOT EXISTS banking.<name> USING DELTA LOCATION '<path>' TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`
3. `ALTER TABLE banking.<name> SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` — ensures CDF on pre-existing tables

Called in `main()` before the stream starts (Silver) or before the write (Gold).

**Table registry:**

| Job | Hive table | Conf key |
|-----|------------|----------|
| silver transactions | `banking.transactions` | `spark.silver.output.path` |
| silver fraud_cases | `banking.fraud_cases` | `spark.silver.fraud_cases.output.path` |
| gold customer | `banking.customer_features` | `spark.gold.customer.output.path` |
| gold terminal | `banking.terminal_features` | `spark.gold.terminal.output.path` |
| gold ml_features | `banking.ml_features` | `spark.gold.ml.output.path` |

---

## File Changes

### Modified
- `src/batch_processing/spark-defaults.conf` — replace `watermark.path` keys with `checkpoint.path`
- `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` — `availableNow` stream, Hive registration, remove watermark imports; `JOB_NAME` → `"silver-transactions-streaming"`
- `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py` — same pattern; else-branch uses `.save(path)` instead of `.saveAsTable()` (table already registered by pre-registration function)
- `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py` — add `enableHiveSupport`, `register_customer_features_gold_table()`
- `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py` — add `enableHiveSupport`, `register_terminal_features_gold_table()`
- `src/batch_processing/gold/silver_transactions_ml_features_gold.py` — add `enableHiveSupport`, `register_ml_features_gold_table()`
- `src/tests/batch_processing/test_unit_silver_transactions_main.py` — rewrite for streaming pattern

### Deleted
- `src/batch_processing/utils/watermark.py`
- `src/tests/batch_processing/test_unit_watermark.py`

---

## Key Decisions

- **`saveAsTable` in fraud_cases else-branch**: replaced with `.save(path)` — Hive registration is already handled by `register_fraud_cases_silver_table()` called before stream start.
- **`spark-defaults.conf` already has `spark.hadoop.hive.metastore.uris`**: Gold jobs read this automatically; `build_spark_session` adds `enableHiveSupport()` explicitly for clarity.
- **CDF on Gold tables**: included via TBLPROPERTIES even though Gold is written with `replaceWhere` — keeps the table queryable via CDF for downstream consumers.
- **`spark-defaults.conf` `hive.metastore.uris` stays**: batch jobs load from conf; no per-job `.config()` needed (unlike Bronze streaming which sets it explicitly as a safety net).

---

## Testing

- `test_unit_silver_transactions_main.py`: rewritten — 3 test classes:
  - `TestBuildSparkSession` — verifies `enableHiveSupport` present
  - `TestRegisterTransactionsSilverTable` — mocks `spark.sql`, asserts CREATE + ALTER called
  - `TestMain` — mocks `readStream`, verifies `foreachBatch` + `trigger(availableNow=True)` + `awaitTermination`
- `test_unit_watermark.py`: deleted
- Gold test files: no changes needed — existing tests cover `compute_*` and `write_gold_partition`; new `register_*` functions follow same mock pattern as Bronze tests
