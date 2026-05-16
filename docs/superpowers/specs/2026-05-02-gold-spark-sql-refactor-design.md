# Gold Layer Spark SQL Refactor — Design Spec

**Date:** 2026-05-02
**Branch:** `feat/spark-bronze-stream`
**Files in scope:**
- `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py`
- `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py`
- `src/tests/batch_processing/test_unit_gold_customer_main.py` (new)
- `src/tests/batch_processing/test_unit_gold_terminal_main.py` (new)

---

## Problem

Both Gold batch jobs use Python loops to build `F.count(F.when(...))` / `F.avg(F.when(...))` aggregation expressions dynamically over `WINDOW_DAYS = [1, 7, 30]`. This is hard to read and maintain — you must trace the loop to understand what columns are produced. The terminal job compounds this with a 2-step aggregation (intermediate `_nb_tx_{days}d` / `_nb_fraud_{days}d` columns) required because PySpark's DataFrame API does not allow an aggregation Column expression inside `F.when`. There are also no tests for either Gold job.

---

## Approach: SQL Compute Layer (Approach A)

Replace the dynamic Python loop logic inside `compute_customer_features` and `compute_terminal_features` with `createOrReplaceTempView` + `spark.sql("""...""")`. All other Python — config reading, data loading, date arithmetic, and writing — is unchanged.

### Why this approach

- **SQL is the right abstraction for transformations.** Window aggregations are native SQL idioms; `CASE WHEN event_date >= date_sub(...) THEN 1 END` is immediately readable without tracing Python control flow.
- **Eliminates the 2-step terminal aggregation.** Spark SQL allows `SUM(...) / COUNT(...)` across the same conditional expressions in one `GROUP BY`, which removes the intermediate `_nb_tx_{days}d` columns and the second Python loop.
- **No structural change.** Keeping the existing function boundaries (`load_...`, `compute_...`, `write_gold_partition`) minimises diff size and makes the change easy to review.
- **Functions stay unit-testable.** Each `compute_*` function takes a DataFrame, registers a view, runs SQL, and returns a DataFrame — easy to test with a small in-memory fixture.

---

## Detailed Design

### Customer Gold job

**`compute_customer_features(spark, silver_df, feature_date)`** — replaces the Python loop:

```sql
SELECT
    customer_id,
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)  THEN 1 END) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
    COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)  THEN amount END), 0) AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)  THEN 1 END) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
    COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)  THEN amount END), 0) AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29) THEN 1 END) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,
    COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29) THEN amount END), 0) AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
    DATE '{feature_date}' AS feature_date
FROM silver_txn
GROUP BY customer_id
```

Window semantics (unchanged from original):
- 1D: `event_date == feature_date` (offset 0)
- 7D: `[feature_date - 6, feature_date]` (offset 6)
- 30D: `[feature_date - 29, feature_date]` (offset 29)

The function signature gains a `spark` parameter (needed to call `spark.sql()`). `main()` is updated to pass it.

**What is removed:**
- `WINDOW_DAYS = [1, 7, 30]` constant (no longer needed)
- `import datetime` `F.date_sub` loop inside the function
- `from pyspark.sql import types as T` import (no longer needed)

**What is unchanged:**
- `resolve_feature_date(spark)`
- `write_gold_partition(spark, df, gold_path, feature_date, label)` — only `row_count = df.count()` is removed (extra scan, same rationale as Silver simplification)
- `main()` — cosmetic update only (pass `spark` to `compute_customer_features`)

---

### Terminal Gold job

**`compute_terminal_features(spark, labeled_df, feature_date)`** — replaces the 2-step Python loop:

```sql
SELECT
    terminal_id,
    -- NB_TX windows (upper bound already enforced by Silver filter in load_labeled_transactions)
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 8)  THEN 1 END) AS TERMINAL_NB_TX_1DAY_WINDOW,
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 14) THEN 1 END) AS TERMINAL_NB_TX_7DAY_WINDOW,
    COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 37) THEN 1 END) AS TERMINAL_NB_TX_30DAY_WINDOW,
    -- RISK windows: SUM(is_fraud) / COUNT(*) for the window; 0.0 if no transactions
    CASE WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 8)  THEN 1 END) > 0
         THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 8)  THEN is_fraud END) * 1.0
              / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 8)  THEN 1 END)
         ELSE 0.0 END AS TERMINAL_RISK_1DAY_WINDOW,
    CASE WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 14) THEN 1 END) > 0
         THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 14) THEN is_fraud END) * 1.0
              / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 14) THEN 1 END)
         ELSE 0.0 END AS TERMINAL_RISK_7DAY_WINDOW,
    CASE WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 37) THEN 1 END) > 0
         THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 37) THEN is_fraud END) * 1.0
              / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 37) THEN 1 END)
         ELSE 0.0 END AS TERMINAL_RISK_30DAY_WINDOW,
    DATE '{feature_date}' AS feature_date
FROM labeled_txn
GROUP BY terminal_id
```

Delay-offset semantics (unchanged, `DELAY_DAYS = 7`):
- 1D window:  `[feature_date - 8,  feature_date - 7]`  → offset = DELAY + 1  = 8
- 7D window:  `[feature_date - 14, feature_date - 7]`  → offset = DELAY + 7  = 14
- 30D window: `[feature_date - 37, feature_date - 7]`  → offset = DELAY + 30 = 37

The Spark SQL engine optimises repeated identical `COUNT(CASE WHEN ...)` sub-expressions, so there is no performance penalty vs the original 2-step approach.

**What is removed:**
- `WINDOW_DAYS = [1, 7, 30]` constant
- Step 1 loop building `_nb_tx_{days}d` / `_nb_fraud_{days}d` intermediate columns
- Step 2 loop deriving `TERMINAL_NB_TX_*` / `TERMINAL_RISK_*` and calling `.drop()`
- `from pyspark.sql import types as T` (no longer needed in `compute_terminal_features`)

**What is unchanged:**
- `DELAY_DAYS = 7` constant
- `load_labeled_transactions(spark, silver_txn_path, silver_fraud_path, lookback_start, delay_end)` — Python join logic is already readable
- `resolve_feature_date(spark)`
- `write_gold_partition(spark, df, gold_path, feature_date, label)` — remove `row_count = df.count()` only
- `main()` — cosmetic update only (pass `spark` to `compute_terminal_features`)

---

## Tests

Test files follow the Silver unit-test pattern: pytest + PySpark local mode, real Spark session (no mocking of DataFrame operations), mock only `DeltaTable` and config where needed.

### `test_unit_gold_customer_main.py`

**`TestResolveFeatureDate`**
- `test_conf_set`: conf returns `"2024-01-15"` → returns `date(2024, 1, 15)`
- `test_conf_blank_defaults_to_yesterday`: conf returns `""` → returns `date.today() - timedelta(1)`

**`TestComputeCustomerFeatures`**
- `test_count_and_avg_per_window`: provide rows for a single customer spanning 31 days; assert correct `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_*` counts and `CUSTOMER_AVG_AMOUNT_WINDOW_*` values for all 3 windows
- `test_zero_transactions_in_window_gives_zero_avg`: customer with no rows in 1D window → `CUSTOMER_AVG_AMOUNT_WINDOW_1D = 0`
- `test_feature_date_column_set`: output has `feature_date` column equal to the input date

**`TestWriteGoldPartition`**
- `test_first_run_creates_table`: `DeltaTable.isDeltaTable` returns `False` → `partitionBy("feature_date")` write called
- `test_existing_table_uses_replace_where`: `DeltaTable.isDeltaTable` returns `True` → `replaceWhere` option set; no `partitionBy`

### `test_unit_gold_terminal_main.py`

**`TestResolveFeatureDate`** — same two cases as customer

**`TestLoadLabeledTransactions`**
- `test_join_attaches_is_fraud`: transaction with matching fraud case → `is_fraud = 1`
- `test_null_fraud_becomes_zero`: transaction with no fraud case → `is_fraud = 0`
- `test_filters_to_lookback_window`: rows outside `[lookback_start, delay_end]` are excluded

**`TestComputeTerminalFeatures`**
- `test_nb_tx_counts_per_window`: rows for a single terminal across different dates; verify counts for 1D/7D/30D windows with delay offset
- `test_risk_calculation`: provide 4 transactions for a window, 1 fraud → `TERMINAL_RISK = 0.25`
- `test_zero_tx_gives_zero_risk`: terminal with no rows in 1D window → `TERMINAL_RISK_1DAY_WINDOW = 0.0`
- `test_feature_date_column_set`: output has `feature_date` column

**`TestWriteGoldPartition`** — same two cases as customer

---

## File Summary

| File | Action | Net change |
|---|---|---|
| `silver_transactions_window_aggregate_customer_gold.py` | Replace `compute_customer_features`, remove `row_count`, add `spark` param | ~−30 lines |
| `silver_transactions_window_aggregate_terminal_gold.py` | Replace `compute_terminal_features`, remove `row_count`, add `spark` param | ~−50 lines |
| `test_unit_gold_customer_main.py` | New — 5 test cases | ~+120 lines |
| `test_unit_gold_terminal_main.py` | New — 8 test cases | ~+200 lines |

---

## Out of Scope

- Extracting shared `resolve_feature_date` / `write_gold_partition` into a `gold_utils.py` (follow-up)
- Adding a watermark/CDF incremental layer to Gold (separate design)
- Changing the output schema or column names (must stay aligned with `api/models.py`)
