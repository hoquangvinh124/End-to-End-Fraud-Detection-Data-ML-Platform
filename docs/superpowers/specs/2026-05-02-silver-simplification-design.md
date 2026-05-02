# Silver Jobs Simplification

**Date:** 2026-05-02  
**Branch:** `feat/spark-bronze-stream`  
**Scope:** `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` and `cdc_fraud_cases_normalize_merge_silver.py`

## Problem

Both Silver jobs contain defensive checks and merge logic that either can't occur (dead code) or duplicate guarantees already provided by Delta CDF version ordering, adding unnecessary complexity and extra Spark actions.

## Design

### Transactions Silver

`banking.transactions` has no `updated_at` and no mutable columns — transactions are immutable. Debezium only emits `_op='r'` (snapshot) or `_op='c'` (insert). Therefore:

- **Remove** `W.Window` LSN dedup — a given `transaction_id` appears at most once per batch.
- **Remove** `whenMatchedDelete` and `whenMatchedUpdateAll` MERGE branches — dead code.
- **Simplify** MERGE to `whenNotMatchedInsertAll` only (idempotency on re-run).
- **Drop** `_lsn` from the Silver output schema (`cast_types` drops the column).
- No cross-batch LSN guard needed.

### Fraud_cases Silver

`banking.fraud_cases` has `case_status` and `resolved_at` that change when a case is resolved — updates are real. However, cases are never deleted.

- **Keep** `W.Window` LSN dedup — a case can be opened and closed in the same batch window, producing multiple CDC events for the same `case_id`.
- **Keep** `whenMatchedUpdateAll` — required for status/resolved_at updates.
- **Remove** `whenMatchedDelete` — fraud cases are never deleted in the source schema.
- **Remove** cross-batch LSN guard (`bronze._lsn >= silver._lsn`) — Delta CDF version ordering (`startingVersion=last+1, endingVersion=current`) already ensures no out-of-order processing between runs. Simplify match conditions to just `bronze._cdc_op != 'd'`.

### Both Jobs — Remove Defensive Noise

The following are removed from both jobs:

| What | Why |
|---|---|
| `DeltaTable.isDeltaTable(spark, bronze_path)` guard | Bronze job always creates Delta; next line fails with clear error if it doesn't exist |
| `try/except` around CDF read with string-matching | Exception propagates either way; Delta's own error is readable |
| `bronze_df.isEmpty()` early exit | Bronze is append-only; any version range passing the watermark gate has inserts. Even if empty, the downstream MERGE is a no-op and watermark should still advance |
| `count()` before write in `write_quarantine` and `merge_to_silver` | Extra Spark action (full scan) just for a log number |
| Verbose module/function docstrings | Reduce to 1–2 sentences; detail lives in the plan doc |

### Tests

- **Remove** `TestMainCDFReadError.test_raises_runtime_error_on_retention_exceeded` — tests the `try/except` block being removed.
- All other tests remain unchanged.

## Non-Goals

- Gold layer jobs — out of scope.
- `watermark.py` — no changes.
- `validate_and_split` / `write_quarantine` logic — no changes.
- First-run `else` branch in `merge_to_silver` — no changes.
