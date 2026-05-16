# Silver Incremental Processing via Delta CDF

**Date:** 2026-05-02
**Status:** Approved

## Problem

The Silver batch jobs (`cdc_transactions_normalize_merge_silver.py`,
`cdc_fraud_cases_normalize_merge_silver.py`) currently use Spark Structured
Streaming with `trigger(availableNow=True)` and a MinIO checkpoint. When
migrated to native Spark batch jobs (no Structured Streaming), the checkpoint
mechanism disappears and the jobs have no way to know which Bronze data has
already been processed — causing them to re-read all Bronze data on every
nightly run.

## Goal

Convert both Silver jobs to native Spark batch jobs that read only new Bronze
data since the last successful run, using Delta Lake's Change Data Feed (CDF)
as the incremental mechanism. Compute cost per run scales with daily volume,
not total Bronze history.

## Approach: Bronze → Delta + CDF, Silver batch with Delta watermark

### Why CDF

Delta CDF lets a batch reader specify `startingVersion` and `endingVersion` to
get only the rows committed to Bronze since the last run. Because Bronze is
append-only (Kafka CDC events), all CDF entries are `insert` type — CDF adds
no semantic complexity, only precise version-range reads at O(new commits) cost
instead of O(all files).

### Components

#### 1. Bronze output format: Parquet → Delta + CDF

Both Bronze streaming jobs switch their output from `format("parquet")` to
`format("delta")`. CDF is enabled globally via a spark-defaults.conf property
so every new Delta table created in this Spark session has it on by default:

```
spark.databricks.delta.properties.defaults.enableChangeDataFeed  true
```

Bronze streaming logic (Kafka read, Avro unwrap, CDC metadata columns) is
unchanged.

#### 2. Watermark table (`s3a://silver/_watermarks/`)

A shared Delta table that persists the last successfully processed Bronze
version per job between nightly runs. Schema:

| column               | type      | description                              |
|----------------------|-----------|------------------------------------------|
| `job_name`           | STRING    | e.g. `"silver-transactions"`             |
| `last_bronze_version`| LONG      | Last Bronze Delta version fully merged   |
| `updated_at`         | TIMESTAMP | Wall-clock time of last successful run   |

One row per `job_name`. Updates use MERGE on `job_name` so the table stays
small. First run: no row exists → `read_watermark` returns `None` → job reads
from Bronze version 0.

#### 3. Silver batch jobs (rewrite `main()`)

`cast_types`, `validate_and_split`, `write_quarantine`, and `merge_to_silver`
functions are unchanged. Only the entry point changes:

```
1. read_watermark(job_name) → last_version (None on first run)
2. current_version = DeltaTable.forPath(bronze_path).history(1).first()["version"]
3. if last_version >= current_version: log "no new data", exit 0
4. df = spark.read.format("delta")
         .option("readChangeFeed", "true")
         .option("startingVersion", 0 if last_version is None else last_version + 1)
         .option("endingVersion", current_version)
         .load(bronze_path)
5. df = df.filter(_change_type == "insert").drop(_change_type, _commit_version, _commit_timestamp)
6. cast_types() → validate_and_split() → write_quarantine() → merge_to_silver()
7. write_watermark(job_name, current_version)   ← only after successful MERGE
```

#### 4. Shared util: `src/batch_processing/utils/watermark.py`

```python
def read_watermark(spark, watermark_path, job_name) -> int | None
def write_watermark(spark, watermark_path, job_name, version: int) -> None
```

`write_watermark` uses a Delta MERGE on `job_name` (upsert). Both Silver jobs
import from `utils.watermark` — no duplication.

### Data flow

```
Kafka ──► Bronze streaming job ──► s3a://bronze/cdc/<topic>  (Delta + CDF)
                                           │
                        nightly Airflow    │  readChangeFeed
                                           ▼  startingVersion = last+1
                              Silver batch job
                                    │
                          cast / validate / quarantine
                                    │
                                    ▼
                        s3a://silver/<topic>  (Delta MERGE)
                                    │
                         on success │
                                    ▼
                     s3a://silver/_watermarks/  (MERGE job_name)
```

## Error Handling

| Scenario | Behaviour |
|---|---|
| Bronze path is not a Delta table (CDF not enabled) | Raise `RuntimeError` with clear message; job exits non-zero; Airflow retries |
| `startingVersion` exceeds Bronze log retention | Raise `RuntimeError` with instruction to reset watermark to `None` |
| Silver MERGE fails | Watermark is **not** updated; next run retries the same version range (safe: MERGE is idempotent via LSN guard) |
| Quarantine write fails | Log warning; Silver MERGE proceeds (quarantine is audit-only, not blocking) |
| No new Bronze data | Exit 0 cleanly with log message — Airflow marks run as success |

### Idempotency

If the job crashes after MERGE but before writing the watermark, the next run
re-processes the same Bronze version range. The Silver MERGE update condition
`bronze._lsn >= silver._lsn` makes this safe — replaying the same events
never regresses Silver to a stale state.

## Files Changed

| File | Change |
|---|---|
| `src/batch_processing/spark-defaults.conf` | Add `enableChangeDataFeed` default property |
| `src/batch_processing/bronze/cdc_transactions_to_bronze.py` | `format("delta")` |
| `src/batch_processing/bronze/cdc_fraud_cases_to_bronze.py` | `format("delta")` |
| `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` | Rewrite `main()` — batch CDF read + watermark |
| `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py` | Rewrite `main()` — batch CDF read + watermark |
| `src/batch_processing/utils/watermark.py` | **New** — shared `read_watermark` / `write_watermark` |

## Out of Scope

- Gold layer jobs (not affected by Bronze format change)
- Existing Bronze Parquet data migration (first Silver run reads from version 0 of the new Delta table; old Parquet data is handled separately if needed)
- Schema evolution on Bronze Delta table
