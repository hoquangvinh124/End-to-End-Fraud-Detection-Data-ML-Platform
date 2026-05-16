# Silver Transactions Batch Job — Design

## 1. Problem Statement

The Bronze layer stores raw Debezium CDC events as Parquet files on MinIO. Those events need to be
promoted to the Silver layer as a canonical, typed, deduplicated Delta table that downstream Gold
jobs and Trino queries can trust.

## 2. Scope

This design covers one job: `silver_transactions_batch.py` — a Spark Structured Streaming job run
in batch mode (single-pass, `availableNow=True` trigger) that transforms `bronze.transactions`
Parquet files into the `silver.transactions` Delta table.

Out of scope: Silver customer/terminal dimension tables, Gold aggregations, Airflow DAG definition.

## 3. Execution Model

The job uses **Spark Structured Streaming with `trigger(availableNow=True)`**. This processes all
Bronze Parquet files that have not yet been checkpointed, then exits cleanly. Airflow schedules it
via DockerOperator (or KubernetesPodOperator in GKE). The checkpoint at
`s3a://silver/_checkpoints/transactions_silver` tracks progress, making the job fully incremental
and idempotent on retry.

## 4. Data Flow

```
s3a://bronze/cdc/transactions/*.parquet
    │
    ▼
readStream (Parquet, BRONZE_SCHEMA)
    │
    ├── cast_types()        — event_timestamp, amount, CDC ms → TIMESTAMP, DECIMAL
    │                         + add event_date (DATE), _silver_updated_at (TIMESTAMP)
    │
    ├── validate_and_split() — split rows into valid_df and quarantine_df
    │       rules:
    │         • transaction_id IS NULL
    │         • amount IS NULL OR amount <= 0
    │         • event_timestamp IS NULL
    │
    ├── quarantine_df → MERGE → s3a://silver/quarantine/transactions/ (Delta)
    │                    (append new quarantine rows, deduplicated by transaction_id + _error_reason)
    │
    └── valid_df → dedup (latest _lsn per transaction_id within batch)
                       │
                       └─→ Delta MERGE → s3a://silver/transactions/ (Delta, partitioned by event_date)
                               whenMatchedDelete   (if _cdc_op = 'd')
                               whenMatchedUpdateAll
                               whenNotMatchedInsertAll
```

## 5. Silver Transactions Schema

| Column               | Type           | Source                     |
|----------------------|----------------|----------------------------|
| `transaction_id`     | BIGINT         | Bronze                     |
| `event_timestamp`    | TIMESTAMP UTC  | Bronze (cast from STRING)  |
| `event_date`         | DATE           | Derived from event_timestamp (partition key) |
| `customer_id`        | STRING         | Bronze                     |
| `account_id`         | STRING         | Bronze                     |
| `card_id`            | STRING         | Bronze                     |
| `terminal_id`        | STRING         | Bronze                     |
| `amount`             | DECIMAL(18,2)  | Bronze (cast from STRING)  |
| `currency_code`      | STRING         | Bronze                     |
| `transaction_type`   | STRING         | Bronze                     |
| `channel_type`       | STRING         | Bronze                     |
| `auth_status`        | STRING         | Bronze                     |
| `tx_time_seconds`    | INTEGER        | Bronze                     |
| `tx_time_days`       | INTEGER        | Bronze                     |
| `is_weekend`         | BOOLEAN        | Bronze                     |
| `is_night`           | BOOLEAN        | Bronze                     |
| `_cdc_op`            | STRING         | Bronze `_op`               |
| `_source_ts`         | TIMESTAMP      | Derived from `_source_ts_ms / 1000` |
| `_cdc_ts`            | TIMESTAMP      | Derived from `_cdc_ts_ms / 1000`   |
| `_silver_updated_at` | TIMESTAMP      | `current_timestamp()` at write time |

## 6. Quarantine Schema

| Column              | Type      | Description                        |
|---------------------|-----------|------------------------------------|
| `transaction_id`    | BIGINT    | From Bronze (may be NULL)          |
| `_error_reason`     | STRING    | e.g. `"amount must be > 0"`        |
| `_quarantine_ts`    | TIMESTAMP | `current_timestamp()` at write time |
| + all Bronze columns as-is | | For debug/reprocessing           |

## 7. File Structure

```
src/batch_processing/
  silver/
    silver_transactions_batch.py
  bronze/
    cdc_transactions_to_bronze.py  (existing)
  Dockerfile                       (existing, shared image)
  spark-defaults.conf              (existing, add spark.silver.* keys)
  docker-compose.batch_processing.yml (existing, add silver service)
```

## 8. spark-defaults.conf additions

```properties
spark.silver.bronze.input.path     s3a://bronze/cdc/transactions
spark.silver.output.path           s3a://silver/transactions
spark.silver.checkpoint.path       s3a://silver/_checkpoints/transactions_silver
spark.silver.quarantine.path       s3a://silver/quarantine/transactions
```

## 9. Key Design Decisions

- **No dimension joins in Silver** — Customer/terminal enrichment belongs in Gold where window
  aggregations happen. Silver stays clean and fast.
- **`availableNow=True` over `trigger(once=True)`** — `once=True` is deprecated in Spark 3.3+.
  `availableNow=True` is the recommended single-pass batch trigger in Spark 4.x.
- **`_lsn` for deduplication** — PostgreSQL LSN is monotonically increasing, making it a
  reliable ordering key when multiple CDC events arrive for the same `transaction_id`.
- **Delta MERGE for Silver** — Idempotent, handles late CDC updates (e.g., `auth_status` change
  arriving a day after the original INSERT event).
- **Quarantine via MERGE not append** — Avoids growing quarantine table with duplicate bad records
  across reruns. Dedup by `(transaction_id, _error_reason)`.
