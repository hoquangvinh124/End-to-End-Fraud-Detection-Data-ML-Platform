# Design: dbt + Trino Transform Layer + ClickHouse Gold

**Date:** 2026-05-12  
**Status:** Approved  
**Scope:** Replace Spark Silver/Gold batch jobs with dbt+Trino; introduce ClickHouse as the Gold/marts serving layer.

---

## 1. Problem Statement

Currently Spark handles all three layers:
- **Bronze**: CDC Kafka events → Delta Lake (MinIO) ✅ keep
- **Silver**: Bronze CDF → normalized Delta tables (3 Spark jobs)
- **Gold**: Silver → window aggregations + ML features (3 Spark jobs)

This means Spark is both an ingestion engine and a transformation engine — violating single-responsibility and making SQL-level lineage, testing, and documentation harder. The Gold layer also doubles as the Feast offline store and BI query surface, but Delta on MinIO is not ideal for ad-hoc BI access.

---

## 2. Proposed Architecture

```
PostgreSQL OLTP → Debezium → Kafka
                                  ↓
                     Spark Structured Streaming
                         [INGESTION ONLY]
                                  ↓
              Bronze: MinIO / Delta Lake (Hive Metastore)
                                  ↓
                     Trino (query + compute engine)
                       dbt-trino adapter (one profile)
                        ↙                    ↘
       models/staging/                  models/intermediate/
    MinIO / Delta Lake                       + models/marts/
   (Trino Delta catalog)                    ClickHouse Gold
   Partition overwrite                    (Trino ClickHouse
     strategy (daily)                         connector)
                                          ↙      ↓       ↘
                                    Feast    Grafana   Airflow
                                   offline    BI dash  → Redis
                                    store              (online)
```

### Layer Mapping

| Layer | dbt folder | Storage | Trino catalog | Incremental strategy |
|---|---|---|---|---|
| Staging (= Silver) | `models/staging/` | MinIO / Delta Lake | `lakehouse.staging.*` | `delete+insert` (safe) |
| Intermediate | `models/intermediate/` | ClickHouse | `clickhouse.intermediate.*` | `merge` |
| Marts (= Gold) | `models/marts/` | ClickHouse | `clickhouse.marts.*` | `merge` |

---

## 3. Key Design Decisions

### 3.1 Spark Scope Reduction

Spark responsibility is limited to **Bronze ingestion only**:
- ✅ Keep: `src/batch_processing/` Bronze Structured Streaming jobs
- 🗑 Delete: `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py`
- 🗑 Delete: `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py`
- 🗑 Delete: `src/batch_processing/gold/silver_transactions_ml_features_gold.py`
- 🗑 Delete: `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py`
- 🗑 Delete: `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py`

### 3.2 dbt Project Structure

One dbt project at `src/dbt/`, one dbt-trino profile, two Trino catalogs as write targets:

```
src/dbt/
├── dbt_project.yml
├── profiles.yml              # Trino connection; OVERWRITE session property for MinIO
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml       # Bronze Delta tables as dbt sources
│   │   ├── staging.yml       # model configs + tests
│   │   ├── stg_transactions.sql
│   │   └── stg_fraud_cases.sql
│   ├── intermediate/
│   │   ├── intermediate.yml
│   │   ├── int_customer_window_features.sql
│   │   └── int_terminal_window_features.sql
│   └── marts/
│       ├── marts.yml
│       └── mart_fraud_ml_features.sql
└── tests/
    └── generic/              # custom singular tests if needed
```

### 3.3 Incremental Strategy Per Layer

**Staging → MinIO/Delta Lake (delete+insert strategy):**

The existing Trino catalog (`lakehouse.properties`) uses `connector.name=delta_lake`, **not** `connector.name=hive`. The Hive `insert_existing_partitions_behavior=OVERWRITE` session property does not apply here.

Safe incremental strategy for the Delta Lake connector: `delete+insert` (dbt deletes matching rows by `unique_key`, then inserts new batch). This is equivalent to Spark's MERGE-on-primary-key pattern.

```sql
-- stg_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    database='lakehouse',
    schema='staging'
) }}
SELECT
    transaction_id,
    event_timestamp,
    CAST(event_timestamp AS DATE)  AS event_date,
    customer_id,
    terminal_id,
    amount,
    _cdc_op,
    _bronze_ingested_at,
    current_timestamp               AS _staging_updated_at
FROM {{ source('bronze', 'transactions') }}
{% if is_incremental() %}
  WHERE _bronze_ingested_at > (SELECT MAX(_staging_updated_at) FROM {{ this }})
{% endif %}
```

⚠️ **Note**: If Trino 480 Delta Lake connector supports `MERGE`, switch to `incremental_strategy='merge'` for better atomicity. Verify during implementation.  
→ Airflow passes `--vars '{"target_date": "2026-05-11"}'` to dbt for backfill support.

**Intermediate + Marts → ClickHouse (merge strategy):**

```sql
-- int_customer_window_features.sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    database='clickhouse',    schema='intermediate'
) }}
SELECT
    customer_id,
    feature_date,
    COUNT(*) FILTER (WHERE event_date = feature_date)            AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
    ...
FROM {{ ref('stg_transactions') }}
{% if is_incremental() %}
  WHERE feature_date >= CURRENT_DATE - INTERVAL '1' DAY
{% endif %}
```

### 3.4 ClickHouse Service

Added to `src/lakehouse/docker-compose.lakehouse.yml`:

```yaml
clickhouse:
  image: clickhouse/clickhouse-server:head-distroless
  container_name: clickhouse
  hostname: clickhouse
  ports:
    - "8123:8123"    # HTTP interface (Trino JDBC)
    - "19000:9000"   # Native TCP (remapped; MinIO occupies host 9000)
  volumes:
    - clickhouse_data:/var/lib/clickhouse
  healthcheck:
    test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8123/ping || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 5
```

Port 9000 inside container is ClickHouse native TCP; mapped to host 19000 to avoid conflict with MinIO's host 9000.

### 3.5 Trino ClickHouse Catalog

New file `src/lakehouse/trino/catalog/clickhouse.properties`:

```properties
connector.name=clickhouse
connection-url=jdbc:clickhouse://clickhouse:8123/
connection-user=default
connection-password=
```

This allows dbt-trino to write marts models to ClickHouse via `CREATE TABLE AS SELECT` and `MERGE` through Trino.

### 3.6 Feast Offline Store → ClickHouse

The current Feast offline store uses `S3FileSource` (reads Gold Delta Parquet from MinIO). With Gold moving to ClickHouse, the offline store changes.

**Implementation**: Custom `ClickHouseOfflineStore` using `clickhouse-connect` Python SDK.

```yaml
# feature_store.yaml
offline_store:
  type: src.feature_store.offline_store.clickhouse.ClickHouseOfflineStore
  host: clickhouse
  port: 8123
  database: marts
```

The custom store implements:
- `get_historical_features(entity_df, feature_refs)` — SQL ASOF JOIN or window-based point-in-time join on ClickHouse
- `pull_latest_from_table_or_query()` — for materialization to Redis

⚠️ **Risk**: No official Feast ClickHouse plugin. Custom implementation is required. This is the highest-complexity item in this design and will be treated as a separate implementation sub-task.

**Fallback**: If custom offline store proves too complex, Feast reads from staging Delta (MinIO) via `SparkFileSource`, while ClickHouse serves only BI and Airflow-triggered Redis materialization (two separate Gold consumers).

### 3.7 Airflow DAG Changes

Remove Spark Silver/Gold operators. Add DbtTaskGroup (astronomer-cosmos):

```
bronze_spark_trigger
    → dbt_staging_group (cosmos DbtTaskGroup, select=staging)
    → dbt_intermediate_group (cosmos DbtTaskGroup, select=intermediate)
    → dbt_marts_group (cosmos DbtTaskGroup, select=marts)
    → materialize_to_redis (reads ClickHouse marts → Redis)
```

---

## 4. Error Handling

| Failure point | Behavior |
|---|---|
| Spark Bronze write failure | Kafka offset not committed → auto-retry on restart |
| dbt staging model failure | Partition overwrite is atomic; previous day partition intact |
| dbt marts model failure | ClickHouse MERGE is transactional; previous state preserved |
| Trino ClickHouse connector down | dbt fails, Airflow marks task failed → does not cascade to Redis materialization |
| Redis materialization failure | Online serving uses stale features (acceptable for fraud detection latency SLO) |

---

## 5. Testing Strategy

**dbt tests (defined in `.yml` files):**
- `staging.yml`: `not_null(transaction_id)`, `unique(transaction_id)`, `accepted_values(_cdc_op: [r, c, u, d])`
- `intermediate.yml`: `not_null` on all window feature columns, `relationships` to staging
- `marts.yml`: `not_null` on all 15 feature columns matching `TransactionRequest`, `dbt_utils.accepted_range(TX_AMOUNT, min_value=0)`

**dbt `dbt test --select staging` runs in CI** after schema changes to staging models.

---

## 6. Files Changed

| Action | Path |
|---|---|
| CREATE | `src/dbt/` (full dbt project) |
| CREATE | `src/lakehouse/trino/catalog/clickhouse.properties` |
| MODIFY | `src/lakehouse/docker-compose.lakehouse.yml` (add ClickHouse service) |
| MODIFY | `src/feature_store/feature_store.yaml` (offline store → ClickHouse) |
| MODIFY | `src/feature_store/` feature views (update source paths) |
| CREATE | `src/feature_store/offline_store/clickhouse.py` (custom offline store) |
| MODIFY | `src/orchestration/` Airflow DAGs (add cosmos DbtTaskGroup, remove Silver/Gold Spark) |
| DELETE | `src/batch_processing/silver/` (2 files) |
| DELETE | `src/batch_processing/gold/` (3 files) |
| MODIFY | `docs/architecture.md` |

---

## 7. Open Questions / Out of Scope

- **ClickHouse table engine selection** (ReplacingMergeTree vs AggregatingMergeTree for intermediate tables) — resolved during implementation.
- **dbt-trino Delta connector MERGE support**: If Trino 480 Delta connector does not support MERGE for staging, fall back to `delete+insert`.
- **Feast custom ClickHouse offline store**: Complex item, implemented as a separate sub-task after the dbt+ClickHouse pipeline is validated.
- **Backfill**: dbt supports `--vars '{"target_date": "..."}'` for partition-based backfill; Airflow catchup handles historical range.
