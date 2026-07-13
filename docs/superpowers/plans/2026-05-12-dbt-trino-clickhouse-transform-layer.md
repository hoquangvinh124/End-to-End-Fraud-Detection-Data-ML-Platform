# dbt + Trino + ClickHouse Transform Layer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Spark Silver/Gold batch jobs with dbt+Trino (staging on Delta, marts on ClickHouse), add ClickHouse as the Gold/serving layer, and update Airflow + Feast accordingly.

**Architecture:** One dbt project (`src/dbt/`) using `dbt-trino==1.10.1` targets two Trino catalogs: `lakehouse` (Delta Lake connector → staging models on MinIO) and `clickhouse` (ClickHouse connector → intermediate + marts). Staging uses `merge` incremental strategy; intermediate/marts use `materialized='table'` + MergeTree engine properties since the Trino ClickHouse connector only supports INSERT + TRUNCATE (no MERGE/DELETE). Airflow uses `astronomer-cosmos==1.14.1` DbtTaskGroups to replace the 5 Spark Silver/Gold tasks.

**Tech Stack:**
- `dbt-trino==1.10.1`, `dbt-core~=1.10.0`
- `clickhouse/clickhouse-server:head-distroless` (ClickHouse Gold layer)
- `trinodb/trino:481` (upgraded from 480)
- `clickhouse-connect==0.15.1` (for Feast materialization)
- `astronomer-cosmos==1.14.1` (Airflow DbtTaskGroup)

**Critical connector finding:** Trino ClickHouse connector supports only INSERT + TRUNCATE (no DELETE, UPDATE, MERGE). Intermediate + marts models therefore use `materialized='table'` (DROP + CTAS) with MergeTree engine properties via Trino `WITH (engine='MergeTree', order_by=ARRAY[...])`.

---

## File Map

| Action | Path |
|--------|------|
| MODIFY | `src/lakehouse/docker-compose.lakehouse.yml` — add ClickHouse service, upgrade Trino to 481 |
| CREATE | `src/lakehouse/trino/catalog/clickhouse.properties` — ClickHouse Trino catalog |
| CREATE | `src/lakehouse/scripts/register_bronze_tables.sql` — one-time HMS table registration |
| MODIFY | `pyproject.toml` — add `dbt` dependency group + `clickhouse-connect` |
| CREATE | `src/dbt/dbt_project.yml` |
| CREATE | `src/dbt/profiles.yml` |
| CREATE | `src/dbt/packages.yml` |
| CREATE | `src/dbt/models/staging/sources.yml` |
| CREATE | `src/dbt/models/staging/staging.yml` |
| CREATE | `src/dbt/models/staging/stg_transactions.sql` |
| CREATE | `src/dbt/models/staging/stg_fraud_cases.sql` |
| CREATE | `src/dbt/models/intermediate/intermediate.yml` |
| CREATE | `src/dbt/models/intermediate/int_customer_window_features.sql` |
| CREATE | `src/dbt/models/intermediate/int_terminal_window_features.sql` |
| CREATE | `src/dbt/models/marts/marts.yml` |
| CREATE | `src/dbt/models/marts/mart_fraud_ml_features.sql` |
| MODIFY | `src/orchestration/dags/feature_pipeline_daily.py` — replace silver/dq/gold with cosmos DbtTaskGroups |
| MODIFY | `src/orchestration/docker-compose.airflow.yml` — add cosmos + dbt deps, mount dbt volume |
| MODIFY | `src/feature_store/materialize_to_redis.py` — read from ClickHouse, use write_to_online_store |
| MODIFY | `src/feature_store/feature_store.yaml` — add clickhouse-connect dep note |
| DELETE | `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` |
| DELETE | `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py` |
| DELETE | `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py` |
| DELETE | `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py` |
| DELETE | `src/batch_processing/gold/silver_transactions_ml_features_gold.py` |
| MODIFY | `src/batch_processing/spark-defaults.conf` — remove silver/gold config entries |
| MODIFY | `docs/architecture.md` — update data flow diagram |

---

## Task 1: ClickHouse + Trino Infrastructure

**Files:**
- Modify: `src/lakehouse/docker-compose.lakehouse.yml`
- Create: `src/lakehouse/trino/catalog/clickhouse.properties`
- Create: `src/lakehouse/scripts/register_bronze_tables.sql`

- [ ] **Step 1.1: Add ClickHouse service and upgrade Trino in docker-compose**

Replace `src/lakehouse/docker-compose.lakehouse.yml` with this content (add ClickHouse service + `clickhouse_data` volume + upgrade Trino to 481):

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2025-09-07T16-13-09Z
    container_name: lakehouse-minio
    hostname: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minio}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minio12345}
    command: server --console-address ":9001" /data
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD-SHELL", "curl -I http://127.0.0.1:9000/minio/health/live || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10

  minio-init:
    image: minio/mc:RELEASE.2025-08-13T08-35-41Z
    container_name: minio-client
    entrypoint: ["/bin/sh", "/init/create-buckets.sh"]
    volumes:
      - ./init:/init:ro
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:-minio}
      MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:-minio12345}
    depends_on:
      minio:
        condition: service_healthy
    restart: "no"

  metastore-db:
    image: postgres:16
    container_name: metastore-db
    hostname: metastore-db
    ports:
      - "5434:5432"
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore
    volumes:
      - metastore_db_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hive -d metastore"]
      interval: 10s
      timeout: 5s
      retries: 5

  hive-metastore:
    image: starburstdata/hive:3.1.2-e.18
    container_name: hive-metastore
    hostname: hive-metastore
    ports:
      - "9083:9083"
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://metastore-db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      S3_ENDPOINT: http://minio:9000
      S3_ACCESS_KEY: ${MINIO_ROOT_USER:-minio}
      S3_SECRET_KEY: ${MINIO_ROOT_PASSWORD:-minio12345}
      S3_PATH_STYLE_ACCESS: "true"
    depends_on:
      metastore-db:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully

  trino:
    image: trinodb/trino:481
    container_name: trino
    hostname: trino
    ports:
      - "8090:8080"
    volumes:
      - ./trino/etc:/usr/lib/trino/etc:ro
      - ./trino/catalog:/etc/trino/catalog:ro
    depends_on:
      hive-metastore:
        condition: service_started

  clickhouse:
    image: clickhouse/clickhouse-server:head-distroless
    container_name: clickhouse
    hostname: clickhouse
    ports:
      - "8123:8123"
      - "19000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8123/ping || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  minio_data:
  metastore_db_data:
  clickhouse_data:
```

> Port mapping rationale: MinIO occupies host port 9000, so ClickHouse native TCP is remapped to 19000. HTTP port 8123 is free (Trino JDBC uses this port to connect).

- [ ] **Step 1.2: Create Trino ClickHouse catalog**

Create `src/lakehouse/trino/catalog/clickhouse.properties`:

```properties
connector.name=clickhouse
connection-url=jdbc:clickhouse://clickhouse:8123/
connection-user=default
connection-password=
clickhouse.map-string-as-varchar=true
```

> `map-string-as-varchar=true` ensures ClickHouse `String` type maps to Trino `VARCHAR` (not `VARBINARY`), which is needed for schema/table names and any string feature columns.

- [ ] **Step 1.3: Create Bronze table registration script**

Create `src/lakehouse/scripts/register_bronze_tables.sql`:

```sql
-- Run once after Spark Bronze ingestion has written at least one batch.
-- Registers the existing Bronze Delta tables into the Hive Metastore so
-- Trino's lakehouse catalog can query them as dbt sources.

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
```

> Run this via Trino CLI: `docker exec -i trino trino --execute "$(cat register_bronze_tables.sql)"` OR paste into the Trino web UI at http://localhost:8090.
> Only needed once per environment. Subsequent dbt runs do not need this.

- [ ] **Step 1.4: Start the lakehouse stack and verify ClickHouse responds**

```bash
cd src/lakehouse
docker compose -f docker-compose.lakehouse.yml up -d

# Wait ~30s then check ClickHouse HTTP ping
curl -s http://localhost:8123/ping
# Expected: Ok.

# Check Trino can connect to ClickHouse
curl -s http://localhost:8090/v1/info
# Expected: JSON with serverVersion: "481"
```

- [ ] **Step 1.5: Commit**

```bash
git add src/lakehouse/docker-compose.lakehouse.yml \
        src/lakehouse/trino/catalog/clickhouse.properties \
        src/lakehouse/scripts/register_bronze_tables.sql
git commit -m "infra: add ClickHouse service, upgrade Trino to 481, add ClickHouse catalog

- clickhouse/clickhouse-server:head-distroless on ports 8123 (HTTP) and 19000 (native TCP)
- Trino upgraded from 480 → 481
- New Trino catalog: clickhouse.properties (connector.name=clickhouse, port 8123)
- Bronze table registration script for HMS one-time setup

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 2: Add Python Dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 2.1: Add `dbt` dependency group and `clickhouse-connect` to pyproject.toml**

Add after the existing `[dependency-groups] dev = [...]` block:

```toml
[dependency-groups]
dbt = [
    "dbt-core~=1.10.0",
    "dbt-trino==1.10.1",
]
```

Also add `clickhouse-connect==0.15.1` to the main `[project] dependencies` list (needed at runtime by `materialize_to_redis.py`):

```toml
dependencies = [
    ...existing entries...
    "clickhouse-connect==0.15.1",
]
```

- [ ] **Step 2.2: Install and verify**

```bash
uv sync --frozen --group dev --group dbt
uv run --group dbt dbt --version
# Expected: Core: 1.10.x  Plugins: trino: 1.10.1
```

- [ ] **Step 2.3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "deps: add dbt-trino 1.10.1, dbt-core 1.10.x, clickhouse-connect 0.15.1

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 3: dbt Project Scaffold

**Files:**
- Create: `src/dbt/dbt_project.yml`
- Create: `src/dbt/profiles.yml`
- Create: `src/dbt/packages.yml`

- [ ] **Step 3.1: Create `src/dbt/dbt_project.yml`**

```yaml
name: fraud_detection
version: '1.0.0'
config-version: 2

profile: fraud_detection

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  fraud_detection:
    staging:
      +database: lakehouse
      +schema: staging
      +materialized: incremental
    intermediate:
      +database: clickhouse
      +schema: intermediate
      +materialized: table
    marts:
      +database: clickhouse
      +schema: marts
      +materialized: table
```

- [ ] **Step 3.2: Create `src/dbt/profiles.yml`**

```yaml
fraud_detection:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      host: "{{ env_var('TRINO_HOST', 'localhost') }}"
      port: "{{ env_var('TRINO_PORT', '8090') | int }}"
      database: lakehouse
      schema: staging
      http_scheme: http
      threads: 4
      session_properties:
        query_max_execution_time: 30m
```

> Inside Airflow/Docker containers, set `TRINO_HOST=trino` and `TRINO_PORT=8080` to reach the Trino container via Docker internal network.

- [ ] **Step 3.3: Create `src/dbt/packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

- [ ] **Step 3.4: Install dbt packages and verify project parses**

```bash
cd src/dbt
uv run --group dbt dbt deps --project-dir . --profiles-dir .
uv run --group dbt dbt parse --project-dir . --profiles-dir .
# Expected: "Done." with no errors
```

- [ ] **Step 3.5: Commit**

```bash
git add src/dbt/
git commit -m "feat(dbt): scaffold fraud_detection dbt project with trino + clickhouse catalogs

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 4: Staging Models

**Context:** The Bronze Delta tables (`s3://bronze/cdc/transactions` and `s3://bronze/cdc/fraud_cases`) are registered in HMS via the script from Task 1. The staging models write to `lakehouse.staging.*` using Delta Lake via the Trino `lakehouse` catalog with `merge` incremental strategy.

**Bronze transactions schema** (from OLTP `banking.transactions` via Debezium CDC + metadata fields):
- `transaction_id` BIGINT, `event_timestamp` TIMESTAMP, `customer_id` TEXT (numeric), `terminal_id` TEXT (numeric), `amount` NUMERIC(12,2), `is_weekend` BOOLEAN, `is_night` BOOLEAN, `_op` TEXT, `_ingested_at` TIMESTAMP

**Bronze fraud_cases schema** (from OLTP `banking.fraud_cases`):
- `case_id` TEXT, `transaction_id` BIGINT, `case_status` TEXT, `resolved_at` TIMESTAMP, `_op` TEXT, `_ingested_at` TIMESTAMP

**Files:**
- Create: `src/dbt/models/staging/sources.yml`
- Create: `src/dbt/models/staging/staging.yml`
- Create: `src/dbt/models/staging/stg_transactions.sql`
- Create: `src/dbt/models/staging/stg_fraud_cases.sql`

- [ ] **Step 4.1: Write the schema tests first (staging.yml + sources.yml)**

Create `src/dbt/models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    database: lakehouse
    schema: bronze
    tables:
      - name: transactions
        description: "CDC Bronze table: banking.transactions via Debezium"
      - name: fraud_cases
        description: "CDC Bronze table: banking.fraud_cases via Debezium"
```

Create `src/dbt/models/staging/staging.yml`:

```yaml
version: 2

models:
  - name: stg_transactions
    description: "Normalized transactions: Bronze CDC → Silver-equivalent staging"
    columns:
      - name: transaction_id
        tests:
          - not_null
          - unique
      - name: event_timestamp
        tests:
          - not_null
      - name: customer_id
        tests:
          - not_null
      - name: terminal_id
        tests:
          - not_null
      - name: amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: false
      - name: _cdc_op
        tests:
          - accepted_values:
              values: ['r', 'c', 'u', 'd']

  - name: stg_fraud_cases
    description: "Normalized fraud cases: Bronze CDC → staging with is_fraud derived"
    columns:
      - name: transaction_id
        tests:
          - not_null
          - unique
      - name: is_fraud
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

- [ ] **Step 4.2: Create `stg_transactions.sql`**

Create `src/dbt/models/staging/stg_transactions.sql`:

```sql
{{ config(
    materialized      = 'incremental',
    unique_key        = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change  = 'fail'
) }}

SELECT
    transaction_id                                AS transaction_id,
    CAST(event_timestamp AS TIMESTAMP)            AS event_timestamp,
    CAST(DATE(event_timestamp) AS DATE)           AS event_date,
    TRY_CAST(customer_id AS BIGINT)               AS customer_id,
    TRY_CAST(terminal_id AS BIGINT)               AS terminal_id,
    CAST(amount AS DECIMAL(12, 2))                AS amount,
    CAST(is_weekend AS BOOLEAN)                   AS is_weekend,
    CAST(is_night AS BOOLEAN)                     AS is_night,
    _op                                           AS _cdc_op,
    _ingested_at                                  AS _bronze_ingested_at,
    CURRENT_TIMESTAMP                             AS _staging_updated_at
FROM {{ source('bronze', 'transactions') }}
{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_staging_updated_at) FROM {{ this }})
{% endif %}
```

- [ ] **Step 4.3: Create `stg_fraud_cases.sql`**

Create `src/dbt/models/staging/stg_fraud_cases.sql`:

```sql
{{ config(
    materialized      = 'incremental',
    unique_key        = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change  = 'fail'
) }}

SELECT
    transaction_id,
    CAST(
        (case_status = 'confirmed' AND resolved_at IS NOT NULL)
        AS BOOLEAN
    )                                             AS is_fraud,
    _op                                           AS _cdc_op,
    _ingested_at                                  AS _bronze_ingested_at,
    CURRENT_TIMESTAMP                             AS _staging_updated_at
FROM {{ source('bronze', 'fraud_cases') }}
{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_staging_updated_at) FROM {{ this }})
{% endif %}
```

- [ ] **Step 4.4: Run staging models (requires Trino + Bronze tables available)**

```bash
cd src/dbt
uv run --group dbt dbt run --select staging --project-dir . --profiles-dir .
# Expected:
# 2 of 2 START incremental model lakehouse.staging.stg_transactions
# 2 of 2 START incremental model lakehouse.staging.stg_fraud_cases
# Finished running 2 incremental models ... OK
```

- [ ] **Step 4.5: Run staging tests**

```bash
uv run --group dbt dbt test --select staging --project-dir . --profiles-dir .
# Expected: 0 failures
# (unique, not_null, accepted_values, accepted_range checks all pass)
```

- [ ] **Step 4.6: Commit**

```bash
git add src/dbt/models/staging/
git commit -m "feat(dbt): add staging models stg_transactions + stg_fraud_cases

- Incremental merge on transaction_id via Delta Lake connector (MERGE supported)
- stg_transactions: type casts + is_weekend/is_night from Bronze OLTP columns
- stg_fraud_cases: derives is_fraud = (case_status='confirmed' AND resolved_at IS NOT NULL)
- Schema tests: not_null, unique, accepted_values, dbt_utils.accepted_range

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 5: Intermediate Models

**Context:** Intermediate models write to ClickHouse via Trino's `clickhouse` catalog. They rebuild daily (full table refresh). The `properties` config block sets the ClickHouse table engine to `MergeTree` via Trino's `WITH (...)` DDL clause.

**Customer window features** — matches existing `gold/customer_features` output:
- `customer_id`, `feature_date`, `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D/7D/30D`, `CUSTOMER_AVG_AMOUNT_WINDOW_1D/7D/30D`

**Terminal window features** — matches existing `gold/terminal_features` output:
- `terminal_id`, `feature_date`, `TERMINAL_NB_TX_1DAY/7DAY/30DAY_WINDOW`, `TERMINAL_RISK_1DAY/7DAY/30DAY_WINDOW`

The terminal features use a 7-day **delay offset** (fraud labels may not be confirmed within 7 days): windows are offset by 7 days from `feature_date`.

**Files:**
- Create: `src/dbt/models/intermediate/intermediate.yml`
- Create: `src/dbt/models/intermediate/int_customer_window_features.sql`
- Create: `src/dbt/models/intermediate/int_terminal_window_features.sql`

- [ ] **Step 5.1: Write intermediate schema tests first**

Create `src/dbt/models/intermediate/intermediate.yml`:

```yaml
version: 2

models:
  - name: int_customer_window_features
    description: "1d/7d/30d rolling aggregates per customer_id as of feature_date"
    columns:
      - name: customer_id
        tests: [not_null, unique]
      - name: feature_date
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_1D
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_7D
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_30D
        tests: [not_null]

  - name: int_terminal_window_features
    description: "1d/7d/30d risk + tx-count aggregates per terminal_id (7-day delay offset)"
    columns:
      - name: terminal_id
        tests: [not_null, unique]
      - name: feature_date
        tests: [not_null]
      - name: TERMINAL_NB_TX_1DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_NB_TX_7DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_NB_TX_30DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_RISK_1DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_RISK_7DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_RISK_30DAY_WINDOW
        tests: [not_null]
```

- [ ] **Step 5.2: Create `int_customer_window_features.sql`**

Create `src/dbt/models/intermediate/int_customer_window_features.sql`:

```sql
{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'customer_id']"
    }
) }}

-- feature_date is injected via Airflow --vars; defaults to yesterday for local runs
{% set feature_date = var('feature_date', 'current_date - interval \'1\' day') %}

SELECT
    customer_id,
    CAST({{ feature_date }} AS DATE)                                                AS feature_date,

    COUNT(CASE WHEN event_date = CAST({{ feature_date }} AS DATE) THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,

    COUNT(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '6' DAY THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,

    COUNT(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    COALESCE(
        AVG(CASE WHEN event_date = CAST({{ feature_date }} AS DATE) THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,

    COALESCE(
        AVG(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '6' DAY THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,

    COALESCE(
        AVG(CASE WHEN event_date >= CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY THEN amount END),
        0.0
    )   AS CUSTOMER_AVG_AMOUNT_WINDOW_30D

FROM {{ ref('stg_transactions') }}
WHERE event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '29' DAY
                     AND CAST({{ feature_date }} AS DATE)
GROUP BY customer_id
```

- [ ] **Step 5.3: Create `int_terminal_window_features.sql`**

The terminal features use a 7-day label-delay offset. Window bounds:
- 1D: `[feature_date - 8, feature_date - 7]`
- 7D: `[feature_date - 14, feature_date - 7]`
- 30D: `[feature_date - 37, feature_date - 7]`

Create `src/dbt/models/intermediate/int_terminal_window_features.sql`:

```sql
{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'terminal_id']"
    }
) }}

{% set feature_date = var('feature_date', 'current_date - interval \'1\' day') %}

-- Attach fraud label; missing fraud_case rows → is_fraud = false (legitimate)
WITH labeled AS (
    SELECT
        t.terminal_id,
        t.event_date,
        COALESCE(f.is_fraud, false) AS is_fraud
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_fraud_cases') }} f
        ON t.transaction_id = f.transaction_id
    WHERE t.event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                           AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
)

SELECT
    terminal_id,
    CAST({{ feature_date }} AS DATE)                                                AS feature_date,

    -- 1-day window (delay-offset: [fd-8, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_1DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '8' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_1DAY_WINDOW,

    -- 7-day window (delay-offset: [fd-14, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_7DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '14' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_7DAY_WINDOW,

    -- 30-day window (delay-offset: [fd-37, fd-7])
    COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                   AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
               THEN 1 END)
        AS TERMINAL_NB_TX_30DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                         AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                     AND is_fraud THEN 1.0 END)
            /
            NULLIF(COUNT(CASE WHEN event_date BETWEEN CAST({{ feature_date }} AS DATE) - INTERVAL '37' DAY
                                              AND CAST({{ feature_date }} AS DATE) - INTERVAL '7' DAY
                              THEN 1 END), 0)
        AS DOUBLE),
        0.0
    )   AS TERMINAL_RISK_30DAY_WINDOW

FROM labeled
GROUP BY terminal_id
```

- [ ] **Step 5.4: Run intermediate models**

```bash
cd src/dbt
uv run --group dbt dbt run --select intermediate --project-dir . --profiles-dir .
# Expected: 2 of 2 models run OK in clickhouse.intermediate
```

- [ ] **Step 5.5: Run intermediate tests**

```bash
uv run --group dbt dbt test --select intermediate --project-dir . --profiles-dir .
# Expected: 0 failures
```

- [ ] **Step 5.6: Commit**

```bash
git add src/dbt/models/intermediate/
git commit -m "feat(dbt): add intermediate models for customer + terminal window features

- int_customer_window_features: 1d/7d/30d aggregates, MergeTree engine, ClickHouse
- int_terminal_window_features: fraud-delay-offset windows, LEFT JOIN stg_fraud_cases
- Both use materialized='table' (DROP+CTAS daily) — Trino ClickHouse connector limitation
- feature_date injected via --vars from Airflow; defaults to yesterday for local runs

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 6: Marts Model

**Context:** `mart_fraud_ml_features` is the flat ML feature table joining staging transactions with intermediate customer + terminal features. Output matches `api/models.py` `TransactionRequest` FEATURE_COLUMNS exactly. Written to `clickhouse.marts.*`.

**Files:**
- Create: `src/dbt/models/marts/marts.yml`
- Create: `src/dbt/models/marts/mart_fraud_ml_features.sql`

- [ ] **Step 6.1: Write marts schema tests**

Create `src/dbt/models/marts/marts.yml`:

```yaml
version: 2

models:
  - name: mart_fraud_ml_features
    description: "Flat ML feature table: one row per transaction with all 15 fraud model features + TX_FRAUD label"
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: event_timestamp
        tests: [not_null]
      - name: TX_AMOUNT
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: false
      - name: IS_WEEKEND
        tests: [not_null]
      - name: IS_NIGHT
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_1D
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_7D
        tests: [not_null]
      - name: CUSTOMER_AVG_AMOUNT_WINDOW_30D
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D
        tests: [not_null]
      - name: CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D
        tests: [not_null]
      - name: TERMINAL_RISK_1DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_RISK_7DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_RISK_30DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_NB_TX_1DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_NB_TX_7DAY_WINDOW
        tests: [not_null]
      - name: TERMINAL_NB_TX_30DAY_WINDOW
        tests: [not_null]
      - name: TX_FRAUD
        tests:
          - not_null
          - accepted_values:
              values: [0, 1]
      - name: feature_date
        tests: [not_null]
```

- [ ] **Step 6.2: Create `mart_fraud_ml_features.sql`**

Create `src/dbt/models/marts/mart_fraud_ml_features.sql`:

```sql
{{ config(
    materialized = 'table',
    properties   = {
        "engine"   : "'MergeTree'",
        "order_by" : "ARRAY['feature_date', 'transaction_id']"
    }
) }}

{% set feature_date = var('feature_date', 'current_date - interval \'1\' day') %}

SELECT
    t.transaction_id,
    t.event_timestamp,

    -- Transaction features (direct from staging)
    CAST(t.amount AS DOUBLE)                                          AS TX_AMOUNT,
    t.is_weekend                                                      AS IS_WEEKEND,
    t.is_night                                                        AS IS_NIGHT,

    -- Customer window features (LEFT JOIN: new customers default to 0)
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_1D  AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_7D  AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
    COALESCE(CAST(c.CUSTOMER_AVG_AMOUNT_WINDOW_30D AS DOUBLE), 0.0)  AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,  0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,  0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
    COALESCE(c.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D, 0)        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    -- Terminal window features (LEFT JOIN: new terminals default to 0)
    COALESCE(CAST(tm.TERMINAL_RISK_1DAY_WINDOW  AS DOUBLE), 0.0)     AS TERMINAL_RISK_1DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_7DAY_WINDOW  AS DOUBLE), 0.0)     AS TERMINAL_RISK_7DAY_WINDOW,
    COALESCE(CAST(tm.TERMINAL_RISK_30DAY_WINDOW AS DOUBLE), 0.0)     AS TERMINAL_RISK_30DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_1DAY_WINDOW,  0)                      AS TERMINAL_NB_TX_1DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_7DAY_WINDOW,  0)                      AS TERMINAL_NB_TX_7DAY_WINDOW,
    COALESCE(tm.TERMINAL_NB_TX_30DAY_WINDOW, 0)                      AS TERMINAL_NB_TX_30DAY_WINDOW,

    -- Fraud label (LEFT JOIN: missing fraud_case → legitimate = 0)
    COALESCE(CAST(f.is_fraud AS INTEGER), 0)                         AS TX_FRAUD,

    CAST({{ feature_date }} AS DATE)                                 AS feature_date

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('int_customer_window_features') }} c
    ON t.customer_id = c.customer_id
LEFT JOIN {{ ref('int_terminal_window_features') }} tm
    ON t.terminal_id = tm.terminal_id
LEFT JOIN {{ ref('stg_fraud_cases') }} f
    ON t.transaction_id = f.transaction_id
WHERE t.event_date = CAST({{ feature_date }} AS DATE)
```

- [ ] **Step 6.3: Run full dbt pipeline**

```bash
cd src/dbt
uv run --group dbt dbt run --project-dir . --profiles-dir . \
    --vars '{"feature_date": "2026-05-11"}'
# Expected: 5 models run OK (2 staging + 2 intermediate + 1 mart)
```

- [ ] **Step 6.4: Run all dbt tests**

```bash
uv run --group dbt dbt test --project-dir . --profiles-dir . \
    --vars '{"feature_date": "2026-05-11"}'
# Expected: 0 failures across all models
```

- [ ] **Step 6.5: Verify mart row count in ClickHouse**

```bash
curl -s "http://localhost:8123/?query=SELECT+count()+FROM+marts.mart_fraud_ml_features"
# Expected: a non-zero number (equals Bronze transaction count for that feature_date)
```

- [ ] **Step 6.6: Commit**

```bash
git add src/dbt/models/marts/
git commit -m "feat(dbt): add mart_fraud_ml_features — flat ML feature table on ClickHouse

- Joins stg_transactions + int_customer_window_features + int_terminal_window_features + stg_fraud_cases
- 19 columns: transaction_id, event_timestamp, TX_AMOUNT, IS_WEEKEND, IS_NIGHT, 6 customer, 6 terminal, TX_FRAUD, feature_date
- Column names match api/models.py TransactionRequest FEATURE_COLUMNS exactly
- Schema tests: not_null + unique on transaction_id, accepted_values on TX_FRAUD

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 7: Airflow DAG Refactor

**Context:** Replace the `silver` TaskGroup (2 Spark tasks), `dq` TaskGroup (2 gate tasks), and `gold` TaskGroup (3 Spark tasks) with three cosmos `DbtTaskGroup`s. The `bronze` TaskGroup stays unchanged.

New dependency graph:
```
bronze.ingest_transactions ──┐
                              ├──► dbt_staging ──► dbt_intermediate ──► dbt_marts ──► materialize_online_features
bronze.ingest_fraud_cases  ──┘
```

**Files:**
- Modify: `src/orchestration/dags/feature_pipeline_daily.py`
- Modify: `src/orchestration/docker-compose.airflow.yml`

- [ ] **Step 7.1: Update Airflow docker-compose to add cosmos + dbt-trino dependencies and mount dbt volume**

In `src/orchestration/docker-compose.airflow.yml`, update the `_PIP_ADDITIONAL_REQUIREMENTS` env var and add a volume mount for the dbt project:

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:3.0.2
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__API_SERVER__EXPOSE_CONFIG: "true"
    SPARK_BATCH_IMAGE: "mlops-batch:latest"
    DOCKER_NETWORK: "mlops_default"
    MATERIALIZE_SCRIPT_PATH: "/opt/airflow/feature_store/materialize_to_redis.py"
    TRINO_HOST: "trino"
    TRINO_PORT: "8080"
    _PIP_ADDITIONAL_REQUIREMENTS: >-
      apache-airflow-providers-docker>=3.9.0
      apache-airflow-providers-standard>=1.0.0
      astronomer-cosmos==1.14.1
      dbt-core~=1.10.0
      dbt-trino==1.10.1
  volumes:
    - ./dags:/opt/airflow/dags:ro
    - ../feature_store:/opt/airflow/feature_store:ro
    - ../../src/dbt:/opt/airflow/dbt:ro
    - airflow_logs:/opt/airflow/logs
    - /var/run/docker.sock:/var/run/docker.sock
  depends_on:
    airflow-postgres:
      condition: service_healthy
```

> Note: `TRINO_HOST=trino` and `TRINO_PORT=8080` set environment variables that `profiles.yml` picks up via `env_var()` so dbt inside Airflow connects to Trino via Docker internal network (not the external host port 8090).

- [ ] **Step 7.2: Replace `feature_pipeline_daily.py` with the refactored DAG**

Replace the entire file `src/orchestration/dags/feature_pipeline_daily.py`:

```python
"""feature_pipeline_daily — daily batch pipeline: Bronze → dbt (staging/intermediate/marts) → Redis.

Dependency graph:

  bronze.ingest_transactions ──┐
                                ├──► dbt_staging ──► dbt_intermediate ──► dbt_marts ──► materialize_online_features
  bronze.ingest_fraud_cases  ──┘

dbt_staging writes normalized Silver-equivalent tables to MinIO/Delta via Trino.
dbt_intermediate writes customer + terminal window features to ClickHouse.
dbt_marts writes the flat ML feature table to ClickHouse.
materialize_online_features reads ClickHouse intermediate tables → pushes to Redis.

On any task failure a Discord alert is sent to the team webhook.
"""
from __future__ import annotations

import json
import os
import pathlib
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup
from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig
from cosmos.profiles import TrinoProfileMapping

_SPARK_IMAGE = os.environ.get("SPARK_BATCH_IMAGE", "mlops-batch:latest")
_DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "mlops_default")
_DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "",
)
_MATERIALIZE_SCRIPT = os.environ.get(
    "MATERIALIZE_SCRIPT_PATH",
    str(pathlib.Path(__file__).resolve().parents[2] / "feature_store" / "materialize_to_redis.py"),
)

_DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------


def _notify_discord_failure(context: dict[str, Any]) -> None:
    """Post a concise failure alert to Discord when any task fails."""
    ti = context["task_instance"]
    msg = (
        f"🔴 **Airflow task FAILED**\n"
        f"DAG: `{ti.dag_id}`  Task: `{ti.task_id}`\n"
        f"Run: `{ti.run_id}`\n"
        f"Exception: `{context.get('exception', 'n/a')}`\n"
        f"Logs: {ti.log_url}"
    )
    data = json.dumps({"content": msg[:1900]}).encode("utf-8")
    req = urllib.request.Request(
        _DISCORD_WEBHOOK,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spark_task(
    task_id: str,
    script: str,
    doc_md: str = "",
    extra_conf: dict[str, str] | None = None,
) -> DockerOperator:
    """Return a DockerOperator running spark-submit inside the batch image."""
    conf_flags = " ".join(f"--conf {k}={v}" for k, v in (extra_conf or {}).items())
    cmd = f"spark-submit {conf_flags} {script}".strip()

    op = DockerOperator(
        task_id=task_id,
        image=_SPARK_IMAGE,
        command=cmd,
        network_mode=_DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=2),
        on_failure_callback=_notify_discord_failure,
    )
    if doc_md:
        op.doc_md = doc_md
    return op


def _dbt_task_group(
    group_id: str,
    select: str,
    dag_run_conf_key: str = "ds",
) -> DbtTaskGroup:
    """Return a cosmos DbtTaskGroup running dbt models for the given selector.

    feature_date is passed as a dbt var so models filter to the Airflow logical date.
    Trino connection is read from TRINO_HOST/TRINO_PORT env vars (set in docker-compose).
    """
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=_DBT_PROJECT_DIR,
            project_name="fraud_detection",
        ),
        profile_config=ProfileConfig(
            profile_name="fraud_detection",
            target_name="dev",
            profile_mapping=TrinoProfileMapping(
                conn_id="trino_default",
                profile_args={
                    "database": "lakehouse",
                    "schema": "staging",
                    "http_scheme": "http",
                    "threads": 4,
                },
            ),
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
        ),
        operator_args={
            "dbt_args": ["--select", select, "--vars", '{"feature_date": "{{ ds }}"}'],
            "on_failure_callback": _notify_discord_failure,
        },
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="feature_pipeline_daily",
    description="Daily batch pipeline: Bronze (Spark) → dbt staging/intermediate/marts → Redis",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch", "features", "dbt", "clickhouse"],
    doc_md=__doc__,
):
    # ── Bronze CDC ingestion (Spark) ──────────────────────────────────────
    with TaskGroup("bronze"):
        ingest_transactions = _spark_task(
            task_id="ingest_transactions",
            script="/opt/cdc_ingestion/cdc_transactions_to_bronze.py",
            doc_md="Reads CDC rows from `cdc.transactions` Kafka topic → appends to Bronze Delta `s3a://bronze/cdc/transactions`.",
        )

        ingest_fraud_cases = _spark_task(
            task_id="ingest_fraud_cases",
            script="/opt/cdc_ingestion/cdc_fraud_cases_to_bronze.py",
            doc_md="Reads CDC rows from `cdc.fraud_cases` Kafka topic → appends to `s3a://bronze/cdc/fraud_cases`.",
        )

    # ── dbt transform layers ──────────────────────────────────────────────
    dbt_staging = _dbt_task_group(
        group_id="dbt_staging",
        select="staging",
    )

    dbt_intermediate = _dbt_task_group(
        group_id="dbt_intermediate",
        select="intermediate",
    )

    dbt_marts = _dbt_task_group(
        group_id="dbt_marts",
        select="marts",
    )

    # ── Feast → Redis materialization ─────────────────────────────────────
    materialize_online_features = BashOperator(
        task_id="materialize_online_features",
        bash_command=f"uv run python {_MATERIALIZE_SCRIPT} --feature-date {{{{ ds }}}}",
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=_notify_discord_failure,
        doc_md=(
            "Reads customer + terminal features from ClickHouse intermediate tables "
            "for `{{ ds }}` and pushes to Redis via `feast write_to_online_store`."
        ),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    [ingest_transactions, ingest_fraud_cases] >> dbt_staging
    dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_marts
    dbt_marts >> materialize_online_features
```

> **Airflow connection required:** Add a Trino connection named `trino_default` in the Airflow UI (Admin → Connections):
> - Conn Type: HTTP (or Trino if the trino provider is installed)
> - Host: `trino`
> - Port: `8080`
> - Schema: `lakehouse`

- [ ] **Step 7.3: Verify DAG parses without errors**

```bash
# Parse the DAG file (requires Airflow + cosmos installed locally or in container)
python -c "import ast; ast.parse(open('src/orchestration/dags/feature_pipeline_daily.py').read()); print('Syntax OK')"
# Expected: Syntax OK
```

- [ ] **Step 7.4: Commit**

```bash
git add src/orchestration/dags/feature_pipeline_daily.py \
        src/orchestration/docker-compose.airflow.yml
git commit -m "feat(airflow): replace Spark Silver/Gold with cosmos DbtTaskGroups

- Remove silver TaskGroup (2 Spark tasks), dq TaskGroup (2 gate tasks), gold TaskGroup (3 Spark tasks)
- Add dbt_staging, dbt_intermediate, dbt_marts DbtTaskGroups via astronomer-cosmos 1.14.1
- New DAG flow: bronze → dbt_staging → dbt_intermediate → dbt_marts → materialize_online_features
- Add TRINO_HOST/TRINO_PORT env vars to docker-compose for dbt inside Airflow containers
- Mount src/dbt/ as /opt/airflow/dbt volume

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 8: Update Feast Materialization

**Context:** `materialize_to_redis.py` currently calls `store.materialize_incremental()` which reads from the offline store (currently `type: file` pointing to stale Gold S3 paths). Replace this with direct ClickHouse reads via `clickhouse-connect` + `store.write_to_online_store()`. This bypasses the offline store abstraction and is the pragmatic path since there is no official Feast ClickHouse plugin.

Feature views (`customer_features_view`, `terminal_features_view`) keep their existing definitions; the `source` field is unused in this code path.

**Files:**
- Modify: `src/feature_store/materialize_to_redis.py`

- [ ] **Step 8.1: Replace `materialize_to_redis.py`**

```python
"""Materialize online feature views (customer, terminal) to Redis from ClickHouse.

Reads the latest feature_date partition from ClickHouse intermediate tables using
clickhouse-connect and pushes directly to Redis via feast.write_to_online_store().
This bypasses the offline store abstraction (no official Feast ClickHouse plugin).

Usage:
    uv run python src/feature_store/materialize_to_redis.py
    uv run python src/feature_store/materialize_to_redis.py --feature-date 2026-05-11
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone
from pathlib import Path

import clickhouse_connect
import pandas as pd
from feast import FeatureStore

_CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
_CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))


def _get_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(host=_CLICKHOUSE_HOST, port=_CLICKHOUSE_PORT)


def _resolve_feature_date(feature_date_str: str | None) -> str:
    """Return YYYY-MM-DD for the target date; defaults to yesterday."""
    if feature_date_str:
        return feature_date_str
    return (date.today() - __import__("datetime").timedelta(days=1)).isoformat()


def materialize(store: FeatureStore, feature_date: str) -> None:
    client = _get_client()

    # -- Customer features --------------------------------------------------
    customer_df: pd.DataFrame = client.query_df(f"""
        SELECT
            customer_id,
            feature_date                              AS event_timestamp,
            CUSTOMER_AVG_AMOUNT_WINDOW_1D,
            CUSTOMER_AVG_AMOUNT_WINDOW_7D,
            CUSTOMER_AVG_AMOUNT_WINDOW_30D,
            CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
            CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
            CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D
        FROM intermediate.int_customer_window_features
        WHERE feature_date = toDate('{feature_date}')
    """)
    if not customer_df.empty:
        customer_df["event_timestamp"] = pd.to_datetime(
            customer_df["event_timestamp"], utc=True
        )
        store.write_to_online_store(
            feature_view_name="customer_features_view",
            df=customer_df,
        )
        print(f"[materialize] pushed {len(customer_df)} customer rows for {feature_date}")

    # -- Terminal features --------------------------------------------------
    terminal_df: pd.DataFrame = client.query_df(f"""
        SELECT
            terminal_id,
            feature_date                    AS event_timestamp,
            TERMINAL_RISK_1DAY_WINDOW,
            TERMINAL_RISK_7DAY_WINDOW,
            TERMINAL_RISK_30DAY_WINDOW,
            TERMINAL_NB_TX_1DAY_WINDOW,
            TERMINAL_NB_TX_7DAY_WINDOW,
            TERMINAL_NB_TX_30DAY_WINDOW
        FROM intermediate.int_terminal_window_features
        WHERE feature_date = toDate('{feature_date}')
    """)
    if not terminal_df.empty:
        terminal_df["event_timestamp"] = pd.to_datetime(
            terminal_df["event_timestamp"], utc=True
        )
        store.write_to_online_store(
            feature_view_name="terminal_features_view",
            df=terminal_df,
        )
        print(f"[materialize] pushed {len(terminal_df)} terminal rows for {feature_date}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize online features to Redis from ClickHouse")
    parser.add_argument(
        "--feature-date",
        type=str,
        default=None,
        help="ISO date YYYY-MM-DD to materialize; defaults to yesterday",
    )
    args = parser.parse_args()

    feature_date = _resolve_feature_date(args.feature_date)
    repo_path = Path(__file__).parent
    store = FeatureStore(repo_path=str(repo_path))
    materialize(store, feature_date)


if __name__ == "__main__":
    main()
```

- [ ] **Step 8.2: Verify the script imports cleanly**

```bash
uv run python -c "import src.feature_store.materialize_to_redis; print('Import OK')"
# Expected: Import OK
```

- [ ] **Step 8.3: Add `CLICKHOUSE_HOST` + `CLICKHOUSE_PORT` env vars to Airflow docker-compose**

In `src/orchestration/docker-compose.airflow.yml`, add to the `x-airflow-common` environment block:

```yaml
    CLICKHOUSE_HOST: "clickhouse"
    CLICKHOUSE_PORT: "8123"
```

- [ ] **Step 8.4: Commit**

```bash
git add src/feature_store/materialize_to_redis.py \
        src/orchestration/docker-compose.airflow.yml
git commit -m "feat(feast): materialize from ClickHouse intermediate tables via clickhouse-connect

- Replace store.materialize_incremental() (S3/file offline store) with direct ClickHouse reads
- Uses clickhouse-connect 0.15.1 to query intermediate.int_customer/terminal_window_features
- write_to_online_store() pushes directly to Redis (bypasses offline store abstraction)
- Accepts --feature-date YYYY-MM-DD arg; defaults to yesterday
- Add CLICKHOUSE_HOST/PORT env vars to Airflow docker-compose

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 9: Delete Spark Silver/Gold Jobs

**Files:**
- Delete: `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py`
- Delete: `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py`
- Delete: `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py`
- Delete: `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py`
- Delete: `src/batch_processing/gold/silver_transactions_ml_features_gold.py`
- Modify: `src/batch_processing/spark-defaults.conf`

- [ ] **Step 9.1: Delete the 5 Spark Silver/Gold job files**

```bash
git rm src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py
git rm src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py
git rm src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py
git rm src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py
git rm src/batch_processing/gold/silver_transactions_ml_features_gold.py
```

- [ ] **Step 9.2: Remove Silver/Gold config entries from spark-defaults.conf**

Remove these lines from `src/batch_processing/spark-defaults.conf` (keep only Bronze config):

```conf
# Lines to REMOVE:
# spark.silver.bronze.input.path      ...
# spark.silver.output.path            ...
# spark.silver.quarantine.path        ...
# spark.silver.watermark.path         ...
# spark.silver.fraud_cases.*          ...
# spark.gold.*                        ...
```

Final `spark-defaults.conf` content (Bronze config only):

```conf
# Spark configuration for batch processing jobs.
# Spark loads this file automatically from $SPARK_HOME/conf/spark-defaults.conf.

spark.master                                        local[*]

# Timezone — all timestamps interpreted as UTC
spark.sql.session.timeZone                          UTC

# Delta Lake extensions (required for all Delta read/write operations)
spark.sql.extensions                                io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                     org.apache.spark.sql.delta.catalog.DeltaCatalog

# Delta Lake — enable Change Data Feed on all new Delta tables by default
spark.databricks.delta.properties.defaults.enableChangeDataFeed  true

# S3A / MinIO
spark.hadoop.fs.s3a.endpoint                        http://minio:9000
spark.hadoop.fs.s3a.access.key                      minio
spark.hadoop.fs.s3a.secret.key                      minio12345
spark.hadoop.fs.s3a.path.style.access               true
spark.hadoop.fs.s3a.connection.ssl.enabled          false
spark.hadoop.fs.s3a.aws.credentials.provider        org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Bronze streaming job (spark.bronze.*)
spark.bronze.topic                                  cdc.transactions
spark.bronze.bootstrap.servers                      kafka:9092
spark.bronze.output.path                            s3a://bronze/cdc/transactions
spark.bronze.checkpoint.path                        s3a://bronze/_checkpoints/cdc_transactions_bronze
spark.bronze.trigger.interval                       5 minutes
spark.bronze.schema.registry.url                    http://schema-registry:8081

# Bronze fraud_cases streaming job (spark.bronze.fraud_cases.*)
spark.bronze.fraud_cases.topic                      cdc.fraud_cases
spark.bronze.fraud_cases.output.path                s3a://bronze/cdc/fraud_cases
spark.bronze.fraud_cases.checkpoint.path            s3a://bronze/_checkpoints/cdc_fraud_cases_bronze
```

- [ ] **Step 9.3: Verify no remaining references to deleted Silver/Gold scripts**

```bash
grep -r "cdc_transactions_normalize_merge_silver\|cdc_fraud_cases_normalize_merge_silver\|silver_transactions_window_aggregate\|silver_transactions_ml_features" src/ --include="*.py" --include="*.yml"
# Expected: no output (no references remain)
```

- [ ] **Step 9.4: Run existing API tests to ensure nothing broke**

```bash
uv run pytest src/tests/ -q
# Expected: all tests pass (tests cover api/, not batch_processing/)
```

- [ ] **Step 9.5: Commit**

```bash
git add src/batch_processing/spark-defaults.conf
git commit -m "feat: delete Spark Silver/Gold jobs — replaced by dbt+Trino+ClickHouse

- Remove cdc_transactions_normalize_merge_silver.py
- Remove cdc_fraud_cases_normalize_merge_silver.py
- Remove silver_transactions_window_aggregate_customer_gold.py
- Remove silver_transactions_window_aggregate_terminal_gold.py
- Remove silver_transactions_ml_features_gold.py
- Clean spark-defaults.conf: Bronze config only (silver.* and gold.* entries removed)
- Spark now handles Bronze ingestion ONLY

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 10: Update docs/architecture.md

**Files:**
- Modify: `docs/architecture.md`

- [ ] **Step 10.1: Update the architecture doc data flow section**

Open `docs/architecture.md` and update the medallion architecture data flow section to reflect:
- Spark scope: Bronze ingestion only
- dbt+Trino: Bronze → Staging (Delta) → Intermediate + Marts (ClickHouse)
- ClickHouse Gold layer replaces Delta Gold
- Airflow: cosmos DbtTaskGroups replace Spark Silver/Gold DockerOperators

Add or update the architecture diagram and component table to reflect the new tool stack.

- [ ] **Step 10.2: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: update architecture.md for dbt+Trino+ClickHouse transform layer

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Implementation Notes

### Trino ClickHouse Connector Limitation (critical)
The Trino ClickHouse connector supports only INSERT + TRUNCATE — no DELETE, UPDATE, or MERGE. This is why intermediate and marts models use `materialized='table'` (DROP + CTAS daily rebuild) instead of incremental merge. If incremental semantics are needed in future, switch to the native `dbt-clickhouse==1.10.0` adapter for those models.

### Bronze Table Registration (one-time)
`src/lakehouse/scripts/register_bronze_tables.sql` must be run once after Spark has written the first Bronze batch. Subsequent pipeline runs do not need this.

### Airflow Trino Connection
After deploying the updated Airflow compose, create an Airflow connection in the UI:
- **Conn ID**: `trino_default`
- **Conn Type**: HTTP
- **Host**: `trino`
- **Port**: `8080`
- **Schema**: `lakehouse`

### Port Conflict Note
Both `docker-compose.lakehouse.yml` (Trino) and `docker-compose.airflow.yml` (Airflow API server) map host port `8090`. If running both simultaneously, change one of them (e.g., Airflow API server to `8092:8080`).

### Feast Offline Store (Future)
The current implementation bypasses the Feast offline store with direct ClickHouse reads in `materialize_to_redis.py`. A full custom `ClickHouseOfflineStore` implementation (for `get_historical_features` / training data) is a separate future task. `clickhouse-connect==0.15.1` is already installed as a dependency for when that work begins.
