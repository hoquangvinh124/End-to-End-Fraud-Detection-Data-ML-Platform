# Gold per-Entity + Feast ClickHouse Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the flat Gold mart with three per-entity Gold tables in ClickHouse `ml_features`, and rewire Feast onto the contributed ClickHouse offline store so Feast performs point-in-time joins across per-entity FeatureViews instead of consuming a pre-joined flat table.

**Architecture:** dbt materializes three `ml_features.*` marts directly (no ephemeral intermediate layer). Feast `feature_store.yaml` switches offline store from `file` to `feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore`. Three FeatureViews use `ClickhouseSource` pointing at the marts. The broken `materialize_to_redis.py` is deleted; native `store.materialize` handles Redis. Airflow DAG drops `dbt_intermediate` and repoints the materialize task.

**Tech Stack:** dbt-trino 1.10.1, ClickHouse (MergeTree), Feast `feast[redis,clickhouse]>=0.40`, `ClickhouseSource`, Redis, Airflow 3 + astronomer-cosmos.

**Spec:** `docs/specs/2026-06-24-gold-feast-clickhouse-design.md`

---

## File Map

| Path | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | MODIFY | Add `clickhouse` extra to feast dep |
| `src/dbt/dbt_project.yml` | MODIFY | Remove intermediate block; `+schema: gold` → `ml_features` |
| `src/dbt/models/intermediate/` | DELETE | Entire directory — logic folds into marts |
| `src/dbt/models/marts/machine_learning/mart_fraud_ml_features.sql` | DELETE | Flat mart replaced by 3 per-entity marts |
| `src/dbt/models/marts/machine_learning/mart_customer_window_features.sql` | CREATE | Customer window aggregates → `ml_features.customer_window_features` |
| `src/dbt/models/marts/machine_learning/mart_terminal_window_features.sql` | CREATE | Terminal risk/nb_tx (7-day delay) → `ml_features.terminal_window_features` |
| `src/dbt/models/marts/machine_learning/mart_transaction_features.sql` | CREATE | Transaction facts + label → `ml_features.transaction_features` |
| `src/dbt/models/marts/machine_learning/machine_learning.yml` | MODIFY | Tests for 3 new marts replacing flat mart tests |
| `src/feature_store/feature_store.yaml` | MODIFY | Offline store: file → ClickHouse contrib |
| `src/feature_store/feature_views/customer_features.py` | MODIFY | FileSource → ClickhouseSource |
| `src/feature_store/feature_views/terminal_features.py` | MODIFY | FileSource → ClickhouseSource |
| `src/feature_store/feature_views/fraud_ml_features.py` | DELETE → RENAME | Replaced by `transaction_features.py` |
| `src/feature_store/feature_views/transaction_features.py` | CREATE | ClickhouseSource on `ml_features.transaction_features`, offline-only |
| `src/feature_store/feature_views/__init__.py` | MODIFY | Re-export 3 views |
| `src/feature_store/materialize_to_redis.py` | DELETE | Broken; replaced by `store.materialize` |
| `src/orchestration/dags/feature_pipeline_daily.py` | MODIFY | Remove `dbt_intermediate`; repoint materialize task |
| `src/orchestration/docker-compose.airflow.yml` | MODIFY | Remove `MATERIALIZE_SCRIPT_PATH` env |
| `src/tests/feast/test_unit_feast_feature_views.py` | MODIFY | ClickhouseSource assertions, rename view |
| `src/tests/feast/test_unit_feast_materialize.py` | DELETE | Tested a deleted script |
| `src/tests/feast/test_unit_feast_entities.py` | unchanged | No changes needed |
| `docs/architecture.md` | MODIFY | Feature Store section: ClickHouse offline, `ml_features` tables |
| `docs/fraud-data-platform-detailed.md` | MODIFY | §9.4, §12.2: reconcile Gold=ClickHouse reality |

---

## Task 1: Add `clickhouse` Feast extra

**Files:**
- Modify: `pyproject.toml:73`

The contributed ClickHouse offline store installs via the `feast[clickhouse]` extra. The repo currently has `feast[redis]>=0.40`.

- [ ] **Step 1: Edit the feast dependency**

Open `pyproject.toml`. In `[dependency-groups].dev`, replace:
```toml
    "feast[redis]>=0.40",
```
with:
```toml
    "feast[redis,clickhouse]>=0.40",
```

- [ ] **Step 2: Sync and verify import**

Run:
```bash
uv sync
```
Then verify the contrib store imports:
```bash
uv run python -c "from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse import ClickhouseOfflineStore; print('ok')"
```
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(feast): add clickhouse extra for contributed offline store"
```

---

## Task 2: Delete the old intermediate and flat mart models

**Files:**
- Delete: `src/dbt/models/intermediate/` (entire directory)
- Delete: `src/dbt/models/marts/machine_learning/mart_fraud_ml_features.sql`

These are replaced in Tasks 3-5. Deleting first avoids name clashes and makes the structural change explicit.

- [ ] **Step 1: Delete intermediate directory**

Run:
```bash
Remove-Item -Recurse -LiteralPath "src\dbt\models\intermediate"
```

This removes `int_customers_windowed.sql`, `int_terminals_windowed.sql`, `customer.yml`, `terminal.yml`.

- [ ] **Step 2: Delete flat mart**

Run:
```bash
Remove-Item -LiteralPath "src\dbt\models\marts\machine_learning\mart_fraud_ml_features.sql"
```

- [ ] **Step 3: Commit**

```bash
git add -A src/dbt/models/
git commit -m "refactor(dbt): remove ephemeral intermediates and flat mart_fraud_ml_features"
```

---

## Task 3: Create `mart_customer_window_features.sql`

**Files:**
- Create: `src/dbt/models/marts/machine_learning/mart_customer_window_features.sql`

This mart owns customer 1d/7d/30d rolling aggregates. The SQL body is the current `int_customers_windowed.sql` logic (now materialized, not ephemeral).

- [ ] **Step 1: Write the model**

Create `src/dbt/models/marts/machine_learning/mart_customer_window_features.sql`:

```sql
{{ config(
    materialized = 'incremental',
    unique_key   = ['customer_id', 'feature_date'],
    incremental_strategy = 'append',
    views_enabled = false,
    properties   = {
        "engine": "'MergeTree'"
    }
) }}

{% set today      = modules.datetime.date.today().isoformat() %}
{% set start_date = var('start_date', today) %}
{% set end_date   = var('end_date',   start_date) %}


SELECT
    t.customer_id,
    d.fd                                                                         AS feature_date,

    COUNT(CASE WHEN t.event_date = d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,

    COUNT(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '6' DAY AND d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,

    COUNT(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '29' DAY AND d.fd THEN 1 END)
        AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,

    COALESCE(AVG(CASE WHEN t.event_date = d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,

    COALESCE(AVG(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '6' DAY AND d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,

    COALESCE(AVG(CASE WHEN t.event_date BETWEEN d.fd - INTERVAL '29' DAY AND d.fd THEN t.amount END), 0.0)
        AS CUSTOMER_AVG_AMOUNT_WINDOW_30D


FROM {{ ref('stg_transactions') }} t
CROSS JOIN (
    SELECT CAST(d AS DATE) AS fd
    FROM UNNEST(SEQUENCE(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL '1' DAY)) AS t(d)
) d
WHERE t.event_date BETWEEN DATE '{{ start_date }}' - INTERVAL '29' DAY AND DATE '{{ end_date }}'
GROUP BY t.customer_id, d.fd
```

- [ ] **Step 2: Commit**

```bash
git add src/dbt/models/marts/machine_learning/mart_customer_window_features.sql
git commit -m "feat(dbt): add mart_customer_window_features (per-entity gold)"
```

---

## Task 4: Create `mart_terminal_window_features.sql`

**Files:**
- Create: `src/dbt/models/marts/machine_learning/mart_terminal_window_features.sql`

Terminal risk/nb_tx aggregates with 7-day delay offset. SQL body is the current `int_terminals_windowed.sql` logic, now materialized.

- [ ] **Step 1: Write the model**

Create `src/dbt/models/marts/machine_learning/mart_terminal_window_features.sql`:

```sql
{{ config(
    materialized = 'incremental',
    unique_key   = ['terminal_id', 'feature_date'],
    incremental_strategy = 'append',
    views_enabled = false,
    properties   = {
        "engine": "'MergeTree'"
    }
) }}

{% set today      = modules.datetime.date.today().isoformat() %}
{% set start_date = var('start_date', today) %}
{% set end_date   = var('end_date',   start_date) %}


SELECT
    src.terminal_id,
    d.fd                                                                         AS feature_date,

    -- 1-day window (delay-offset: fd-7)
    COUNT(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_1DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date = d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_1DAY_WINDOW,

    -- 7-day window (delay-offset: [fd-13, fd-7])
    COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_7DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '13' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_7DAY_WINDOW,

    -- 30-day window (delay-offset: [fd-36, fd-7])
    COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END)
        AS TERMINAL_NB_TX_30DAY_WINDOW,

    COALESCE(
        CAST(
            SUM(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY AND src.is_fraud THEN 1.0 END)
            / NULLIF(COUNT(CASE WHEN src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY THEN 1 END), 0)
        AS DOUBLE),
        0.0
    ) AS TERMINAL_RISK_30DAY_WINDOW


FROM (
    SELECT t.terminal_id, t.event_date, COALESCE(f.is_fraud, false) AS is_fraud
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_fraud_cases') }} f ON t.transaction_id = f.transaction_id
    WHERE t.event_date BETWEEN DATE '{{ start_date }}' - INTERVAL '36' DAY AND DATE '{{ end_date }}'
) src
CROSS JOIN (
    SELECT CAST(d AS DATE) AS fd
    FROM UNNEST(SEQUENCE(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL '1' DAY)) AS t(d)
) d
WHERE src.event_date BETWEEN d.fd - INTERVAL '36' DAY AND d.fd - INTERVAL '7' DAY
GROUP BY src.terminal_id, d.fd
```

- [ ] **Step 2: Commit**

```bash
git add src/dbt/models/marts/machine_learning/mart_terminal_window_features.sql
git commit -m "feat(dbt): add mart_terminal_window_features (per-entity gold)"
```

---

## Task 5: Create `mart_transaction_features.sql`

**Files:**
- Create: `src/dbt/models/marts/machine_learning/mart_transaction_features.sql`

Transaction-level facts (amount, weekend/night flags) + deduped fraud label. Adapted from the non-join portion of the old `mart_fraud_ml_features.sql` — no LEFT JOIN to customer/terminal (Feast does that join now).

- [ ] **Step 1: Write the model**

Create `src/dbt/models/marts/machine_learning/mart_transaction_features.sql`:

```sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'transaction_id',
    incremental_strategy = 'append',
    views_enabled = false,
    properties   = {
        "engine": "'MergeTree'"
    }
) }}

{# ------------------------------------------------------------------
   Backfill: --var start_date 2024-01-01 --var end_date 2024-01-31
   Single day: --var start_date 2024-01-15
   Default (no vars): today
   ------------------------------------------------------------------ #}
{% set today      = modules.datetime.date.today().isoformat() %}
{% set start_date = var('start_date', today) %}
{% set end_date   = var('end_date',   start_date) %}


WITH params AS (
    SELECT CAST(d AS DATE) AS fd
    FROM UNNEST(SEQUENCE(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL '1' DAY)) AS t(d)
),

fraud_per_tx AS (
    SELECT
        transaction_id,
        IF(MAX(is_fraud), 1, 0)  AS tx_fraud_int
    FROM {{ ref('stg_fraud_cases') }}
    GROUP BY transaction_id
)

SELECT
    t.transaction_id,
    t.customer_id,
    t.terminal_id,
    CAST(t.event_timestamp AS timestamp(0))                                          AS event_timestamp,

    -- Transaction features (direct from staging)
    CAST(t.amount    AS DOUBLE)                                              AS TX_AMOUNT,
    t.is_weekend                                                             AS IS_WEEKEND,
    t.is_night                                                               AS IS_NIGHT,

    -- Fraud label (deduped subquery: missing fraud_case → 0 = legitimate)
    COALESCE(f.tx_fraud_int, 0)                                              AS TX_FRAUD,

    p.fd                                                                     AS feature_date

FROM {{ ref('stg_transactions') }} t
CROSS JOIN params p
LEFT JOIN fraud_per_tx f
    ON t.transaction_id = f.transaction_id
WHERE t.event_date = p.fd
```

- [ ] **Step 2: Commit**

```bash
git add src/dbt/models/marts/machine_learning/mart_transaction_features.sql
git commit -m "feat(dbt): add mart_transaction_features (per-entity gold)"
```

---

## Task 6: Update `dbt_project.yml`

**Files:**
- Modify: `src/dbt/dbt_project.yml`

Remove the `intermediate:` block (directory deleted in Task 2) and change the marts schema from `gold` to `ml_features`. The `generate_schema_name` macro passes the schema through unchanged, so ClickHouse database becomes `ml_features`.

- [ ] **Step 1: Edit dbt_project.yml**

Open `src/dbt/dbt_project.yml`. Replace the entire `models:` block (lines 13-33):

```yaml
models:
  ABC_Bank:
    staging:
      silver:
        +database: lakehouse
        +schema: silver
        +materialized: view
    marts:
      machine_learning:
        +database: clickhouse
        +schema: ml_features
        +materialized: incremental
```

This removes the `intermediate.customer` and `intermediate.terminal` sub-blocks and changes `+schema: gold` → `+schema: ml_features`.

- [ ] **Step 2: Commit**

```bash
git add src/dbt/dbt_project.yml
git commit -m "refactor(dbt): drop intermediate config, marts schema gold -> ml_features"
```

---

## Task 7: Rewrite `machine_learning.yml` tests

**Files:**
- Modify: `src/dbt/models/marts/machine_learning/machine_learning.yml`

Replace the single flat-model test block with three model blocks. Each mart gets `not_null` on every column plus key uniqueness. `accepted_values`/`accepted_range` move to `mart_transaction_features`.

- [ ] **Step 1: Write the new YAML**

Replace the entire contents of `src/dbt/models/marts/machine_learning/machine_learning.yml`:

```yaml
version: 2

models:
  - name: mart_customer_window_features
    description: "1d/7d/30d rolling aggregates per customer_id as of feature_date"
    tests:
      - unique_combination_of_columns:
          combination_of_columns:
            - customer_id
            - feature_date
    columns:
      - name: customer_id
        tests: [not_null]
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

  - name: mart_terminal_window_features
    description: "1d/7d/30d risk + tx-count aggregates per terminal_id (7-day delay offset)"
    tests:
      - unique_combination_of_columns:
          combination_of_columns:
            - terminal_id
            - feature_date
    columns:
      - name: terminal_id
        tests: [not_null]
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

  - name: mart_transaction_features
    description: "Transaction-level facts + deduped fraud label (offline-only, not request-time safe)"
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: event_timestamp
        data_type: timestamp(0)
        tests: [not_null]
      - name: customer_id
        tests: [not_null]
      - name: terminal_id
        tests: [not_null]
      - name: TX_AMOUNT
        tests:
          - not_null
          - accepted_range:
              min_value: 0
              inclusive: false
      - name: IS_WEEKEND
        tests: [not_null]
      - name: IS_NIGHT
        tests: [not_null]
      - name: TX_FRAUD
        tests:
          - not_null
          - accepted_values:
              values: [0, 1]
              quote: false
      - name: feature_date
        tests: [not_null]
```

- [ ] **Step 2: Commit**

```bash
git add src/dbt/models/marts/machine_learning/machine_learning.yml
git commit -m "test(dbt): rewrite machine_learning.yml for 3 per-entity marts"
```

---

## Task 8: Update `feature_store.yaml` — ClickHouse offline store

**Files:**
- Modify: `src/feature_store/feature_store.yaml`

Switch offline store from `file` to the contributed ClickHouse store. Host/port default to Docker service names (in-container); env overrides for local runs. `database: ml_features` matches the dbt Gold schema.

- [ ] **Step 1: Rewrite feature_store.yaml**

Replace the entire contents of `src/feature_store/feature_store.yaml`:

```yaml
project: fraud_detection
registry: data/feast_registry.db
provider: local
offline_store:
  type: feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore
  host: ${CLICKHOUSE_HOST:-clickhouse}
  port: ${CLICKHOUSE_PORT:-8123}
  database: ml_features
  user: ${CLICKHOUSE_USER:-default}
  password: ${CLICKHOUSE_PASSWORD:-}
  use_temporary_tables_for_entity_df: true
online_store:
  type: redis
  connection_string: "redis:6379"
entity_key_serialization_version: 2
```

- [ ] **Step 2: Commit**

```bash
git add src/feature_store/feature_store.yaml
git commit -m "feat(feast): switch offline store to contributed ClickHouse connector"
```

---

## Task 9: Rewrite `customer_features.py` with `ClickhouseSource`

**Files:**
- Modify: `src/feature_store/feature_views/customer_features.py`

Replace `FileSource` (pointing at non-existent `s3://gold/customer_features/`) with `ClickhouseSource` querying `ml_features.customer_window_features`. Cast `feature_date` to `DateTime` in the query so Feast's timestamp field works.

- [ ] **Step 1: Write the failing test**

Open `src/tests/feast/test_unit_feast_feature_views.py`. Replace the `customer_features_view` test section (the block under `# --- customer_features_view ---`) with:

```python
# --- customer_features_view ---

def test_customer_features_view_name():
    assert customer_features_view.name == "customer_features_view"


def test_customer_features_view_ttl():
    assert customer_features_view.ttl == timedelta(days=2)


def test_customer_features_view_feature_names():
    expected = {
        "CUSTOMER_AVG_AMOUNT_WINDOW_1D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_7D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_30D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D",
    }
    assert {f.name for f in customer_features_view.schema} == expected


def test_customer_features_view_is_online():
    assert customer_features_view.online is True


def test_customer_features_view_source_is_clickhouse():
    from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
        ClickhouseSource,
    )
    assert isinstance(customer_features_view.source, ClickhouseSource)


def test_customer_features_view_source_query_targets_ml_features():
    assert "ml_features.customer_window_features" in customer_features_view.source.query
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v -k customer
```
Expected: FAIL — `test_customer_features_view_source_is_clickhouse` fails (current source is `FileSource`).

- [ ] **Step 3: Rewrite customer_features.py**

Replace the entire contents of `src/feature_store/feature_views/customer_features.py`:

```python
from datetime import timedelta

from feast import FeatureView, Field
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
)
from feast.types import Float64, Int64

from feature_store.entities import customer

_source = ClickhouseSource(
    name="ml_features.customer_window_features",
    query=(
        "SELECT"
        " customer_id,"
        " toDateTime(feature_date) AS event_timestamp,"
        " CUSTOMER_AVG_AMOUNT_WINDOW_1D,"
        " CUSTOMER_AVG_AMOUNT_WINDOW_7D,"
        " CUSTOMER_AVG_AMOUNT_WINDOW_30D,"
        " CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,"
        " CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,"
        " CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D"
        " FROM ml_features.customer_window_features"
    ),
    timestamp_field="event_timestamp",
)

customer_features_view = FeatureView(
    name="customer_features_view",
    entities=[customer],
    ttl=timedelta(days=2),
    schema=[
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_1D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_7D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_30D", dtype=Float64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D", dtype=Int64),
    ],
    source=_source,
    online=True,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v -k customer
```
Expected: PASS (all customer tests)

- [ ] **Step 5: Commit**

```bash
git add src/feature_store/feature_views/customer_features.py src/tests/feast/test_unit_feast_feature_views.py
git commit -m "feat(feast): customer_features_view -> ClickhouseSource on ml_features"
```

---

## Task 10: Rewrite `terminal_features.py` with `ClickhouseSource`

**Files:**
- Modify: `src/feature_store/feature_views/terminal_features.py`

Same pattern as Task 9, pointing at `ml_features.terminal_window_features`.

- [ ] **Step 1: Write the failing test**

In `src/tests/feast/test_unit_feast_feature_views.py`, replace the `terminal_features_view` test section (block under `# --- terminal_features_view ---`) with:

```python
# --- terminal_features_view ---

def test_terminal_features_view_name():
    assert terminal_features_view.name == "terminal_features_view"


def test_terminal_features_view_ttl():
    assert terminal_features_view.ttl == timedelta(days=2)


def test_terminal_features_view_feature_names():
    expected = {
        "TERMINAL_RISK_1DAY_WINDOW",
        "TERMINAL_RISK_7DAY_WINDOW",
        "TERMINAL_RISK_30DAY_WINDOW",
        "TERMINAL_NB_TX_1DAY_WINDOW",
        "TERMINAL_NB_TX_7DAY_WINDOW",
        "TERMINAL_NB_TX_30DAY_WINDOW",
    }
    assert {f.name for f in terminal_features_view.schema} == expected


def test_terminal_features_view_is_online():
    assert terminal_features_view.online is True


def test_terminal_features_view_source_is_clickhouse():
    from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
        ClickhouseSource,
    )
    assert isinstance(terminal_features_view.source, ClickhouseSource)


def test_terminal_features_view_source_query_targets_ml_features():
    assert "ml_features.terminal_window_features" in terminal_features_view.source.query
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v -k terminal
```
Expected: FAIL — `test_terminal_features_view_source_is_clickhouse` fails (current source is `FileSource`).

- [ ] **Step 3: Rewrite terminal_features.py**

Replace the entire contents of `src/feature_store/feature_views/terminal_features.py`:

```python
from datetime import timedelta

from feast import FeatureView, Field
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
)
from feast.types import Float64, Int64

from feature_store.entities import terminal

_source = ClickhouseSource(
    name="ml_features.terminal_window_features",
    query=(
        "SELECT"
        " terminal_id,"
        " toDateTime(feature_date) AS event_timestamp,"
        " TERMINAL_RISK_1DAY_WINDOW,"
        " TERMINAL_RISK_7DAY_WINDOW,"
        " TERMINAL_RISK_30DAY_WINDOW,"
        " TERMINAL_NB_TX_1DAY_WINDOW,"
        " TERMINAL_NB_TX_7DAY_WINDOW,"
        " TERMINAL_NB_TX_30DAY_WINDOW"
        " FROM ml_features.terminal_window_features"
    ),
    timestamp_field="event_timestamp",
)

terminal_features_view = FeatureView(
    name="terminal_features_view",
    entities=[terminal],
    ttl=timedelta(days=2),
    schema=[
        Field(name="TERMINAL_RISK_1DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_7DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_30DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_NB_TX_1DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_7DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_30DAY_WINDOW", dtype=Int64),
    ],
    source=_source,
    online=True,
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v -k terminal
```
Expected: PASS (all terminal tests)

- [ ] **Step 5: Commit**

```bash
git add src/feature_store/feature_views/terminal_features.py src/tests/feast/test_unit_feast_feature_views.py
git commit -m "feat(feast): terminal_features_view -> ClickhouseSource on ml_features"
```

---

## Task 11: Create `transaction_features.py`, delete `fraud_ml_features.py`

**Files:**
- Delete: `src/feature_store/feature_views/fraud_ml_features.py`
- Create: `src/feature_store/feature_views/transaction_features.py`

Rename + rewrite. The view now has three entities (transaction, customer, terminal) so Feast can auto-join it with the customer and terminal views. `online=False` because the label is not request-time safe and the request-time columns arrive in the inference request.

- [ ] **Step 1: Write the failing test**

In `src/tests/feast/test_unit_feast_feature_views.py`, replace the `fraud_ml_features_view` test section (block under `# --- fraud_ml_features_view ---`) with:

```python
# --- transaction_features_view ---

def test_transaction_features_view_name():
    assert transaction_features_view.name == "transaction_features_view"


def test_transaction_features_view_ttl():
    assert transaction_features_view.ttl == timedelta(days=365)


def test_transaction_features_view_feature_names():
    expected = {
        "TX_AMOUNT",
        "IS_WEEKEND",
        "IS_NIGHT",
        "TX_FRAUD",
    }
    assert {f.name for f in transaction_features_view.schema} == expected


def test_transaction_features_view_is_offline():
    assert transaction_features_view.online is False


def test_transaction_features_view_source_is_clickhouse():
    from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
        ClickhouseSource,
    )
    assert isinstance(transaction_features_view.source, ClickhouseSource)


def test_transaction_features_view_source_query_targets_ml_features():
    assert "ml_features.transaction_features" in transaction_features_view.source.query


def test_transaction_features_view_has_three_entities():
    entity_names = {e.name for e in transaction_features_view.entities}
    assert entity_names == {"transaction", "customer", "terminal"}
```

- [ ] **Step 2: Update the test imports**

At the top of `src/tests/feast/test_unit_feast_feature_views.py`, replace:

```python
from feature_store.feature_views.customer_features import customer_features_view
from feature_store.feature_views.fraud_ml_features import fraud_ml_features_view
from feature_store.feature_views.terminal_features import terminal_features_view
```

with:

```python
from feature_store.feature_views.customer_features import customer_features_view
from feature_store.feature_views.terminal_features import terminal_features_view
from feature_store.feature_views.transaction_features import transaction_features_view
```

- [ ] **Step 3: Run tests to verify they fail**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v -k transaction
```
Expected: FAIL — `ImportError: cannot import name 'transaction_features_view'`

- [ ] **Step 4: Create transaction_features.py**

Create `src/feature_store/feature_views/transaction_features.py`:

```python
from datetime import timedelta

from feast import FeatureView, Field
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
)
from feast.types import Bool, Float64, Int64

from feature_store.entities import customer, terminal, transaction

_source = ClickhouseSource(
    name="ml_features.transaction_features",
    query=(
        "SELECT"
        " transaction_id,"
        " customer_id,"
        " terminal_id,"
        " event_timestamp,"
        " TX_AMOUNT,"
        " IS_WEEKEND,"
        " IS_NIGHT,"
        " TX_FRAUD"
        " FROM ml_features.transaction_features"
    ),
    timestamp_field="event_timestamp",
)

transaction_features_view = FeatureView(
    name="transaction_features_view",
    entities=[transaction, customer, terminal],
    ttl=timedelta(days=365),
    schema=[
        Field(name="TX_AMOUNT", dtype=Float64),
        Field(name="IS_WEEKEND", dtype=Bool),
        Field(name="IS_NIGHT", dtype=Bool),
        Field(name="TX_FRAUD", dtype=Int64),
    ],
    source=_source,
    online=False,
)
```

- [ ] **Step 5: Delete the old fraud_ml_features.py**

Run:
```bash
Remove-Item -LiteralPath "src\feature_store\feature_views\fraud_ml_features.py"
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
uv run pytest src/tests/feast/test_unit_feast_feature_views.py -v
```
Expected: PASS (all feature view tests pass)

- [ ] **Step 7: Commit**

```bash
git add src/feature_store/feature_views/transaction_features.py src/tests/feast/test_unit_feast_feature_views.py
git add -A src/feature_store/feature_views/fraud_ml_features.py
git commit -m "feat(feast): transaction_features_view (3 entities, offline-only) replaces fraud_ml_features_view"
```

---

## Task 12: Re-export views in `feature_views/__init__.py`

**Files:**
- Modify: `src/feature_store/feature_views/__init__.py`

Currently empty. Feast `apply` discovers views via the module import path; re-exporting ensures all three are registered regardless of how the repo is loaded.

- [ ] **Step 1: Write the __init__.py**

Replace the entire contents of `src/feature_store/feature_views/__init__.py`:

```python
from feature_store.feature_views.customer_features import customer_features_view
from feature_store.feature_views.terminal_features import terminal_features_view
from feature_store.feature_views.transaction_features import transaction_features_view

__all__ = [
    "customer_features_view",
    "terminal_features_view",
    "transaction_features_view",
]
```

- [ ] **Step 2: Verify import**

Run:
```bash
uv run python -c "from feature_store.feature_views import customer_features_view, terminal_features_view, transaction_features_view; print('ok')"
```
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/feature_store/feature_views/__init__.py
git commit -m "feat(feast): re-export 3 feature views from feature_views package"
```

---

## Task 13: Delete `materialize_to_redis.py` and its tests

**Files:**
- Delete: `src/feature_store/materialize_to_redis.py`
- Delete: `src/tests/feast/test_unit_feast_materialize.py`

The script queries non-existent tables and is replaced by Feast's native `store.materialize`. Its unit tests assert behavior of deleted code.

- [ ] **Step 1: Delete both files**

Run:
```bash
Remove-Item -LiteralPath "src\feature_store\materialize_to_redis.py"
Remove-Item -LiteralPath "src\tests\feast\test_unit_feast_materialize.py"
```

- [ ] **Step 2: Verify remaining Feast tests still pass**

Run:
```bash
uv run pytest src/tests/feast/ -v
```
Expected: PASS (entities + feature_views tests, no import errors from the deleted module)

- [ ] **Step 3: Commit**

```bash
git add -A src/feature_store/materialize_to_redis.py src/tests/feast/test_unit_feast_materialize.py
git commit -m "refactor(feast): delete broken materialize_to_redis.py and its tests"
```

---

## Task 14: Update Airflow DAG — remove `dbt_intermediate`, repoint materialize task

**Files:**
- Modify: `src/orchestration/dags/feature_pipeline_daily.py`

Remove the `dbt_intermediate` task group (intermediate models deleted in Task 2) and replace the `materialize_online_features` BashOperator (which called the deleted script) with a `feast materialize-incremental` command. Update the docstring and dependency graph.

- [ ] **Step 1: Update the module docstring**

In `src/orchestration/dags/feature_pipeline_daily.py`, replace lines 1-15 (the docstring):

```python
"""feature_pipeline_daily — daily batch pipeline: Bronze → dbt (staging/marts) → Redis.

Dependency graph:

  bronze.ingest_transactions ──┐
                                ├──► dbt_staging ──► dbt_marts ──► materialize_online_features
  bronze.ingest_fraud_cases  ──┘

dbt_staging writes normalized Silver-equivalent tables to MinIO/Delta via Trino.
dbt_marts writes three per-entity Gold tables to ClickHouse ml_features schema:
  customer_window_features, terminal_window_features, transaction_features.
materialize_online_features calls `feast materialize-incremental` which reads the
latest customer + terminal feature rows from ClickHouse and pushes them to Redis.
The transaction_features view is offline-only and skipped by materialize.

On any task failure a Discord alert is sent to the team webhook.
"""
```

- [ ] **Step 2: Remove the `_MATERIALIZE_SCRIPT` env var reference**

Replace lines 46-49:

```python
_DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "",
)
_MATERIALIZE_SCRIPT = os.environ.get(
    "MATERIALIZE_SCRIPT_PATH",
    str(pathlib.Path(__file__).resolve().parents[2] / "feature_store" / "materialize_to_redis.py"),
)
_DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
```

with:

```python
_DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "",
)
_FEAST_REPO_DIR = os.environ.get(
    "FEAST_REPO_DIR",
    str(pathlib.Path(__file__).resolve().parents[2] / "feature_store"),
)
_DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
```

- [ ] **Step 3: Remove the `dbt_intermediate` task group**

Replace lines 201-204:

```python
    dbt_intermediate = _dbt_task_group(
        group_id="dbt_intermediate",
        select="intermediate",
    )

    dbt_marts = _dbt_task_group(
        group_id="dbt_marts",
        select="marts",
    )
```

with:

```python
    dbt_marts = _dbt_task_group(
        group_id="dbt_marts",
        select="marts",
    )
```

- [ ] **Step 4: Replace the `materialize_online_features` BashOperator**

Replace lines 212-224 (the BashOperator block):

```python
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
```

with:

```python
    materialize_online_features = BashOperator(
        task_id="materialize_online_features",
        bash_command=(
            f"cd {_FEAST_REPO_DIR} && "
            "feast materialize-incremental $(date -u +'%%Y-%%m-%%dT%%H:%%M:%%S')"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=_notify_discord_failure,
        doc_md=(
            "Runs `feast materialize-incremental` from the feature_store repo. "
            "Reads latest customer + terminal feature rows from ClickHouse "
            "ml_features and pushes them to Redis. The transaction_features "
            "view is offline-only and skipped."
        ),
    )
```

- [ ] **Step 5: Update the dependency graph**

Replace lines 227-231:

```python
    # ── Dependencies ──────────────────────────────────────────────────────
    [ingest_transactions, ingest_fraud_cases] >> [normalize_transactions, normalize_fraud_cases]
    [normalize_transactions, normalize_fraud_cases] >> dbt_staging
    dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_marts
    dbt_marts >> materialize_online_features
```

with:

```python
    # ── Dependencies ──────────────────────────────────────────────────────
    [ingest_transactions, ingest_fraud_cases] >> [normalize_transactions, normalize_fraud_cases]
    [normalize_transactions, normalize_fraud_cases] >> dbt_staging
    dbt_staging >> dbt_marts
    dbt_marts >> materialize_online_features
```

- [ ] **Step 6: Verify the DAG parses**

Run:
```bash
uv run python -c "import ast; ast.parse(open('src/orchestration/dags/feature_pipeline_daily.py').read()); print('syntax ok')"
```
Expected: `syntax ok`

- [ ] **Step 7: Commit**

```bash
git add src/orchestration/dags/feature_pipeline_daily.py
git commit -m "refactor(airflow): drop dbt_intermediate, repoint materialize to feast materialize-incremental"
```

---

## Task 15: Update `docker-compose.airflow.yml` — remove dead env var

**Files:**
- Modify: `src/orchestration/docker-compose.airflow.yml`

Remove the `MATERIALIZE_SCRIPT_PATH` env var (script deleted in Task 13). Add `FEAST_REPO_DIR` env var matching the new DAG reference. The `feature_store` volume mount at `/opt/airflow/feature_store` already exists (line 55) and stays.

- [ ] **Step 1: Edit the env vars**

In `src/orchestration/docker-compose.airflow.yml`, replace lines 38-44:

```yaml
    # Absolute in-container path to materialize_to_redis.py (feature_store/ is
    # mounted at /opt/airflow/feature_store — see volumes below).
    MATERIALIZE_SCRIPT_PATH: "/opt/airflow/feature_store/materialize_to_redis.py"
    TRINO_HOST: "trino"
    TRINO_PORT: "8080"
    CLICKHOUSE_HOST: "clickhouse"
    CLICKHOUSE_PORT: "8123"
```

with:

```yaml
    # In-container path to the Feast feature_store repo (mounted at
    # /opt/airflow/feature_store — see volumes below). Used by
    # materialize_online_features to run `feast materialize-incremental`.
    FEAST_REPO_DIR: "/opt/airflow/feature_store"
    TRINO_HOST: "trino"
    TRINO_PORT: "8080"
    CLICKHOUSE_HOST: "clickhouse"
    CLICKHOUSE_PORT: "8123"
```

- [ ] **Step 2: Commit**

```bash
git add src/orchestration/docker-compose.airflow.yml
git commit -m "chore(airflow): replace MATERIALIZE_SCRIPT_PATH with FEAST_REPO_DIR env"
```

---

## Task 16: Update `docs/architecture.md`

**Files:**
- Modify: `docs/architecture.md`

Reconcile the Feature Store and Data Pipeline bullets with the new reality: Gold per-entity tables in ClickHouse `ml_features`, Feast offline store is the contributed ClickHouse connector (not MinIO file).

- [ ] **Step 1: Edit the Data Pipeline ClickHouse bullet**

In `docs/architecture.md`, replace line 25:

```
- **ClickHouse** (head-distroless): Gold/serving layer storing mart tables only (MergeTree engine)
  - `marts.mart_fraud_ml_features` — flat ML feature table joining all features
```

with:

```
- **ClickHouse** (head-distroless): Gold/serving layer storing per-entity mart tables only (MergeTree engine)
  - `ml_features.customer_window_features` — 1d/7d/30d rolling customer aggregates
  - `ml_features.terminal_window_features` — 1d/7d/30d terminal risk + tx-count (7-day delay offset)
  - `ml_features.transaction_features` — transaction facts + deduped fraud label (offline-only)
```

- [ ] **Step 2: Edit the Airflow bullet**

Replace line 26:

```
- **Airflow**: orchestration with **astronomer-cosmos 1.14.1** DbtTaskGroups replacing Spark Silver/Gold batch jobs → `[bronze] → dbt_staging → dbt_intermediate → dbt_marts → materialize_online_features`
```

with:

```
- **Airflow**: orchestration with **astronomer-cosmos 1.14.1** DbtTaskGroups replacing Spark Silver/Gold batch jobs → `[bronze] → dbt_staging → dbt_marts → materialize_online_features`
```

- [ ] **Step 3: Edit the Feature Store section**

Replace lines 29-34:

```
### Feature Store (green section)
- **Feast**: feature store (`src/feature_store/`)
  - Offline store: **file** (reads Gold Delta Parquet files from MinIO via S3 FileSource for training data from historical Gold)
  - Online store: **Redis** (<10ms lookup)
  - Three feature views: `fraud_ml_features_view` (offline, training), `customer_features_view` + `terminal_features_view` (online+offline)
  - Materialization: direct ClickHouse reads via **clickhouse-connect 0.15.1** → `write_to_online_store()` (bypasses offline store file-based reads)
```

with:

```
### Feature Store (green section)
- **Feast**: feature store (`src/feature_store/`)
  - Offline store: **contributed ClickHouse connector** (`feast[clickhouse]`) reading `ml_features.*` tables directly for point-in-time joins
  - Online store: **Redis** (<10ms lookup)
  - Three feature views: `customer_features_view` + `terminal_features_view` (online+offline, ClickhouseSource), `transaction_features_view` (offline-only, 3 entities for auto-join)
  - Materialization: `feast materialize-incremental` reads latest customer/terminal rows from ClickHouse → Redis
```

- [ ] **Step 4: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: reconcile architecture with ClickHouse Gold + Feast ClickHouse connector"
```

---

## Task 17: Update `docs/fraud-data-platform-detailed.md`

**Files:**
- Modify: `docs/fraud-data-platform-detailed.md`

§9.4 and §12.2 describe Gold as Delta on MinIO and Feast offline as file-based. Reconcile with the ClickHouse Gold reality so docs match code.

- [ ] **Step 1: Edit §9.4 Gold contract table**

In `docs/fraud-data-platform-detailed.md`, find the Gold table at §9.4 (around line 429-437). Replace:

```
| Gold table | Purpose |
| --- | --- |
| `gold.customer_window_features` | customer velocity and rolling statistics |
| `gold.terminal_window_features` | terminal activity and risk-related aggregates |
| `gold.training_dataset` | point-in-time aligned dataset for model training |
| `gold.fraud_monitoring_mart` | dashboards, fraud rate trends, operations reporting |
| `gold.feature_export_snapshot` | curated export set for Feast or materialization jobs |
```

with:

```
| Gold table | Purpose |
| --- | --- |
| `ml_features.customer_window_features` | customer velocity and rolling statistics (ClickHouse MergeTree) |
| `ml_features.terminal_window_features` | terminal activity and risk-related aggregates (ClickHouse MergeTree) |
| `ml_features.transaction_features` | transaction facts + deduped fraud label (ClickHouse MergeTree, offline-only) |

> **Note:** Gold per-entity tables live in ClickHouse `ml_features` (not MinIO Delta). MinIO Delta remains the Bronze/Silver layer only. Feast assembles the point-in-time training dataset via `get_historical_features` across the three ClickHouse sources — there is no flat `training_dataset` mart.
```

- [ ] **Step 2: Edit §12.2 Source of truth for historical features**

Find §12.2 (around line 578-585). Replace:

```
### 12.2 Source of truth for historical features

The cleanest conceptual design is:

- Gold feature tables are the system of record for curated historical features
- Feast definitions reference those curated sources for historical retrieval
- Redis stores only the subset needed for low-latency serving

This is better than treating Redis or raw CDC as the historical feature source.
```

with:

```
### 12.2 Source of truth for historical features

The cleanest conceptual design is:

- Gold per-entity feature tables in ClickHouse `ml_features` are the system of record for curated historical features
- Feast uses the contributed ClickHouse offline store (`feast[clickhouse]`) to read those tables directly and perform point-in-time joins via `get_historical_features`
- Redis stores only the subset needed for low-latency serving (customer + terminal views; transaction view is offline-only)

This is better than treating Redis or raw CDC as the historical feature source.
```

- [ ] **Step 3: Commit**

```bash
git add docs/fraud-data-platform-detailed.md
git commit -m "docs: reconcile detailed architecture §9.4 §12.2 with ClickHouse Gold"
```

---

## Task 18: Run full test suite and lint

**Files:**
- none (verification only)

Final gate before declaring done. Confirms no import errors from deleted modules, no lint failures, and all Feast unit tests pass.

- [ ] **Step 1: Run Feast tests**

Run:
```bash
uv run pytest src/tests/feast/ -v
```
Expected: PASS — entities (7 tests) + feature_views (all customer/terminal/transaction tests). No `test_unit_feast_materialize` (deleted).

- [ ] **Step 2: Run full test suite**

Run:
```bash
uv run pytest src/tests/ -v
```
Expected: PASS — no import errors from the deleted `materialize_to_redis` or `fraud_ml_features` modules.

- [ ] **Step 3: Run lint**

Run:
```bash
uv run ruff check .
```
Expected: no errors (unused imports from deleted modules would surface here)

- [ ] **Step 4: Commit if any cleanup needed**

If lint or tests revealed issues fixed in this step, commit them:
```bash
git add -A
git commit -m "chore: fix lint/test issues from gold-feast refactor"
```

---

## Verification (manual, minimal Docker)

This is the end-to-end smoke test described in the spec. **Start only the components needed** — do not bring up the full stack (RAM constraint).

- [ ] **Step 1: Start ClickHouse + Redis only**

```bash
docker compose up -d clickhouse redis
```

- [ ] **Step 2: Run dbt to populate ml_features tables**

Requires Trino + MinIO for the dbt-trino path (Silver sources live on MinIO). Start those too:
```bash
docker compose up -d trino minio
```
Then from the dbt project dir:
```bash
cd src/dbt
dbt run -s mart_customer_window_features mart_terminal_window_features mart_transaction_features --vars 'start_date: 2024-01-01' 'end_date: 2024-01-07'
dbt test -s mart_customer_window_features mart_terminal_window_features mart_transaction_features
```
Expected: 3 models built in ClickHouse `ml_features`, all tests pass.

- [ ] **Step 3: Feast apply + materialize**

```bash
cd src/feature_store
feast apply
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```
Expected: `customer_features_view` and `terminal_features_view` materialize to Redis; `transaction_features_view` skipped (offline-only).

- [ ] **Step 4: Verify Redis keys**

```bash
docker exec -it <redis-container> redis-cli KEYS "*" | head
```
Expected: customer + terminal keys present.

- [ ] **Step 5: get_historical_features smoke test**

```bash
uv run python -c "
from feast import FeatureStore
import pandas as pd
store = FeatureStore(repo_path='src/feature_store')
entity_df = pd.DataFrame({
    'transaction_id': [1],
    'customer_id': [1],
    'terminal_id': [1],
    'event_timestamp': pd.to_datetime(['2024-01-07']),
})
features = store.get_historical_features(
    entity_df=entity_df,
    features=[
        'customer_features_view:CUSTOMER_AVG_AMOUNT_WINDOW_1D',
        'terminal_features_view:TERMINAL_RISK_1DAY_WINDOW',
        'transaction_features_view:TX_AMOUNT',
    ],
)
print(features.to_df())
"
```
Expected: a DataFrame with the joined features (may contain nulls if no matching feature rows exist for the test entity — that is acceptable, the join mechanism is what is being verified).

- [ ] **Step 6: Stop the minimal stack**

```bash
docker compose stop clickhouse redis trino minio
```
