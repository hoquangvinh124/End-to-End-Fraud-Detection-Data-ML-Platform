# Gold Layer per-Entity + Feast ClickHouse Connector Design

**Goal:** Replace the single flat Gold mart (`marts.mart_fraud_ml_features`) with per-entity Gold tables in ClickHouse, and rewire Feast onto the contributed ClickHouse offline store so that Feast performs point-in-time joins across the per-entity views instead of consuming a pre-joined flat table.

**Why:** The current design contradicts the architecture intent (`docs/fraud-data-platform-detailed.md` §9.4, §12.2) which calls for Gold tables per consumer/entity (`customer_window_features`, `terminal_window_features`, `training_dataset`). The intermediate tables that hold the per-entity window logic are configured `materialized: ephemeral`, so they never materialize as queryable tables — yet Feast's `customer_features_view` / `terminal_features_view` `FileSource` point at `s3://gold/...` paths that contain nothing. The standalone materialization script (`materialize_to_redis.py`) queries `intermediate.int_customer_window_features` / `intermediate.int_terminal_window_features`, tables that do not exist (wrong name, wrong schema, and ephemeral). Feast is therefore non-functional end to end.

**Tech stack:** dbt-trino (ClickHouse catalog), ClickHouse (MergeTree, new database `ml_features`), Feast contributed ClickHouse offline store (`feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore` + `ClickhouseSource`), Redis online store (unchanged).

**Scope of this spec:** dbt Gold model restructure, Feast `feature_store.yaml` + FeatureView rewrites, removal of the broken materialization script, dependency wiring, doc/test updates. Source simulation, Bronze/Silver, Flink, training pipeline, and serving are out of scope.

---

## Operating constraint

When building or running end-to-end tests, start only the Docker Compose components each step requires (for this spec: ClickHouse, Redis, and Trino/MinIO when dbt runs). Do not bring up the full stack — it is RAM-heavy and unnecessary for verifying this slice.

---

## Components

### 1. dbt Gold layer — three per-entity marts in `ml_features`

Delete `models/marts/machine_learning/mart_fraud_ml_features.sql` and its flat-table tests. Delete the entire `models/intermediate/` directory (`int_customers_windowed.sql`, `int_terminals_windowed.sql`, `customer.yml`, `terminal.yml`) — with Feast performing the cross-entity join, each mart is independent and the ephemeral intermediate layer adds indirection with no reuse, hiding each mart's real responsibility. Fold the window logic directly into the mart that owns it.

Three new mart models under `models/marts/machine_learning/`, all materialized `incremental` in ClickHouse database `ml_features`:

#### 1.1 `mart_customer_window_features.sql` → `ml_features.customer_window_features`

Keys: `customer_id`, `feature_date`. Columns (6): `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D`, `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D`, `CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D`, `CUSTOMER_AVG_AMOUNT_WINDOW_1D`, `CUSTOMER_AVG_AMOUNT_WINDOW_7D`, `CUSTOMER_AVG_AMOUNT_WINDOW_30D`.

SQL is the current `int_customers_windowed.sql` body verbatim (CROSS JOIN date sequence, CASE-when window aggregates, GROUP BY customer_id, feature_date), now materialized instead of ephemeral. `incremental` + `unique_key=['customer_id','feature_date']`.

#### 1.2 `mart_terminal_window_features.sql` → `ml_features.terminal_window_features`

Keys: `terminal_id`, `feature_date`. Columns (6): `TERMINAL_NB_TX_1DAY_WINDOW`, `TERMINAL_NB_TX_7DAY_WINDOW`, `TERMINAL_NB_TX_30DAY_WINDOW`, `TERMINAL_RISK_1DAY_WINDOW`, `TERMINAL_RISK_7DAY_WINDOW`, `TERMINAL_RISK_30DAY_WINDOW`.

SQL is the current `int_terminals_windowed.sql` body verbatim (7-day delay-offset windows for risk, GROUP BY terminal_id, feature_date), now materialized. `incremental` + `unique_key=['terminal_id','feature_date']`.

#### 1.3 `mart_transaction_features.sql` → `ml_features.transaction_features`

Keys: `transaction_id` (plus `customer_id`, `terminal_id` as entity join keys for Feast, plus `event_timestamp`). Columns: `TX_AMOUNT`, `IS_WEEKEND`, `IS_NIGHT`, `TX_FRAUD` (label, deduped from `stg_fraud_cases`). SQL adapted from the non-join portion of the current `mart_fraud_ml_features.sql`: `stg_transactions` CROSS JOIN params + the `fraud_per_tx` dedup subquery, no LEFT JOIN to customer/terminal intermediates. `incremental` + `unique_key='transaction_id'`.

#### 1.4 `dbt_project.yml`

Change `models.ABC_Bank.marts.machine_learning`:
- `+schema: gold` → `+schema: ml_features` (the `generate_schema_name` macro passes the schema through unchanged, so the ClickHouse database becomes `ml_features`).
- Keep `+database: clickhouse`, `+materialized: incremental`.

Delete the `models.ABC_Bank.intermediate` block entirely.

#### 1.5 `machine_learning.yml` (tests)

Replace the single flat-model test block with three model blocks. Each mart keeps `not_null` on every feature column plus `unique_combination_of_columns` on its key pair (customer_id+feature_date, terminal_id+feature_date) or `unique`+`not_null` on `transaction_id`. `accepted_values` on `TX_FRAUD` and `accepted_range` on `TX_AMOUNT` move to `mart_transaction_features`.

### 2. Feast — contributed ClickHouse offline store + `ClickhouseSource`

#### 2.1 `feature_store.yaml`

`offline_store` changes from `file` to the contributed ClickHouse store:

```yaml
project: fraud_detection
registry: data/feast_registry.db
provider: local
offline_store:
  type: feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStore
  host: clickhouse
  port: 8123
  database: ml_features
  user: ${CLICKHOUSE_USER:-default}
  password: ${CLICKHOUSE_PASSWORD:-}
  use_temporary_tables_for_entity_df: true
online_store:
  type: redis
  connection_string: "redis:6379"
entity_key_serialization_version: 2
```

Host/port default to the Docker service names so it runs in-container; override via env for local runs. `database: ml_features` matches the dbt Gold schema. `online_store` Redis unchanged.

#### 2.2 FeatureViews (rewrite all three with `ClickhouseSource`)

`entities.py` unchanged (transaction, customer, terminal; INT64).

`feature_views/customer_features.py` — `ClickhouseSource(query="SELECT customer_id, toDateTime(feature_date) AS event_timestamp, <6 cols> FROM ml_features.customer_window_features", timestamp_field="event_timestamp")`; `customer_features_view` entities=[customer], ttl=2d, online=True.

`feature_views/terminal_features.py` — analogous, `FROM ml_features.terminal_window_features`, entities=[terminal], online=True.

`feature_views/transaction_features.py` (renamed from `fraud_ml_features.py`) — `ClickhouseSource(query="SELECT transaction_id, customer_id, terminal_id, event_timestamp, TX_AMOUNT, IS_WEEKEND, IS_NIGHT, TX_FRAUD FROM ml_features.transaction_features", timestamp_field="event_timestamp")`; entities=[transaction, customer, terminal], ttl=365d, online=False (see §3).

`feature_views/__init__.py` re-exports the three view objects (currently empty, which is why the Feast registry may not discover them depending on apply path).

#### 2.3 Why `transaction_features_view` is `online=False`

The view has two column families, neither belongs in Redis:
- `TX_FRAUD` is a delayed label (chargeback/analyst, T+7/T+30). At scoring time it does not exist for the incoming transaction, so materializing it copies data that is never usable online. Architecture doc §12.6 explicitly marks `transaction_id → delayed fraud label → not request-time safe → no`.
- `TX_AMOUNT`, `IS_WEEKEND`, `IS_NIGHT` are request-time attributes already present in the inference request; the serving transformer computes `is_weekend`/`is_night` from the timestamp and receives `amount` directly. A Redis fetch would re-read data the client just sent.

The view is therefore offline-only: its sole consumer is `get_historical_features` point-in-time join to assemble training datasets.

### 3. Materialization + offline training retrieval

#### 3.1 Delete `materialize_to_redis.py`

It queries non-existent tables (`intermediate.int_customer_window_features` / `intermediate.int_terminal_window_features` — wrong name, wrong schema, and the real intermediates are ephemeral). Replace with Feast's native path.

#### 3.2 Online materialization

Use `store.materialize(start_date, end_date)` or `feast materialize-incremental`. The ClickHouse offline store implements `pull_latest_from_table_or_query`, which reads the latest `feature_date` row per entity from `customer_window_features` / `terminal_window_features` and writes to Redis. Only the two `online=True` views materialize; `transaction_features_view` is skipped. Airflow task `materialize_online_features` calls this instead of the deleted script.

#### 3.3 Offline training retrieval

`store.get_historical_features(entity_df=transactions[['transaction_id','customer_id','terminal_id','event_timestamp']], features=[customer_features_view cols + terminal_features_view cols + transaction_features_view cols])` → Feast executes a point-in-time join across the three ClickHouse sources and returns the training DataFrame. This is the "auto-join" the design targets: no flat mart, Feast assembles the row per transaction using only feature rows with `feature_date ≤ event_timestamp` (for customer/terminal) and the exact `event_timestamp` match (for transaction features).

Point-in-time correctness: terminal risk rows are computed with a 7-day delay offset (`feature_date = D` uses data up to `D-7`), so joining at `feature_date ≤ event_timestamp` cannot leak future labels — consistent with architecture doc §13.4.

### 4. Cleanup, dependencies, docs, tests

#### 4.1 Dependencies

Verify the contributed ClickHouse offline store's runtime needs. The repo already has `clickhouse-connect==0.15.1` (HTTP client). The contrib store may additionally require `clickhouse-driver` (native protocol) — add to `[dependency-groups].dev` only if import/install fails. Pin `feast[redis]>=0.40` is already present.

#### 4.2 Docs

- `docs/architecture.md` §Feature Store: change "Offline store: file (reads Gold Delta Parquet from MinIO)" → "Offline store: Feast contributed ClickHouse store reading `ml_features` tables in ClickHouse". Update the Gold bullet to name the three `ml_features.*` tables.
- `docs/fraud-data-platform-detailed.md` §9.4 and §12.2: reconcile the MinIO-Parquet language with the ClickHouse-Gold reality (Gold per-entity tables live in ClickHouse `ml_features`, not MinIO Delta). Keep MinIO Delta for Bronze/Silver only.
- Note the divergence from the original "Gold = Delta on MinIO" intent explicitly so the docs match the code.

#### 4.3 Tests

- `src/tests/feast/test_unit_feast_feature_views.py`: update expected view names (`transaction_features_view`), assert `isinstance(source, ClickhouseSource)`, assert query contains the right `ml_features.*` table, keep TTL/online-flag/feature-name assertions.
- `src/tests/feast/test_unit_feast_entities.py`: unchanged.
- `src/tests/feast/test_unit_feast_materialize.py`: the broken-script unit tests are deleted with the script. Replace with a unit test that asserts `transaction_features_view.online is False` is excluded from a materialize call (mock `FeatureStore.materialize`), if feasible without a live ClickHouse.

#### 4.4 Airflow

If an Airflow DAG task invokes `materialize_to_redis.py`, repoint it to `feast materialize-incremental` (or a thin wrapper that calls `store.materialize`). Check `src/orchestration/` for the reference and update the import/command.

---

## Data flow (target)

1. dbt (Airflow DbtTaskGroup) builds Silver staging views (unchanged) then materializes the three `ml_features.*` marts in ClickHouse.
2. `feast apply` registers entities + three `ClickhouseSource`-backed FeatureViews against the ClickHouse offline store.
3. Online: `store.materialize` → ClickHouse offline store reads latest customer/terminal feature rows → writes to Redis. Transaction view skipped (offline-only).
4. Offline training: `store.get_historical_features` with an entity DataFrame of transaction ids + timestamps → point-in-time join across the three ClickHouse sources → training DataFrame → MLflow.

## Error handling

- dbt incremental `unique_key` collisions: rely on ClickHouse `ReplacingMergeTree` or `MergeTree` + dbt incremental merge semantics (existing pattern). Verify `unique_key` multi-column support for the two window marts.
- Feast `materialize` on an empty `ml_features.*` table (fresh backfill window): ClickHouse store returns zero rows; log and continue, do not fail the Airflow task.
- `get_historical_features` when a transaction's `customer_id`/`terminal_id` has no earlier feature row: Feast returns nulls for those feature columns; downstream training handles via imputation (existing behavior preserved from the flat-mart `COALESCE(...,0.0)`).

## Testing strategy

- dbt: `dbt run -s mart_customer_window_features mart_terminal_window_features mart_transaction_features` then `dbt test` against the three models — verify row counts and the `unique_combination` constraints.
- Feast unit tests: `pytest src/tests/feast/` — view/entity/metadata assertions without a live cluster.
- Feast e2e (manual, minimal Docker): start only `clickhouse` + `redis` services; run dbt to populate `ml_features.*`; `feast apply`; `feast materialize-incremental` and confirm Redis keys appear; `get_historical_features` on a small entity DataFrame and confirm the joined output. Do not start the full stack.

## Out of scope

- Flink streaming feature computation (§10 of the architecture doc) — future work; this spec is the batch/dbt path.
- Serving path (Traefik → KServe → Triton transformer fetching Redis) — separate spec.
- Training pipeline DAG beyond the materialize task repoint.
- Bronze/Silver schema changes.
