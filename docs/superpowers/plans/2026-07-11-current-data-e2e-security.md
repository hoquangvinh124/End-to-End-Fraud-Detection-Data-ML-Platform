# Current Data E2E and Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Append deterministic 10%-scale daily banking data through 2026-07-11, propagate it through the complete MLOps stack, and make the repository demonstrably ready for public GitHub publication.

**Architecture:** Extend the existing OLTP generator with database-aware resume and overlap protection, then use the current Docker Compose services and Airflow/dbt/Feast/MLflow components for propagation. Verification records timestamps and counts at every persistent boundary. Security work follows the Codex Security repository-scan workflow and fixes only validated findings.

**Tech Stack:** Python 3.11, pandas, psycopg, PostgreSQL, Debezium, Kafka, Spark, MinIO/Delta, Trino, dbt, ClickHouse, Feast, Redis, MLflow, ONNX Runtime, FastAPI, Docker Compose, pytest, Ruff.

## Global Constraints

- Preserve all existing historical seed and generated records.
- Append from the day after the latest OLTP transaction through 2026-07-11 at `daily_scale=0.10`.
- Rerunning after the end date must be a successful no-op and must not duplicate rows.
- Do not delete Docker volumes or reset databases.
- Preserve unrelated working-tree changes.
- Do not publish or retain credentials in tracked source.
- Completion requires live OLTP-to-online-inference evidence and no known unresolved high-severity security finding.

---

### Task 1: Make Current-Data Generation Resumable

**Files:**
- Modify: `src/scripts/generate_current_oltp_data.py`
- Create: `src/tests/scripts/test_generate_current_oltp_data.py`

**Interfaces:**
- Consumes: PostgreSQL `banking.transactions.event_timestamp`.
- Produces: `fetch_latest_transaction_date(connection) -> date | None` and `resolve_generation_range(latest_date, start_date, end_date, days) -> tuple[date, date] | None`.

- [ ] Write tests proving an empty database uses the requested/default range, an existing database resumes on the next date, an overlapping explicit range raises `ValueError`, and a database already current returns `None`.
- [ ] Run `uv run pytest src/tests/scripts/test_generate_current_oltp_data.py -v` and confirm the new tests fail because the interfaces do not exist.
- [ ] Implement the two interfaces, change `--daily-scale` default to `0.10`, and make `main()` connect before resolving the database-aware target range.
- [ ] Print a clear no-op result and exit successfully when the latest transaction is already on or after the requested end date.
- [ ] Run the focused tests and Ruff on both files; expect all to pass.

### Task 2: Remove the Exposed Notification Secret

**Files:**
- Modify: `src/orchestration/dags/feature_pipeline_daily.py`
- Modify: `src/orchestration/docker-compose.airflow.yml`
- Create or modify: `src/tests/orchestration/test_feature_pipeline_daily.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: optional `DISCORD_WEBHOOK_URL` environment variable.
- Produces: a failure callback that returns without network access when the variable is unset.

- [ ] Add a test that imports the DAG with no webhook configured and proves `_notify_discord_failure` performs no request.
- [ ] Run the focused test and confirm it fails against the hard-coded default.
- [ ] Replace the default webhook with an empty string, guard the callback, and pass the optional variable through Compose.
- [ ] Add a placeholder-only `DISCORD_WEBHOOK_URL=` entry to `.env.example`.
- [ ] Search tracked source for credential-like Discord webhook URLs and confirm none remain in the working tree.

### Task 3: Add a Reproducible E2E Verification Script

**Files:**
- Create: `scripts/e2e_demo.ps1`
- Create: `src/tests/scripts/test_e2e_demo_contract.py`

**Interfaces:**
- Consumes: the master Docker Compose project and local service ports.
- Produces: non-zero exit on any failed health/data/model/inference assertion and a concise final evidence table on success.

- [ ] Add a contract test requiring checks for Compose health, OLTP max timestamp, Gold max timestamp, Redis count, MLflow/API health, and `/predict-online`.
- [ ] Run the contract test and confirm failure because the script is absent.
- [ ] Implement the PowerShell script with strict error handling and JSON parsing rather than output substring matching.
- [ ] Run the contract test and PowerShell parser validation; expect success.

### Task 4: Start the Data Platform and Append Current Data

**Files:**
- Runtime state only; no destructive database changes.

**Interfaces:**
- Consumes: 183 historical pickle files and the existing OLTP database.
- Produces: append-only OLTP rows through 2026-07-11.

- [ ] Start PostgreSQL, Kafka, Schema Registry, Kafka Connect, connector initialization, MinIO, Hive Metastore, Trino, and ClickHouse in dependency order.
- [ ] Verify health and connector status before generating data.
- [ ] Query and record the existing OLTP count/date range.
- [ ] Run `generate_current_oltp_data.py --end-date 2026-07-11 --daily-scale 0.10`.
- [ ] Query and record the new OLTP counts/date range, then rerun the generator and prove it inserts zero duplicate rows.

### Task 5: Propagate Data to Gold

**Files:**
- Modify only pipeline code/config if a reproducible runtime defect is discovered, using a failing focused test before each behavioral fix.

**Interfaces:**
- Consumes: Debezium transaction and fraud-case CDC events.
- Produces: ClickHouse `gold.mart_fraud_ml_features` containing 2026-07-11 rows.

- [ ] Confirm both Debezium connectors are running and Kafka offsets advance.
- [ ] Run Bronze ingestion and Silver normalization jobs to bounded completion.
- [ ] Run dbt staging, intermediate, marts, and dbt tests.
- [ ] Query Gold count/date range and require `MAX(event_timestamp) = 2026-07-11`.
- [ ] If a stage fails, capture logs, identify root cause, add a focused regression test where practical, apply the minimal fix, and rerun from that stage.

### Task 6: Materialize, Retrain, and Infer

**Files:**
- Modify only runtime code/config when backed by a focused regression test.

**Interfaces:**
- Consumes: current ClickHouse Gold mart.
- Produces: current Redis features, a new MLflow model version aliased `champion`, and a valid online prediction from ONNX Runtime.

- [ ] Apply the Feast repository and materialize 2026-07-11 customer and terminal features.
- [ ] Record Redis key count and fetch a known current entity's features.
- [ ] Train from the latest Gold window, log artifacts/metrics, register the model, and move `champion` to the new version.
- [ ] Rebuild/restart FastAPI and verify health reports the registry model loaded.
- [ ] Call `/predict-online` with a current entity and require a probability in `[0, 1]`.

### Task 7: Repository-Wide Security Scan and Remediation

**Files:**
- Create: `.codex/security-scans/<scan-id>/...` scan artifacts as required by Codex Security.
- Modify: validated vulnerable source/configuration files only.

**Interfaces:**
- Consumes: the entire checked-out repository and its deployment configuration.
- Produces: threat model, coverage ledger, validated findings, attack paths, canonical scan contract, final report, and verified remediations.

- [ ] Run Codex Security capability preflight and resolve the repository-wide scan artifact paths.
- [ ] Complete threat modeling, finding discovery, validation, and attack-path analysis in sequence.
- [ ] Fix confirmed in-scope findings after adding focused regression checks where applicable.
- [ ] Rerun security checks and relevant tests; explicitly record suppressed, not-applicable, reportable, and deferred rows.
- [ ] Finalize the canonical report and confirm no unresolved high-severity finding remains.

### Task 8: GitHub Readiness and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `.gitignore`
- Modify: CI/workflow files only if verification exposes a defect.

**Interfaces:**
- Consumes: live E2E evidence and security scan results.
- Produces: a reproducible public portfolio repository with honest claims.

- [ ] Write README sections for architecture, quick start, one-command demo, E2E evidence, test commands, and the required webhook-rotation warning.
- [ ] Ensure `.env`, data, runtime logs, `.coverage`, ML artifacts, and scan-private artifacts are ignored or intentionally documented.
- [ ] Run `uv run ruff check .`, the complete pytest suite with configured coverage, and `docker compose config --quiet`.
- [ ] Run `scripts/e2e_demo.ps1` against the live stack.
- [ ] Check Airflow DAG import errors and Prometheus/Grafana/MLflow/API health.
- [ ] Inspect `git diff --check`, tracked secret candidates, and final `git status`; report any user-owned dirty artifacts without deleting them.
- [ ] Record exact counts, timestamps, model version, prediction response, test totals, security disposition, and any residual operational caveat.
