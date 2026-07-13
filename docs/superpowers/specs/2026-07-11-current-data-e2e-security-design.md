# Current Data E2E and Security Design

## Goal

Make the repository ready for a public GitHub portfolio by extending the existing banking data through 2026-07-11, proving the complete data and ML path end to end, and resolving validated security issues.

## Scope

- Preserve all existing historical seed and generated records.
- Append generated OLTP transactions from the day after the latest existing transaction through 2026-07-11.
- Generate 10% of one historical seed day's rows for each target day, using deterministic sampling and shifted timestamps.
- Propagate the new records through PostgreSQL, Debezium/Kafka, Bronze, Silver/dbt, ClickHouse Gold, Feast/Redis, MLflow/ONNX, and FastAPI online inference.
- Make the generation and demo workflow safe to rerun without duplicating an already populated date.
- Run repository tests, lint, Docker Compose validation, service health checks, and an end-to-end prediction.
- Run a repository-wide Codex Security scan, validate candidates, fix confirmed in-scope findings, and rerun relevant verification.
- Add public-facing documentation and a reproducible one-command demo path.

## Data Design

The OLTP generator will query the current maximum `banking.transactions.event_timestamp`. Its default resume date is the following calendar day. An explicit start date that overlaps existing generated dates will fail unless a deliberate replacement mode is introduced later; this design does not delete or replace existing records.

Each generated day deterministically samples 10% of a rotating historical pickle file. Transaction timestamps retain the source time of day while receiving the target date. Transaction identifiers continue from the current maximum. Fraud labels and delayed fraud-case visibility retain the existing scenario rules.

The generator must treat an empty requested range as a successful no-op, so rerunning it after reaching 2026-07-11 does not duplicate data.

## Pipeline Design

The local stack will be started in dependency order and checked for health. New OLTP inserts must be observed in Kafka CDC topics, persisted to Bronze, normalized into Silver, transformed by dbt into `gold.mart_fraud_ml_features`, materialized into Redis by Feast, and used for a new MLflow model version with the `champion` alias.

The final API container must load the registry-managed ONNX artifact and complete `/predict-online` using entity features read from Redis. Evidence will include row counts and maximum timestamps at OLTP and Gold, Redis key count, MLflow model version, service health, and the prediction response.

## Failure Handling

- No destructive reset or volume deletion is part of the workflow.
- Service startup and pipeline commands fail fast on non-zero status.
- Date-range validation rejects invalid or overlapping manual ranges.
- If an upstream layer does not advance, downstream model training is not reported as current.
- Existing unrelated working-tree changes are preserved.

## Security Design

The scan covers the checked-out repository. It follows threat modeling, finding discovery, validation, and attack-path analysis before reporting. Confirmed findings within the repository are fixed and retested.

The hard-coded Discord webhook in the Airflow DAG will be removed and replaced by an environment-only configuration with a safe disabled default. Because the credential has already appeared in source history, repository cleanup cannot revoke it; the owner must rotate or delete that webhook before making the repository public.

Secrets, generated coverage databases, runtime logs, local environment files, model artifacts, and data files will be checked against `.gitignore` and tracked-file state before GitHub readiness is claimed.

## Testing and Acceptance

Completion requires all of the following:

1. Generator unit tests prove resume-date calculation, deterministic 10% sampling, overlap rejection, and no-op reruns.
2. Existing unit and integration tests pass at the repository's configured coverage threshold.
3. Ruff and Docker Compose configuration validation pass.
4. OLTP and ClickHouse Gold both contain records dated 2026-07-11.
5. Redis contains materialized online features for the current feature date.
6. MLflow Registry contains a newly trained model version with alias `champion` and an ONNX artifact.
7. FastAPI health reports the registry model loaded and `/predict-online` returns a valid prediction.
8. Airflow imports the DAG without errors; observability endpoints are healthy.
9. The Codex Security scan report has explicit coverage and every candidate has a validated disposition.
10. README documents architecture, startup, E2E proof, security limitations, and portfolio-safe claims.

No promise of absolute defect absence is made; GitHub readiness means the documented automated checks and live E2E acceptance criteria pass with no known unresolved high-severity finding.
