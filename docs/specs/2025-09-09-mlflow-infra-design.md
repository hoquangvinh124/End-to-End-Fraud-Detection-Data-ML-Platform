# MLflow Infrastructure Design

**Goal:** Stand up a self-contained MLflow tracking server + model registry backed by a dedicated PostgreSQL and MinIO artifact store, wired into the existing Docker Compose stack.

**Architecture:** New `src/mlflow/` subsystem folder following the per-subsystem pattern already used for CDC, lakehouse, batch, orchestration, and monitoring. MLflow server keeps its own Postgres for tracking metadata (runs, experiments, model versions) so the compose is self-explanatory. Artifacts land in a new MinIO bucket `mlops-artifacts` that is already reachable inside the Docker network.

**Tech stack:** `ghcr.io/mlflow/mlflow:v2.22.0`, `postgres:16`, MinIO (existing `lakehouse-minio`), `mlflow` Python SDK.

**Scope of this spec:** Infrastructure only — Docker Compose, bucket creation, dependency wiring, and a throwaway smoke-test script. Training pipeline and API registry integration are separate specs.

---

## Components

### 1. `src/mlflow/docker-compose.mlflow.yml`

Two services:

| Service | Image | Role |
|---|---|---|
| `mlflow-postgres` | `postgres:16` | Tracks experiments, runs, params, metrics, model versions. Internal only (no host port). |
| `mlflow-server` | `ghcr.io/mlflow/mlflow:v2.22.0` | Tracking server + model registry UI at `localhost:5000`. |

`mlflow-server` startup command:
```
mlflow server
  --backend-store-uri postgresql://mlflow:mlflow@mlflow-postgres/mlflow
  --artifacts-destination s3://mlops-artifacts/mlflow
  --host 0.0.0.0
  --port 5000
```

`mlflow-server` environment variables:
```
MLFLOW_S3_ENDPOINT_URL=http://minio:9000
AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER:-minio}
AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD:-minio12345}
```

`mlflow-server` depends on:
- `mlflow-postgres` (service_healthy)
- `minio` (service_healthy) — from lakehouse compose via root include

### 2. `src/lakehouse/init/create-buckets.sh`

Add `mlops-artifacts` to the existing bucket loop:
```sh
for bucket in bronze silver gold mlops-artifacts; do
```

### 3. `docker-compose.yml` (root)

Add to the `include:` list:
```yaml
- src/mlflow/docker-compose.mlflow.yml
```

### 4. `pyproject.toml`

Add to `[dependency-groups] dev`:
```
mlflow>=2.22.0,<3
```

### 5. `scripts/smoke_test_mlflow.py` *(temporary — delete after training pipeline)*

Connects to `http://localhost:5000`, logs a dummy experiment with metrics and a `LogisticRegression` model, verifies the model appears in the registry, then cleans up. If it exits with `✓ Cleaned up`, the full stack is working end-to-end.

---

## Data Flow

```
MLflow Python SDK (any script / notebook)
    │
    ├── tracking calls (log_params, log_metrics, log_model)
    │       ↓
    │   mlflow-server:5000
    │       ├── metadata → mlflow-postgres:5432/mlflow
    │       └── artifact blobs → MinIO s3://mlops-artifacts/mlflow/
    │
    └── registry calls (register_model, transition_model_version_stage)
            ↓
        mlflow-server:5000
            └── model version records → mlflow-postgres:5432/mlflow
```

Environment variables downstream consumers (e.g. training scripts, API) will need:
```
MLFLOW_TRACKING_URI=http://localhost:5000   # or http://mlflow-server:5000 inside Docker
MLFLOW_S3_ENDPOINT_URL=http://minio:9000    # only when writing artifacts directly
```

---

## Error Handling

| Failure | Behaviour |
|---|---|
| `mlflow-postgres` not ready | `mlflow-server` `depends_on` healthcheck prevents premature start |
| MinIO not ready | `mlflow-server` `depends_on` `minio` healthcheck |
| `mlops-artifacts` bucket missing | `create-buckets.sh` idempotent (`--ignore-existing`); bucket is created before server starts |
| S3 credential mismatch | Server logs `NoCredentialsError`; fix via env vars |

---

## Testing

**Smoke test (manual, one-time):**
```bash
# 1. Start the stack (lakehouse must be up for MinIO)
docker compose -f src/lakehouse/docker-compose.lakehouse.yml \
               -f src/mlflow/docker-compose.mlflow.yml up -d

# 2. Wait for MLflow UI
curl --retry 10 --retry-delay 3 http://localhost:5000/health

# 3. Run smoke test
uv run python scripts/smoke_test_mlflow.py
# Expected output:
# ✓ Tracking OK | ✓ Registry OK (version 1)
# ✓ Cleaned up. Delete this file after training pipeline is ready.

# 4. Delete smoke test
del scripts\smoke_test_mlflow.py
```

**No automated tests for infra** — Docker Compose services are not unit-testable. Smoke test above is the acceptance gate.

---

## File Summary

| Path | Action |
|---|---|
| `src/mlflow/docker-compose.mlflow.yml` | CREATE |
| `src/lakehouse/init/create-buckets.sh` | MODIFY — add `mlops-artifacts` |
| `docker-compose.yml` | MODIFY — add include |
| `scripts/smoke_test_mlflow.py` | CREATE (temporary) |
| `pyproject.toml` | MODIFY — add `mlflow>=2.22.0,<3` to dev group |

---

## Out of Scope (separate specs)

- Training pipeline Airflow DAG
- API model loading from registry (replace local `.pkl` load)
- Feast offline store alignment for training data
- MLflow model promotion workflow (auto vs manual staging)
