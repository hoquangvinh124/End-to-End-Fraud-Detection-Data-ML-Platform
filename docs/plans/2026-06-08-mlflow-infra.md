# MLflow Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a self-contained MLflow tracking server + model registry backed by a dedicated PostgreSQL and MinIO artifact store, wired into the existing Docker Compose stack.

**Architecture:** New `src/mlflow/` subsystem folder mirroring the pattern of `src/lakehouse/`, `src/monitoring/`, etc. `mlflow-server` gets its own `mlflow-postgres` (no shared backend confusion). Artifact blobs go into a new `mlops-artifacts` MinIO bucket. Server uses `--serve-artifacts` so downstream clients only need `MLFLOW_TRACKING_URI` — no S3 credentials required on the client side.

**Tech Stack:** `ghcr.io/mlflow/mlflow:v2.22.0`, `postgres:16`, MinIO (existing `lakehouse-minio`), `mlflow>=2.22.0` Python SDK, `uv`.

**Spec:** `docs/specs/2025-09-09-mlflow-infra-design.md`

---

## File Map

| Path | Action | Responsibility |
|---|---|---|
| `src/mlflow/docker-compose.mlflow.yml` | CREATE | MLflow tracking server + dedicated Postgres |
| `src/lakehouse/init/create-buckets.sh` | MODIFY | Add `mlops-artifacts` bucket to MinIO init |
| `docker-compose.yml` | MODIFY | Include mlflow compose in master stack |
| `pyproject.toml` | MODIFY | Add `mlflow>=2.22.0,<3` to `[dependency-groups] dev` |
| `scripts/smoke_test_mlflow.py` | CREATE (temp) | One-time verify script — delete after passing |

---

## Task 1: Add `mlops-artifacts` MinIO bucket

**Files:**
- Modify: `src/lakehouse/init/create-buckets.sh`

This script runs at `minio-init` container startup. It is idempotent (`--ignore-existing`). Adding the bucket here ensures it exists before `mlflow-server` starts, regardless of stack startup order.

- [ ] **Step 1: Edit the bucket loop**

Open `src/lakehouse/init/create-buckets.sh`. Replace:
```sh
for bucket in bronze silver gold; do
```
with:
```sh
for bucket in bronze silver gold mlops-artifacts; do
```

Full file after change:
```sh
#!/bin/sh
set -e

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

for bucket in bronze silver gold mlops-artifacts; do
    mc mb --ignore-existing "local/${bucket}"
    echo "Bucket '${bucket}' ready"
done
```

- [ ] **Step 2: Commit**

```bash
git add src/lakehouse/init/create-buckets.sh
git commit -m "feat(lakehouse): add mlops-artifacts bucket for MLflow artifacts

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 2: Create `src/mlflow/docker-compose.mlflow.yml`

**Files:**
- Create: `src/mlflow/docker-compose.mlflow.yml`

Two services: `mlflow-postgres` (internal, no host port, owns the MLflow database) and `mlflow-server` (exposes port 5000, serves tracking + registry + proxies artifacts to MinIO). `--serve-artifacts` means the server handles all S3 communication; clients only need `MLFLOW_TRACKING_URI`.

- [ ] **Step 1: Create the directory**

```bash
mkdir src\mlflow
```

- [ ] **Step 2: Create the compose file**

Create `src/mlflow/docker-compose.mlflow.yml` with this exact content:

```yaml
# MLflow tracking server + model registry.
#
# Usage (standalone — requires MinIO from lakehouse):
#   docker compose -f src/lakehouse/docker-compose.lakehouse.yml \
#                  -f src/mlflow/docker-compose.mlflow.yml up -d
#   open http://localhost:5000
#
# Usage (full stack via root compose):
#   docker compose up -d mlflow-server
#
# Artifact storage: MinIO s3://mlops-artifacts/mlflow/
#   Bucket is created by src/lakehouse/init/create-buckets.sh on first minio-init run.
#
# Backend metadata: mlflow-postgres (internal, port not exposed)
#   Database: mlflow / User: mlflow / Password: mlflow
#
# --serve-artifacts: server proxies artifact uploads/downloads to MinIO.
#   Downstream clients only need MLFLOW_TRACKING_URI=http://localhost:5000.

services:
  mlflow-postgres:
    image: postgres:16
    container_name: mlflow-postgres
    hostname: mlflow-postgres
    environment:
      POSTGRES_USER: mlflow
      POSTGRES_PASSWORD: mlflow
      POSTGRES_DB: mlflow
    volumes:
      - mlflow_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mlflow -d mlflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  mlflow-server:
    image: ghcr.io/mlflow/mlflow:v2.22.0
    container_name: mlflow-server
    hostname: mlflow-server
    ports:
      - "5000:5000"
    environment:
      MLFLOW_S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:-minio}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:-minio12345}
    command: >
      mlflow server
        --backend-store-uri postgresql://mlflow:mlflow@mlflow-postgres/mlflow
        --artifacts-destination s3://mlops-artifacts/mlflow
        --serve-artifacts
        --host 0.0.0.0
        --port 5000
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s
    depends_on:
      mlflow-postgres:
        condition: service_healthy
      minio:
        condition: service_healthy
    restart: unless-stopped

volumes:
  mlflow_postgres_data:
```

- [ ] **Step 3: Commit**

```bash
git add src/mlflow/docker-compose.mlflow.yml
git commit -m "feat(mlflow): add MLflow tracking server and model registry compose

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 3: Wire into root compose and add Python dependency

**Files:**
- Modify: `docker-compose.yml`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add mlflow include to root compose**

Open `docker-compose.yml`. Current `include:` block:
```yaml
include:
  - postgres/docker-compose.oltp.yml
  - src/cdc/docker-compose.cdc.yml
  - src/lakehouse/docker-compose.lakehouse.yml
  - src/monitoring/docker-compose.observability.yml
  - src/cdc_ingestion/docker-compose.cdc_ingestion.yml
```

Replace with (add mlflow line at end):
```yaml
include:
  - postgres/docker-compose.oltp.yml
  - src/cdc/docker-compose.cdc.yml
  - src/lakehouse/docker-compose.lakehouse.yml
  - src/monitoring/docker-compose.observability.yml
  - src/cdc_ingestion/docker-compose.cdc_ingestion.yml
  - src/mlflow/docker-compose.mlflow.yml
```

- [ ] **Step 2: Add `mlflow` to pyproject.toml dev group**

Open `pyproject.toml`. Current `[dependency-groups] dev` block ends with:
```toml
    "psycopg[binary]>=3.2.10",
]
```

Replace that closing section with:
```toml
    "psycopg[binary]>=3.2.10",
    "mlflow>=2.22.0,<3",
]
```

Full `[dependency-groups] dev` after change:
```toml
[dependency-groups]
dev = [
    "feast[redis]>=0.40",
    "ruff>=0.9.0",
    "catboost>=1.2.8",
    "delta-spark>=4.2.0",
    "httpx>=0.28.1",
    "lightgbm>=4.6.0",
    "matplotlib>=3.10.0",
    "optuna>=4.7.0",
    "pyspark>=4.1.1",
    "pytest>=9.0.2",
    "pytest-cov>=7.0.0",
    "pytest-mock>=3.15.1",
    "ydata-profiling[notebook]>=4.18.1",
    "psycopg[binary]>=3.2.10",
    "mlflow>=2.22.0,<3",
]
```

- [ ] **Step 3: Sync uv lockfile**

```bash
uv sync --group dev
```

Expected: lockfile updated with mlflow + its transitive deps. No errors.

- [ ] **Step 4: Run existing tests to confirm nothing broken**

```bash
uv run pytest src/tests/ -q
```

Expected: all tests pass (no mlflow import in existing tests).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml pyproject.toml uv.lock
git commit -m "feat(mlflow): wire MLflow compose into root stack, add mlflow SDK dep

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 4: Create smoke-test script

**Files:**
- Create: `scripts/smoke_test_mlflow.py` *(temporary — delete after training pipeline is ready)*

This script verifies the full stack: SDK → tracking server → Postgres (metadata) → MinIO (artifacts) → model registry. It logs a dummy experiment, registers a toy model, asserts it appears in the registry, then cleans up all created resources.

- [ ] **Step 1: Create `scripts/` directory if it doesn't exist**

```bash
if not exist scripts mkdir scripts
```

- [ ] **Step 2: Create the smoke test**

Create `scripts/smoke_test_mlflow.py`:

```python
"""
MLflow infrastructure smoke test.

Run:  uv run python scripts/smoke_test_mlflow.py
Requires: MLflow server running at http://localhost:5000
          (docker compose -f src/lakehouse/docker-compose.lakehouse.yml
                          -f src/mlflow/docker-compose.mlflow.yml up -d)

DELETE THIS FILE after the training pipeline is wired up and adds a real model.
"""

import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.linear_model import LogisticRegression

TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "smoke-test"
MODEL_NAME = "smoke-test-model"


def smoke_test() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Log a run with params, metrics, and a model artifact
    with mlflow.start_run() as run:
        mlflow.log_params({"n_features": 4, "solver": "lbfgs"})
        mlflow.log_metric("accuracy", 0.95)

        rng = np.random.default_rng(seed=42)
        X = rng.standard_normal((100, 4))
        y = (X[:, 0] > 0).astype(int)
        model = LogisticRegression(solver="lbfgs").fit(X, y)

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        run_id = run.info.run_id

    print(f"✓ Tracking OK  (run_id={run_id[:8]}…)")

    # Verify model appears in registry
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    assert len(versions) > 0, "Model was not registered!"
    print(f"✓ Registry OK  (version {versions[0].version})")

    # Cleanup — remove model and experiment so the registry stays clean
    client.delete_registered_model(MODEL_NAME)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    mlflow.delete_experiment(experiment.experiment_id)
    print("✓ Cleaned up.")
    print()
    print("Stack is healthy. Delete scripts/smoke_test_mlflow.py when done.")


if __name__ == "__main__":
    smoke_test()
```

- [ ] **Step 3: Commit**

```bash
git add scripts/smoke_test_mlflow.py
git commit -m "chore: add MLflow smoke test script (temporary)

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 5: Bring up the stack and verify end-to-end

No code changes — this task is acceptance testing and final cleanup.

- [ ] **Step 1: Start MinIO + MLflow**

```bash
docker compose -f src/lakehouse/docker-compose.lakehouse.yml -f src/mlflow/docker-compose.mlflow.yml up -d
```

Expected: 5 containers start — `lakehouse-minio`, `minio-client`, `metastore-db`, `hive-metastore`, `mlflow-postgres`, `mlflow-server`.

- [ ] **Step 2: Wait for MLflow server to be ready**

```bash
docker compose -f src/lakehouse/docker-compose.lakehouse.yml -f src/mlflow/docker-compose.mlflow.yml ps
```

Wait until `mlflow-server` status shows `healthy`. Typically 20–40 seconds.

Alternatively poll:
```bash
curl --retry 15 --retry-delay 3 --retry-connrefused http://localhost:5000/health
```

Expected response: `{"status": "OK"}`

- [ ] **Step 3: Run the smoke test**

```bash
uv run python scripts/smoke_test_mlflow.py
```

Expected output:
```
✓ Tracking OK  (run_id=XXXXXXXX…)
✓ Registry OK  (version 1)
✓ Cleaned up.

Stack is healthy. Delete scripts/smoke_test_mlflow.py when done.
```

If the script fails, check:
- `docker logs mlflow-server` — server-side errors (Postgres connection, S3 errors)
- `docker logs mlflow-postgres` — DB init errors
- Confirm `mlops-artifacts` bucket exists: open http://localhost:9001 (MinIO console, user: `minio`, pass: `minio12345`)

- [ ] **Step 4: Confirm UI in browser**

Open `http://localhost:5000`. You should see the MLflow UI with no experiments (they were cleaned up). This confirms the web UI is reachable.

- [ ] **Step 5: Delete the smoke test script**

```bash
git rm scripts/smoke_test_mlflow.py
```

- [ ] **Step 6: Final commit**

```bash
git commit -m "chore: remove MLflow smoke test after successful verification

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Done ✓

MLflow is up. What's next:
- **Training pipeline spec** — Airflow DAG that loads from Gold/Feast offline → trains XGBoost → logs to MLflow → registers model
- **API registry integration spec** — replace local `.pkl` load with `mlflow.pyfunc.load_model("models:/fraud-detection/Production")`
