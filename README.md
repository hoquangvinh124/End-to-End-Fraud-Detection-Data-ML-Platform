# End-to-End Banking Fraud Detection MLOps Platform

[![CI](https://github.com/hoquangvinh124/MLOps/actions/workflows/ci.yml/badge.svg)](https://github.com/hoquangvinh124/MLOps/actions/workflows/ci.yml)
[![Build](https://github.com/hoquangvinh124/MLOps/actions/workflows/build.yml/badge.svg)](https://github.com/hoquangvinh124/MLOps/actions/workflows/build.yml)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

A local-first MLOps platform that moves synthetic banking transactions through CDC and lakehouse layers, produces fraud features, trains and registers an ONNX model, and serves online predictions through an observable API.

The repository demonstrates two connected engineering systems:

- **Banking lakehouse:** PostgreSQL OLTP -> Debezium/Kafka -> Bronze/Silver Delta Lake -> dbt/Trino -> ClickHouse Gold.
- **ML lifecycle:** ClickHouse offline features -> Feast/Redis online features -> MLflow Registry -> ONNX Runtime/FastAPI -> Airflow and observability.

## Table of Contents

- [What This Project Demonstrates](#what-this-project-demonstrates)
- [Verified Demo Snapshot](#verified-demo-snapshot)
- [High-Level System Architecture](#high-level-system-architecture)
- [Repository Structure](#repository-structure)
- [Installation and Running](#installation-and-running)
- [Service Endpoints](#service-endpoints)
- [Inference API](#inference-api)
- [Automated Verification](#automated-verification)
- [Demo Video](#demo-video)
- [CI and Container Delivery](#ci-and-container-delivery)
- [Current Scope and Roadmap](#current-scope-and-roadmap)
- [Configuration and Security](#configuration-and-security)
- [License](#license)

## What This Project Demonstrates

- Change data capture from a normalized PostgreSQL banking workload using Debezium, Kafka, and Schema Registry.
- Bronze ingestion and idempotent Silver normalization with Spark, Delta Lake, MinIO, and Hive Metastore.
- Tested feature transformations with dbt and Trino, materialized as a ClickHouse Gold feature mart.
- ClickHouse Gold as the offline training source, with Feast definitions and Redis materialization for low-latency customer and terminal features.
- Model experiment tracking, ONNX artifact logging, Registry versioning, and promotion through the MLflow `champion` alias.
- FastAPI inference that loads the promoted model and combines request-time fields with online features.
- Daily orchestration in Airflow, plus metrics, logs, and traces through Prometheus, Grafana, Loki, OpenTelemetry, and Jaeger.
- GitHub Actions quality gates and container delivery to GitHub Container Registry.

## Verified Demo Snapshot

The values below come from the last successful local end-to-end run on **2026-07-11**. They are reproducible evidence from that run, not fixed dataset guarantees.

| Check | Last verified result |
| --- | ---: |
| PostgreSQL OLTP transactions | 4,477,440 rows through 2026-07-11 |
| ClickHouse Gold feature mart | 36,515 rows through 2026-07-11 |
| Gold transaction ID uniqueness | 36,515 / 36,515 unique |
| Redis online store | 10,275 keys |
| Promoted MLflow model | `fraud-detection:champion` -> version 2 |
| dbt validation | 52 / 52 checks passed |
| Python test suite | 89 tests passed |
| API coverage | 87.98% (CI gate: 80%) |
| Online prediction | Valid probability returned from `/predict-online` |

> **E2E result screenshot placeholder**
>
> Capture the PowerShell output from a successful `scripts/e2e_demo.ps1` run, including row counts, Redis keys, model version, probability, and `result: PASS`. Hide usernames, tokens, and unrelated terminal history. Save it as `docs/assets/e2e-result.png`, then replace this callout with `![Successful end-to-end verification](docs/assets/e2e-result.png)`.

## High-Level System Architecture

```text
PostgreSQL OLTP
      |
      | Debezium CDC
      v
Kafka + Schema Registry
      |
      | Spark ingestion
      v
MinIO Bronze (Delta) -> Spark normalize/merge -> MinIO Silver (Delta)
                                                   |
                                                   | Trino + dbt
                                                   v
                                      ClickHouse Gold feature mart
                                         |                     |
                             Feast materialization      Offline training
                                         |                     |
                                         v                     v
                                      Redis              MLflow Registry
                                         |                 champion alias
                                         +---------+-----------+
                                                   |
                                                   v
                                      FastAPI + ONNX Runtime
                                                   |
                                      metrics, logs, and traces
                                                   |
                                                   v
                              Prometheus / Grafana / Loki / Jaeger

Airflow schedules and monitors the batch, feature, and training lifecycle.
```

> **Architecture diagram placeholder**
>
> Draw only the implemented local architecture shown above. Include service boundaries, storage layers, orchestration, model serving, and observability; keep GKE, KServe, and Argo CD out because they are roadmap items. Save it as `docs/assets/architecture-overview.png`, then replace this callout with `![Implemented platform architecture](docs/assets/architecture-overview.png)`.

### Orchestration Flow

The `feature_pipeline_daily` Airflow DAG executes the lifecycle in dependency order:

```text
CDC ingestion -> Bronze -> Silver -> dbt staging -> dbt intermediate -> dbt marts
-> Feast materialization -> train, register, and promote model
```

> **Airflow screenshot placeholder**
>
> Capture the `feature_pipeline_daily` Graph view after a successful run. Show task groups, green task states, logical date, and total duration. Hide login details and URLs containing tokens. Save it as `docs/assets/airflow-dag.png`, then replace this callout with `![Successful Airflow feature and model pipeline](docs/assets/airflow-dag.png)`.

### Model Lifecycle

Training reads a bounded, time-ordered dataset from `gold.mart_fraud_ml_features`, logs metrics and artifacts to MLflow, exports an ONNX model, creates a Registry version, and moves the `champion` alias to the new version. The API resolves that alias on startup and prefers ONNX Runtime for inference.

> **MLflow screenshot placeholder**
>
> Capture the `fraud-detection` registered model page with the `champion` alias, model version, relevant metrics, and ONNX artifact visible. Hide storage credentials and signed URLs. Save it as `docs/assets/mlflow-registry.png`, then replace this callout with `![MLflow model promoted with the champion alias](docs/assets/mlflow-registry.png)`.

### Observability

OpenTelemetry instruments the inference API and exports telemetry to the local monitoring stack. Prometheus stores metrics, Grafana provides dashboards, Loki stores logs, and Jaeger exposes distributed traces.

> **Grafana screenshot placeholder**
>
> Capture a representative inference window showing request rate, latency, error rate, and model prediction metrics. Use a readable time range and hide local credentials. Save it as `docs/assets/grafana-dashboard.png`, then replace this callout with `![Fraud inference observability dashboard](docs/assets/grafana-dashboard.png)`.

## Repository Structure

```text
MLOps/
|-- .github/workflows/       # CI checks and GHCR container publishing
|-- data/                    # Local synthetic seed files (not shipped in images)
|-- docs/                    # Architecture, design notes, and future media assets
|-- models/                  # Local development model fallback
|-- postgres/                # PostgreSQL OLTP service and schema initialization
|-- scripts/                 # Operator-facing E2E verification
|-- src/
|   |-- api/                 # FastAPI, registry model loading, and inference
|   |-- batch_processing/    # Bronze-to-Silver Spark normalization
|   |-- cdc/                 # Kafka, Schema Registry, and Debezium Connect
|   |-- cdc_ingestion/       # Kafka CDC to Bronze ingestion jobs
|   |-- dbt/                 # Staging, intermediate, Gold models, and tests
|   |-- feature_store/       # Feast entities, feature views, and Redis materialization
|   |-- lakehouse/           # MinIO, Hive Metastore, Trino, and ClickHouse
|   |-- mlflow/              # MLflow tracking server and artifact configuration
|   |-- monitoring/          # OpenTelemetry, Prometheus, Grafana, Loki, and Jaeger
|   |-- orchestration/       # Airflow deployment and daily pipeline DAG
|   |-- scripts/             # Seed loading and current-date data generation
|   |-- tests/               # Unit, integration, contract, and orchestration tests
|   `-- training/            # Offline training, ONNX export, and registration
|-- docker-compose.yml       # Master local platform definition
|-- pyproject.toml           # Python dependencies and tool configuration
`-- README.md                # Project overview and operating guide
```

## Installation and Running

### Prerequisites

- Docker Desktop with Docker Compose v2 and Linux containers.
- PowerShell 7 for the E2E verifier.
- Python 3.11 and [`uv`](https://docs.astral.sh/uv/) for development and tests.
- Recommended for the complete stack: 8 CPU cores, 16 GB RAM, and at least 30 GB of free disk space.

### Quick Start

Create a local configuration file, validate Compose, and start the platform:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d
docker compose ps
```

The master Compose file includes the OLTP, CDC, lakehouse, feature store, MLflow, API, Airflow, batch processing, and observability sub-stacks. Initial builds and health checks can take several minutes.

Stop the stack while preserving data:

```powershell
docker compose down
```

Delete local volumes only when a clean rebuild is intentional:

```powershell
docker compose down --volumes
```

### Full Data and Model Rebuild

For an environment whose volumes already contain the verified dataset, start Compose and proceed directly to [Automated Verification](#automated-verification).

To rebuild from seed data:

1. Load the original synthetic seed files into PostgreSQL.
2. Extend the OLTP timeline through the required snapshot date.
3. Open Airflow, unpause `feature_pipeline_daily`, and trigger it for the target logical date.
4. Run the E2E verifier using the same date.

```powershell
uv sync --frozen --group dev
uv run python src/scripts/load_oltp_seed.py --data-dir data
uv run python src/scripts/generate_current_oltp_data.py `
  --end-date 2026-07-11 `
  --daily-scale 0.10
```

When `--start-date` is omitted, the generator reads the latest OLTP transaction date and resumes on the following day. Generation is deterministic and append-oriented by default. Use `--reset` only when intentionally replacing the OLTP dataset.

## Service Endpoints

| Service | URL | Purpose |
| --- | --- | --- |
| FastAPI | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive inference API |
| MLflow | [http://localhost:5000](http://localhost:5000) | Experiments, artifacts, and Model Registry |
| Airflow | [http://localhost:8089](http://localhost:8089) | Pipeline scheduling and task logs |
| Grafana | [http://localhost:3000](http://localhost:3000) | Operational dashboards |
| Prometheus | [http://localhost:9090](http://localhost:9090) | Metrics and target health |
| Jaeger | [http://localhost:16686](http://localhost:16686) | Distributed traces |
| Kafka UI | [http://localhost:8080](http://localhost:8080) | Topics, messages, and consumer state |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | Lakehouse objects and MLflow artifacts |
| Trino | [http://localhost:8090](http://localhost:8090) | SQL query endpoint |

Credentials are configured through `.env`; do not use the example defaults outside local development.

## Inference API

Confirm that the promoted Registry model is loaded:

```powershell
Invoke-RestMethod http://localhost:8000/health
```

Run online inference using request-time attributes and customer/terminal features retrieved from Feast and Redis:

```powershell
$body = @{
  customer_id = 1
  terminal_id = 42
  TX_AMOUNT = 150.50
  TX_DATETIME = "2026-07-11T12:30:00Z"
} | ConvertTo-Json

Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8000/predict-online `
  -ContentType "application/json" `
  -Body $body
```

A successful response contains `is_fraud`, `fraud_probability`, `model_version`, and `timestamp`. Use an entity present in the materialized feature date; the E2E script selects one automatically from Gold.

## Automated Verification

Run the same static checks and tests used by CI:

```powershell
uv sync --frozen --group dev
uv run ruff check .
uv run pytest --cov=api --cov-report=term-missing --cov-fail-under=80
```

Validate dbt while Trino, MinIO, Hive Metastore, and ClickHouse are running:

```powershell
Set-Location src/dbt
uv run dbt deps
uv run dbt build --profiles-dir .
Set-Location ../..
```

Verify PostgreSQL, ClickHouse, Redis, MLflow, the loaded model, and online inference together:

```powershell
.\scripts\e2e_demo.ps1 -ExpectedDate "2026-07-11"
```

## Demo Video

> **Demo video placeholder**
>
> Record a 3-5 minute walkthrough covering the architecture, successful Airflow DAG, MLflow `champion` model, online API prediction, Grafana dashboard, and final E2E output. Hide credentials and local notifications. Upload it to YouTube, optionally save a thumbnail as `docs/assets/demo-thumbnail.png`, then replace this callout with `[![Platform demo](docs/assets/demo-thumbnail.png)](YOUR_YOUTUBE_URL)`. If no custom thumbnail is used, link it as `[Watch the platform demo](YOUR_YOUTUBE_URL)`.

## CI and Container Delivery

GitHub Actions runs Ruff and pytest with an 80% API coverage gate for pull requests and protected branch pushes. After CI succeeds on an internal push to `main` or `vinh-branch`, the Build workflow publishes the API image to `ghcr.io/hoquangvinh124/mlops` with a commit-SHA tag.

This is continuous delivery to a container registry, not automatic deployment to a runtime environment. The complete platform is currently deployed locally through Docker Compose.

## Current Scope and Roadmap

| Implemented locally | Planned production evolution |
| --- | --- |
| Docker Compose deployment | Kubernetes/GKE deployment |
| FastAPI with ONNX Runtime | KServe-managed model serving |
| GitHub Actions CI and GHCR publishing | Argo CD GitOps promotion and rollback |
| Airflow `LocalExecutor` | Managed or Kubernetes-native orchestration |
| MinIO S3-compatible object storage | Cloud object storage and managed metadata services |

The planned column is intentionally not represented as completed work. This repository is a reproducible local platform and engineering demonstration, not a production banking system.

## Configuration and Security

- Copy `.env.example` to `.env` and replace all defaults before using a shared environment.
- Keep `.env`, webhooks, tokens, and cloud credentials out of Git history.
- The optional Discord failure webhook is read from `DISCORD_WEBHOOK_URL`; leaving it empty disables notifications.
- Restrict API CORS, add authentication, enable TLS, and move secrets to a secret manager before any non-local deployment.
- Use synthetic banking data only. No real customer or payment data is required.

## License

No open-source license has been declared yet. All rights are reserved by the repository owner unless a license is added.
