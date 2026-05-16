# MLOps Fraud Detection Platform — Architecture Context

## Project
- **Domain**: Credit card fraud detection (financial)
- **Solo developer** academic project covering 3 rubrics (Basic MLOps, Serving Pipeline, Data Pipeline)
- **Target**: GKE deployment, Docker Compose for local dev

## Finalized Architecture

### CI/CD
- GitHub Actions CI: lint (runs `uv run ruff check .`) -> test (runs `uv run pytest --cov=api --cov-report=term-missing --cov-fail-under=80`)
- GitHub Actions Build: after CI succeeds on `vinh-branch` or `main`, build and publish `ghcr.io/hoquangvinh124/mlops` with a bare short SHA tag plus `latest` on `main` or `vinh-branch` on `vinh-branch`
- Future CD target: deployment flow remains separate from CI and image publishing
- ArgoCD: GitOps deploy to GKE

### Data Pipeline (blue section)
- **Kafka** (Strimzi): CDC simulator Python producer → topics
- **Flink**: stream processing with tumbling + sliding + session windows
- **Spark**: Bronze ingestion only (CDC → Delta Lake bronze layer on MinIO)
- **MinIO**: S3-compatible data lake
- **Delta Lake**: table format on MinIO (Bronze + Staging layers, ACID)
- **dbt + Trino** (dbt-trino 1.10.1, Trino 481): transforms Bronze → Staging (Delta Lake on MinIO via Trino lakehouse catalog) → Intermediate + Marts (ClickHouse via Trino clickhouse catalog)
- **Trino**: query engine over Delta Lake tables on MinIO (Bronze/Staging) AND ClickHouse tables (Intermediate/Marts)
- **ClickHouse** (head-distroless): Gold/serving layer storing intermediate + mart tables (MergeTree engine)
  - `intermediate.int_customer_window_features` — customer 1D/7D/30D window features
  - `intermediate.int_terminal_window_features` — terminal 1D/7D/30D window features with fraud delay offset
  - `marts.mart_fraud_ml_features` — flat ML feature table joining all features
- **Airflow**: orchestration with **astronomer-cosmos 1.14.1** DbtTaskGroups replacing Spark Silver/Gold batch jobs → `[bronze] → dbt_staging → dbt_intermediate → dbt_marts → materialize_online_features`
- **>100GB** data via high-throughput Kafka producer
- **DataHub**: central metadata store (PostgreSQL) for data catalog, schema registry, feature definitions

### Feature Store (green section)
- **Feast**: feature store (`src/feature_store/`)
  - Offline store: **file** (reads Gold Delta Parquet files from MinIO via S3 FileSource for training data from historical Gold)
  - Online store: **Redis** (<10ms lookup)
  - Three feature views: `fraud_ml_features_view` (offline, training), `customer_features_view` + `terminal_features_view` (online+offline)
  - Materialization: direct ClickHouse reads via **clickhouse-connect 0.15.1** → `write_to_online_store()` (bypasses offline store file-based reads)

### Training & Registry (green section)
- **MLflow**: experiment tracking + model registry
  - Backend: PostgreSQL
  - Artifacts: **GCS bucket**
- **Airflow DAG**: ingest → validate → feature_eng → train → evaluate → register
- Auto-deploy promoted model to KServe/Triton

### Model Serving (orange section)
- **FastAPI**: gateway (/health, /metrics, route to KServe for /predict)
- **KServe InferenceService**: main prediction path
  - Transformer: Feast online lookup (Redis) → build feature vector
  - Predictor: **Triton Inference Server** (ONNX runtime)
- XGBoost → ONNX export
- **Knative Eventing**: capture prediction CloudEvents → OTel Collector

### Observability
- **Prometheus**: metrics
- **Loki**: logs
- **Jaeger** (with OpenTelemetry): distributed tracing (rubric requires Jaeger specifically)
- **OTel Collector**: central telemetry pipeline
- **Grafana**: single pane of glass (queries Prometheus, Loki, Jaeger)
- OTel auto-instrumentation

### Storage (shared)
- PostgreSQL: 1 instance, multiple DBs (mlflow_db, airflow_db)
- Redis: standalone (Feast online store)
- MinIO: data lake (Delta Lake tables for Bronze/Staging, raw data)
- ClickHouse: Gold/serving layer (Intermediate + Marts tables, MergeTree engine)
- GCS: MLflow artifacts, model registry
