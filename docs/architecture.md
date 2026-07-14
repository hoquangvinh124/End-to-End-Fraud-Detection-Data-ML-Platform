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
- **Kafka + Debezium**: PostgreSQL row-level CDC → Kafka topics
- **Flink**: stream processing with tumbling + sliding + session windows
- **Spark**: Bronze ingestion only (CDC → Delta Lake bronze layer on MinIO)
- **MinIO**: S3-compatible data lake
- **Delta Lake**: table format on MinIO (Bronze + Silver layers: Staging + Intermediate incremental merge, ACID)
- **dbt + Trino** (dbt-trino 1.10.1, Trino 481): transforms Bronze → **Silver** Staging + Intermediate (Delta Lake on MinIO via Trino lakehouse catalog, incremental merge) → **Gold** Marts (ClickHouse via Trino clickhouse catalog); dbt-codegen automates source/model YAML generation
- **Trino**: query engine over Delta Lake tables on MinIO (Bronze/Staging/Intermediate) AND ClickHouse tables (Marts); enables cross-catalog joins for mart assembly
- **ClickHouse** (head-distroless): Gold/serving layer storing mart tables only (MergeTree engine)
  - `marts.mart_fraud_ml_features` — flat ML feature table joining all features
- **Airflow**: bounded daily orchestration with a CDC freshness gate → Spark Bronze→Silver normalization → `dbt_staging → dbt_intermediate → dbt_marts → materialize_online_features`. CDC ingestion runs continuously as two Docker services and is not scheduled by Airflow.
- **>100GB** data via high-throughput Kafka producer

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
- **Traefik**: GKE ingress / edge router (TLS termination, host/path routing)
- **KServe InferenceService**: main prediction path behind Traefik
  - Transformer: Feast online lookup (Redis) → build feature vector
  - Predictor: **Triton Inference Server** (ONNX runtime)
- XGBoost → ONNX export
- **FastAPI**: current local/prototype API only, not part of the target GKE serving path
- **Knative Eventing**: capture prediction CloudEvents → OTel Collector

### Observability
- **Prometheus + Alertmanager**: pipeline SLIs, production-like freshness rules, grouped Discord notifications, and resolved notifications
- **Loki**: logs
- **Jaeger** (with OpenTelemetry): distributed tracing (rubric requires Jaeger specifically)
- **OTel Collector**: central telemetry pipeline for Airflow OTLP, Spark micro-batch metrics, the pipeline observer, and scraped infrastructure exporters
- **Grafana**: single pane of glass with the provisioned `Lakehouse Pipeline Overview` dashboard
- **Pipeline observer**: Kafka→Bronze delay plus Bronze/Silver/Gold/Redis health and watermarks; idle topics do not create false stale alerts

### Storage (shared)
- PostgreSQL: 1 instance, multiple DBs (mlflow_db, airflow_db)
- Redis: standalone (Feast online store)
- MinIO: data lake (Delta Lake tables for Bronze + Silver: Staging + Intermediate, raw data)
- ClickHouse: Gold/serving layer (Marts tables only, MergeTree engine)
- GCS: MLflow artifacts, model registry
