# MLOps Fraud Detection Platform — Architecture Context

## Project
- **Domain**: Credit card fraud detection (financial)
- **Solo developer** academic project covering 3 rubrics (Basic MLOps, Serving Pipeline, Data Pipeline)
- **Target**: GKE deployment, Docker Compose for local dev

## Finalized Architecture

### CI/CD
- GitHub Actions CI: lint (Ruff on `api` and `tests`) -> test (pytest with `--cov=api --cov-fail-under=80`)
- Future CD target: build image -> push Artifact Registry
- ArgoCD: GitOps deploy to GKE

### Data Pipeline (blue section)
- **Kafka** (Strimzi): CDC simulator Python producer → topics
- **Flink**: stream processing with tumbling + sliding + session windows
- **Spark**: batch processing, daily feature engineering
- **MinIO**: S3-compatible data lake
- **Apache Iceberg**: table format on MinIO (ACID)
- **Trino**: query engine over Iceberg tables on MinIO
- **Airflow**: orchestration of Spark/Flink jobs, data validation DAGs
- **>100GB** data via high-throughput Kafka producer
- **DataHub**: central metadata store (PostgreSQL) for data catalog, schema registry, feature definitions

### Feature Store (green section)
- **Feast**: feature store
  - Offline store: **PostgreSQL**
  - Online store: **Redis** (<10ms lookup)
- **Kafka → Feast push service → Redis**: real-time feature refresh

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
- PostgreSQL: 1 instance, multiple DBs (mlflow_db, airflow_db, feast_db)
- Redis: standalone (Feast online store)
- MinIO: data lake (Iceberg tables, raw data)
- GCS: MLflow artifacts, model registry
