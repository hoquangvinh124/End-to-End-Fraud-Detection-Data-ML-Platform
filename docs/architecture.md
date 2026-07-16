# Banking Fraud Detection Platform Architecture

## Scope

This document describes the architecture implemented by the repository and the
local Docker Compose stack. Planned cloud components are listed separately and
must not be interpreted as completed work.

## Implemented Data Platform

### Source and CDC

- PostgreSQL stores the synthetic banking OLTP workload.
- Debezium captures row-level changes and publishes Avro events to Kafka.
- Confluent Schema Registry supplies the transaction and fraud-case schemas.
- Two Spark Structured Streaming services run continuously and write CDC
  micro-batches to the MinIO Bronze layer.

### Lakehouse Layers

- Bronze preserves append-only CDC history and source metadata.
- Spark normalizes Bronze events, validates required fields, quarantines invalid
  rows, deduplicates by PostgreSQL LSN, and applies update/delete-aware Delta
  Lake merges into Silver.
- Trino exposes the Delta Lake tables stored in MinIO.
- dbt builds staging, intermediate, and incremental feature models.
- ClickHouse stores the `gold.mart_fraud_ml_features` serving mart.

### Orchestration

CDC ingestion is a long-running service and is not scheduled by Airflow. The
daily `feature_pipeline_daily` DAG performs the bounded workflow:

```text
CDC freshness gate
  -> Bronze-to-Silver normalization
  -> dbt staging
  -> dbt intermediate
  -> dbt Gold mart
  -> Feast/Redis materialization
  -> train, export ONNX, and register in MLflow
```

Airflow uses task retries, execution timeouts, and OpenTelemetry metrics and
traces. The freshness sensor fails closed when either CDC dataset is unhealthy
or more than five minutes behind.

## Implemented ML Lifecycle

- ClickHouse Gold is the bounded offline training source.
- Feast defines customer and terminal features and materializes them to Redis.
- The training job fits a reproducible scikit-learn pipeline, logs parameters
  and evaluation metrics, exports an ONNX artifact, and registers a model
  version in MLflow.
- MLflow uses PostgreSQL for metadata and MinIO for artifacts.
- The Registry `champion` alias selects the version loaded by the API.
- FastAPI combines request-time fields with Feast/Redis online features and
  performs inference with ONNX Runtime.

The notebook LightGBM evaluation and the registered Logistic Regression model
are separate artifacts and should not be presented as the same model.

## Implemented Observability

- OpenTelemetry Collector receives API, Airflow, Spark, materialization, and
  pipeline-observer telemetry.
- Prometheus stores metrics and evaluates freshness, backlog, failure, and
  availability rules.
- Grafana provisions separate Fraud API and Lakehouse Pipeline dashboards.
- Alertmanager groups alerts and optionally delivers them to Discord using a
  webhook supplied through local configuration.
- Loki stores API logs and Jaeger stores distributed traces.
- Kafka, Redis, ClickHouse, MinIO, and HTTP health exporters provide supporting
  infrastructure signals.

## Delivery Boundary

GitHub Actions runs linting, tests, and an 80% API coverage gate. After a
successful internal branch build, it publishes a commit-addressed API image to
GitHub Container Registry. This is container delivery, not automatic deployment
to a runtime environment.

## Planned Evolution

The following components are roadmap items only:

- Kubernetes or GKE deployment.
- KServe-managed inference and autoscaling.
- Argo CD environment promotion and rollback.
- Managed object storage, metadata services, and secret management.
- Authenticated APIs, TLS, model approval gates, drift monitoring, and a
  ground-truth feedback loop.
