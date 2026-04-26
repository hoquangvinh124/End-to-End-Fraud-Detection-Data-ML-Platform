# CDC Bronze Spark Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Spark Structured Streaming Bronze pipeline that consumes `cdc.transactions`, lightly unwraps Debezium payloads, and writes a Delta table into a MinIO Bronze bucket on a `5 minute` micro-batch trigger.

**Architecture:** Keep Debezium and Kafka Connect as the raw CDC producers, add MinIO as the Bronze object store, and run one Spark Structured Streaming query as the only writer for the Bronze Delta table. The stream should default to `5 minutes` per micro-batch, but allow a shorter trigger override for smoke tests.

**Tech Stack:** Spark Structured Streaming, Delta Lake, MinIO, Kafka, Debezium, Docker Compose, S3A, Ruff, pytest

---

## File map

- Modify: `docker-compose.cdc.yml` — add MinIO, bucket initialization, and the Spark Bronze streaming service.
- Create: `scripts/bronze/transactions_bronze_stream.py` — Spark job that reads Kafka, lightly unwraps Debezium events, and writes Delta to MinIO.
- Create: `scripts/bronze/start-transactions-bronze-stream.sh` — container entrypoint that submits the Spark job with Delta and S3A configuration.
- Create: `docs/bronze-local.md` — explain how to run the Bronze stream and inspect the Delta table in MinIO.
- Modify: `docs/cdc-local.md` — extend the CDC guide with the Bronze landing path and Delta sink behavior.

### Task 1: Extend the CDC stack with MinIO Bronze storage

**Files:**
- Modify: `docker-compose.cdc.yml`

- [ ] **Step 1: Capture the current compose baseline**

Run:

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml config > $env:TEMP\cdc-compose.before-bronze.yml
```

Expected: the rendered config succeeds and contains Kafka, Kafka Connect, Kafka UI, and connector init, but no MinIO service yet.

- [ ] **Step 2: Add a MinIO service for the Bronze bucket**

Extend `docker-compose.cdc.yml` with a service shaped like this:

```yaml
  minio:
    image: minio/minio:latest
    container_name: fraud-bronze-minio
    command: ["server", "/data", "--console-address", ":9001"]
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio12345
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
    volumes:
      - minio_data:/data
```

- [ ] **Step 3: Add a one-shot bucket initialization service**

Add a service shaped like this:

```yaml
  minio-init:
    image: minio/mc:latest
    container_name: fraud-bronze-minio-init
    entrypoint:
      [
        "/bin/sh",
        "-c",
        "mc alias set local http://minio:9000 minio minio12345 && mc mb --ignore-existing local/bronze"
      ]
    depends_on:
      minio:
        condition: service_healthy
    restart: "no"
```

- [ ] **Step 4: Add the new volume declaration**

Update the `volumes:` section to include:

```yaml
  minio_data:
```

- [ ] **Step 5: Re-render the compose stack**

Run:

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml config > $env:TEMP\cdc-compose.with-minio.yml
```

Expected: rendered config succeeds and includes both `minio` and `minio-init`.

### Task 2: Add the Spark Bronze streaming job

**Files:**
- Create: `scripts/bronze/transactions_bronze_stream.py`

- [ ] **Step 1: Create the Spark Bronze stream script**

Create `scripts/bronze/transactions_bronze_stream.py` with a structure like this:

```python
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F, types as T


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--trigger-interval", default="5 minutes")
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("transactions-bronze-stream").getOrCreate()
```

- [ ] **Step 2: Define the Debezium envelope schema**

Add an explicit schema for the parts used by the Bronze transform:

```python
TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.LongType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("account_id", T.StringType()),
        T.StructField("card_id", T.StringType()),
        T.StructField("terminal_id", T.StringType()),
        T.StructField("amount", T.StringType()),
        T.StructField("currency_code", T.StringType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("channel_type", T.StringType()),
        T.StructField("auth_status", T.StringType()),
        T.StructField("tx_time_seconds", T.IntegerType()),
        T.StructField("tx_time_days", T.IntegerType()),
        T.StructField("is_weekend", T.BooleanType()),
        T.StructField("is_night", T.BooleanType()),
        T.StructField("created_at", T.StringType()),
    ]
)

SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("schema", T.StringType()),
        T.StructField("table", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
        T.StructField("snapshot", T.StringType()),
        T.StructField("lsn", T.LongType()),
    ]
)

ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("before", TRANSACTION_SCHEMA),
        T.StructField("after", TRANSACTION_SCHEMA),
        T.StructField("op", T.StringType()),
        T.StructField("source", SOURCE_SCHEMA),
        T.StructField("ts_ms", T.LongType()),
    ]
)
```

- [ ] **Step 3: Implement the light-unwrapping transform**

Use a transform like this:

```python
def build_bronze_rows(raw_df):
    event_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("event")
    )

    payload = F.when(F.col("event.op") == F.lit("d"), F.col("event.before")).otherwise(
        F.col("event.after")
    )

    return event_df.select(
        payload.transaction_id.alias("transaction_id"),
        payload.event_timestamp.alias("event_timestamp"),
        payload.customer_id.alias("customer_id"),
        payload.account_id.alias("account_id"),
        payload.card_id.alias("card_id"),
        payload.terminal_id.alias("terminal_id"),
        payload.amount.alias("amount"),
        payload.currency_code.alias("currency_code"),
        payload.transaction_type.alias("transaction_type"),
        payload.channel_type.alias("channel_type"),
        payload.auth_status.alias("auth_status"),
        payload.tx_time_seconds.alias("tx_time_seconds"),
        payload.tx_time_days.alias("tx_time_days"),
        payload.is_weekend.alias("is_weekend"),
        payload.is_night.alias("is_night"),
        payload.created_at.alias("created_at"),
        F.col("event.op").alias("_op"),
        F.concat_ws(".", F.col("event.source.schema"), F.col("event.source.table")).alias("_source_table"),
        F.col("event.source.ts_ms").alias("_source_ts_ms"),
        F.col("event.ts_ms").alias("_cdc_ts_ms"),
        F.col("event.source.snapshot").alias("_snapshot"),
        F.col("event.source.lsn").alias("_lsn"),
        (F.col("event.op") == F.lit("d")).alias("_deleted"),
        F.current_timestamp().alias("_ingested_at"),
    )
```

- [ ] **Step 4: Implement the Delta streaming sink**

Finish the script with a sink shaped like this:

```python
def main() -> None:
    args = parse_args()
    spark = build_spark_session()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = build_bronze_rows(kafka_df)

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime=args.trigger_interval)
        .start(args.output_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

### Task 3: Add a Spark submit entrypoint for Delta and MinIO

**Files:**
- Create: `scripts/bronze/start-transactions-bronze-stream.sh`

- [ ] **Step 1: Create the Spark submit wrapper**

Create `scripts/bronze/start-transactions-bronze-stream.sh` with this content:

```sh
#!/bin/sh
set -eu

SPARK_HOME="${SPARK_HOME:-/opt/bitnami/spark}"
TOPIC="${BRONZE_TOPIC:-cdc.transactions}"
BOOTSTRAP_SERVERS="${BRONZE_BOOTSTRAP_SERVERS:-kafka:9092}"
OUTPUT_PATH="${BRONZE_OUTPUT_PATH:-s3a://bronze/cdc/transactions}"
CHECKPOINT_PATH="${BRONZE_CHECKPOINT_PATH:-s3a://bronze/_checkpoints/cdc_transactions_bronze}"
TRIGGER_INTERVAL="${BRONZE_TRIGGER_INTERVAL:-5 minutes}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /opt/bronze/transactions_bronze_stream.py \
  --topic "$TOPIC" \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --output-path "$OUTPUT_PATH" \
  --checkpoint-path "$CHECKPOINT_PATH" \
  --trigger-interval "$TRIGGER_INTERVAL"
```

- [ ] **Step 2: Verify the wrapper is shell-valid**

Run:

```powershell
& 'C:\Program Files\Git\bin\bash.exe' -n .worktrees/kafka-debezium-cdc/scripts/bronze/start-transactions-bronze-stream.sh
```

Expected: no syntax output and exit code `0`.

### Task 4: Wire the Spark Bronze stream into the compose stack

**Files:**
- Modify: `docker-compose.cdc.yml`

- [ ] **Step 1: Add the Spark streaming service**

Extend `docker-compose.cdc.yml` with a service shaped like this:

```yaml
  spark-bronze-stream:
    image: bitnami/spark:3.5.1
    container_name: fraud-bronze-spark
    entrypoint: ["/bin/sh", "/opt/bronze/start-transactions-bronze-stream.sh"]
    environment:
      BRONZE_TOPIC: cdc.transactions
      BRONZE_BOOTSTRAP_SERVERS: kafka:9092
      BRONZE_OUTPUT_PATH: s3a://bronze/cdc/transactions
      BRONZE_CHECKPOINT_PATH: s3a://bronze/_checkpoints/cdc_transactions_bronze
      BRONZE_TRIGGER_INTERVAL: 5 minutes
    volumes:
      - ./scripts/bronze:/opt/bronze:ro
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy
      minio-init:
        condition: service_completed_successfully
      connector-init:
        condition: service_completed_successfully
```

- [ ] **Step 2: Render the full stack**

Run:

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml config > $env:TEMP\cdc-compose.with-spark-bronze.yml
```

Expected: rendered config succeeds and includes `minio`, `minio-init`, and `spark-bronze-stream`.

### Task 5: Prove Delta Bronze landing to MinIO

**Files:**
- Use existing: `docker-compose.oltp.yml`
- Use existing: `docker-compose.cdc.yml`

- [ ] **Step 1: Start the stack with a short trigger for smoke testing**

Run:

```powershell
$env:BRONZE_TRIGGER_INTERVAL = '30 seconds'
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml up -d --build
```

Expected: Kafka, Connect, MinIO, and Spark Bronze stream all start without fatal errors.

- [ ] **Step 2: Insert one smoke-test transaction into the OLTP source**

Use the same transaction insert pattern already used for the CDC smoke test so Debezium emits one clear event into `cdc.transactions`.

- [ ] **Step 3: Verify Delta files were created in MinIO storage**

Run:

```powershell
docker exec fraud-bronze-minio sh -c "find /data/bronze -maxdepth 4 -type f | sort"
```

Expected:

- the table path contains at least one data file under `cdc/transactions`
- `_delta_log` contains JSON log files

- [ ] **Step 4: Verify the streaming checkpoint path exists**

Run:

```powershell
docker exec fraud-bronze-minio sh -c "find /data/bronze/_checkpoints -maxdepth 4 -type f | sort"
```

Expected: checkpoint files exist under `_checkpoints/cdc_transactions_bronze`.

- [ ] **Step 5: Restore the production-like trigger default**

Run:

```powershell
Remove-Item Env:BRONZE_TRIGGER_INTERVAL -ErrorAction SilentlyContinue
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml up -d
```

Expected: the stack returns to the default `5 minute` trigger behavior.

### Task 6: Document local usage and validate the repo

**Files:**
- Create: `docs/bronze-local.md`
- Modify: `docs/cdc-local.md`

- [ ] **Step 1: Create `docs/bronze-local.md`**

Document these items explicitly:

- the path from Debezium topic to Spark micro-batch to Delta table
- MinIO endpoints and Bronze bucket name
- the default `5 minute` trigger and the smoke-test override pattern
- the Delta table path and checkpoint path in MinIO
- the `_`-prefixed Bronze metadata fields

- [ ] **Step 2: Update `docs/cdc-local.md`**

Extend the CDC guide so it now explains the full local path:

- PostgreSQL -> Debezium -> Kafka topic `cdc.transactions` -> Spark Structured Streaming -> Delta table in MinIO Bronze bucket

- [ ] **Step 3: Run focused validation**

Run:

```powershell
uv run ruff check .worktrees/kafka-debezium-cdc/scripts .worktrees/kafka-debezium-cdc/docs
uv run pytest --cov=api --cov-report=term-missing --cov-fail-under=80
```

Expected:

- Ruff passes for the new Bronze stream scripts and docs-adjacent Python files
- the existing API test suite still passes

## Self-review

- **Spec alignment:** This plan implements the chosen Spark Structured Streaming + Delta + MinIO architecture rather than the earlier standalone Bronze Writer design.
- **Scope control:** The plan stays on Bronze landing for `cdc.transactions` only and does not pull Iceberg, Silver, or multi-topic CDC into the first slice.
- **Validation path:** The plan verifies both Delta table artifacts and checkpoint artifacts in MinIO, which matches the real runtime contract of Structured Streaming to Delta.