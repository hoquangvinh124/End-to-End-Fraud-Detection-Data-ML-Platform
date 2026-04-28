#!/bin/sh
# Spark-submit wrapper for the transactions CDC Bronze streaming job.
# All parameters are configurable via environment variables.
set -eu

SPARK_HOME="${SPARK_HOME:-/opt/bitnami/spark}"

TOPIC="${BRONZE_TOPIC:-cdc.transactions}"
BOOTSTRAP_SERVERS="${BRONZE_BOOTSTRAP_SERVERS:-kafka:9092}"
OUTPUT_PATH="${BRONZE_OUTPUT_PATH:-s3a://bronze/cdc/transactions}"
CHECKPOINT_PATH="${BRONZE_CHECKPOINT_PATH:-s3a://bronze/_checkpoints/cdc_transactions_bronze}"
TRIGGER_INTERVAL="${BRONZE_TRIGGER_INTERVAL:-5 minutes}"

MINIO_ENDPOINT="${BRONZE_MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${BRONZE_MINIO_ACCESS_KEY:-minio}"
MINIO_SECRET_KEY="${BRONZE_MINIO_SECRET_KEY:-minio12345}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[*]" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  /opt/bronze/transactions_bronze_stream.py \
  --topic "${TOPIC}" \
  --bootstrap-servers "${BOOTSTRAP_SERVERS}" \
  --output-path "${OUTPUT_PATH}" \
  --checkpoint-path "${CHECKPOINT_PATH}" \
  --trigger-interval "${TRIGGER_INTERVAL}"
