#!/bin/sh
# Minimal entrypoint: passes dynamic env-var values as --conf overrides.
# Static Spark properties live in spark-defaults.conf (copied into image).
set -eu

exec /opt/bitnami/spark/bin/spark-submit \
  --conf "spark.hadoop.fs.s3a.endpoint=${BRONZE_MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.access.key=${BRONZE_MINIO_ACCESS_KEY:-minio}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${BRONZE_MINIO_SECRET_KEY:-minio12345}" \
  /opt/bronze/cdc_transactions_to_bronze.py \
  --topic "${BRONZE_TOPIC:-cdc.transactions}" \
  --bootstrap-servers "${BRONZE_BOOTSTRAP_SERVERS:-kafka:9092}" \
  --output-path "${BRONZE_OUTPUT_PATH:-s3a://bronze/cdc/transactions}" \
  --checkpoint-path "${BRONZE_CHECKPOINT_PATH:-s3a://bronze/_checkpoints/cdc_transactions_bronze}" \
  --trigger-interval "${BRONZE_TRIGGER_INTERVAL:-5 minutes}"