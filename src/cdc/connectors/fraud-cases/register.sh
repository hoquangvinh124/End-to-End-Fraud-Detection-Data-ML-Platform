#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-fraud-cases-cdc-connector}"
CONFIG_PATH="${CONFIG_PATH:-/config/fraud-cases-connector.json}"

DB_HOST="${DB_HOST:-postgres-oltp}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-fraud_bank}"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null; do
  echo "Waiting for Kafka Connect at $CONNECT_URL..."
  sleep 5
done

sed \
  -e "s|\${DB_HOST}|${DB_HOST}|g" \
  -e "s|\${DB_PORT}|${DB_PORT}|g" \
  -e "s|\${DB_USER}|${DB_USER}|g" \
  -e "s|\${DB_PASSWORD}|${DB_PASSWORD}|g" \
  -e "s|\${DB_NAME}|${DB_NAME}|g" \
  "$CONFIG_PATH" \
| curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  --data @- \
  "$CONNECT_URL/connectors/$CONNECTOR_NAME/config"

curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
