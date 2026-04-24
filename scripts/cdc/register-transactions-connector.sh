#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-transactions-cdc-connector}"
CONFIG_PATH="${CONFIG_PATH:-/config/transactions-connector.json}"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null; do
  echo "Waiting for Kafka Connect at $CONNECT_URL..."
  sleep 5
done

curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  --data @"$CONFIG_PATH" \
  "$CONNECT_URL/connectors/$CONNECTOR_NAME/config"

curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
