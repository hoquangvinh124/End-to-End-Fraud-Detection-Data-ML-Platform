#!/usr/bin/env bash
# Registers Bronze Delta tables in the Hive Metastore via Trino.
# Safe to run multiple times (unregister errors on first run are ignored).
#
# Usage:
#   ./register_bronze_tables.sh
#
# Prerequisites: Trino container must be running (trinodb/trino:481).

set -e

TRINO_CONTAINER="${TRINO_CONTAINER:-trino}"

echo "Unregistering existing Bronze table entries (errors on first run are expected and ignored)..."
docker exec -i "$TRINO_CONTAINER" trino \
  --execute "CALL lakehouse.system.unregister_table(schema_name => 'bronze', table_name => 'transactions');" \
  2>/dev/null || true

docker exec -i "$TRINO_CONTAINER" trino \
  --execute "CALL lakehouse.system.unregister_table(schema_name => 'bronze', table_name => 'fraud_cases');" \
  2>/dev/null || true

echo "Registering Bronze tables..."
docker exec -i "$TRINO_CONTAINER" trino < "$(dirname "$0")/register_bronze_tables.sql"

echo "Done. Bronze tables registered in lakehouse.bronze."
