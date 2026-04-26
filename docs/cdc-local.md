# Local CDC Stack — Usage Guide

This document explains how to run the Kafka/Debezium CDC pipeline locally and observe
change-data-capture events flowing from the PostgreSQL OLTP source into the `cdc.transactions`
Kafka topic.

## Stack components

| Service | Image | Port |
|---|---|---|
| Kafka (KRaft) | `apache/kafka:3.8.0` | `9092` |
| Kafka Connect / Debezium | `quay.io/debezium/connect:3.0` | `8083` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | `8080` |
| PostgreSQL OLTP | defined in `docker-compose.oltp.yml` | `5432` |

---

## Prerequisites

1. **Docker Desktop** running (Engine ≥ 20.10).
2. **Copy `.env.example` to `.env`** and adjust values if needed (defaults work for local dev):
   ```powershell
   Copy-Item .env.example .env
   ```
3. **Seed data loaded** — the OLTP database must be initialised with the banking schema and
   transaction rows before starting the CDC stack.  Follow the steps in
   [`docs/architecture.md`](architecture.md) or run the seed script directly:
   ```powershell
   docker compose -f docker-compose.oltp.yml up -d
   # wait for postgres to become healthy, then seed:
   docker exec fraud-oltp-postgres psql -U postgres -d fraud_bank -f /docker-entrypoint-initdb.d/01_schema.sql
   ```

---

## Start the full stack

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml up -d
```

The `connector-init` service automatically calls
`scripts/cdc/register-transactions-connector.sh` once Kafka Connect is healthy.
Allow ~30–60 seconds for all health-checks to pass and the connector to register.

> **Port conflict warning:** `docker-compose.observability.yml` also binds port **8080**
> (Grafana).  Do **not** run the observability stack and the CDC stack simultaneously —
> they will conflict on port 8080.  Stop one before starting the other:
> ```powershell
> docker compose -f docker-compose.observability.yml down
> ```

---

## Check connector status

```powershell
Invoke-RestMethod http://localhost:8083/connectors/transactions-cdc-connector/status |
    ConvertTo-Json -Depth 5
```

A healthy connector shows `"state": "RUNNING"` for both the connector and its task:

```json
{
  "name": "transactions-cdc-connector",
  "connector": { "state": "RUNNING", "worker_id": "..." },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "..." }]
}
```

List all registered connectors:

```powershell
Invoke-RestMethod http://localhost:8083/connectors
```

---

## Inspect the topic in Kafka UI

Open **<http://localhost:8080>** in a browser.

- Select the **local** cluster → **Topics** → **`cdc.transactions`**.
- The **Messages** tab shows each captured row change (INSERT / UPDATE / DELETE) as a
  JSON envelope produced by Debezium.

---

## Consume messages from the command line

The image is `apache/kafka:3.8.0`, so the Kafka scripts live at `/opt/kafka/bin/`.

```powershell
docker exec -it fraud-cdc-kafka `
    /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic cdc.transactions `
    --from-beginning
```

Press **Ctrl+C** to stop the consumer.

To see only the last *N* messages (e.g. 10), add `--max-messages 10`.

---

## Trigger a new CDC event

Insert a row into the source database while the consumer is running to see the event arrive
in real time:

```powershell
docker exec fraud-oltp-postgres psql -U postgres -d fraud_bank -c `
    "INSERT INTO banking.transactions (id, account_id, amount, transaction_type, created_at) `
     VALUES (gen_random_uuid(), 'acc-demo', 99.99, 'purchase', now());"
```

---

## Stop the stack

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml down
```

Add `-v` to also remove named volumes (this resets Kafka offsets and the Postgres WAL slot):

```powershell
docker compose -f docker-compose.oltp.yml -f docker-compose.cdc.yml down -v
```
