# CDC Bronze Spark Streaming Design

## Problem

The local CDC stack already captures `banking.transactions` changes from PostgreSQL into Kafka through Debezium, but the project still needs a clear offline landing path from Kafka into the Bronze layer.

The Bronze contract should not preserve the full Debezium envelope as the query surface. Instead, the Bronze landing should lightly unwrap the payload so business columns are flat and easy to consume, while CDC metadata remains available in `_`-prefixed fields such as `_op`.

## Chosen approach

Use Spark Structured Streaming as the Bronze landing job.

The end-to-end flow is:

1. PostgreSQL OLTP emits row changes through Debezium.
2. Kafka stores raw CDC messages in `cdc.transactions`.
3. A Spark Structured Streaming query consumes `cdc.transactions`.
4. Spark lightly unwraps the Debezium event into flat business fields plus `_`-prefixed metadata fields.
5. Spark writes the result to a Delta table in the MinIO Bronze bucket.

This replaces the earlier idea of a standalone Bronze Writer consumer. The Bronze landing logic is now owned by a single Spark micro-batch query rather than by a custom Kafka consumer service.

## Streaming model

The Bronze job should run in micro-batch mode rather than continuous processing.

Recommended trigger model:

- default trigger interval: every `5 minutes`
- development override: allow a shorter trigger interval for smoke tests without changing the production-like default

The `5 minute` trigger keeps the Bronze landing clearly in the offline or near-offline domain rather than pretending to be a low-latency serving path.

## Bronze contract

Each Bronze row represents one CDC event.

Business columns stay flat and close to the source transaction schema, for example:

- `transaction_id`
- `event_timestamp`
- `customer_id`
- `account_id`
- `card_id`
- `terminal_id`
- `amount`
- `currency_code`
- `transaction_type`
- `channel_type`
- `auth_status`
- `tx_time_seconds`
- `tx_time_days`
- `is_weekend`
- `is_night`
- `created_at`

CDC metadata is preserved in separate `_`-prefixed columns, for example:

- `_op`
- `_source_table`
- `_source_ts_ms`
- `_cdc_ts_ms`
- `_snapshot`
- `_lsn`
- `_deleted`
- `_ingested_at`

This keeps Bronze queryable without forcing every downstream consumer to re-parse the Debezium envelope.

## Event mapping rules

### Create, update, and snapshot-read events

- use the Debezium `after` payload as the source of business columns
- preserve `_op` exactly as emitted, such as `c`, `u`, or `r`
- set `_deleted = false`

### Delete events

- use the Debezium `before` payload to preserve the last known business state where available
- preserve `_op = d`
- set `_deleted = true`

### Snapshot events

- keep snapshot-read events in the same Bronze Delta table as streaming events
- preserve the snapshot flag in `_snapshot`
- do not split snapshot and streaming output into separate Bronze tables

## Delta table sink design

The Bronze sink should be a Delta table stored in MinIO, not an Iceberg table.

Recommended target paths:

- Bronze table path: `s3a://bronze/cdc/transactions`
- Structured Streaming checkpoint path: `s3a://bronze/_checkpoints/cdc_transactions_bronze`

The Bronze table should be append-only from the perspective of the stream. Updates and deletes from the source are represented as new Bronze event rows, not as in-place mutations to a business snapshot table.

## Spark and Delta requirements

The Spark session must be configured for Delta and S3-compatible object storage.

Required Delta behavior:

- write with `format("delta")`
- set an explicit `checkpointLocation`
- use `outputMode("append")`

Required MinIO or S3A behavior:

- set `fs.s3a.endpoint` to the MinIO service URL
- enable path-style access
- disable SSL for the local MinIO setup if served over HTTP
- provide explicit access key and secret key

Because this design has exactly one Spark writer per Bronze table in the local environment, no multi-cluster Delta log-store coordination is required in v1. If the project later introduces multiple concurrent Spark writers or multiple clusters against the same Delta table, dedicated log-store coordination becomes a separate design problem.

## Storage semantics

Delta on MinIO becomes the Bronze contract, so the important artifacts are:

- business data files in the table path
- the Delta transaction log under `_delta_log`
- Structured Streaming checkpoint state under the checkpoint path

This is materially different from the earlier Parquet-file landing design. The table is now queryable as a Delta table and carries Delta transaction-log semantics rather than being a directory of raw Parquet batches only.

## Reliability posture

The Bronze streaming job should rely on Structured Streaming plus Delta sink semantics for failure recovery.

Minimum requirements:

- explicit checkpoint location must always be set
- Kafka source offsets must be tracked through Spark checkpointing
- the job must be restartable without reprocessing the full topic from scratch in normal operation
- malformed payloads should be handled explicitly through quarantine or filtered error handling rather than silent drops

## Scope boundaries

This design does not include:

- Iceberg
- Silver transformations
- multi-topic CDC landing for `customers`, `accounts`, `cards`, `terminals`, or `fraud_cases`
- online serving or Redis refresh
- multi-cluster Delta coordination

Those remain later phases after the first Bronze landing contract is stable.

## Immediate next step

Implement a Spark Structured Streaming job that reads `cdc.transactions`, lightly unwraps the Debezium envelope into flat business columns plus `_`-prefixed metadata fields, and writes a Delta Bronze table into the MinIO Bronze bucket every `5 minutes`.