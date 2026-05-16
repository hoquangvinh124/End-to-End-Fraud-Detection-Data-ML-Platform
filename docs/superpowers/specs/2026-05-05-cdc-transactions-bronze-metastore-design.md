# CDC transactions bronze metastore registration

## Problem

`src/cdc_ingestion/cdc_transactions_to_bronze.py` currently writes Delta files to the MinIO bronze path only. The job does not register a Hive metastore table, so downstream Spark/SQL tools cannot query the bronze data through a catalog name. The bronze table also needs Change Data Feed enabled for incremental readers.

## Proposed approach

Keep the existing path-based streaming write to `s3a://bronze/cdc/transactions`, but make the job responsible for its own catalog setup:

1. Build the Spark session with Hive support and the Hive metastore URI configured in the script.
2. Ensure the `banking` database exists.
3. Create `banking.transactions` as an external Delta table pointing at the bronze path.
4. Enable `delta.enableChangeDataFeed = true` on that table.
5. Leave the streaming write target as the MinIO path so the current data layout and checkpointing stay unchanged.

## Implementation notes

- Do not change the shared `src/cdc_ingestion/spark-defaults.conf`.
- Keep the change isolated to `cdc_transactions_to_bronze.py`.
- Reuse the same external table name used by the Silver job: `banking.transactions`.
- If the table already exists, the job should leave it intact.

## TODOs

1. Update the Spark session builder in `cdc_transactions_to_bronze.py` to enable Hive support and point at the Hive metastore.
2. Add a helper that creates the `banking` database and external Delta table with CDF enabled.
3. Wire the helper into `main()` before the streaming query starts.
4. Verify the change with the repo's relevant lint/test commands.

## Notes

- This keeps the Bronze ingest contract stable for existing consumers while exposing the table through Hive metastore for SQL access.
- CDF must be enabled at table creation time so downstream incremental readers can consume Bronze changes without rewriting the storage layout.
