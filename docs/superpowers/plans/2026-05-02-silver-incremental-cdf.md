# Silver Incremental CDF Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert both Silver batch jobs from Structured Streaming (checkpoint-based) to native Spark batch jobs that read only new Bronze data via Delta CDF and a watermark Delta table.

**Architecture:** Bronze streaming jobs change output from Parquet → Delta with CDF enabled via spark-defaults.conf default property. A shared `utils/watermark.py` persists `last_bronze_version` per job in a Delta table at `s3a://silver/_watermarks/`. Each Silver `main()` does: read watermark → batch CDF read (startingVersion = last+1, endingVersion = current) → cast/validate/quarantine/MERGE → write watermark.

**Tech Stack:** PySpark, Delta Lake (`delta.tables.DeltaTable`), MinIO (s3a), `uv run pytest` for tests.

---

## File Map

| Path | Action | Responsibility |
|---|---|---|
| `src/batch_processing/spark-defaults.conf` | Modify | Add `enableChangeDataFeed` default; add `spark.silver.*.watermark.path`; remove Silver checkpoint keys |
| `src/batch_processing/Dockerfile` | Modify | Add `ENV PYTHONPATH /opt` so Silver jobs can import `utils.*` at runtime |
| `pyproject.toml` | Modify | Add `"src/batch_processing"` to pytest `pythonpath` so `utils.*` resolves in tests |
| `src/batch_processing/utils/watermark.py` | **Create** | `read_watermark` / `write_watermark` shared util |
| `src/tests/batch_processing/__init__.py` | **Create** | Make directory a Python package |
| `src/tests/batch_processing/test_unit_watermark.py` | **Create** | Unit tests for watermark util (mock-based) |
| `src/tests/batch_processing/test_unit_silver_transactions_main.py` | **Create** | Unit tests for transactions `main()` logic |
| `src/batch_processing/bronze/cdc_transactions_to_bronze.py` | Modify | `format("parquet")` → `format("delta")` |
| `src/batch_processing/bronze/cdc_fraud_cases_to_bronze.py` | Modify | `format("parquet")` → `format("delta")` |
| `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` | Modify | Remove `BRONZE_SCHEMA`, `process_batch`, streaming code; rewrite `main()` with CDF batch read + watermark |
| `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py` | Modify | Same rewrite as transactions |

---

## Task 1: spark-defaults.conf, Dockerfile, pyproject.toml — Config groundwork

**Files:**
- Modify: `src/batch_processing/spark-defaults.conf`
- Modify: `src/batch_processing/Dockerfile`
- Modify: `pyproject.toml`

- [ ] **Step 1: Edit spark-defaults.conf**

  Replace the `# Silver batch job (spark.silver.*)` block. Full updated Silver + Gold section:

  ```
  # Delta Lake — enable Change Data Feed on all new Delta tables by default
  spark.databricks.delta.properties.defaults.enableChangeDataFeed  true

  # Silver transactions batch job (spark.silver.*)
  spark.silver.bronze.input.path                      s3a://bronze/cdc/transactions
  spark.silver.output.path                            s3a://silver/transactions
  spark.silver.quarantine.path                        s3a://silver/quarantine/transactions
  spark.silver.watermark.path                         s3a://silver/_watermarks/

  # Silver fraud_cases batch job (spark.silver.fraud_cases.*)
  spark.silver.fraud_cases.bronze.input.path          s3a://bronze/cdc/fraud_cases
  spark.silver.fraud_cases.output.path                s3a://silver/fraud_cases
  spark.silver.fraud_cases.quarantine.path            s3a://silver/quarantine/fraud_cases
  spark.silver.fraud_cases.watermark.path             s3a://silver/_watermarks/
  ```

  Remove these two lines (checkpoint paths no longer used by Silver):
  ```
  spark.silver.checkpoint.path                        s3a://silver/_checkpoints/transactions_silver
  spark.silver.fraud_cases.checkpoint.path            s3a://silver/_checkpoints/fraud_cases_silver
  ```

- [ ] **Step 2: Edit Dockerfile — add PYTHONPATH**

  The Dockerfile copies `utils/` to `/opt/utils/`. Silver jobs import
  `from utils.watermark import ...`, which requires `/opt` in `PYTHONPATH`.

  Add `ENV PYTHONPATH /opt` immediately before the final `USER spark` line:

  ```dockerfile
  # Add /opt to PYTHONPATH so Silver/Gold jobs can import utils.* at runtime.
  ENV PYTHONPATH /opt

  USER spark
  ```

- [ ] **Step 3: Edit pyproject.toml — extend pytest pythonpath**

  `pythonpath = ["src"]` lets tests import `batch_processing.*` but not `utils.*`
  (which resolves to `src/batch_processing/utils/` on disk). Add the package root:

  ```toml
  [tool.pytest.ini_options]
  pythonpath = ["src", "src/batch_processing"]
  testpaths = ["src/tests"]
  ```

- [ ] **Step 4: Commit**

  ```bash
  cd C:\MLOps\.worktrees\spark-bronze
  git add src/batch_processing/spark-defaults.conf \
          src/batch_processing/Dockerfile \
          pyproject.toml
  git commit -m "config: add Delta CDF default, watermark paths, fix PYTHONPATH for utils

  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

---

## Task 2: utils/watermark.py — Shared watermark util

**Files:**
- Create: `src/batch_processing/utils/watermark.py`
- Create: `src/tests/batch_processing/__init__.py`
- Create: `src/tests/batch_processing/test_unit_watermark.py`

- [ ] **Step 1: Write the failing tests**

  Create `src/tests/batch_processing/__init__.py` (empty file).

  Create `src/tests/batch_processing/test_unit_watermark.py`:

  ```python
  """Unit tests for batch_processing.utils.watermark.

  Uses unittest.mock throughout — no real SparkSession or Delta table needed.
  """
  from __future__ import annotations

  from unittest.mock import MagicMock, call, patch

  import pytest

  from batch_processing.utils.watermark import read_watermark, write_watermark


  # ---------------------------------------------------------------------------
  # read_watermark
  # ---------------------------------------------------------------------------


  class TestReadWatermark:
      def test_returns_none_when_no_delta_table(self):
          spark = MagicMock()
          with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
              mock_dt.isDeltaTable.return_value = False
              result = read_watermark(spark, "s3a://silver/_watermarks/", "silver-transactions")
          assert result is None
          mock_dt.isDeltaTable.assert_called_once_with(spark, "s3a://silver/_watermarks/")

      def test_returns_none_when_no_row_for_job_name(self):
          spark = MagicMock()
          (
              spark.read.format.return_value
              .load.return_value
              .filter.return_value
              .select.return_value
              .first.return_value
          ) = None
          with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
              mock_dt.isDeltaTable.return_value = True
              result = read_watermark(spark, "s3a://silver/_watermarks/", "silver-transactions")
          assert result is None

      def test_returns_version_int_when_row_exists(self):
          spark = MagicMock()
          mock_row = {"last_bronze_version": 42}
          (
              spark.read.format.return_value
              .load.return_value
              .filter.return_value
              .select.return_value
              .first.return_value
          ) = mock_row
          with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
              mock_dt.isDeltaTable.return_value = True
              result = read_watermark(spark, "s3a://silver/_watermarks/", "silver-transactions")
          assert result == 42
          assert isinstance(result, int)


  # ---------------------------------------------------------------------------
  # write_watermark
  # ---------------------------------------------------------------------------


  class TestWriteWatermark:
      def test_initial_write_when_no_table(self):
          spark = MagicMock()
          with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
              mock_dt.isDeltaTable.return_value = False
              write_watermark(spark, "s3a://silver/_watermarks/", "silver-transactions", 5)
          (
              spark.createDataFrame.return_value
              .write.format.return_value
              .save.assert_called_once_with("s3a://silver/_watermarks/")
          )

      def test_merge_when_table_exists(self):
          spark = MagicMock()
          mock_merge_builder = MagicMock()
          with patch("batch_processing.utils.watermark.DeltaTable") as mock_dt:
              mock_dt.isDeltaTable.return_value = True
              mock_delta = MagicMock()
              mock_dt.forPath.return_value = mock_delta
              mock_delta.alias.return_value.merge.return_value = mock_merge_builder
              mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
              mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
              write_watermark(spark, "s3a://silver/_watermarks/", "silver-transactions", 5)
          mock_dt.forPath.assert_called_once_with(spark, "s3a://silver/_watermarks/")
          mock_merge_builder.execute.assert_called_once()
  ```

- [ ] **Step 2: Run tests to verify they fail**

  ```bash
  cd C:\MLOps\.worktrees\spark-bronze
  uv run pytest src/tests/batch_processing/test_unit_watermark.py -v
  ```

  Expected: `ModuleNotFoundError: No module named 'batch_processing'` (module doesn't exist yet).

- [ ] **Step 3: Create `src/batch_processing/utils/watermark.py`**

  ```python
  """Shared watermark utility for Silver batch jobs.

  Persists the last successfully processed Bronze Delta version per job in a
  small Delta table (``s3a://silver/_watermarks/``). Silver jobs call
  ``read_watermark`` before reading Bronze CDF and ``write_watermark`` only
  after a successful Silver MERGE — guaranteeing idempotent re-runs.
  """
  from __future__ import annotations

  import datetime

  from delta.tables import DeltaTable
  from pyspark.sql import SparkSession
  from pyspark.sql import functions as F
  from pyspark.sql import types as T

  _SCHEMA = T.StructType(
      [
          T.StructField("job_name", T.StringType(), nullable=False),
          T.StructField("last_bronze_version", T.LongType(), nullable=False),
          T.StructField("updated_at", T.TimestampType(), nullable=False),
      ]
  )


  def read_watermark(
      spark: SparkSession, watermark_path: str, job_name: str
  ) -> int | None:
      """Return the last successfully processed Bronze Delta version for *job_name*.

      Returns ``None`` when no watermark table or no row for *job_name* exists
      (first run) — callers treat ``None`` as ``startingVersion=0``.
      """
      if not DeltaTable.isDeltaTable(spark, watermark_path):
          return None
      row = (
          spark.read.format("delta")
          .load(watermark_path)
          .filter(F.col("job_name") == job_name)
          .select("last_bronze_version")
          .first()
      )
      return int(row["last_bronze_version"]) if row else None


  def write_watermark(
      spark: SparkSession, watermark_path: str, job_name: str, version: int
  ) -> None:
      """Upsert the watermark for *job_name* to *version*.

      Uses a Delta MERGE on ``job_name`` so the table stays at one row per job.
      Falls back to an initial Delta write when the table does not yet exist.
      """
      new_row = spark.createDataFrame(
          [(job_name, version, datetime.datetime.utcnow())],
          schema=_SCHEMA,
      )
      if DeltaTable.isDeltaTable(spark, watermark_path):
          (
              DeltaTable.forPath(spark, watermark_path)
              .alias("wm")
              .merge(new_row.alias("new"), "wm.job_name = new.job_name")
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute()
          )
      else:
          new_row.write.format("delta").save(watermark_path)
  ```

- [ ] **Step 4: Run tests to verify they pass**

  ```bash
  uv run pytest src/tests/batch_processing/test_unit_watermark.py -v
  ```

  Expected: `5 passed`.

- [ ] **Step 5: Commit**

  ```bash
  git add src/batch_processing/utils/watermark.py \
          src/tests/batch_processing/__init__.py \
          src/tests/batch_processing/test_unit_watermark.py
  git commit -m "feat: add shared watermark util for Silver batch CDF jobs

  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

---

## Task 3: Bronze jobs — Change output format from Parquet to Delta

**Files:**
- Modify: `src/batch_processing/bronze/cdc_transactions_to_bronze.py`
- Modify: `src/batch_processing/bronze/cdc_fraud_cases_to_bronze.py`

- [ ] **Step 1: Edit `cdc_transactions_to_bronze.py`**

  In `main()`, change the single line inside the `writeStream` chain:

  ```python
  # BEFORE
  query = (
      bronze_df.writeStream.format("parquet")
      .outputMode("append")
      ...
  )

  # AFTER
  query = (
      bronze_df.writeStream.format("delta")
      .outputMode("append")
      ...
  )
  ```

  All other lines in `main()` are unchanged.

- [ ] **Step 2: Edit `cdc_fraud_cases_to_bronze.py`**

  Locate the `writeStream` block in `main()` and apply the identical one-word change:

  ```python
  # BEFORE
  bronze_df.writeStream.format("parquet")

  # AFTER
  bronze_df.writeStream.format("delta")
  ```

- [ ] **Step 3: Lint check**

  ```bash
  uv run ruff check src/batch_processing/bronze/
  ```

  Expected: no errors.

- [ ] **Step 4: Commit**

  ```bash
  git add src/batch_processing/bronze/cdc_transactions_to_bronze.py \
          src/batch_processing/bronze/cdc_fraud_cases_to_bronze.py
  git commit -m "feat: switch Bronze output from Parquet to Delta format

  CDF is automatically enabled by the spark-defaults.conf table property
  added in Task 1 (enableChangeDataFeed = true).

  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

---

## Task 4: Silver transactions job — Rewrite main()

**Files:**
- Modify: `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py`
- Create: `src/tests/batch_processing/test_unit_silver_transactions_main.py`

- [ ] **Step 1: Write failing tests for new main() behaviour**

  Create `src/tests/batch_processing/test_unit_silver_transactions_main.py`:

  ```python
  """Unit tests for cdc_transactions_normalize_merge_silver.main() logic.

  Validates version-range logic, no-data early exit, CDF column dropping,
  error handling, and watermark write-after-MERGE ordering.
  All Spark/Delta interactions are mocked.
  """
  from __future__ import annotations

  from unittest.mock import MagicMock, call, patch

  import pytest


  # The module under test — import after patching heavy deps
  MODULE = "batch_processing.silver.cdc_transactions_normalize_merge_silver"


  def _make_spark_mock(bronze_is_delta: bool = True, current_version: int = 10):
      spark = MagicMock()
      # DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
      spark._bronze_version = current_version
      return spark


  class TestMainNoNewData:
      def test_exits_early_when_last_equals_current(self, capsys):
          with (
              patch(f"{MODULE}.DeltaTable") as mock_dt,
              patch(f"{MODULE}.read_watermark", return_value=10),
              patch(f"{MODULE}.write_watermark") as mock_wm,
              patch(f"{MODULE}.build_spark_session") as mock_build,
          ):
              mock_dt.isDeltaTable.return_value = True
              mock_dt.forPath.return_value.history.return_value.first.return_value = {
                  "version": 10
              }
              mock_build.return_value.conf.get.side_effect = lambda k: {
                  "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                  "spark.silver.output.path": "s3a://silver/transactions",
                  "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                  "spark.silver.watermark.path": "s3a://silver/_watermarks/",
              }[k]

              from batch_processing.silver.cdc_transactions_normalize_merge_silver import main
              main()

          mock_wm.assert_not_called()
          captured = capsys.readouterr()
          assert "no new data" in captured.out


  class TestMainCDFReadError:
      def test_raises_runtime_error_on_retention_exceeded(self):
          with (
              patch(f"{MODULE}.DeltaTable") as mock_dt,
              patch(f"{MODULE}.read_watermark", return_value=0),
              patch(f"{MODULE}.build_spark_session") as mock_build,
          ):
              mock_dt.isDeltaTable.return_value = True
              mock_dt.forPath.return_value.history.return_value.first.return_value = {
                  "version": 5
              }
              spark = mock_build.return_value
              spark.conf.get.side_effect = lambda k: {
                  "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                  "spark.silver.output.path": "s3a://silver/transactions",
                  "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                  "spark.silver.watermark.path": "s3a://silver/_watermarks/",
              }[k]
              spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.side_effect = Exception(
                  "outside the range of retained versions"
              )

              from batch_processing.silver.cdc_transactions_normalize_merge_silver import main

              with pytest.raises(RuntimeError, match="outside log retention"):
                  main()


  class TestMainWatermarkWrittenAfterMerge:
      def test_watermark_written_only_after_successful_merge(self):
          with (
              patch(f"{MODULE}.DeltaTable") as mock_dt,
              patch(f"{MODULE}.read_watermark", return_value=None),
              patch(f"{MODULE}.write_watermark") as mock_wm,
              patch(f"{MODULE}.cast_types") as mock_cast,
              patch(f"{MODULE}.validate_and_split") as mock_split,
              patch(f"{MODULE}.write_quarantine"),
              patch(f"{MODULE}.merge_to_silver") as mock_merge,
              patch(f"{MODULE}.build_spark_session") as mock_build,
          ):
              mock_dt.isDeltaTable.return_value = True
              mock_dt.forPath.return_value.history.return_value.first.return_value = {
                  "version": 3
              }
              spark = mock_build.return_value
              spark.conf.get.side_effect = lambda k: {
                  "spark.silver.bronze.input.path": "s3a://bronze/cdc/transactions",
                  "spark.silver.output.path": "s3a://silver/transactions",
                  "spark.silver.quarantine.path": "s3a://silver/quarantine/transactions",
                  "spark.silver.watermark.path": "s3a://silver/_watermarks/",
              }[k]
              # CDF read returns a non-empty DataFrame mock
              mock_df = MagicMock()
              mock_df.isEmpty.return_value = False
              (
                  spark.read.format.return_value
                  .option.return_value.option.return_value.option.return_value
                  .load.return_value.filter.return_value.drop.return_value
              ) = mock_df
              mock_cast.return_value = mock_df
              mock_split.return_value = (mock_df, mock_df)

              from batch_processing.silver.cdc_transactions_normalize_merge_silver import main
              main()

          mock_merge.assert_called_once()
          mock_wm.assert_called_once_with(
              spark, "s3a://silver/_watermarks/", "silver-transactions", 3
          )
  ```

- [ ] **Step 2: Run tests to verify they fail**

  ```bash
  uv run pytest src/tests/batch_processing/test_unit_silver_transactions_main.py -v
  ```

  Expected: tests fail because `main()` still uses the old streaming code.

- [ ] **Step 3: Rewrite `cdc_transactions_normalize_merge_silver.py`**

  Replace the entire file with the following (all transform functions unchanged, only `BRONZE_SCHEMA`, `process_batch`, and `main` are different):

  ```python
  """CDC transactions normalize-merge to Silver: Bronze Delta (CDF) → Silver Delta.

  Reads new Bronze CDC rows incrementally via Delta Change Data Feed, normalises
  them into canonical Silver rows, and MERGEs the result into a partitioned
  Silver Delta table.

  Incremental state is tracked by a Delta watermark table
  (``spark.silver.watermark.path``): the last successfully processed Bronze
  Delta version is persisted there and read at job start so each nightly run
  only processes new Bronze commits.

  Pipeline steps:
    1. Read watermark → last_bronze_version (None on first run → startingVersion=0).
    2. Read current Bronze Delta version from history.
    3. If no new commits, exit 0.
    4. Batch CDF read: startingVersion=last+1, endingVersion=current.
       Filter _change_type='insert', drop CDF metadata columns.
    5. cast_types → validate_and_split → write_quarantine → merge_to_silver.
    6. write_watermark(current_version) — only after successful MERGE.

  Run:
      spark-submit /opt/silver/cdc_transactions_normalize_merge_silver.py

  All configuration is loaded from spark-defaults.conf (``spark.silver.*`` namespace).
  """
  from __future__ import annotations

  from delta.tables import DeltaTable
  from pyspark.sql import DataFrame, SparkSession
  from pyspark.sql import functions as F
  from pyspark.sql import types as T
  from pyspark.sql import window as W

  from utils.watermark import read_watermark, write_watermark

  # CDF metadata columns added by Delta — dropped before cast_types so the
  # downstream functions see the same schema as before.
  _CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

  JOB_NAME = "silver-transactions"


  def build_spark_session() -> SparkSession:
      return SparkSession.builder.appName("silver-transactions-batch").getOrCreate()


  # ---------------------------------------------------------------------------
  # Transform
  # ---------------------------------------------------------------------------


  def cast_types(df: DataFrame) -> DataFrame:
      """Cast Bronze raw types to Silver canonical types.

      Both ``_lsn`` and ``_source_ts`` are kept in the output:
      ``_lsn`` is preserved in Silver to guard cross-batch MERGE ordering;
      ``_source_ts`` serves as a tie-breaker when two events share the same LSN.
      """
      return (
          df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
          .withColumn("event_date", F.to_date("event_timestamp"))
          .withColumn("created_at", F.to_timestamp("created_at"))
          .withColumn("amount", F.col("amount").cast(T.DecimalType(18, 2)))
          .withColumnRenamed("_op", "_cdc_op")
          .withColumn(
              "_source_ts", (F.col("_source_ts_ms") / 1000).cast(T.TimestampType())
          )
          .withColumn("_cdc_ts", (F.col("_cdc_ts_ms") / 1000).cast(T.TimestampType()))
          .withColumn("_silver_updated_at", F.current_timestamp())
          .drop(
              "_source_table",
              "_snapshot",
              "_ingested_at",
              "_source_ts_ms",
              "_cdc_ts_ms",
              "_deleted",
          )
      )


  # ---------------------------------------------------------------------------
  # Validation and quarantine
  # ---------------------------------------------------------------------------


  def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
      """Split rows into (valid_df, quarantine_df).

      Quarantine rows keep all columns plus ``_error_reason`` (first failed
      rule) and ``_quarantine_ts`` (wall-clock time of quarantine write).
      """
      null_id = F.col("transaction_id").isNull()
      null_ts = F.col("event_timestamp").isNull()
      bad_amount = F.col("amount").isNull() | (F.col("amount") <= 0)

      error_reason = (
          F.when(null_id, F.lit("transaction_id is null"))
          .when(null_ts, F.lit("event_timestamp is null"))
          .when(bad_amount, F.lit("amount must be > 0"))
      )

      flagged = df.withColumn("_error_reason", error_reason)
      valid_df = flagged.filter(F.col("_error_reason").isNull()).drop("_error_reason")
      quarantine_df = (
          flagged.filter(F.col("_error_reason").isNotNull())
          .withColumn("_quarantine_ts", F.current_timestamp())
      )
      return valid_df, quarantine_df


  def write_quarantine(quarantine_path: str, quarantine_df: DataFrame) -> None:
      """Append invalid rows to the quarantine Delta table.

      Quarantine is an audit log — every bad row from every run is preserved.
      MERGE-based dedup is intentionally avoided here because rows with a NULL
      ``transaction_id`` and the same ``_error_reason`` would collapse into a
      single record and hide the true volume of data quality failures.
      """
      if quarantine_df.isEmpty():
          return

      bad_count = quarantine_df.count()
      quarantine_df.write.format("delta").mode("append").save(quarantine_path)
      print(f"[{JOB_NAME}] quarantined {bad_count:,} bad rows → {quarantine_path}")


  # ---------------------------------------------------------------------------
  # Merge into Silver Delta table
  # ---------------------------------------------------------------------------


  def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
      """Deduplicate by LSN and MERGE into the Silver Delta table.

      PostgreSQL LSN (Log Sequence Number) is monotonically increasing within a
      Postgres instance, making it the correct primary ordering key when multiple
      CDC events for the same ``transaction_id`` arrive in the same batch.
      ``_source_ts`` is used as a tie-breaker for events sharing the same LSN.

      ``_lsn`` is **kept** in the Silver schema so the MERGE can guard against
      late or out-of-order Bronze files overwriting newer Silver rows across
      separate batch runs. The update condition ``bronze._lsn >= silver._lsn``
      ensures older events never regress the Silver row to a stale state.
      """
      window = W.Window.partitionBy("transaction_id").orderBy(
          F.desc("_lsn"), F.desc("_source_ts")
      )
      dedup_df = (
          batch_df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
      )

      good_count = dedup_df.count()

      if DeltaTable.isDeltaTable(spark, silver_path):
          (
              DeltaTable.forPath(spark, silver_path)
              .alias("silver")
              .merge(
                  dedup_df.alias("bronze"),
                  "silver.transaction_id = bronze.transaction_id",
              )
              .whenMatchedDelete(
                  condition="bronze._cdc_op = 'd'"
                  " AND (silver._lsn IS NULL OR bronze._lsn >= silver._lsn)"
              )
              .whenMatchedUpdateAll(
                  condition="bronze._cdc_op != 'd'"
                  " AND (silver._lsn IS NULL OR bronze._lsn >= silver._lsn)"
              )
              .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
              .execute()
          )
      else:
          (
              dedup_df.filter(F.col("_cdc_op") != "d")
              .write.format("delta")
              .partitionBy("event_date")
              .save(silver_path)
          )

      print(f"[{JOB_NAME}] merged {good_count:,} rows → {silver_path}")


  # ---------------------------------------------------------------------------
  # Entry point
  # ---------------------------------------------------------------------------


  def main() -> None:
      spark = build_spark_session()

      bronze_path = spark.conf.get("spark.silver.bronze.input.path")
      silver_path = spark.conf.get("spark.silver.output.path")
      quarantine_path = spark.conf.get("spark.silver.quarantine.path")
      watermark_path = spark.conf.get("spark.silver.watermark.path")

      if not DeltaTable.isDeltaTable(spark, bronze_path):
          raise RuntimeError(
              f"Bronze path {bronze_path!r} is not a Delta table. "
              "Ensure the Bronze streaming job has been updated to write Delta "
              "format and at least one micro-batch has been committed."
          )

      last_version: int | None = read_watermark(spark, watermark_path, JOB_NAME)
      current_version = int(
          DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
      )

      if last_version is not None and last_version >= current_version:
          print(
              f"[{JOB_NAME}] no new data "
              f"(last_processed={last_version}, bronze_current={current_version}), exiting."
          )
          return

      start_version = 0 if last_version is None else last_version + 1
      print(f"[{JOB_NAME}] reading Bronze CDF versions {start_version}–{current_version}")

      try:
          bronze_df = (
              spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", start_version)
              .option("endingVersion", current_version)
              .load(bronze_path)
              .filter(F.col("_change_type") == "insert")
              .drop(*_CDF_META_COLS)
          )
      except Exception as exc:
          msg = str(exc)
          if "outside the range" in msg or "is not enabled" in msg:
              raise RuntimeError(
                  f"Bronze CDF read failed for {JOB_NAME}: {msg}. "
                  "If startingVersion is outside log retention, reset the "
                  f"watermark by deleting the '{JOB_NAME}' row from "
                  f"{watermark_path!r} and re-run."
              ) from exc
          raise

      if bronze_df.isEmpty():
          print(f"[{JOB_NAME}] CDF returned 0 insert rows, exiting.")
          return

      typed_df = cast_types(bronze_df)
      valid_df, quarantine_df = validate_and_split(typed_df)
      write_quarantine(quarantine_path, quarantine_df)
      merge_to_silver(spark, silver_path, valid_df)
      write_watermark(spark, watermark_path, JOB_NAME, current_version)
      print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


  if __name__ == "__main__":
      main()
  ```

- [ ] **Step 4: Run tests to verify they pass**

  ```bash
  uv run pytest src/tests/batch_processing/test_unit_silver_transactions_main.py -v
  ```

  Expected: `3 passed`.

- [ ] **Step 5: Lint check**

  ```bash
  uv run ruff check src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py
  ```

  Expected: no errors.

- [ ] **Step 6: Commit**

  ```bash
  git add src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py \
          src/tests/batch_processing/test_unit_silver_transactions_main.py
  git commit -m "feat: rewrite Silver transactions job — batch CDF read + Delta watermark

  Removes Structured Streaming (readStream/foreachBatch/trigger/checkpoint).
  Reads only new Bronze Delta commits via CDF (startingVersion = last+1).
  Watermark persisted in s3a://silver/_watermarks/ after successful MERGE.

  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

---

## Task 5: Silver fraud_cases job — Rewrite main()

**Files:**
- Modify: `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py`

- [ ] **Step 1: Rewrite `cdc_fraud_cases_normalize_merge_silver.py`**

  Replace only the imports block, remove `BRONZE_SCHEMA` and `process_batch`, and rewrite `main()`. All other functions (`cast_types`, `validate_and_split`, `write_quarantine`, `merge_to_silver`) are unchanged.

  **Updated top of file** (replace everything from the top down to the `def build_spark_session():` line):

  ```python
  """CDC fraud cases normalize-merge to Silver: Bronze Delta (CDF) → Silver Delta.

  Reads new Bronze fraud_cases CDC rows incrementally via Delta Change Data Feed,
  normalises them into canonical Silver rows, and MERGEs the result into a
  partitioned Silver Delta table.

  ``is_fraud`` is derived in Silver (not kept as a raw column) so all downstream
  consumers — Gold terminal features, training dataset, reporting — can read a
  single clean boolean without reimplementing the business rule:

    is_fraud = 1  iff  case_status = 'confirmed' AND resolved_at IS NOT NULL
    is_fraud = 0  otherwise (open, dismissed, or not yet resolved)

  CDC lifecycle:
    - INSERT when investigation opens (resolved_at NULL, case_status='open')
    - UPDATE when investigation closes (resolved_at set, case_status='confirmed'/'dismissed')
    - Silver MERGE keeps the latest version per case_id (LSN-ordered)

  Incremental state is tracked by the shared Delta watermark table
  (``spark.silver.fraud_cases.watermark.path``).

  Run:
      spark-submit /opt/silver/cdc_fraud_cases_normalize_merge_silver.py

  All configuration is loaded from spark-defaults.conf
  (``spark.silver.fraud_cases.*`` namespace).
  """
  from __future__ import annotations

  from delta.tables import DeltaTable
  from pyspark.sql import DataFrame, SparkSession
  from pyspark.sql import functions as F
  from pyspark.sql import types as T
  from pyspark.sql import window as W

  from utils.watermark import read_watermark, write_watermark

  _CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

  JOB_NAME = "silver-fraud-cases"


  def build_spark_session() -> SparkSession:
      return SparkSession.builder.appName("silver-fraud-cases-batch").getOrCreate()
  ```

  **Remove** the entire `BRONZE_SCHEMA` block (the `T.StructType([...])` definition at the top of the old file) and the `process_batch` function.

  **Replace `main()`** (replace from `def main() -> None:` to end of file):

  ```python
  def main() -> None:
      spark = build_spark_session()

      bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
      silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
      quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
      watermark_path = spark.conf.get("spark.silver.fraud_cases.watermark.path")

      if not DeltaTable.isDeltaTable(spark, bronze_path):
          raise RuntimeError(
              f"Bronze path {bronze_path!r} is not a Delta table. "
              "Ensure the Bronze streaming job has been updated to write Delta "
              "format and at least one micro-batch has been committed."
          )

      last_version: int | None = read_watermark(spark, watermark_path, JOB_NAME)
      current_version = int(
          DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
      )

      if last_version is not None and last_version >= current_version:
          print(
              f"[{JOB_NAME}] no new data "
              f"(last_processed={last_version}, bronze_current={current_version}), exiting."
          )
          return

      start_version = 0 if last_version is None else last_version + 1
      print(f"[{JOB_NAME}] reading Bronze CDF versions {start_version}–{current_version}")

      try:
          bronze_df = (
              spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", start_version)
              .option("endingVersion", current_version)
              .load(bronze_path)
              .filter(F.col("_change_type") == "insert")
              .drop(*_CDF_META_COLS)
          )
      except Exception as exc:
          msg = str(exc)
          if "outside the range" in msg or "is not enabled" in msg:
              raise RuntimeError(
                  f"Bronze CDF read failed for {JOB_NAME}: {msg}. "
                  "If startingVersion is outside log retention, reset the "
                  f"watermark by deleting the '{JOB_NAME}' row from "
                  f"{watermark_path!r} and re-run."
              ) from exc
          raise

      if bronze_df.isEmpty():
          print(f"[{JOB_NAME}] CDF returned 0 insert rows, exiting.")
          return

      typed_df = cast_types(bronze_df)
      valid_df, quarantine_df = validate_and_split(typed_df)
      write_quarantine(quarantine_path, quarantine_df)
      merge_to_silver(spark, silver_path, valid_df)
      write_watermark(spark, watermark_path, JOB_NAME, current_version)
      print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


  if __name__ == "__main__":
      main()
  ```

- [ ] **Step 2: Lint check**

  ```bash
  uv run ruff check src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py
  ```

  Expected: no errors.

- [ ] **Step 3: Run full test suite**

  ```bash
  uv run pytest src/tests/ -v
  ```

  Expected: all tests pass (watermark + silver_transactions_main tests).

- [ ] **Step 4: Commit**

  ```bash
  git add src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py
  git commit -m "feat: rewrite Silver fraud_cases job — batch CDF read + Delta watermark

  Same pattern as transactions: removes Structured Streaming, reads only
  new Bronze Delta commits via CDF, persists watermark after successful MERGE.

  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

---

## Done

After Task 5, the full pipeline is:

```
Kafka → Bronze streaming (Delta + CDF) → s3a://bronze/cdc/<topic>
                                                  ↓
                                    nightly Airflow trigger
                                                  ↓
                             Silver batch job (spark-submit)
                             reads CDF startingVersion = last+1
                                                  ↓
                               Silver Delta MERGE + quarantine
                                                  ↓
                              update s3a://silver/_watermarks/
```

**Airflow note:** Remove the `checkpointLocation` option from any existing Airflow DAG `SparkSubmitOperator` config for the Silver jobs — it is no longer read.
