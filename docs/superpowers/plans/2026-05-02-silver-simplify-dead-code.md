# Silver Jobs Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove dead CDC/LSN logic and defensive noise from both Silver batch jobs, aligning code with actual business semantics.

**Architecture:** `banking.transactions` has no mutable columns — transactions are immutable, so Silver transactions MERGE simplifies to `whenNotMatchedInsertAll` with no LSN dedup and `_lsn` dropped from output. `banking.fraud_cases` has `case_status`/`resolved_at` that change, so Silver fraud_cases keeps Window LSN dedup + `whenMatchedUpdateAll`, but drops `whenMatchedDelete` (cases never deleted) and the cross-batch LSN guard (CDF version ordering already guarantees ordering).

**Tech Stack:** PySpark, delta-spark, uv, pytest, ruff

**Spec:** `docs/superpowers/specs/2026-05-02-silver-simplification-design.md`

**Worktree:** `C:\MLOps\.worktrees\spark-bronze` (branch `feat/spark-bronze-stream`)

---

### Task 1: Simplify Transactions Silver job

**Files:**
- Modify: `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py`
- Modify: `src/tests/batch_processing/test_unit_silver_transactions_main.py`

- [ ] **Step 1: Replace the transactions Silver job with the simplified version**

Replace the entire contents of `src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py` with:

```python
"""Silver transactions batch job: Bronze Delta (CDF) → Silver Delta.

Reads new CDC rows incrementally via Delta Change Data Feed, normalises them,
and MERGEs the result into a Silver Delta table.
All configuration from spark-defaults.conf (``spark.silver.*`` namespace).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from utils.watermark import read_watermark, write_watermark

_CDF_META_COLS = ("_change_type", "_commit_version", "_commit_timestamp")

JOB_NAME = "silver-transactions"


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("silver-transactions-batch").getOrCreate()


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types."""
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
            "_lsn",
        )
    )


# ---------------------------------------------------------------------------
# Validation and quarantine
# ---------------------------------------------------------------------------


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid_df, quarantine_df) based on null/range checks."""
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
    """Append invalid rows to the quarantine Delta table (audit log)."""
    if quarantine_df.isEmpty():
        return
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[{JOB_NAME}] quarantined bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """MERGE new Bronze rows into the Silver transactions Delta table."""
    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                batch_df.alias("bronze"),
                "silver.transaction_id = bronze.transaction_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").partitionBy("event_date").save(silver_path)
    print(f"[{JOB_NAME}] merged rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.output.path")
    quarantine_path = spark.conf.get("spark.silver.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.watermark.path")

    last_version: int | None = read_watermark(spark, watermark_path, JOB_NAME)
    current_version = int(
        DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
    )

    if last_version is not None and last_version >= current_version:
        print(
            f"[{JOB_NAME}] no new data "
            f"(last_processed={last_version}, "
            f"bronze_current={current_version}), exiting."
        )
        return

    start_version = 0 if last_version is None else last_version + 1
    print(f"[{JOB_NAME}] reading Bronze CDF versions {start_version}–{current_version}")

    bronze_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", current_version)
        .load(bronze_path)
        .filter(F.col("_change_type") == "insert")
        .drop(*_CDF_META_COLS)
    )

    typed_df = cast_types(bronze_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    write_watermark(spark, watermark_path, JOB_NAME, current_version)
    print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


if __name__ == "__main__":
    main()
```

Key changes vs previous version:
- `cast_types`: added `"_lsn"` to `.drop(...)`, shortened docstring
- Removed `from pyspark.sql import window as W` import (no longer used)
- `write_quarantine`: removed `bad_count = quarantine_df.count()`, updated print
- `merge_to_silver`: removed Window dedup block, removed `good_count`, MERGE is now `whenNotMatchedInsertAll()` only, shortened docstring
- `main()`: removed `isDeltaTable(bronze_path)` guard block, replaced `try/except` around CDF read with plain assignment, removed `bronze_df.isEmpty()` guard

- [ ] **Step 2: Delete `TestMainCDFReadError` from the test file**

In `src/tests/batch_processing/test_unit_silver_transactions_main.py`, delete the entire `TestMainCDFReadError` class (lines 54–84):

```python
class TestMainCDFReadError:
    def test_raises_runtime_error_on_retention_exceeded(self):
        ...
```

The class tests the `try/except` block that was removed. The other 3 classes (`TestMainNoNewData`, `TestMainWatermarkWrittenAfterMerge`, `TestMainAllRowsQuarantined`) are unaffected and should remain.

- [ ] **Step 3: Run lint**

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run ruff check src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py src/tests/batch_processing/test_unit_silver_transactions_main.py
```

Expected: no output (clean).

- [ ] **Step 4: Run tests**

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run pytest src/tests/batch_processing/ -q
```

Expected: 9 passed (was 10 — the deleted test is gone), 0 failed.

- [ ] **Step 5: Commit**

```bash
cd C:\MLOps\.worktrees\spark-bronze
git add src/batch_processing/silver/cdc_transactions_normalize_merge_silver.py \
        src/tests/batch_processing/test_unit_silver_transactions_main.py
git commit -m "refactor: simplify Silver transactions — insert-only MERGE, drop dead LSN/CDC logic

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 2: Simplify Fraud Cases Silver job

**Files:**
- Modify: `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py`

- [ ] **Step 1: Replace the fraud_cases Silver job with the simplified version**

Replace the entire contents of `src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py` with:

```python
"""Silver fraud_cases batch job: Bronze Delta (CDF) → Silver Delta.

Reads new CDC rows incrementally via Delta Change Data Feed, normalises them,
and MERGEs the result into a Silver Delta table.

``is_fraud`` is derived in Silver: case_status='confirmed' AND resolved_at IS NOT NULL.
All configuration from spark-defaults.conf (``spark.silver.fraud_cases.*``).
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


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def cast_types(df: DataFrame) -> DataFrame:
    """Cast Bronze raw types to Silver canonical types and derive is_fraud.

    is_fraud = True iff case_status = 'confirmed' AND resolved_at IS NOT NULL.
    """
    return (
        df.withColumn("reported_at", F.to_timestamp("reported_at"))
        .withColumn("resolved_at", F.to_timestamp("resolved_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn(
            "loss_amount", F.col("loss_amount").cast(T.DecimalType(12, 2))
        )
        .withColumn(
            "is_fraud",
            (F.col("case_status") == F.lit("confirmed"))
            & F.col("resolved_at").isNotNull(),
        )
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

    Rules: case_id, transaction_id, and reported_at must not be null.
    """
    null_case_id = F.col("case_id").isNull()
    null_txn_id = F.col("transaction_id").isNull()
    null_reported = F.col("reported_at").isNull()

    error_reason = (
        F.when(null_case_id, F.lit("case_id is null"))
        .when(null_txn_id, F.lit("transaction_id is null"))
        .when(null_reported, F.lit("reported_at is null"))
    )

    flagged = df.withColumn("_error_reason", error_reason)
    valid_df = flagged.filter(F.col("_error_reason").isNull()).drop("_error_reason")
    quarantine_df = (
        flagged.filter(F.col("_error_reason").isNotNull())
        .withColumn("_quarantine_ts", F.current_timestamp())
    )
    return valid_df, quarantine_df


def write_quarantine(quarantine_path: str, quarantine_df: DataFrame) -> None:
    """Append invalid rows to the quarantine Delta table (audit log)."""
    if quarantine_df.isEmpty():
        return
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"[silver-fraud_cases] quarantined bad rows → {quarantine_path}")


# ---------------------------------------------------------------------------
# Merge into Silver Delta table
# ---------------------------------------------------------------------------


def merge_to_silver(spark: SparkSession, silver_path: str, batch_df: DataFrame) -> None:
    """Deduplicate by LSN within batch and MERGE into the Silver fraud_cases Delta table."""
    window = W.Window.partitionBy("case_id").orderBy(
        F.desc("_lsn"), F.desc("_source_ts")
    )
    dedup_df = (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("silver")
            .merge(
                dedup_df.alias("bronze"),
                "silver.case_id = bronze.case_id",
            )
            .whenMatchedUpdateAll(condition="bronze._cdc_op != 'd'")
            .whenNotMatchedInsertAll(condition="bronze._cdc_op != 'd'")
            .execute()
        )
    else:
        (
            dedup_df.filter(F.col("_cdc_op") != "d")
            .withColumn("reported_date", F.to_date("reported_at"))
            .write.format("delta")
            .partitionBy("reported_date")
            .save(silver_path)
        )

    print(f"[silver-fraud_cases] merged rows → {silver_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    spark = build_spark_session()

    bronze_path = spark.conf.get("spark.silver.fraud_cases.bronze.input.path")
    silver_path = spark.conf.get("spark.silver.fraud_cases.output.path")
    quarantine_path = spark.conf.get("spark.silver.fraud_cases.quarantine.path")
    watermark_path = spark.conf.get("spark.silver.fraud_cases.watermark.path")

    last_version: int | None = read_watermark(spark, watermark_path, JOB_NAME)
    current_version = int(
        DeltaTable.forPath(spark, bronze_path).history(1).first()["version"]
    )

    if last_version is not None and last_version >= current_version:
        print(
            f"[{JOB_NAME}] no new data "
            f"(last_processed={last_version}, "
            f"bronze_current={current_version}), exiting."
        )
        return

    start_version = 0 if last_version is None else last_version + 1
    print(f"[{JOB_NAME}] reading Bronze CDF versions {start_version}–{current_version}")

    bronze_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", current_version)
        .load(bronze_path)
        .filter(F.col("_change_type") == "insert")
        .drop(*_CDF_META_COLS)
    )

    typed_df = cast_types(bronze_df)
    valid_df, quarantine_df = validate_and_split(typed_df)
    write_quarantine(quarantine_path, quarantine_df)
    merge_to_silver(spark, silver_path, valid_df)
    write_watermark(spark, watermark_path, JOB_NAME, current_version)
    print(f"[{JOB_NAME}] watermark updated to Bronze version {current_version}.")


if __name__ == "__main__":
    main()
```

Key changes vs previous version:
- `write_quarantine`: removed `bad_count = quarantine_df.count()`, updated print
- `merge_to_silver`: removed `good_count = dedup_df.count()`, removed `whenMatchedDelete` branch entirely, removed cross-batch LSN guard from conditions (was `"bronze._cdc_op != 'd' AND (silver._lsn IS NULL OR bronze._lsn >= silver._lsn)"`), shortened docstring
- `main()`: removed `isDeltaTable(bronze_path)` guard, replaced `try/except` CDF read with plain assignment, removed `bronze_df.isEmpty()` guard, shortened module docstring

- [ ] **Step 2: Run lint**

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run ruff check src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py
```

Expected: no output (clean).

- [ ] **Step 3: Run full test suite**

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run pytest src/tests/batch_processing/ -q
```

Expected: 9 passed, 0 failed.

- [ ] **Step 4: Commit**

```bash
cd C:\MLOps\.worktrees\spark-bronze
git add src/batch_processing/silver/cdc_fraud_cases_normalize_merge_silver.py
git commit -m "refactor: simplify Silver fraud_cases — drop whenMatchedDelete and LSN guard

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```
