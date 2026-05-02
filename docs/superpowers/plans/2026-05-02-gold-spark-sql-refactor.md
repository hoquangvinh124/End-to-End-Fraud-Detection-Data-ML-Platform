# Gold Spark SQL Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Python-loop aggregations in both Gold batch jobs with Spark SQL, add unit tests for both jobs.

**Architecture:** Each `compute_*` function registers its input DataFrame as a Spark temp view, then executes one `spark.sql("""...""")` query using `COUNT(CASE WHEN ...)` / `AVG(CASE WHEN ...)` / `SUM(CASE WHEN ...) / COUNT(CASE WHEN ...)` conditional aggregation. The terminal job's 2-step aggregation (intermediate `_nb_tx_*` / `_nb_fraud_*` columns + second loop) collapses to one SQL query. Function boundaries (`load_*`, `compute_*`, `write_gold_partition`) are preserved; only the internals change.

**Tech Stack:** PySpark ≥ 3.x local mode, Spark SQL, Delta Lake, pytest, pytest-mock

---

## File Map

| File | Action | Change |
|---|---|---|
| `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py` | Modify | Replace `compute_customer_features` with SQL; add `spark` param; remove `row_count`; remove `WINDOW_DAYS`, `T` import |
| `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py` | Modify | Extract `_attach_fraud_label`; replace `compute_terminal_features` with SQL; add `spark` param; remove `row_count`; remove `WINDOW_DAYS` |
| `src/tests/batch_processing/test_unit_gold_customer_main.py` | Create | 7 tests covering `resolve_feature_date`, `compute_customer_features`, `write_gold_partition` |
| `src/tests/batch_processing/test_unit_gold_terminal_main.py` | Create | 10 tests covering `resolve_feature_date`, `_attach_fraud_label`, `compute_terminal_features`, `write_gold_partition` |

---

## Task 1: Customer Gold — SQL refactor + tests

**Files:**
- Create: `src/tests/batch_processing/test_unit_gold_customer_main.py`
- Modify: `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py`

### Step 1 — Write the failing test file

Create `src/tests/batch_processing/test_unit_gold_customer_main.py`:

```python
"""Unit tests for Silver → Gold customer window-features batch job.

Tests: resolve_feature_date (conf/default), compute_customer_features (SQL window
logic for all 3 windows), write_gold_partition (first-run vs replaceWhere).
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

import batch_processing.gold.silver_transactions_window_aggregate_customer_gold as mod

MODULE = "batch_processing.gold.silver_transactions_window_aggregate_customer_gold"

FEATURE_DATE = datetime.date(2024, 1, 15)

_TXN_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("amount", T.DecimalType(18, 2)),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("test-gold-customer").getOrCreate()


class TestResolveFeatureDate:
    def test_conf_set(self, spark):
        spark.conf.set("spark.gold.feature.date", "2024-01-15")
        assert mod.resolve_feature_date(spark) == datetime.date(2024, 1, 15)

    def test_conf_blank_defaults_to_yesterday(self, spark):
        spark.conf.set("spark.gold.feature.date", "")
        expected = datetime.date.today() - datetime.timedelta(days=1)
        assert mod.resolve_feature_date(spark) == expected


class TestComputeCustomerFeatures:
    def test_count_and_avg_per_window(self, spark):
        """Rows for one customer spanning 31 days; verify all 3 window counts and avgs."""
        # Window cutoffs for FEATURE_DATE=2024-01-15:
        #   1D → event_date >= 2024-01-15  (offset 0)
        #   7D → event_date >= 2024-01-09  (offset 6)
        #  30D → event_date >= 2023-12-17  (offset 29)
        rows = [
            ("C001", datetime.date(2024, 1, 15), Decimal("100.00")),  # 1D, 7D, 30D
            ("C001", datetime.date(2024, 1, 15), Decimal("200.00")),  # 1D, 7D, 30D
            ("C001", datetime.date(2024, 1, 10), Decimal("50.00")),   # 7D, 30D only
            ("C001", datetime.date(2023, 12, 20), Decimal("300.00")), # 30D only
        ]
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C001'").first()

        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 2
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D"] == 3
        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D"] == 4
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == pytest.approx(150.0)
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_7D"]) == pytest.approx(116.67, abs=0.01)
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_30D"]) == pytest.approx(162.5)

    def test_zero_transactions_in_window_gives_zero_avg(self, spark):
        """Customer with no 1D transactions gets AVG_1D = 0, not NULL."""
        rows = [("C002", datetime.date(2023, 12, 20), Decimal("100.00"))]  # 30D only
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C002'").first()

        assert row["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] == 0
        assert float(row["CUSTOMER_AVG_AMOUNT_WINDOW_1D"]) == 0.0

    def test_feature_date_column_set(self, spark):
        rows = [("C003", datetime.date(2024, 1, 15), Decimal("50.00"))]
        df = spark.createDataFrame(rows, schema=_TXN_SCHEMA)
        result = mod.compute_customer_features(spark, df, FEATURE_DATE)
        row = result.filter("customer_id = 'C003'").first()
        assert row["feature_date"] == FEATURE_DATE


class TestWriteGoldPartition:
    def test_first_run_creates_table(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=False)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.partitionBy.assert_called_once_with("feature_date")
        mock_writer.option.assert_not_called()

    def test_existing_table_uses_replace_where(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=True)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.option.assert_called_once_with("replaceWhere", f"feature_date = '{FEATURE_DATE}'")
        mock_writer.partitionBy.assert_not_called()
```

### Step 2 — Run to confirm tests fail

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run pytest src/tests/batch_processing/test_unit_gold_customer_main.py -v
```

Expected failures:
- `TestComputeCustomerFeatures` — `compute_customer_features()` takes 2 args but test passes 3
- `TestWriteGoldPartition` — `write_gold_partition` calls `df.count()` which is not on the MagicMock chain we need

### Step 3 — Refactor `compute_customer_features` to SQL

In `src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py`, apply all changes at once:

**a) Remove `WINDOW_DAYS` constant and `T` import:**

```python
# BEFORE (lines 38-44)
from pyspark.sql import types as T

# Window sizes (days) — must match the feature names in api/models.py
WINDOW_DAYS = [1, 7, 30]
```

```python
# AFTER — delete both; keep all other imports
```

**b) Replace `compute_customer_features`:**

```python
def compute_customer_features(
    spark: SparkSession, silver_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per customer with rolling window features as of ``feature_date``.

    Windows: 1D=[fd, fd], 7D=[fd-6, fd], 30D=[fd-29, fd].
    Null avg (no transactions in a window) is coalesced to 0.0 to match .fillna(0).
    """
    silver_df.createOrReplaceTempView("silver_txn")
    return spark.sql(f"""
        SELECT
            customer_id,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)  THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
            COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 0)  THEN amount END), 0)
                AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)  THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
            COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 6)  THEN amount END), 0)
                AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29) THEN 1 END)
                AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D,
            COALESCE(AVG(CASE WHEN event_date >= date_sub(DATE '{feature_date}', 29) THEN amount END), 0)
                AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
            DATE '{feature_date}' AS feature_date
        FROM silver_txn
        GROUP BY customer_id
    """)
```

**c) Remove `row_count = df.count()` from `write_gold_partition` and update print:**

```python
def write_gold_partition(
    spark: SparkSession,
    df: DataFrame,
    gold_path: str,
    feature_date: datetime.date,
    label: str,
) -> None:
    """Overwrite only the ``feature_date`` partition in the Gold Delta table.

    ``replaceWhere`` leaves every other historical partition untouched so
    reruns are fully idempotent — no duplicate feature rows accumulate.
    On first run the table does not yet exist: fall back to a plain
    partitioned write that creates the Delta table from scratch.
    """
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, gold_path):
        writer = writer.option("replaceWhere", f"feature_date = '{feature_date}'")
    else:
        writer = writer.partitionBy("feature_date")
    writer.save(gold_path)
    print(f"[gold] {label}: feature_date={feature_date} → {gold_path}")
```

**d) Update `main()` — pass `spark` to `compute_customer_features`:**

```python
# BEFORE
customer_df = compute_customer_features(silver_df, feature_date)

# AFTER
customer_df = compute_customer_features(spark, silver_df, feature_date)
```

### Step 4 — Run tests, confirm all pass

```bash
uv run pytest src/tests/batch_processing/test_unit_gold_customer_main.py -v
```

Expected: **7 passed**

### Step 5 — Lint check

```bash
uv run ruff check src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py src/tests/batch_processing/test_unit_gold_customer_main.py
```

Expected: no errors.

### Step 6 — Commit

```bash
cd C:\MLOps\.worktrees\spark-bronze
git add src/batch_processing/gold/silver_transactions_window_aggregate_customer_gold.py \
        src/tests/batch_processing/test_unit_gold_customer_main.py
git commit -m "refactor: replace customer Gold Python loop with Spark SQL + add tests

- compute_customer_features: register temp view, one spark.sql() with COUNT/COALESCE(AVG) CASE WHEN per window
- write_gold_partition: remove row_count = df.count() (extra full scan, logging only)
- Remove WINDOW_DAYS constant and unused pyspark.sql.types import
- Add test_unit_gold_customer_main.py: 7 tests for resolve_feature_date, compute, write

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Task 2: Terminal Gold — SQL refactor + tests

**Files:**
- Create: `src/tests/batch_processing/test_unit_gold_terminal_main.py`
- Modify: `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py`

### Step 1 — Write the failing test file

Create `src/tests/batch_processing/test_unit_gold_terminal_main.py`:

```python
"""Unit tests for Silver → Gold terminal window-features batch job.

Tests: resolve_feature_date (conf/default), _attach_fraud_label (join logic),
compute_terminal_features (SQL window counts and risk with delay offset),
write_gold_partition (first-run vs replaceWhere).
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

import batch_processing.gold.silver_transactions_window_aggregate_terminal_gold as mod

MODULE = "batch_processing.gold.silver_transactions_window_aggregate_terminal_gold"

FEATURE_DATE = datetime.date(2024, 1, 15)
# DELAY_DAYS = 7 → delay_end = fd - 7 = 2024-01-08

_TXN_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("terminal_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
])
_FRAUD_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("is_fraud", T.IntegerType()),
])
_LABELED_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.LongType()),
    T.StructField("terminal_id", T.StringType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("is_fraud", T.IntegerType()),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("test-gold-terminal").getOrCreate()


class TestResolveFeatureDate:
    def test_conf_set(self, spark):
        spark.conf.set("spark.gold.feature.date", "2024-01-15")
        assert mod.resolve_feature_date(spark) == datetime.date(2024, 1, 15)

    def test_conf_blank_defaults_to_yesterday(self, spark):
        spark.conf.set("spark.gold.feature.date", "")
        expected = datetime.date.today() - datetime.timedelta(days=1)
        assert mod.resolve_feature_date(spark) == expected


class TestAttachFraudLabel:
    """Tests for the private _attach_fraud_label helper (join logic in load_labeled_transactions)."""

    def test_join_attaches_is_fraud(self, spark):
        txn_df = spark.createDataFrame(
            [(1, "T001", datetime.date(2024, 1, 8))], schema=_TXN_SCHEMA
        )
        fraud_df = spark.createDataFrame([(1, 1)], schema=_FRAUD_SCHEMA)

        result = mod._attach_fraud_label(txn_df, fraud_df)

        assert result.first()["is_fraud"] == 1

    def test_null_fraud_becomes_zero(self, spark):
        txn_df = spark.createDataFrame(
            [(2, "T001", datetime.date(2024, 1, 8))], schema=_TXN_SCHEMA
        )
        fraud_df = spark.createDataFrame([], schema=_FRAUD_SCHEMA)

        result = mod._attach_fraud_label(txn_df, fraud_df)

        assert result.first()["is_fraud"] == 0


class TestComputeTerminalFeatures:
    def test_nb_tx_counts_per_window(self, spark):
        """Verify NB_TX counts for all 3 windows with DELAY_DAYS=7.

        Window cutoffs for FEATURE_DATE=2024-01-15:
          1D → event_date >= date_sub(fd, 8)  = 2024-01-07
          7D → event_date >= date_sub(fd, 14) = 2024-01-01
         30D → event_date >= date_sub(fd, 37) = 2023-12-09
        Upper bound (fd-7 = 2024-01-08) already enforced by load_labeled_transactions.
        """
        rows = [
            (1, "T001", datetime.date(2024, 1, 8), 0),   # 1D, 7D, 30D
            (2, "T001", datetime.date(2024, 1, 7), 0),   # 1D, 7D, 30D
            (3, "T001", datetime.date(2024, 1, 1), 0),   # 7D, 30D (not 1D: 01-01 < 01-07)
            (4, "T001", datetime.date(2023, 12, 15), 0), # 30D only
        ]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T001'").first()

        assert row["TERMINAL_NB_TX_1DAY_WINDOW"] == 2
        assert row["TERMINAL_NB_TX_7DAY_WINDOW"] == 3
        assert row["TERMINAL_NB_TX_30DAY_WINDOW"] == 4

    def test_risk_calculation(self, spark):
        """4 transactions in 1D window, 1 fraud → RISK_1D = 0.25."""
        rows = [
            (1, "T001", datetime.date(2024, 1, 8), 1),  # fraud
            (2, "T001", datetime.date(2024, 1, 8), 0),
            (3, "T001", datetime.date(2024, 1, 7), 0),
            (4, "T001", datetime.date(2024, 1, 7), 0),
        ]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T001'").first()

        assert row["TERMINAL_RISK_1DAY_WINDOW"] == pytest.approx(0.25)

    def test_zero_tx_gives_zero_risk(self, spark):
        """Terminal with no 1D transactions → RISK_1D = 0.0 (not division-by-zero)."""
        rows = [(1, "T002", datetime.date(2023, 12, 15), 0)]  # 30D only
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T002'").first()

        assert row["TERMINAL_RISK_1DAY_WINDOW"] == 0.0
        assert row["TERMINAL_NB_TX_1DAY_WINDOW"] == 0

    def test_feature_date_column_set(self, spark):
        rows = [(1, "T003", datetime.date(2024, 1, 8), 0)]
        df = spark.createDataFrame(rows, schema=_LABELED_SCHEMA)
        result = mod.compute_terminal_features(spark, df, FEATURE_DATE)
        row = result.filter("terminal_id = 'T003'").first()
        assert row["feature_date"] == FEATURE_DATE


class TestWriteGoldPartition:
    def test_first_run_creates_table(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=False)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.partitionBy.assert_called_once_with("feature_date")
        mock_writer.option.assert_not_called()

    def test_existing_table_uses_replace_where(self, spark, mocker):
        mocker.patch(f"{MODULE}.DeltaTable.isDeltaTable", return_value=True)
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save.return_value = None
        df = MagicMock()
        df.write = mock_writer

        mod.write_gold_partition(spark, df, "/fake/gold", FEATURE_DATE, "test")

        mock_writer.option.assert_called_once_with("replaceWhere", f"feature_date = '{FEATURE_DATE}'")
        mock_writer.partitionBy.assert_not_called()
```

### Step 2 — Run to confirm tests fail

```bash
cd C:\MLOps\.worktrees\spark-bronze
uv run pytest src/tests/batch_processing/test_unit_gold_terminal_main.py -v
```

Expected failures:
- `TestAttachFraudLabel` — `_attach_fraud_label` does not exist yet
- `TestComputeTerminalFeatures` — `compute_terminal_features()` takes 2 args but test passes 3

### Step 3 — Refactor the terminal Gold job

In `src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py`, apply all changes at once:

**a) Remove `WINDOW_DAYS` constant (keep `DELAY_DAYS = 7`):**

```python
# BEFORE
DELAY_DAYS = 7          # label delay offset (days)
WINDOW_DAYS = [1, 7, 30]  # window sizes — must match api/models.py

# AFTER
DELAY_DAYS = 7  # label delay offset (days)
```

**b) Add `_attach_fraud_label` private helper** immediately before `load_labeled_transactions`:

```python
def _attach_fraud_label(txn_df: DataFrame, fraud_df: DataFrame) -> DataFrame:
    """Left-join transactions with fraud cases; missing entry → is_fraud = 0."""
    return txn_df.join(fraud_df, on="transaction_id", how="left").withColumn(
        "is_fraud", F.coalesce(F.col("is_fraud"), F.lit(0))
    )
```

**c) Update `load_labeled_transactions`** to call `_attach_fraud_label` instead of inlining the join:

```python
def load_labeled_transactions(
    spark: SparkSession,
    silver_txn_path: str,
    silver_fraud_path: str,
    lookback_start: datetime.date,
    delay_end: datetime.date,
) -> DataFrame:
    """Join Silver transactions with Silver fraud cases to attach is_fraud.

    Both tables are filtered to the delay-offset look-back window
    [lookback_start, delay_end] before the join to minimise shuffle size.

    Transactions with no matching fraud_case record are treated as non-fraud
    (is_fraud = 0): a missing entry means the transaction was never flagged.
    """
    txn_df = (
        spark.read.format("delta")
        .load(silver_txn_path)
        .filter(
            (F.col("event_date") >= F.lit(lookback_start))
            & (F.col("event_date") <= F.lit(delay_end))
        )
        .select("transaction_id", "terminal_id", "event_date")
    )
    fraud_df = (
        spark.read.format("delta")
        .load(silver_fraud_path)
        .select("transaction_id", F.col("is_fraud").cast(T.IntegerType()))
    )
    return _attach_fraud_label(txn_df, fraud_df)
```

**d) Replace `compute_terminal_features` with SQL version:**

```python
def compute_terminal_features(
    spark: SparkSession, labeled_df: DataFrame, feature_date: datetime.date
) -> DataFrame:
    """One row per terminal with fraud-rate window features as of ``feature_date``.

    Each window W covers [feature_date - (DELAY_DAYS + W), feature_date - DELAY_DAYS].
    The upper bound is enforced by the Silver filter in load_labeled_transactions.
    Zero-transaction windows get RISK = 0.0 via the CASE guard.
    """
    labeled_df.createOrReplaceTempView("labeled_txn")
    return spark.sql(f"""
        SELECT
            terminal_id,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})  THEN 1 END) AS TERMINAL_NB_TX_1DAY_WINDOW,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})  THEN 1 END) AS TERMINAL_NB_TX_7DAY_WINDOW,
            COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30}) THEN 1 END) AS TERMINAL_NB_TX_30DAY_WINDOW,
            CASE
                WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})  THEN 1 END) > 0
                THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})  THEN is_fraud END) * 1.0
                   / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 1})  THEN 1 END)
                ELSE 0.0
            END AS TERMINAL_RISK_1DAY_WINDOW,
            CASE
                WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})  THEN 1 END) > 0
                THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})  THEN is_fraud END) * 1.0
                   / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 7})  THEN 1 END)
                ELSE 0.0
            END AS TERMINAL_RISK_7DAY_WINDOW,
            CASE
                WHEN COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30}) THEN 1 END) > 0
                THEN SUM(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30}) THEN is_fraud END) * 1.0
                   / COUNT(CASE WHEN event_date >= date_sub(DATE '{feature_date}', {DELAY_DAYS + 30}) THEN 1 END)
                ELSE 0.0
            END AS TERMINAL_RISK_30DAY_WINDOW,
            DATE '{feature_date}' AS feature_date
        FROM labeled_txn
        GROUP BY terminal_id
    """)
```

**e) Remove `row_count = df.count()` from `write_gold_partition` and update print:**

```python
def write_gold_partition(
    spark: SparkSession,
    df: DataFrame,
    gold_path: str,
    feature_date: datetime.date,
    label: str,
) -> None:
    """Overwrite only the ``feature_date`` partition in the Gold Delta table.

    ``replaceWhere`` leaves every other historical partition untouched so
    reruns are fully idempotent.  On first run (table absent) falls back to
    a plain partitioned write that creates the Delta table from scratch.
    """
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, gold_path):
        writer = writer.option("replaceWhere", f"feature_date = '{feature_date}'")
    else:
        writer = writer.partitionBy("feature_date")
    writer.save(gold_path)
    print(f"[gold] {label}: feature_date={feature_date} → {gold_path}")
```

**f) Update `main()`** — replace `max(WINDOW_DAYS)` with `30`, pass `spark` to compute:

```python
def main() -> None:
    spark = build_spark_session()

    silver_txn_path = spark.conf.get("spark.gold.silver.input.path")
    silver_fraud_path = spark.conf.get("spark.gold.silver.fraud.path")
    gold_path = spark.conf.get("spark.gold.terminal.output.path")

    feature_date = resolve_feature_date(spark)
    # Max look-back = delay + max_window = 7 + 30 = 37 days
    lookback_start = feature_date - datetime.timedelta(days=DELAY_DAYS + 30)
    delay_end = feature_date - datetime.timedelta(days=DELAY_DAYS)

    print(
        f"[gold] terminal features: feature_date={feature_date}"
        f"  window=[{lookback_start}, {delay_end}]"
    )

    labeled_df = load_labeled_transactions(
        spark, silver_txn_path, silver_fraud_path, lookback_start, delay_end
    )
    terminal_df = compute_terminal_features(spark, labeled_df, feature_date)
    write_gold_partition(spark, terminal_df, gold_path, feature_date, "terminal_features")
```

### Step 4 — Run tests, confirm all pass

```bash
uv run pytest src/tests/batch_processing/test_unit_gold_terminal_main.py -v
```

Expected: **10 passed**

### Step 5 — Run full test suite to confirm no regressions

```bash
uv run pytest src/tests/batch_processing/ -v
```

Expected: 9 silver + 7 customer gold + 10 terminal gold = **26 passed**

### Step 6 — Lint check

```bash
uv run ruff check src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py src/tests/batch_processing/test_unit_gold_terminal_main.py
```

Expected: no errors.

### Step 7 — Commit

```bash
cd C:\MLOps\.worktrees\spark-bronze
git add src/batch_processing/gold/silver_transactions_window_aggregate_terminal_gold.py \
        src/tests/batch_processing/test_unit_gold_terminal_main.py
git commit -m "refactor: replace terminal Gold 2-step loop with Spark SQL + add tests

- Extract _attach_fraud_label helper from load_labeled_transactions
- compute_terminal_features: one spark.sql() with CASE WHEN COUNT/SUM per window;
  eliminates intermediate _nb_tx_*/_nb_fraud_* columns and second loop
- write_gold_partition: remove row_count = df.count()
- Remove WINDOW_DAYS constant; inline 30 in main() lookback calculation
- Add test_unit_gold_terminal_main.py: 10 tests covering resolve_feature_date,
  _attach_fraud_label join logic, compute window counts + risk, write partition

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Out of Scope

- Extracting shared `resolve_feature_date` / `write_gold_partition` into `gold_utils.py` (follow-up)
- Adding watermark/CDF incremental reads to Gold (separate design)
- Changing output schema or column names (must stay aligned with `api/models.py`)
- `test_unit_silver_fraud_cases_main.py` (pre-existing gap, separate follow-up)
