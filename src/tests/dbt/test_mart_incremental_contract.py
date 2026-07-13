from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MART_PATH = (
    REPO_ROOT
    / "src"
    / "dbt"
    / "models"
    / "marts"
    / "machine_learning"
    / "mart_fraud_ml_features.sql"
)


def test_ml_mart_skips_existing_transaction_ids_on_retry() -> None:
    source = MART_PATH.read_text(encoding="utf-8")

    assert "incremental_strategy = 'append'" in source
    assert "unique_key   = 'transaction_id'" in source
    assert "LEFT JOIN {{ this }} existing" in source
    assert "existing.transaction_id IS NULL" in source
