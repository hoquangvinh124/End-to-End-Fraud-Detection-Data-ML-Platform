"""Materialize online feature views (customer, terminal) to Redis.

Usage:
    uv run python src/feature_store/materialize_to_redis.py
    uv run python src/feature_store/materialize_to_redis.py --start-date 2024-01-01

Only views with online=True are materialized. fraud_ml_features_view is offline-only (for training)
and is intentionally excluded from materialization.
"""
import argparse
from datetime import datetime, timezone
from pathlib import Path

from feast import FeatureStore


def materialize(store: FeatureStore, start_date: datetime | None = None) -> None:
    end_date = datetime.now(tz=timezone.utc)
    if start_date:
        store.materialize(start_date=start_date, end_date=end_date)
    else:
        store.materialize_incremental(end_date=end_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize online features to Redis")
    parser.add_argument("--start-date", type=str, default=None, help="ISO start date (YYYY-MM-DD)")
    args = parser.parse_args()

    repo_path = Path(__file__).parent
    store = FeatureStore(repo_path=str(repo_path))

    start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc) if args.start_date else None
    materialize(store, start_date)


if __name__ == "__main__":
    main()
