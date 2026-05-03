"""Unit tests for feature_store.materialize_to_redis."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from feature_store.materialize_to_redis import materialize


def test_materialize_incremental_called_when_no_start_date() -> None:
    store = MagicMock()
    materialize(store, start_date=None)
    store.materialize_incremental.assert_called_once()
    _, kwargs = store.materialize_incremental.call_args
    end_date = kwargs["end_date"]
    assert isinstance(end_date, datetime)
    assert end_date.tzinfo is not None
    store.materialize.assert_not_called()


def test_materialize_range_called_when_start_date_given() -> None:
    store = MagicMock()
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    materialize(store, start_date=start)
    store.materialize.assert_called_once()
    _, kwargs = store.materialize.call_args
    assert kwargs["start_date"] == start
    end_date = kwargs["end_date"]
    assert isinstance(end_date, datetime)
    assert end_date.tzinfo is not None
    store.materialize_incremental.assert_not_called()
