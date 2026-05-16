"""Unit tests for feature_store.materialize_to_redis."""
from unittest.mock import MagicMock, patch

import pandas as pd

from feature_store.materialize_to_redis import _resolve_feature_date, materialize


def _make_client(customer_rows: int = 2, terminal_rows: int = 3) -> MagicMock:
    """Return a mock clickhouse-connect client with non-empty DataFrames."""
    client = MagicMock()
    client.query_df.side_effect = [
        pd.DataFrame(
            {
                "customer_id": range(customer_rows),
                "event_timestamp": ["2026-05-11"] * customer_rows,
                "CUSTOMER_AVG_AMOUNT_WINDOW_1D": [1.0] * customer_rows,
                "CUSTOMER_AVG_AMOUNT_WINDOW_7D": [1.0] * customer_rows,
                "CUSTOMER_AVG_AMOUNT_WINDOW_30D": [1.0] * customer_rows,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D": [1.0] * customer_rows,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D": [1.0] * customer_rows,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D": [1.0] * customer_rows,
            }
        ),
        pd.DataFrame(
            {
                "terminal_id": range(terminal_rows),
                "event_timestamp": ["2026-05-11"] * terminal_rows,
                "TERMINAL_RISK_1DAY_WINDOW": [0.1] * terminal_rows,
                "TERMINAL_RISK_7DAY_WINDOW": [0.1] * terminal_rows,
                "TERMINAL_RISK_30DAY_WINDOW": [0.1] * terminal_rows,
                "TERMINAL_NB_TX_1DAY_WINDOW": [1.0] * terminal_rows,
                "TERMINAL_NB_TX_7DAY_WINDOW": [1.0] * terminal_rows,
                "TERMINAL_NB_TX_30DAY_WINDOW": [1.0] * terminal_rows,
            }
        ),
    ]
    return client


def test_write_to_online_store_called_for_both_views() -> None:
    store = MagicMock()
    client = _make_client()
    with patch("feature_store.materialize_to_redis._get_client", return_value=client):
        materialize(store, "2026-05-11")
    assert store.write_to_online_store.call_count == 2
    calls = {c.kwargs["feature_view_name"] for c in store.write_to_online_store.call_args_list}
    assert calls == {"customer_features_view", "terminal_features_view"}
    client.close.assert_called_once()


def test_empty_customer_df_skips_write(capsys) -> None:
    store = MagicMock()
    client = MagicMock()
    client.query_df.side_effect = [
        pd.DataFrame(),  # empty customer
        pd.DataFrame({"terminal_id": [1], "event_timestamp": ["2026-05-11"],
                       "TERMINAL_RISK_1DAY_WINDOW": [0.1], "TERMINAL_RISK_7DAY_WINDOW": [0.1],
                       "TERMINAL_RISK_30DAY_WINDOW": [0.1], "TERMINAL_NB_TX_1DAY_WINDOW": [1.0],
                       "TERMINAL_NB_TX_7DAY_WINDOW": [1.0], "TERMINAL_NB_TX_30DAY_WINDOW": [1.0]}),
    ]
    with patch("feature_store.materialize_to_redis._get_client", return_value=client):
        materialize(store, "2026-05-11")
    out = capsys.readouterr().out
    assert "WARNING" in out and "customer" in out
    # only terminal write happened
    assert store.write_to_online_store.call_count == 1
    assert store.write_to_online_store.call_args.kwargs["feature_view_name"] == "terminal_features_view"


def test_resolve_feature_date_defaults_to_yesterday() -> None:
    result = _resolve_feature_date(None)
    from datetime import date, timedelta
    assert result == (date.today() - timedelta(days=1)).isoformat()


def test_resolve_feature_date_returns_provided_date() -> None:
    assert _resolve_feature_date("2026-05-11") == "2026-05-11"


def test_resolve_feature_date_rejects_bad_format() -> None:
    import pytest
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        _resolve_feature_date("not-a-date")
