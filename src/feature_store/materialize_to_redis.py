"""Materialize online feature views (customer, terminal) to Redis from ClickHouse.

Reads the latest feature_date partition from ClickHouse intermediate tables using
clickhouse-connect and pushes directly to Redis via feast.write_to_online_store().
This bypasses the offline store abstraction (no official Feast ClickHouse plugin).

Usage:
    uv run python src/feature_store/materialize_to_redis.py
    uv run python src/feature_store/materialize_to_redis.py --feature-date 2026-05-11
"""
from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from pathlib import Path

import clickhouse_connect
import pandas as pd
from feast import FeatureStore


def _get_client() -> clickhouse_connect.driver.Client:
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port_str = os.environ.get("CLICKHOUSE_PORT", "8123")
    try:
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f"CLICKHOUSE_PORT must be an integer, got: {port_str!r}"
        ) from None
    return clickhouse_connect.get_client(host=host, port=port)


def _resolve_feature_date(feature_date_str: str | None) -> str:
    """Return YYYY-MM-DD for the target date; defaults to yesterday."""
    if feature_date_str:
        try:
            date.fromisoformat(feature_date_str)
        except ValueError:
            raise ValueError(
                f"--feature-date must be YYYY-MM-DD, got: {feature_date_str!r}"
            ) from None
        return feature_date_str
    return (date.today() - timedelta(days=1)).isoformat()


def materialize(store: FeatureStore, feature_date: str) -> None:
    client = _get_client()
    try:
        # -- Customer features ----------------------------------------------
        customer_df: pd.DataFrame = client.query_df(
            """
            SELECT
                customer_id,
                feature_date                              AS event_timestamp,
                CUSTOMER_AVG_AMOUNT_WINDOW_1D,
                CUSTOMER_AVG_AMOUNT_WINDOW_7D,
                CUSTOMER_AVG_AMOUNT_WINDOW_30D,
                CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
                CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
                CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D
            FROM intermediate.int_customer_window_features
            WHERE feature_date = {fd:Date}
            """,
            parameters={"fd": feature_date},
        )
        if customer_df.empty:
            print(
                f"[materialize] WARNING: 0 customer rows for {feature_date} — "
                "skipping write. Check upstream dbt model."
            )
        else:
            customer_df["event_timestamp"] = pd.to_datetime(
                customer_df["event_timestamp"], utc=True
            )
            store.write_to_online_store(
                feature_view_name="customer_features_view",
                df=customer_df,
            )
            print(f"[materialize] pushed {len(customer_df)} customer rows for {feature_date}")

        # -- Terminal features ----------------------------------------------
        terminal_df: pd.DataFrame = client.query_df(
            """
            SELECT
                terminal_id,
                feature_date                    AS event_timestamp,
                TERMINAL_RISK_1DAY_WINDOW,
                TERMINAL_RISK_7DAY_WINDOW,
                TERMINAL_RISK_30DAY_WINDOW,
                TERMINAL_NB_TX_1DAY_WINDOW,
                TERMINAL_NB_TX_7DAY_WINDOW,
                TERMINAL_NB_TX_30DAY_WINDOW
            FROM intermediate.int_terminal_window_features
            WHERE feature_date = {fd:Date}
            """,
            parameters={"fd": feature_date},
        )
        if terminal_df.empty:
            print(
                f"[materialize] WARNING: 0 terminal rows for {feature_date} — "
                "skipping write. Check upstream dbt model."
            )
        else:
            terminal_df["event_timestamp"] = pd.to_datetime(
                terminal_df["event_timestamp"], utc=True
            )
            store.write_to_online_store(
                feature_view_name="terminal_features_view",
                df=terminal_df,
            )
            print(f"[materialize] pushed {len(terminal_df)} terminal rows for {feature_date}")

    except clickhouse_connect.driver.exceptions.ClickHouseError as exc:
        raise RuntimeError(
            f"ClickHouse query failed for {feature_date}: {exc}"
        ) from exc
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize online features to Redis from ClickHouse"
    )
    parser.add_argument(
        "--feature-date",
        type=str,
        default=None,
        help="ISO date YYYY-MM-DD to materialize; defaults to yesterday",
    )
    args = parser.parse_args()

    feature_date = _resolve_feature_date(args.feature_date)
    repo_path = Path(__file__).parent
    store = FeatureStore(repo_path=str(repo_path))
    materialize(store, feature_date)


if __name__ == "__main__":
    main()
