"""Materialize online feature views (customer, terminal) to Redis from ClickHouse.

Reads the daily gold ML feature mart using clickhouse-connect and pushes the
latest entity feature rows directly to Redis via feast.write_to_online_store().
This bypasses the offline store abstraction because the project does not depend
on an official Feast ClickHouse offline store plugin.

Usage:
    uv run python src/feature_store/materialize_to_redis.py
    uv run python src/feature_store/materialize_to_redis.py --feature-date 2026-05-11
"""
from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
import pandas as pd
from feast import FeatureStore

from pipeline_monitoring.telemetry import MetricSink, OtelMetricSink


def _get_client() -> clickhouse_connect.driver.Client:
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port_str = os.environ.get("CLICKHOUSE_PORT", "8123")
    username = os.environ.get("CLICKHOUSE_USER", "abcbank")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "abcbank")
    try:
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f"CLICKHOUSE_PORT must be an integer, got: {port_str!r}"
        ) from None
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
    )


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


def materialize(
    store: FeatureStore,
    feature_date: str,
    *,
    sink: MetricSink | None = None,
    clock: Callable[[], float] = time.time,
) -> None:
    client = _get_client()
    started_at = clock()
    base_labels = {
        "dataset": "features",
        "pipeline.stage": "online",
        "service.name": "feast-materializer",
    }
    try:
        # -- Customer features ----------------------------------------------
        customer_df: pd.DataFrame = client.query_df(
            """
            SELECT
                toInt64(customer_id) AS customer_id,
                max(mart.event_timestamp) AS event_timestamp,
                argMax(customer_avg_amount_window_1d, mart.event_timestamp) AS CUSTOMER_AVG_AMOUNT_WINDOW_1D,
                argMax(customer_avg_amount_window_7d, mart.event_timestamp) AS CUSTOMER_AVG_AMOUNT_WINDOW_7D,
                argMax(customer_avg_amount_window_30d, mart.event_timestamp) AS CUSTOMER_AVG_AMOUNT_WINDOW_30D,
                toInt64(argMax(customer_number_of_transactions_window_1d, mart.event_timestamp)) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D,
                toInt64(argMax(customer_number_of_transactions_window_7d, mart.event_timestamp)) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D,
                toInt64(argMax(customer_number_of_transactions_window_30d, mart.event_timestamp)) AS CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D
            FROM gold.mart_fraud_ml_features AS mart
            WHERE feature_date = {fd:Date}
            GROUP BY customer_id
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
            customer_df["feature_date"] = customer_df["event_timestamp"]
            store.write_to_online_store(
                feature_view_name="customer_features_view",
                df=customer_df,
            )
            if sink:
                sink.add_counter(
                    "mlops.pipeline.records.processed",
                    float(len(customer_df)),
                    {**base_labels, "dataset": "customer_features"},
                )
            print(f"[materialize] pushed {len(customer_df)} customer rows for {feature_date}")

        # -- Terminal features ----------------------------------------------
        terminal_df: pd.DataFrame = client.query_df(
            """
            SELECT
                toInt64(terminal_id) AS terminal_id,
                max(mart.event_timestamp) AS event_timestamp,
                argMax(terminal_risk_1day_window, mart.event_timestamp) AS TERMINAL_RISK_1DAY_WINDOW,
                argMax(terminal_risk_7day_window, mart.event_timestamp) AS TERMINAL_RISK_7DAY_WINDOW,
                argMax(terminal_risk_30day_window, mart.event_timestamp) AS TERMINAL_RISK_30DAY_WINDOW,
                toInt64(argMax(terminal_nb_tx_1day_window, mart.event_timestamp)) AS TERMINAL_NB_TX_1DAY_WINDOW,
                toInt64(argMax(terminal_nb_tx_7day_window, mart.event_timestamp)) AS TERMINAL_NB_TX_7DAY_WINDOW,
                toInt64(argMax(terminal_nb_tx_30day_window, mart.event_timestamp)) AS TERMINAL_NB_TX_30DAY_WINDOW
            FROM gold.mart_fraud_ml_features AS mart
            WHERE feature_date = {fd:Date}
            GROUP BY terminal_id
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
            terminal_df["feature_date"] = terminal_df["event_timestamp"]
            store.write_to_online_store(
                feature_view_name="terminal_features_view",
                df=terminal_df,
            )
            if sink:
                sink.add_counter(
                    "mlops.pipeline.records.processed",
                    float(len(terminal_df)),
                    {**base_labels, "dataset": "terminal_features"},
                )
            print(f"[materialize] pushed {len(terminal_df)} terminal rows for {feature_date}")

        if sink:
            watermark = datetime.combine(
                date.fromisoformat(feature_date), datetime.min.time(), tzinfo=timezone.utc
            ).timestamp()
            sink.set_gauge(
                "mlops.pipeline.data.watermark.time", watermark, base_labels
            )
            sink.set_gauge(
                "mlops.pipeline.last_success.time", clock(), base_labels
            )
            sink.set_gauge("mlops.pipeline.component.up", 1.0, base_labels)
            sink.record_histogram(
                "mlops.pipeline.batch.duration", clock() - started_at, base_labels
            )

    except clickhouse_connect.driver.exceptions.ClickHouseError as exc:
        if sink:
            sink.set_gauge("mlops.pipeline.component.up", 0.0, base_labels)
            sink.add_counter(
                "mlops.pipeline.failures",
                1.0,
                {**base_labels, "status": "failed"},
            )
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
    sink = OtelMetricSink("feast-materializer")
    materialize(store, feature_date, sink=sink)
    sink.force_flush()


if __name__ == "__main__":
    main()
