"""Reproducible local performance benchmark for portfolio evidence.

The benchmark compares an equivalent daily aggregate on Trino Silver and
ClickHouse Gold, then load-tests the online feature-backed prediction API.
Results describe one local Docker environment and are not production SLAs.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import json
import math
import os
import platform
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("Cannot calculate a percentile from an empty sample")
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between 0 and 1")
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def normalize_summary(row: list[Any]) -> tuple[int, int, float]:
    if len(row) != 3:
        raise ValueError(f"Expected three aggregate columns, received {len(row)}")
    return int(row[0]), int(row[1]), float(row[2])


def assert_equivalent(left: list[Any], right: list[Any]) -> None:
    left_rows, left_unique, left_average = normalize_summary(left)
    right_rows, right_unique, right_average = normalize_summary(right)
    if (left_rows, left_unique) != (right_rows, right_unique):
        raise RuntimeError(
            "Silver and Gold aggregate counts differ: "
            f"silver={(left_rows, left_unique)}, gold={(right_rows, right_unique)}"
        )
    if not math.isclose(left_average, right_average, abs_tol=0.01):
        raise RuntimeError(
            "Silver and Gold average amounts differ: "
            f"silver={left_average}, gold={right_average}"
        )


def build_silver_query(date: str) -> str:
    return f"""
        SELECT count(*), count(DISTINCT transaction_id),
               round(avg(CAST(amount AS double)), 4)
        FROM lakehouse.silver.transactions
        WHERE event_date = DATE '{date}'
    """


def _total_memory_bytes() -> int | None:
    if platform.system() != "Windows":
        return None

    class MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_ulong),
            ("memory_load", ctypes.c_ulong),
            ("total_physical", ctypes.c_ulonglong),
            ("available_physical", ctypes.c_ulonglong),
            ("total_page_file", ctypes.c_ulonglong),
            ("available_page_file", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("available_virtual", ctypes.c_ulonglong),
            ("available_extended_virtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatus()
    status.length = ctypes.sizeof(MemoryStatus)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return None
    return int(status.total_physical)


class TrinoClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        self.client.close()

    def execute(self, query: str) -> list[list[Any]]:
        headers = {
            "X-Trino-User": "portfolio-benchmark",
            "X-Trino-Catalog": "lakehouse",
            "X-Trino-Schema": "silver",
        }
        response = self.client.post(
            f"{self.base_url}/v1/statement", content=query, headers=headers
        )
        response.raise_for_status()
        payload = response.json()
        rows = list(payload.get("data", []))
        while payload.get("nextUri"):
            response = self.client.get(payload["nextUri"], headers=headers)
            response.raise_for_status()
            payload = response.json()
            rows.extend(payload.get("data", []))
        if payload.get("error"):
            raise RuntimeError(f"Trino query failed: {payload['error']}")
        return rows


class ClickHouseClient:
    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        timeout_seconds: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(
            auth=(user, password), timeout=timeout_seconds
        )

    def close(self) -> None:
        self.client.close()

    def execute(self, query: str) -> list[list[Any]]:
        response = self.client.post(
            self.base_url,
            params={"default_format": "JSONCompact"},
            content=query,
        )
        response.raise_for_status()
        return response.json()["data"]


def measure_query(client: Any, query: str, warmups: int, iterations: int) -> dict[str, Any]:
    for _ in range(warmups):
        client.execute(query)

    latencies_ms: list[float] = []
    result: list[list[Any]] = []
    for _ in range(iterations):
        started = time.perf_counter()
        result = client.execute(query)
        latencies_ms.append((time.perf_counter() - started) * 1000)
    if len(result) != 1:
        raise RuntimeError(f"Expected one aggregate row, received {len(result)}")
    return {
        "result": result[0],
        "iterations": iterations,
        "p50_ms": round(percentile(latencies_ms, 0.50), 2),
        "p95_ms": round(percentile(latencies_ms, 0.95), 2),
        "p99_ms": round(percentile(latencies_ms, 0.99), 2),
    }


async def benchmark_api_run(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    requests: int,
    concurrency: int,
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(concurrency)
    latencies_ms: list[float] = []
    errors: list[str] = []

    async def send_one() -> None:
        async with semaphore:
            started = time.perf_counter()
            try:
                response = await client.post(url, json=payload)
                elapsed_ms = (time.perf_counter() - started) * 1000
                if response.status_code != 200:
                    errors.append(f"HTTP {response.status_code}")
                    return
                probability = response.json().get("fraud_probability")
                if probability is None or not 0 <= float(probability) <= 1:
                    errors.append("invalid probability")
                    return
                latencies_ms.append(elapsed_ms)
            except Exception as exc:  # noqa: BLE001
                errors.append(type(exc).__name__)

    started = time.perf_counter()
    await asyncio.gather(*(send_one() for _ in range(requests)))
    duration_seconds = time.perf_counter() - started
    if not latencies_ms:
        raise RuntimeError(f"Every API request failed: {errors[:5]}")
    return {
        "requests": requests,
        "successful_requests": len(latencies_ms),
        "errors": len(errors),
        "error_rate_percent": round(100 * len(errors) / requests, 3),
        "duration_seconds": round(duration_seconds, 3),
        "throughput_rps": round(requests / duration_seconds, 2),
        "p50_ms": round(percentile(latencies_ms, 0.50), 2),
        "p95_ms": round(percentile(latencies_ms, 0.95), 2),
        "p99_ms": round(percentile(latencies_ms, 0.99), 2),
    }


async def benchmark_api(
    base_url: str,
    payload: dict[str, Any],
    warmups: int,
    requests: int,
    concurrency: int,
    runs: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )
    async with httpx.AsyncClient(timeout=timeout_seconds, limits=limits) as client:
        endpoint = f"{base_url.rstrip('/')}/predict-online"
        for _ in range(warmups):
            response = await client.post(endpoint, json=payload)
            response.raise_for_status()
        run_results = [
            await benchmark_api_run(client, endpoint, payload, requests, concurrency)
            for _ in range(runs)
        ]

    total_requests = requests * runs
    total_errors = sum(run["errors"] for run in run_results)
    return {
        "warmup_requests": warmups,
        "requests_per_run": requests,
        "concurrency": concurrency,
        "runs": run_results,
        "median_p95_ms": round(statistics.median(run["p95_ms"] for run in run_results), 2),
        "median_throughput_rps": round(
            statistics.median(run["throughput_rps"] for run in run_results), 2
        ),
        "total_error_rate_percent": round(100 * total_errors / total_requests, 3),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-date", default="2026-07-11")
    parser.add_argument("--trino-url", default="http://localhost:8090")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--query-warmups", type=int, default=10)
    parser.add_argument("--query-iterations", type=int, default=100)
    parser.add_argument("--api-warmups", type=int, default=100)
    parser.add_argument("--api-requests", type=int, default=1000)
    parser.add_argument("--api-concurrency", type=int, default=10)
    parser.add_argument("--api-runs", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/performance-snapshot.json")
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    date = args.expected_date
    silver_query = build_silver_query(date)
    gold_query = f"""
        SELECT count(), uniqExact(transaction_id), round(avg(tx_amount), 4)
        FROM gold.mart_fraud_ml_features
        WHERE feature_date = toDate('{date}')
    """

    trino = TrinoClient(args.trino_url, args.timeout_seconds)
    clickhouse = ClickHouseClient(
        args.clickhouse_url,
        os.environ.get("CLICKHOUSE_USER", "abcbank"),
        os.environ.get("CLICKHOUSE_PASSWORD", "abcbank"),
        args.timeout_seconds,
    )
    try:
        silver = measure_query(
            trino, silver_query, args.query_warmups, args.query_iterations
        )
        gold = measure_query(
            clickhouse, gold_query, args.query_warmups, args.query_iterations
        )
        assert_equivalent(silver["result"], gold["result"])
        entity_rows = clickhouse.execute(
            f"""
            SELECT customer_id, terminal_id, tx_amount, toString(event_timestamp)
            FROM gold.mart_fraud_ml_features
            WHERE feature_date = toDate('{date}')
              AND customer_id IS NOT NULL AND terminal_id IS NOT NULL
            ORDER BY event_timestamp DESC LIMIT 1
            """
        )
    finally:
        trino.close()
        clickhouse.close()

    if len(entity_rows) != 1:
        raise RuntimeError("Could not select a benchmark entity from ClickHouse Gold")
    entity = entity_rows[0]
    payload = {
        "customer_id": int(entity[0]),
        "terminal_id": int(entity[1]),
        "TX_AMOUNT": float(entity[2]),
        "TX_DATETIME": entity[3].replace(" ", "T"),
    }
    api = asyncio.run(
        benchmark_api(
            args.api_url,
            payload,
            args.api_warmups,
            args.api_requests,
            args.api_concurrency,
            args.api_runs,
            args.timeout_seconds,
        )
    )

    speedup = silver["p95_ms"] / gold["p95_ms"]
    result = {
        "generated_at": datetime.now(UTC).isoformat(),
        "disclaimer": "Local Docker benchmark; results are evidence from one run, not a production SLA.",
        "machine": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "total_memory_bytes": _total_memory_bytes(),
        },
        "configuration": {
            "expected_date": date,
            "query_warmups": args.query_warmups,
            "query_iterations": args.query_iterations,
            "api_warmups": args.api_warmups,
            "api_requests_per_run": args.api_requests,
            "api_concurrency": args.api_concurrency,
            "api_runs": args.api_runs,
        },
        "analyst_query": {
            "equivalent_results_verified": True,
            "silver_trino": silver,
            "gold_clickhouse": gold,
            "p95_speedup": round(speedup, 2),
        },
        "online_inference": api,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
