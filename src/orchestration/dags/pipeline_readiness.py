from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

EXPECTED_DATASETS = {"transactions", "fraud_cases"}
_LOGGER = logging.getLogger(__name__)


def _query_datasets(
    prometheus_url: str,
    query: str,
    opener: Callable[..., Any],
) -> set[str]:
    params = urllib.parse.urlencode({"query": query})
    request = urllib.request.Request(
        f"{prometheus_url.rstrip('/')}/api/v1/query?{params}"
    )
    with opener(request, timeout=5) as response:
        payload = json.load(response)
    if payload.get("status") != "success":
        return set()
    return {
        item.get("metric", {}).get("dataset")
        for item in payload.get("data", {}).get("result", [])
        if item.get("metric", {}).get("dataset")
    }


def cdc_freshness_ready(
    prometheus_url: str,
    *,
    max_processing_delay_seconds: int = 300,
    max_heartbeat_age_seconds: int = 120,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> bool:
    """Fail closed unless both CDC datasets satisfy every readiness signal."""
    queries = (
        'min by(dataset) (mlops_pipeline_component_up{pipeline_stage="bronze"}) == 1',
        "time() - max by(dataset) "
        f'(mlops_pipeline_heartbeat_time{{pipeline_stage="bronze"}}) '
        f"<= {max_heartbeat_age_seconds}",
        "max by(dataset) "
        f'(mlops_pipeline_processing_delay{{pipeline_stage="bronze"}}) '
        f"<= {max_processing_delay_seconds}",
    )
    try:
        return all(
            EXPECTED_DATASETS <= _query_datasets(prometheus_url, query, opener)
            for query in queries
        )
    except Exception as exc:
        _LOGGER.warning("CDC freshness check failed closed: %s", exc)
        return False
