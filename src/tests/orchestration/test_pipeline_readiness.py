from __future__ import annotations

import io
import json

from orchestration.dags.pipeline_readiness import cdc_freshness_ready


def _response(datasets: set[str]):
    payload = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"dataset": dataset}, "value": [1, "1"]}
                for dataset in datasets
            ],
        },
    }
    return io.BytesIO(json.dumps(payload).encode())


def test_gate_is_ready_when_all_three_queries_return_both_datasets():
    calls: list[str] = []

    def opener(request, timeout):
        calls.append(request.full_url)
        return _response({"transactions", "fraud_cases"})

    assert cdc_freshness_ready("http://prometheus:9090", opener=opener) is True
    assert len(calls) == 3
    assert all("/api/v1/query?" in url for url in calls)


def test_gate_waits_when_any_signal_is_missing_a_dataset():
    responses = iter(
        [
            _response({"transactions", "fraud_cases"}),
            _response({"transactions"}),
            _response({"transactions", "fraud_cases"}),
        ]
    )

    assert (
        cdc_freshness_ready(
            "http://prometheus:9090", opener=lambda request, timeout: next(responses)
        )
        is False
    )


def test_gate_fails_closed_when_prometheus_is_unavailable():
    def unavailable(request, timeout):
        raise OSError("connection refused")

    assert cdc_freshness_ready("http://prometheus:9090", opener=unavailable) is False
