from __future__ import annotations

import json
import pathlib
import sys
from unittest.mock import patch

MODULE_DIR = pathlib.Path(__file__).resolve().parents[2] / "cdc_ingestion"
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from utils import schema_registry_helpers as helpers  # noqa: E402


class _Response:
    def __init__(self, payload: object):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


def test_fetch_uses_registered_subject_without_kafka_fallback():
    responses = iter(
        [
            _Response(["cdc.transactions-value"]),
            _Response({"schema": '{"type":"record"}'}),
        ]
    )
    with (
        patch.object(
            helpers.urllib.request, "urlopen", side_effect=lambda *_: next(responses)
        ) as urlopen,
        patch.object(helpers, "_latest_schema_id") as latest_schema_id,
    ):
        schema = helpers.fetch_avro_schema(
            "http://schema-registry:8081",
            "cdc.transactions-value",
            retries=1,
            delay=0,
        )

    assert schema == '{"type":"record"}'
    assert urlopen.call_args_list[0].args[0].endswith("/subjects?deleted=true")
    latest_schema_id.assert_not_called()


def test_fetch_restores_missing_subject_from_latest_kafka_schema_id():
    requests: list[object] = []

    def fake_urlopen(request, *_args, **_kwargs):
        requests.append(request)
        url = request.full_url if hasattr(request, "full_url") else request
        if url.endswith("/subjects?deleted=true"):
            return _Response([])
        if url.endswith("/schemas/ids/4"):
            return _Response({"schema": '{"type":"record","name":"Tx"}'})
        if url.endswith("/subjects/cdc.transactions-value/versions"):
            return _Response({"id": 4})
        raise AssertionError(f"unexpected URL: {url}")

    with (
        patch.object(helpers.urllib.request, "urlopen", side_effect=fake_urlopen),
        patch.object(helpers, "_latest_schema_id", return_value=4),
    ):
        schema = helpers.fetch_avro_schema(
            "http://schema-registry:8081",
            "cdc.transactions-value",
            retries=1,
            delay=0,
            kafka_bootstrap_servers="kafka:9092",
        )

    assert schema == '{"type":"record","name":"Tx"}'
    registration = requests[-1]
    assert registration.get_method() == "POST"
    assert json.loads(registration.data) == {"schema": schema}


def test_schema_id_parser_rejects_non_confluent_payloads():
    assert helpers._schema_id_from_value(b"\x00\x00\x00\x00\x04payload") == 4
    try:
        helpers._schema_id_from_value(b"plain-json")
    except ValueError as exc:
        assert "Confluent Avro" in str(exc)
    else:
        raise AssertionError("non-Confluent payload should be rejected")
