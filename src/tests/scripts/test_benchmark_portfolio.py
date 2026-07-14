import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmark_portfolio.py"


def load_module():
    spec = importlib.util.spec_from_file_location("benchmark_portfolio", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_percentile_uses_nearest_rank() -> None:
    module = load_module()

    assert module.percentile([1, 2, 3, 4, 5], 0.5) == 3
    assert module.percentile(list(range(1, 101)), 0.95) == 95


def test_percentile_rejects_empty_samples() -> None:
    module = load_module()

    with pytest.raises(ValueError, match="empty sample"):
        module.percentile([], 0.95)


def test_equivalent_aggregates_allow_rounding_noise() -> None:
    module = load_module()

    module.assert_equivalent([36515, 36515, 104.123], [36515, 36515, 104.129])


def test_equivalent_aggregates_reject_different_counts() -> None:
    module = load_module()

    with pytest.raises(RuntimeError, match="aggregate counts differ"):
        module.assert_equivalent([36515, 36515, 104.12], [36514, 36514, 104.12])


def test_silver_query_matches_runtime_table_schema() -> None:
    module = load_module()

    query = module.build_silver_query("2026-07-11")

    assert "event_date = DATE '2026-07-11'" in query
    assert "_deleted" not in query
