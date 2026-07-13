from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import generate_current_oltp_data as generator  # noqa: E402


def test_fetch_latest_transaction_date_returns_calendar_date() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (datetime(2026, 7, 10, 23, 59),)
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    result = generator.fetch_latest_transaction_date(connection)

    assert result == date(2026, 7, 10)
    cursor.execute.assert_called_once()


def test_resolve_generation_range_resumes_after_latest_date() -> None:
    result = generator.resolve_generation_range(
        latest_date=date(2026, 7, 8),
        start_date=None,
        end_date=date(2026, 7, 11),
        days=None,
    )

    assert result == (date(2026, 7, 9), date(2026, 7, 11))


def test_resolve_generation_range_is_noop_when_already_current() -> None:
    result = generator.resolve_generation_range(
        latest_date=date(2026, 7, 11),
        start_date=None,
        end_date=date(2026, 7, 11),
        days=None,
    )

    assert result is None


def test_resolve_generation_range_rejects_explicit_overlap() -> None:
    with pytest.raises(ValueError, match="overlaps existing OLTP data"):
        generator.resolve_generation_range(
            latest_date=date(2026, 7, 10),
            start_date=date(2026, 7, 10),
            end_date=date(2026, 7, 11),
            days=None,
        )


def test_parse_args_defaults_to_ten_percent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["generate_current_oltp_data.py"])

    args = generator.parse_args()

    assert args.daily_scale == 0.10
