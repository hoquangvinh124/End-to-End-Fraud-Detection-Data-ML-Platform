from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "e2e_demo.ps1"


def test_e2e_demo_covers_every_persistent_boundary() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    required_markers = [
        "Invoke-Docker compose ps",
        "banking.transactions",
        "gold.mart_fraud_ml_features",
        "redis-cli",
        "http://localhost:5000/health",
        "http://localhost:8000/health",
        "http://localhost:8000/predict-online",
        '(Get-Date).ToString("yyyy-MM-dd")',
    ]
    for marker in required_markers:
        assert marker in source


def test_e2e_demo_enables_strict_error_handling() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert '$ErrorActionPreference = "Stop"' in source
    assert "Assert-Equal" in source
    assert "Assert-True" in source
    assert "Wait-HttpJson" in source
