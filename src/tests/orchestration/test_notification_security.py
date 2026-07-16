from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
ALERTMANAGER_CONFIG = (
    REPO_ROOT / "src" / "monitoring" / "alertmanager" / "alertmanager.yml"
)
OBSERVABILITY_COMPOSE = (
    REPO_ROOT / "src" / "monitoring" / "docker-compose.observability.yml"
)
DISCORD_WEBHOOK_MARKER = "discord.com/api/" + "webhooks"


def test_discord_webhook_is_read_from_a_secret_file() -> None:
    config = yaml.safe_load(ALERTMANAGER_CONFIG.read_text(encoding="utf-8"))
    receiver = config["receivers"][0]["discord_configs"][0]
    assert receiver["webhook_url_file"] == "/run/secrets/discord_webhook_url"


def test_empty_webhook_disables_external_delivery() -> None:
    compose = OBSERVABILITY_COMPOSE.read_text(encoding="utf-8")
    assert "DISCORD_WEBHOOK_URL: ${DISCORD_WEBHOOK_URL:-}" in compose
    assert "http://127.0.0.1/discord-disabled" in compose


def test_repository_contains_no_discord_webhook_url() -> None:
    tracked_extensions = {".py", ".md", ".yml", ".yaml", ".json", ".toml"}
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    offenders = []
    for relative_path in result.stdout.splitlines():
        path = REPO_ROOT / relative_path
        if not path.is_file() or path.suffix.lower() not in tracked_extensions:
            continue
        if DISCORD_WEBHOOK_MARKER in path.read_text(encoding="utf-8", errors="ignore"):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []
