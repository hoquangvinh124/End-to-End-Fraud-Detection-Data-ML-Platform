from __future__ import annotations

import ast
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DAG_PATH = REPO_ROOT / "src" / "orchestration" / "dags" / "feature_pipeline_daily.py"
DISCORD_WEBHOOK_MARKER = "discord.com/api/" + "webhooks"


def test_discord_webhook_is_opt_in_and_not_hard_coded() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert DISCORD_WEBHOOK_MARKER not in source
    assignment = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "_DISCORD_WEBHOOK" for target in node.targets)
    )
    assert isinstance(assignment.value, ast.Call)
    assert len(assignment.value.args) == 2
    assert isinstance(assignment.value.args[1], ast.Constant)
    assert assignment.value.args[1].value == ""


def test_failure_callback_returns_when_webhook_is_unset() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    callback = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_notify_discord_failure"
    )

    first_statement = callback.body[1]
    assert isinstance(first_statement, ast.If)
    assert isinstance(first_statement.test, ast.UnaryOp)
    assert isinstance(first_statement.test.op, ast.Not)
    assert isinstance(first_statement.test.operand, ast.Name)
    assert first_statement.test.operand.id == "_DISCORD_WEBHOOK"
    assert isinstance(first_statement.body[0], ast.Return)


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
