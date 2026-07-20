from pathlib import Path

WORKFLOW = Path(".github/workflows/build.yml")


def test_docker_metadata_does_not_resolve_a_branch_from_detached_head() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "ref: ${{ github.event.workflow_run.head_sha }}" in workflow
    assert "context: git" not in workflow
    assert "type=raw,value=${{ github.event.workflow_run.head_sha }}" in workflow


def test_workflow_uses_node24_action_releases() -> None:
    build_workflow = WORKFLOW.read_text(encoding="utf-8")
    ci_workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "actions/checkout@v6" in build_workflow
    assert "docker/login-action@v4" in build_workflow
    assert "docker/setup-buildx-action@v4" in build_workflow
    assert "docker/metadata-action@v6" in build_workflow
    assert "docker/build-push-action@v7" in build_workflow
    assert "actions/checkout@v6" in ci_workflow
