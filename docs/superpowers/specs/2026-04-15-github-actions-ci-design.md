# GitHub Actions CI Design

## Problem

The repository does not yet have a GitHub Actions workflow for validating Python changes on pull requests and pushes to `main`. The project uses `uv`, targets Python 3.11, and needs CI to enforce linting, tests, and a minimum coverage threshold.

## Proposed approach

Add a single workflow at `.github/workflows/ci.yml` with two independent jobs:

- `lint` runs `uv run ruff check .`
- `test` runs `uv run pytest --cov=api --cov-report=term-missing --cov-fail-under=80`

The workflow triggers on pull requests and pushes to `main`.

## Workflow structure

### Triggers

- `pull_request`
- `push` to `main`

### Job: `lint`

Steps:

1. Check out the repository
2. Set up Python 3.11
3. Install `uv` with `astral-sh/setup-uv@v8.0.0`
4. Install project and development dependencies with `uv sync --frozen --group dev`
5. Run `uv run ruff check .`

### Job: `test`

Steps:

1. Check out the repository
2. Set up Python 3.11
3. Install `uv` with `astral-sh/setup-uv@v8.0.0`
4. Install project and development dependencies with `uv sync --frozen --group dev`
5. Run `uv run pytest --cov=api --cov-report=term-missing --cov-fail-under=80`

## Data flow and execution model

- Each workflow run starts from a clean GitHub-hosted runner.
- Both jobs prepare the same pinned dependency environment from `uv.lock`.
- `lint` and `test` run independently so failures are isolated and easier to read in the GitHub UI.
- The workflow result is failed if either job fails.

## Caching

- Cache `uv` dependency artifacts to reduce repeated install time.
- The cache should be invalidated when `uv.lock` changes.

## Error handling and failure behavior

- Lint errors cause the `lint` job to fail immediately.
- Test failures, import errors, or coverage below 80% cause the `test` job to fail.
- No retry or soft-fail behavior is included; CI is intended to block regressions clearly.

## Scope boundaries

This design intentionally does not include:

- Docker image build
- Package publishing
- Deployment
- Coverage artifact upload
- Multi-version Python test matrix

These can be added later without changing the overall workflow layout.

## Testing strategy

The CI workflow should rely on the repository's existing tools only:

- `ruff` for linting
- `pytest` with `pytest-cov` for test execution and coverage enforcement

The expected success condition is:

- `ruff check .` passes
- `pytest` passes
- Coverage for `api` is at least 80%
