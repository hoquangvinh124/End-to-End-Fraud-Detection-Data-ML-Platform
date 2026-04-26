# GHCR Build Workflow Design

## Problem

The repository already has a CI workflow for linting and tests, but it does not yet build and publish the Docker image to GitHub Container Registry (GHCR). The project already has a production Dockerfile and a GitHub repository slug that maps naturally to a GHCR package path.

## Proposed approach

Add a separate GitHub Actions workflow at `.github/workflows/build.yml` dedicated to Docker image build and publish.

The workflow will:

- trigger only on pushes to `vinh-branch` and `main`
- build from `./dockerfile`
- publish to `ghcr.io/hoquangvinh124/mlops`
- use `docker/metadata-action` for tags and OCI labels
- use `docker/build-push-action` to build and push the image

## Workflow structure

### Triggers

- `push` to `vinh-branch`
- `push` to `main`

No pull request trigger is included in this scope.

### Permissions

The workflow should request only the permissions required for checkout and GHCR publishing:

- `contents: read`
- `packages: write`

### Job layout

Use one job, for example `docker`, on `ubuntu-latest`.

Suggested step flow:

1. Check out the repository
2. Log in to GHCR with `docker/login-action` using `GITHUB_TOKEN`
3. Generate image tags and labels with `docker/metadata-action`
4. Build and push the image with `docker/build-push-action`

## Image naming and tags

Image name:

- `ghcr.io/hoquangvinh124/mlops`

Tags:

- on `main`: `latest` and the commit SHA tag
- on `vinh-branch`: `vinh-branch` and the commit SHA tag

The SHA tag is included so every pushed image can be traced back to the exact commit that produced it.

## Build inputs and defaults

- Dockerfile path: `./dockerfile`
- Build context: repository root
- Platform scope: `linux/amd64` only

This design intentionally avoids adding multi-platform builds, image signing, SBOM/provenance, or vulnerability scanning in the first version.

## Failure behavior

- If checkout fails, the workflow fails.
- If GHCR login fails, the workflow fails.
- If Docker build fails, the workflow fails.
- If image push fails, the workflow fails.

There is no soft-fail or retry logic in this scope.

## Documentation updates

Update `docs/architecture.md` so the CI/CD section reflects the current split:

- GitHub Actions CI for lint and tests
- GitHub Actions build workflow for Docker build and GHCR publish
- Future CD/deploy flow remains separate

## Verification strategy

Implementation should verify:

- the workflow file structure is valid
- the image reference is exactly `ghcr.io/hoquangvinh124/mlops`
- tags are generated as designed for `main` and `vinh-branch`
- the existing Dockerfile can still be built successfully in the current environment when feasible

## Scope boundaries

This design does not include:

- pull-request image publishing
- deployment
- Artifact Registry publishing
- image scanning
- image signing
- multi-platform builds
