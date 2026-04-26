# --- STAGE 1: Builder ---
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project --no-dev


# --- STAGE 2: Runner ---
FROM python:3.11-slim-bookworm AS runner

RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 user
WORKDIR /app

COPY --from=builder --chown=user:user /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

COPY --chown=user:user src/api/ ./api/
COPY --chown=user:user models/ ./models/

USER user

EXPOSE 8000

CMD ["opentelemetry-instrument", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
