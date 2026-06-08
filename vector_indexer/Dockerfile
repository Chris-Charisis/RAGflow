# Build context is the REPO ROOT (so the shared `contracts` package is available).
# See compose.yaml: build.context: . / dockerfile: vector_indexer/Dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

# Locked PyPI dependencies (cached layer).
COPY vector_indexer/pyproject.toml vector_indexer/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Shared contract package (local; installed into the project venv, not in the lock).
COPY contracts ./contracts
RUN uv pip install --no-deps ./contracts

# Application source
COPY vector_indexer/cli.py vector_indexer/helpers.py vector_indexer/__init__.py vector_indexer/__main__.py vector_indexer/settings.py vector_indexer/consumer.py ./vector_indexer/
COPY vector_indexer/clients ./vector_indexer/clients
COPY vector_indexer/vector_indexer ./vector_indexer/vector_indexer

CMD ["python", "-m", "vector_indexer"]
