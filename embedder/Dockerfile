# Build context is the REPO ROOT (for the shared `contracts` package).
# See compose.yaml: build.context: . / dockerfile: embedder/Dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY embedder/pyproject.toml embedder/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY contracts ./contracts
RUN uv pip install --no-deps ./contracts

COPY embedder/cli.py embedder/helpers.py embedder/__init__.py embedder/__main__.py embedder/settings.py ./embedder/
COPY embedder/clients ./embedder/clients
COPY embedder/embedder ./embedder/embedder

CMD ["python", "-m", "embedder"]
