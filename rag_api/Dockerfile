# Build context is the REPO ROOT (for the shared `contracts` package, used for obs).
# See compose.yaml: build.context: . / dockerfile: rag_api/Dockerfile
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

COPY rag_api/pyproject.toml rag_api/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY contracts ./contracts
RUN uv pip install --no-deps ./contracts

COPY rag_api/__init__.py rag_api/__main__.py rag_api/cli.py rag_api/app.py rag_api/settings.py ./rag_api/
COPY rag_api/clients ./rag_api/clients
COPY rag_api/rag ./rag_api/rag

EXPOSE 8000
CMD ["python", "-m", "rag_api"]
