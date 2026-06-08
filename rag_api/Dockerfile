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

# Dependency layer (cached) — installs the locked deps into /app/.venv.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Application source (run from source as a package).
COPY __init__.py __main__.py cli.py app.py settings.py ./rag_api/
COPY clients ./rag_api/clients
COPY rag ./rag_api/rag

EXPOSE 8000
CMD ["python", "-m", "rag_api"]
