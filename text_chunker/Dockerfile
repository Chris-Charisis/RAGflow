# Build context is the REPO ROOT (so the shared `contracts` package is available).
# See compose.yaml: build.context: . / dockerfile: text_chunker/Dockerfile
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
COPY text_chunker/pyproject.toml text_chunker/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Shared contract package (local; installed into the project venv, not in the lock).
COPY contracts ./contracts
RUN uv pip install --no-deps ./contracts

# Application source
COPY text_chunker/cli.py text_chunker/helpers.py text_chunker/__init__.py text_chunker/__main__.py text_chunker/settings.py ./text_chunker/
COPY text_chunker/clients ./text_chunker/clients
COPY text_chunker/chunker ./text_chunker/chunker

# Pre-download the tiktoken encoding so the container needs no network at runtime.
ENV TIKTOKEN_CACHE_DIR=/app/.tiktoken
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"

CMD ["python", "-m", "text_chunker"]
