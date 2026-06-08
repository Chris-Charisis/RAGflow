# Build context is the REPO ROOT (so the shared `contracts` package is available).
# See compose.yaml: build.context: . / dockerfile: text_chunker/Dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Shared message contract (installed first for layer caching)
COPY contracts ./contracts
RUN pip install --upgrade pip && pip install --no-cache-dir ./contracts

# Application files
COPY text_chunker/pyproject.toml ./
COPY text_chunker/cli.py text_chunker/helpers.py text_chunker/__init__.py text_chunker/__main__.py text_chunker/settings.py ./text_chunker/
COPY text_chunker/clients ./text_chunker/clients
COPY text_chunker/chunker ./text_chunker/chunker

RUN pip install --no-cache-dir -e .

# Pre-download the tiktoken encoding so the container needs no network at runtime.
ENV TIKTOKEN_CACHE_DIR=/app/.tiktoken
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"

CMD ["python", "-m", "text_chunker"]
