FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Shared message contract (build context is the REPO ROOT — see compose.yaml).
COPY contracts ./contracts
RUN pip install --upgrade pip && pip install --no-cache-dir ./contracts

COPY vector_indexer/pyproject.toml ./
COPY vector_indexer/cli.py vector_indexer/helpers.py vector_indexer/__init__.py vector_indexer/__main__.py vector_indexer/settings.py vector_indexer/consumer.py ./vector_indexer/
COPY vector_indexer/clients ./vector_indexer/clients
COPY vector_indexer/vector_indexer ./vector_indexer/vector_indexer

# Install packages
RUN pip install --no-cache-dir -e .

# Default: run as module, like your other services
CMD ["python", "-m", "vector_indexer"]
