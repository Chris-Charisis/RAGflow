FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY cli.py helpers.py __init__.py __main__.py settings.py consumer.py ./vector_indexer/
COPY clients ./vector_indexer/clients
COPY vector_indexer ./vector_indexer/vector_indexer

# Install packages
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

# Default: run as module, like your other services
CMD ["python", "-m", "vector_indexer"]
