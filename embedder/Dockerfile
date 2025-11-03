FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY cli.py helpers.py __init__.py __main__.py settings.py ./text_chunker/
COPY clients ./embedder/clients
COPY embedder ./embedder/embedder

# Install packages
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

# Default: run as module, like your other services
CMD ["python", "-m", "embedder"]
