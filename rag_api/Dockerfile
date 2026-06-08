FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY __init__.py __main__.py cli.py app.py settings.py ./rag_api/
COPY clients ./rag_api/clients
COPY rag ./rag_api/rag

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

EXPOSE 8000

# Run as a module, like the other services.
CMD ["python", "-m", "rag_api"]
