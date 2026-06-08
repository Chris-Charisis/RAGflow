FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY __init__.py __main__.py cli.py settings.py judge.py evaluate.py ./rag_eval/

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

# Mount your dataset here and read results from here (see compose.yaml volume).
VOLUME ["/data"]

# One-shot: evaluate then exit. Override args via `docker compose run`.
CMD ["python", "-m", "rag_eval"]
