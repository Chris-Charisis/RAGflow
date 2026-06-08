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

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY __init__.py __main__.py cli.py settings.py judge.py evaluate.py ./rag_eval/

# Mount your dataset here and read results from here (see compose.yaml volume).
VOLUME ["/data"]

# One-shot: evaluate then exit. Override args via `docker compose run`.
CMD ["python", "-m", "rag_eval"]
