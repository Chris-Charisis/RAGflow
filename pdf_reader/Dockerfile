FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

# PyMuPDF4LLM ships self-contained wheels — no system pdf/OCR libs needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY cli.py helpers.py __init__.py __main__.py pdf_reader.py settings.py ./pdf_reader/
COPY clients ./pdf_reader/clients

CMD ["python", "-m", "pdf_reader", "--watch"]
