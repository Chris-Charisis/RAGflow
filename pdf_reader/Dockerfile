# Build context is the REPO ROOT (for the shared `contracts` package).
# See compose.yaml: build.context: . / dockerfile: pdf_reader/Dockerfile
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

COPY pdf_reader/pyproject.toml pdf_reader/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Shared contract/mq/obs package (local; not in the lock).
COPY contracts ./contracts
RUN uv pip install --no-deps ./contracts

COPY pdf_reader/cli.py pdf_reader/helpers.py pdf_reader/__init__.py pdf_reader/__main__.py pdf_reader/pdf_reader.py pdf_reader/settings.py ./pdf_reader/
COPY pdf_reader/clients ./pdf_reader/clients

CMD ["python", "-m", "pdf_reader", "--watch"]
