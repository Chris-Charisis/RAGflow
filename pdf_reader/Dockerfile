FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

# System deps for pdf/unstructured/opencv
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
    libjpeg-dev zlib1g-dev libffi-dev \
    libgl1 libglib2.0-0 \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

# Dependency layer from the lock. torch/torchvision resolve to CPU wheels via
# the pytorch-cpu index declared in pyproject.toml — no manual pip step needed.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Application source
COPY cli.py helpers.py __init__.py __main__.py pdf_reader.py settings.py ./pdf_reader/
COPY clients ./pdf_reader/clients

CMD ["python", "-m", "pdf_reader", "--watch"]
