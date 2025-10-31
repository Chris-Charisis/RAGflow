FROM python:3.12-slim

# System deps for pdf/unstructured/opencv
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential \
    libjpeg-dev zlib1g-dev libffi-dev \
    libgl1 libglib2.0-0 \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy application files and folders
COPY pyproject.toml ./
COPY cli.py helpers.py __init__.py __main__.py settings.py ./text_chunker/
COPY clients ./text_chunker/clients
COPY chunker ./text_chunker/chunker

# Install packages
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

# Default command: module entry (same as `python -m text_chunker`)
CMD ["python", "-m", "text_chunker"]