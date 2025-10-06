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

# Force CPU wheels for torch/vision first (prevents CUDA deps)
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
    torch==2.8 torchvision==0.23.0

# Explicitly add unstructured-inference (version compatible with 0.17.x)
RUN pip install --no-cache-dir "unstructured-inference>=0.7,<0.8" opencv-python-headless

# Install python deps first (better layer caching)
COPY pyproject.toml ./
COPY . ./pdf_reader/

# If your package is in a subfolder, copy it now for editable install
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

# Copy the rest (scripts, etc.)
COPY . .

# EXPOSE 8000

# Default command: module entry (same as `python -m pdf_reader`)
CMD ["python", "-m", "pdf_reader", "--watch"]