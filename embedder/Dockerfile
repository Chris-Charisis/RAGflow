FROM python:3.13.9-trixie

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

RUN chown -R appuser:appgroup /app

COPY pyproject.toml .

# Install packages
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

COPY . .

# Default: run as module, like your other services
CMD ["python", "-m", "embedder"]
