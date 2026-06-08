FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md app.py ./

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -e .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
