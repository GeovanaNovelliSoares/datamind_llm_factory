FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential libpq-dev libmagic1 \
    && rm -rf /var/lib/apt/lists/*

FROM base AS deps
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

FROM deps AS api
COPY src/ ./src/
COPY pyproject.toml* ./
RUN mkdir -p data/chroma data/reports mlflow_tracking
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/ready || exit 1
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

FROM deps AS worker
COPY src/ ./src/
RUN mkdir -p data/chroma data/reports mlflow_tracking
CMD ["celery", "-A", "src.workers.celery_app.celery_app", "worker", \
     "--loglevel=info", "--queues=etl", "--concurrency=2"]

FROM python:3.11-slim AS ui
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN pip install streamlit>=1.35.0 httpx>=0.27.0 plotly>=5.22.0 pandas>=2.2.2
COPY app/ ./app/
EXPOSE 8501
CMD ["streamlit", "run", "app/streamlit_app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
