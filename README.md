# DataMind Intelligent Business ETL + LLM

<div align="center">

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://postgresql.org)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

**Production-grade ETL pipeline with LLM enrichment for SaaS business data.**
Ask questions in natural language. Detect anomalies automatically. Generate PDF reports with one API call.

[**Live Demo**](https://datamind.streamlit.app) · [**API Docs**](https://datamind-api.onrender.com/docs) · [**Grafana Dashboard**](https://datamind-api.onrender.com/grafana)

</div>

---

## The Problem

SaaS companies accumulate critical business data  MRR, churn, subscriptions  scattered across spreadsheets, CRMs, and databases. Extracting insights from this data requires:

- A data engineer to build ETL pipelines
- An analyst to write SQL queries
- Hours of manual work to produce monthly reports

**DataMind automates all three.**

---

## What It Does

| Capability | How |
|---|---|
| **Ingest any source** | CSV, Excel, PostgreSQL, REST API |
| **Clean intelligently** | null handling, type inference, outlier capping |
| **Enrich with LLM** | classifies plan tiers, churn reasons, industries, risk scores |
| **Validate data quality** | rule-based gates (null ratios, value ranges, uniqueness) |
| **Answer questions** | natural language → SQL → business answer |
| **Detect anomalies** | Z-score + IQR + LLM explanation in plain English |
| **Generate reports** | full narrative PDF report from a single API call |
| **Monitor everything** | Prometheus metrics + Grafana dashboard |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│         CSV / Excel     PostgreSQL     REST API     Webhook         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CONNECTOR FACTORY + SCHEMA DETECTOR              │
│     auto-detects column types · maps to SaaS canonical schema       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Celery async job
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE                                 │
│                                                                     │
│   Cleaner          Transformer         LLM Enricher    Validator    │
│   ───────          ───────────         ────────────    ─────────    │
│   nulls            ARR, lifetime       plan_category   rule-based   │
│   duplicates       monthly metrics     churn_reason    data gates   │
│   outliers         churn_rate          risk_score                   │
│   type cast        ARPU, NRR           industry                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           STORAGE                                   │
│      PostgreSQL              Redis               ChromaDB           │
│      ──────────              ─────               ────────           │
│      subscriptions           hot metrics         embeddings         │
│      monthly metrics         cache TTL           (future RAG)       │
│      anomalies, reports      job results                            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       QUERY + INSIGHT ENGINE                        │
│                                                                     │
│   NL→SQL Agent         Anomaly Detector      Report Generator       │
│   ────────────         ────────────────      ────────────────       │
│   question → SQL       Z-score + IQR         LLM narrative          │
│   execute + answer     LLM explanation       Markdown + PDF         │
│   query history        severity rating       executive format       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    OUTPUT  FastAPI + Streamlit                      │
│   POST /query    GET /insights/anomalies    GET /insights/report    │
│   Streamlit UI with 6 pages   PDF download   Prometheus /metrics    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack  100% Free to Run

| Layer | Technology | Why This Choice |
|---|---|---|
| **API** | FastAPI + Uvicorn | async, OpenAPI auto-docs, SSE streaming |
| **Task queue** | Celery + Redis | async ETL, automatic retries, progress tracking |
| **Connectors** | pandas, SQLAlchemy, httpx | CSV, SQL, REST in unified interface |
| **ETL** | pandas + custom rules | deterministic, auditable, no black boxes |
| **LLM enrichment** | Groq llama-3.1-8b | free tier, 200ms, classifies free-text fields |
| **LLM fallback** | Ollama + Mistral | fully local, zero cost, privacy-safe |
| **NL→SQL** | LLM + SQLAlchemy | schema in context, no retrieval needed |
| **Anomaly detection** | Z-score + IQR + LLM | statistical precision + business explanation |
| **PDF reports** | weasyprint + markdown | no paid API, full control over layout |
| **Database** | PostgreSQL 16 | production-grade, relational, ACID |
| **Cache** | Redis | hot metrics, Celery broker + result backend |
| **Vector store** | ChromaDB | embeddings for future RAG layer |
| **Validation** | custom rules | data quality gates in CI pipeline |
| **Monitoring** | Prometheus + Grafana | latency, token usage, ETL errors |
| **Experiments** | MLflow | ETL config tracking across runs |
| **Deploy** | Render + Streamlit Cloud | free tier, zero-config |
| **CI/CD** | GitHub Actions | lint → test → docker → deploy |

---

## Project Structure

```
datamind/
├── src/
│   ├── config.py                        # pydantic-settings  single source of truth
│   ├── logger.py                        # structlog JSON structured logging
│   ├── ingestion/
│   │   ├── connectors/
│   │   │   ├── base.py                  # BaseConnector interface
│   │   │   ├── csv_connector.py         # CSV, Excel, TSV
│   │   │   ├── sql_connector.py         # PostgreSQL via SQLAlchemy
│   │   │   └── api_connector.py         # REST API with retry
│   │   ├── schema_detector.py           # auto column mapping to SaaS schema
│   │   └── factory.py                   # connector factory (csv|sql|api)
│   ├── etl/
│   │   ├── cleaner.py                   # nulls, duplicates, types, outliers
│   │   ├── transformer.py               # ARR, lifetime, monthly MRR snapshots
│   │   └── validator.py                 # rule-based data quality gates
│   ├── llm/
│   │   ├── client.py                    # Groq primary + Ollama fallback
│   │   ├── enricher.py                  # plan_category, churn_reason, risk_score
│   │   └── prompts.py                   # all prompt templates in one place
│   ├── query/
│   │   ├── nl_to_sql.py                 # NL → SQL → execute → narrative answer
│   │   ├── anomaly.py                   # Z-score + IQR + LLM explanation
│   │   └── report.py                    # LLM narrative → Markdown → PDF
│   ├── api/
│   │   ├── main.py                      # FastAPI factory + lifespan
│   │   ├── middleware.py                # request_id, structured logging, timing
│   │   ├── schemas.py                   # Pydantic request/response models
│   │   └── routes/
│   │       ├── ingest.py                # POST /ingest/csv, GET /ingest/jobs/:id
│   │       ├── query.py                 # POST /query (NL→SQL)
│   │       ├── insights.py              # anomalies, metrics, report, summary
│   │       └── health.py                # GET /health, /ready
│   ├── workers/
│   │   ├── celery_app.py                # Celery factory
│   │   └── tasks.py                     # run_etl_task (full pipeline)
│   ├── db/
│   │   ├── models.py                    # SQLAlchemy ORM (7 tables)
│   │   └── session.py                   # session factory, init_db
│   └── monitoring/
│       └── metrics.py                   # Prometheus counters, histograms, gauges
├── app/
│   └── streamlit_app.py                 # 6-page Streamlit UI
├── tests/
│   ├── unit/
│   │   ├── test_cleaner.py
│   │   ├── test_transformer.py
│   │   ├── test_validator.py
│   │   └── test_schema_detector.py
│   ├── integration/
│   │   └── test_api.py
│   └── eval/
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── datasources.yml
│       └── dashboards/
├── docs/
│   ├── ADR-001-llm-enrichment.md        # why LLM inside the ETL pipeline
│   └── ADR-002-nl-to-sql.md             # why NL→SQL over full RAG
├── scripts/
│   └── generate_sample_data.py          # generates 500-row SaaS CSV for testing
├── .github/workflows/ci.yml             # lint → test → docker → deploy
├── docker-compose.yml                   # full stack: Postgres, Redis, API, Worker, UI, Grafana, MLflow
├── Dockerfile                           # multi-stage: api | worker | ui
├── Makefile                             # dev shortcuts
├── pyproject.toml
└── requirements.txt
```

---

## Quick Start

### Option A  Docker Compose (recommended)

Everything runs with a single command.

**Services running:**

| Service | URL | Purpose |
|---|---|---|
| API + Swagger | http://localhost:8000/docs | Test all endpoints |
| Streamlit UI | http://localhost:8501 | Upload, query, visualize |
| Grafana | http://localhost:3000 | Metrics dashboard |
| Flower | http://localhost:5555 | Monitor Celery jobs |
| MLflow | http://localhost:5000 | ETL experiment tracking |
| Prometheus | http://localhost:9090 | Raw metrics |

---

## API Reference

### Upload and process a CSV

```bash
curl -X POST http://localhost:8000/api/v1/ingest/csv \
  -F "file=@data/saas_sample.csv" \
  -F "dataset_name=saas_q1_2024" \
  -F "domain=saas"
```

```json
{
  "job_id": "3f2a1b...",
  "dataset_id": "9c4e2d...",
  "status": "queued",
  "message": "Dataset 'saas_q1_2024' queued for ETL. Poll /ingest/jobs/{job_id} for status."
}
```

### Poll ETL job progress

```bash
curl http://localhost:8000/api/v1/ingest/jobs/3f2a1b...
```

```json
{
  "status": "processing",
  "progress": 55,
  "stage": "llm_enrichment",
  "rows_processed": 500,
  "rows_cleaned": 487,
  "rows_enriched": 243
}
```

### Ask a business question

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What plan has the highest churn rate?",
    "dataset_id": "9c4e2d..."
  }'
```

```json
{
  "question": "What plan has the highest churn rate?",
  "sql": "SELECT plan, AVG(churned::int) as churn_rate FROM saas_subscriptions WHERE dataset_id = '9c4e2d...' GROUP BY plan ORDER BY churn_rate DESC LIMIT 1",
  "answer": "The 'starter' plan has the highest churn rate at 34.2%, significantly above the company average of 28.1%. This suggests pricing sensitivity among smaller customers.",
  "result": [{"plan": "starter", "churn_rate": 0.342}],
  "row_count": 1,
  "latency_ms": 842.3,
  "success": true
}
```

### Get anomalies

```bash
curl "http://localhost:8000/api/v1/insights/anomalies?dataset_id=9c4e2d...&severity=high"
```

```json
{
  "total": 3,
  "anomalies": [
    {
      "column_name": "mrr",
      "method": "zscore+iqr",
      "value": 48750.00,
      "score": 6.84,
      "severity": "high",
      "llm_explanation": "This MRR value of $48,750 is approximately 6.8 standard deviations above the mean. This could indicate a data entry error (extra zero), a custom enterprise deal not properly normalized, or a genuine outlier requiring investigation with the sales team."
    }
  ]
}
```

### Generate PDF report

```bash
curl -X POST http://localhost:8000/api/v1/insights/report \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "9c4e2d...", "period": "2024-Q1"}'
```

```json
{
  "period": "2024-Q1",
  "markdown": "# Executive Summary\n\nMRR grew 12.4% quarter-over-quarter...",
  "pdf_url": "/api/v1/insights/report/download/abc123",
  "tokens_used": 1847,
  "success": true
}
```

```bash
# Download the PDF
curl http://localhost:8000/api/v1/insights/report/download/abc123 -o report.pdf
```

---

## ETL Pipeline Detail

The pipeline runs entirely in a Celery worker  non-blocking, retriable, progress-tracked.

```
Stage 1  Fetch (5% → 15%)
  CSV/Excel/SQL/API → pandas DataFrame

Stage 2  Schema Detection (15% → 25%)
  auto-maps column names to canonical SaaS schema
  (e.g. "monthly_revenue" → "mrr", "client_id" → "customer_id")

Stage 3  Clean (25% → 40%)
  drop columns with >50% nulls
  remove duplicates
  infer and cast types (dates, booleans, numerics)
  fill remaining nulls (median for numeric, "unknown" for strings)
  cap outliers via IQR (3× fence)

Stage 4  Transform (40% → 55%)
  compute ARR = MRR × 12
  compute lifetime_months from start_date → churn_date
  build monthly MRR snapshots (MRR, new MRR, churned MRR, NRR, ARPU)

Stage 5  LLM Enrichment (55% → 70%)
  plan_category      → starter | growth | professional | enterprise | custom
  churn_reason_category → price | product | competition | support | usage | other
  churn_risk_score   → 0.0 – 1.0
  industry_category  → tech | finance | healthcare | retail | ...
  runs in batches of 50 rows to respect Groq rate limits

Stage 6  Validation (70% → 80%)
  mrr >= 0 (error)
  customer_id not-null ratio >= 99% (error)
  churn rate <= 95% (warning)
  customer_id unique ratio >= 90% (warning)

Stage 7  Store (80% → 90%)
  upsert saas_subscriptions table
  upsert saas_metrics monthly snapshots

Stage 8  Anomaly Detection (90% → 100%)
  Z-score per numeric column (threshold: 2.5σ)
  IQR per numeric column (1.5× fence)
  LLM generates business explanation for each anomaly
  results stored in anomaly_records table
```

---

## LLM Enrichment Detail

The enricher adds business intelligence that rule-based ETL cannot provide.

**Plan classification example:**

```
Input:  plan="Pro Annual", mrr=499, seats=12
Output: plan_category="professional", confidence=0.91
```

**Churn reason classification example:**

```
Input:  churn_reason="We found a cheaper option that does 80% of what you do"
Output: category="competition", sentiment="negative", confidence=0.95
```

**Churn risk scoring example:**

```
Input:  plan="starter", mrr=49, lifetime=2.3 months, seats=1, country="BR"
Output: churn_risk_score=0.78, risk_level="high", main_factor="low_tenure"
```

All prompts are in `src/llm/prompts.py`  fully auditable and tweakable without touching business logic.
---

## NL→SQL Agent

Converts natural language to safe, read-only PostgreSQL queries.

**How it works:**

1. Full table schema fits in LLM context (<1000 tokens)  no retrieval needed
2. LLM generates SQL with `temperature=0.0` for determinism
3. Query is validated: must start with `SELECT`, no `INSERT/UPDATE/DELETE/DROP`
4. Executes against PostgreSQL, returns up to 100 rows
5. LLM generates a business narrative answer from the result

**Example questions that work:**

```
"What is the total MRR from active customers?"
"Which plan has the highest churn rate?"
"Show the top 5 customers by MRR"
"What percentage of churns were due to pricing?"
"What is the average customer lifetime for the enterprise plan?"
"Which country has the most active customers?"
"Show monthly MRR trend for the last 6 months"
```

**Safety:** Any query that does not start with `SELECT` or contains data-modifying keywords is rejected before execution.

---

## Design Decisions

Full rationale in [`docs/`](docs/):

### [ADR-001](docs/ADR-001-llm-enrichment.md)  LLM enrichment inside the ETL pipeline

> We run LLM enrichment as a pipeline stage (not post-hoc) so that enriched fields are available at query time. The `plan_category`, `churn_reason_category`, and `churn_risk_score` fields are first-class database columns that NL→SQL can filter and aggregate on.

### [ADR-002](docs/ADR-002-nl-to-sql.md)  NL→SQL over full RAG for structured queries

> Structured data belongs in SQL. RAG embeds rows as unstructured text, losing the relational structure needed for aggregations and joins. Our schema fits in the LLM context window, making retrieval unnecessary. Generated SQL is shown to users  making answers verifiable and auditable.

---

## Observability

Every request and ETL run emits structured metrics:

| Metric | Type | Description |
|---|---|---|
| `datamind_etl_duration_seconds` | Histogram | Full pipeline duration per run |
| `datamind_rows_processed_total` | Counter | Total rows through ETL |
| `datamind_rows_enriched_total` | Counter | Rows enriched by LLM |
| `datamind_nl_sql_latency_ms` | Histogram | NL→SQL query latency |
| `datamind_nl_sql_queries_total` | Counter | Queries by status (success/error) |
| `datamind_anomalies_detected_total` | Counter | Anomalies by severity |
| `datamind_reports_generated_total` | Counter | Reports by format |
| `datamind_llm_tokens_used_total` | Counter | Token usage by provider and task |
| `datamind_active_datasets` | Gauge | Datasets with status=ready |

All logs are structured JSON with `request_id` propagated through the full call stack.

---

## Testing

```bash
# All tests
pytest tests/unit/ tests/integration/ -v --cov=src --cov-report=term-missing

# Unit tests only (no infrastructure needed)
pytest tests/unit/ -v

# Single module
pytest tests/unit/test_cleaner.py -v
```

**Coverage targets by module:**

| Module | Coverage |
|---|---|
| `etl/cleaner.py` | 96% |
| `etl/transformer.py` | 91% |
| `etl/validator.py` | 94% |
| `ingestion/schema_detector.py` | 88% |
| `api/routes/` | 76% |
| **Total** | **82%** |

---

## CI/CD Pipeline

```
Push to main
    │
    ├── Lint (ruff check + ruff format)
    │
    ├── Tests (pytest  unit + integration)
    │   ├── PostgreSQL 16 service
    │   └── Redis 7 service
    │
    ├── Docker build (ghcr.io)
    │
    └── Deploy to Render (webhook trigger)
```

The pipeline runs on every push to `main` and every pull request. Docker image is published to GitHub Container Registry on `main` only.

---

## Deployment

### Render (free tier)

The `render.yaml` blueprint defines all services:

```bash
# Deploy automatically via GitHub integration, or manually:
git push origin main  # triggers CI → Docker build → Render deploy
```

Services deployed:
- `datamind-api`  FastAPI web service
- `datamind-worker`  Celery background worker
- `datamind-redis`  Redis instance

### Streamlit Community Cloud (UI)

1. Fork this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select `app/streamlit_app.py`
4. Set `API_BASE` secret to your Render API URL

---

## Local Development Commands

```bash
make install      # install all dependencies
make up           # start full stack with Docker Compose
make down         # stop Docker Compose
make dev          # start API with hot-reload
make worker       # start Celery worker
make ui           # start Streamlit UI
make seed         # generate 500-row sample SaaS CSV
make test         # run all tests with coverage
make test-unit    # run unit tests only
make lint         # lint and auto-fix with ruff
make clean        # remove data/, cache, coverage reports
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` |  | **Required.** Get free at console.groq.com |
| `DATABASE_URL` | postgres://datamind:datamind@localhost/datamind | PostgreSQL connection |
| `REDIS_URL` | redis://localhost:6379/0 | Redis connection |
| `LLM_PROVIDER` | groq | `groq` or `ollama` |
| `LLM_ENRICHMENT_ENABLED` | true | Set `false` to skip LLM enrichment in ETL |
| `LLM_ENRICHMENT_BATCH_SIZE` | 50 | Rows to enrich per ETL run |
| `ANOMALY_Z_THRESHOLD` | 2.5 | Z-score threshold for anomaly detection |
| `ANOMALY_LLM_EXPLAIN` | true | Generate LLM explanation per anomaly |
| `NULL_RATIO_THRESHOLD` | 0.5 | Drop columns with more nulls than this ratio |
| `LOG_FORMAT` | json | `json` (production) or `console` (development) |
| `ENVIRONMENT` | development | `development` or `production` |

---

## Roadmap

- [ ] Webhook connector for real-time ingestion
- [ ] Scheduled ETL runs (Celery Beat)
- [ ] Multi-dataset comparison queries
- [ ] Email delivery for generated reports
- [ ] Slack integration for anomaly alerts
- [ ] Auto-generated golden dataset for NL→SQL accuracy tracking
- [ ] Fine-tuned smaller model for enrichment (reduce Groq dependency)

---
