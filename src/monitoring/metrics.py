"""Prometheus metrics definitions for DataMind."""
from prometheus_client import Counter, Gauge, Histogram

# ── ETL metrics ───────────────────────────────────────────────────────────────
datasets_ingested_total = Counter(
    "datamind_datasets_ingested_total",
    "Total datasets successfully ingested",
    ["source_type"],
)
etl_duration_seconds = Histogram(
    "datamind_etl_duration_seconds",
    "Full ETL pipeline duration",
    buckets=[5, 15, 30, 60, 120, 300, 600],
)
rows_processed_total = Counter(
    "datamind_rows_processed_total",
    "Total rows processed through ETL",
)
rows_enriched_total = Counter(
    "datamind_rows_enriched_total",
    "Total rows enriched by LLM",
)
etl_errors_total = Counter(
    "datamind_etl_errors_total",
    "Total ETL pipeline failures",
    ["stage"],
)
validation_failures_total = Counter(
    "datamind_validation_failures_total",
    "Total data validation rule failures",
)

# ── Query metrics ─────────────────────────────────────────────────────────────
nl_sql_queries_total = Counter(
    "datamind_nl_sql_queries_total",
    "Total NL→SQL queries executed",
    ["status"],
)
nl_sql_latency_ms = Histogram(
    "datamind_nl_sql_latency_ms",
    "NL→SQL query latency in milliseconds",
    buckets=[100, 250, 500, 1000, 2000, 5000],
)
anomalies_detected_total = Counter(
    "datamind_anomalies_detected_total",
    "Total anomalies detected",
    ["severity"],
)
reports_generated_total = Counter(
    "datamind_reports_generated_total",
    "Total reports generated",
    ["format"],
)

# ── LLM metrics ───────────────────────────────────────────────────────────────
llm_tokens_used_total = Counter(
    "datamind_llm_tokens_used_total",
    "Total LLM tokens consumed",
    ["provider", "task"],
)
llm_calls_total = Counter(
    "datamind_llm_calls_total",
    "Total LLM API calls",
    ["provider", "status"],
)

# ── System metrics ────────────────────────────────────────────────────────────
active_datasets = Gauge(
    "datamind_active_datasets",
    "Number of datasets with status=ready",
)
active_etl_jobs = Gauge(
    "datamind_active_etl_jobs",
    "Number of ETL jobs currently running",
)
