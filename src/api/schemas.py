"""Pydantic request/response schemas."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ── Ingest ────────────────────────────────────────────────────────────────────

class IngestResponse(BaseModel):
    job_id: str
    dataset_id: str
    dataset_name: str
    source_type: str
    status: str
    message: str


class JobStatusResponse(BaseModel):
    job_id: str
    dataset_id: str
    status: str
    progress: int = Field(ge=0, le=100)
    stage: str
    rows_processed: int
    rows_cleaned: int
    rows_enriched: int
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None


class DatasetResponse(BaseModel):
    id: str
    name: str
    domain: str
    source_type: str
    row_count: int
    column_count: int
    status: str
    created_at: datetime
    error_message: str | None = None


class DatasetListResponse(BaseModel):
    datasets: list[DatasetResponse]
    total: int


# ── Query ─────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=1000)
    dataset_id: str

    @field_validator("question")
    @classmethod
    def question_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Question cannot be empty.")
        return v.strip()


class QueryResponse(BaseModel):
    question: str
    sql: str
    answer: str
    result: list[dict[str, Any]]
    row_count: int
    latency_ms: float
    success: bool
    error: str | None = None


# ── Insights ──────────────────────────────────────────────────────────────────

class AnomalyResponse(BaseModel):
    id: str
    column_name: str
    method: str
    value: float
    score: float
    severity: str
    llm_explanation: str | None
    created_at: datetime


class AnomalyListResponse(BaseModel):
    anomalies: list[AnomalyResponse]
    total: int
    dataset_id: str


class MetricSnapshot(BaseModel):
    period: str
    mrr: float
    new_mrr: float
    churned_mrr: float
    net_new_mrr: float
    active_customers: int
    churn_rate: float
    nrr: float | None
    arpu: float | None


class MetricsResponse(BaseModel):
    dataset_id: str
    snapshots: list[MetricSnapshot]
    latest: MetricSnapshot | None


# ── Report ────────────────────────────────────────────────────────────────────

class ReportRequest(BaseModel):
    dataset_id: str
    period: str | None = None


class ReportResponse(BaseModel):
    dataset_id: str
    period: str
    markdown: str
    pdf_url: str | None
    tokens_used: int
    success: bool
    error: str | None = None


# ── Health ────────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str
    active_datasets: int
    components: dict[str, str]
