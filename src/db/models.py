"""SQLAlchemy ORM models — SaaS domain (subscriptions, MRR, churn)."""
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Float, ForeignKey,
    Integer, Numeric, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _uuid() -> str:
    return str(uuid.uuid4())


# ── Dataset registry ──────────────────────────────────────────────────────────

class Dataset(Base):
    """Registry of ingested datasets."""
    __tablename__ = "datasets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    domain: Mapped[str] = mapped_column(String(64), nullable=False)  # saas | ecommerce
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)  # csv | sql | api
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    column_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    etl_config: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    validation_report: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    jobs: Mapped[list["ETLJob"]] = relationship("ETLJob", back_populates="dataset", cascade="all, delete-orphan")
    anomalies: Mapped[list["AnomalyRecord"]] = relationship("AnomalyRecord", back_populates="dataset", cascade="all, delete-orphan")


class ETLJob(Base):
    """Tracks each ETL pipeline run."""
    __tablename__ = "etl_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="queued")
    progress: Mapped[int] = mapped_column(Integer, default=0)
    stage: Mapped[str] = mapped_column(String(64), default="queued")
    rows_processed: Mapped[int] = mapped_column(Integer, default=0)
    rows_cleaned: Mapped[int] = mapped_column(Integer, default=0)
    rows_enriched: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="jobs")


# ── SaaS domain tables ────────────────────────────────────────────────────────

class SaasSubscription(Base):
    """Clean SaaS subscription records after ETL."""
    __tablename__ = "saas_subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    customer_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    plan: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_category: Mapped[str | None] = mapped_column(String(64), nullable=True)  # LLM enriched
    mrr: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    arr: Mapped[float] = mapped_column(Numeric(14, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)  # active | churned | trial
    churned: Mapped[bool] = mapped_column(Boolean, default=False)
    churn_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    churn_reason_category: Mapped[str | None] = mapped_column(String(64), nullable=True)  # LLM enriched
    churn_risk_score: Mapped[float | None] = mapped_column(Float, nullable=True)  # LLM enriched
    start_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    churn_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    lifetime_months: Mapped[float | None] = mapped_column(Float, nullable=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)
    industry: Mapped[str | None] = mapped_column(String(128), nullable=True)
    industry_category: Mapped[str | None] = mapped_column(String(64), nullable=True)  # LLM enriched
    seats: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class SaasMetric(Base):
    """Monthly SaaS KPI snapshots."""
    __tablename__ = "saas_metrics"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    period: Mapped[str] = mapped_column(String(7), nullable=False)  # YYYY-MM
    mrr: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    new_mrr: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    expansion_mrr: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    churned_mrr: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    net_new_mrr: Mapped[float] = mapped_column(Numeric(14, 2), default=0)
    active_customers: Mapped[int] = mapped_column(Integer, default=0)
    new_customers: Mapped[int] = mapped_column(Integer, default=0)
    churned_customers: Mapped[int] = mapped_column(Integer, default=0)
    churn_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    nrr: Mapped[float | None] = mapped_column(Float, nullable=True)  # Net Revenue Retention
    arpu: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# ── Analytics tables ──────────────────────────────────────────────────────────

class AnomalyRecord(Base):
    """Detected anomalies with LLM explanation."""
    __tablename__ = "anomaly_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    column_name: Mapped[str] = mapped_column(String(128), nullable=False)
    method: Mapped[str] = mapped_column(String(32), nullable=False)  # zscore | iqr
    value: Mapped[float] = mapped_column(Float, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    row_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_context: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    llm_explanation: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(String(16), default="medium")  # low | medium | high
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="anomalies")


class QueryLog(Base):
    """NL→SQL query history."""
    __tablename__ = "query_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    generated_sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class ReportRecord(Base):
    """Generated PDF/Markdown reports."""
    __tablename__ = "report_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    period: Mapped[str | None] = mapped_column(String(32), nullable=True)
    report_type: Mapped[str] = mapped_column(String(32), default="monthly_saas")
    markdown_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    pdf_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    tokens_used: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
