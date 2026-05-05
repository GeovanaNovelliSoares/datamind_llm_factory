"""Insights routes — anomalies, metrics, PDF report generation."""
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from src.api.schemas import (
    AnomalyListResponse, AnomalyResponse,
    MetricSnapshot, MetricsResponse,
    ReportRequest, ReportResponse,
)
from src.db import AnomalyRecord, Dataset, ReportRecord, SaasMetric, get_db
from src.logger import get_logger
from src.monitoring.metrics import reports_generated_total
from src.query.report import generate_report

logger = get_logger(__name__)
router = APIRouter(prefix="/insights", tags=["insights"])


@router.get("/anomalies", response_model=AnomalyListResponse)
def get_anomalies(
    dataset_id: str,
    severity: str | None = None,
    limit: int = 50,
    db: Session = Depends(get_db),
) -> AnomalyListResponse:
    """Return detected anomalies for a dataset."""
    q = db.query(AnomalyRecord).filter(AnomalyRecord.dataset_id == dataset_id)
    if severity:
        q = q.filter(AnomalyRecord.severity == severity)
    total = q.count()
    records = q.order_by(AnomalyRecord.score.desc()).limit(limit).all()

    return AnomalyListResponse(
        anomalies=[
            AnomalyResponse(
                id=r.id,
                column_name=r.column_name,
                method=r.method,
                value=r.value,
                score=round(r.score, 4),
                severity=r.severity,
                llm_explanation=r.llm_explanation,
                created_at=r.created_at,
            )
            for r in records
        ],
        total=total,
        dataset_id=dataset_id,
    )


@router.get("/metrics", response_model=MetricsResponse)
def get_metrics(
    dataset_id: str,
    periods: int = 12,
    db: Session = Depends(get_db),
) -> MetricsResponse:
    """Return monthly MRR/churn metric snapshots."""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    rows = (
        db.query(SaasMetric)
        .filter(SaasMetric.dataset_id == dataset_id)
        .order_by(SaasMetric.period.asc())
        .limit(periods)
        .all()
    )

    snapshots = [
        MetricSnapshot(
            period=r.period,
            mrr=float(r.mrr or 0),
            new_mrr=float(r.new_mrr or 0),
            churned_mrr=float(r.churned_mrr or 0),
            net_new_mrr=float(r.net_new_mrr or 0),
            active_customers=r.active_customers,
            churn_rate=float(r.churn_rate or 0),
            nrr=float(r.nrr) if r.nrr is not None else None,
            arpu=float(r.arpu) if r.arpu is not None else None,
        )
        for r in rows
    ]

    return MetricsResponse(
        dataset_id=dataset_id,
        snapshots=snapshots,
        latest=snapshots[-1] if snapshots else None,
    )


@router.post("/report", response_model=ReportResponse)
def create_report(
    body: ReportRequest,
    db: Session = Depends(get_db),
) -> ReportResponse:
    """Generate a narrative SaaS performance report as Markdown + PDF."""
    dataset = db.query(Dataset).filter(Dataset.id == body.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{body.dataset_id}' not found.")
    if dataset.status != "ready":
        raise HTTPException(status_code=400, detail="Dataset not ready. Run ETL first.")

    result = generate_report(
        dataset_id=body.dataset_id,
        period=body.period,
        output_dir="./data/reports",
    )

    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Report generation failed."))

    # Persist report record
    report_record = ReportRecord(
        dataset_id=body.dataset_id,
        period=result.get("period"),
        report_type="monthly_saas",
        markdown_content=result.get("markdown"),
        pdf_path=result.get("pdf_path"),
        tokens_used=result.get("tokens_used", 0),
    )
    db.add(report_record)
    db.commit()

    reports_generated_total.labels(format="pdf" if result.get("pdf_path") else "markdown").inc()

    pdf_url = None
    if result.get("pdf_path"):
        pdf_url = f"/api/v1/insights/report/download/{report_record.id}"

    return ReportResponse(
        dataset_id=body.dataset_id,
        period=result.get("period", ""),
        markdown=result.get("markdown", ""),
        pdf_url=pdf_url,
        tokens_used=result.get("tokens_used", 0),
        success=True,
    )


@router.get("/report/download/{report_id}")
def download_report(report_id: str, db: Session = Depends(get_db)) -> FileResponse:
    """Download a generated PDF report."""
    record = db.query(ReportRecord).filter(ReportRecord.id == report_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Report not found.")
    if not record.pdf_path or not Path(record.pdf_path).exists():
        raise HTTPException(status_code=404, detail="PDF file not found on disk.")

    return FileResponse(
        path=record.pdf_path,
        media_type="application/pdf",
        filename=Path(record.pdf_path).name,
    )


@router.get("/summary/{dataset_id}")
def get_summary(dataset_id: str, db: Session = Depends(get_db)) -> dict:
    """Return a quick business summary for dashboard widgets."""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    latest_metric = (
        db.query(SaasMetric)
        .filter(SaasMetric.dataset_id == dataset_id)
        .order_by(SaasMetric.period.desc())
        .first()
    )

    anomaly_counts = {}
    for severity in ["high", "medium", "low"]:
        count = (
            db.query(AnomalyRecord)
            .filter(AnomalyRecord.dataset_id == dataset_id, AnomalyRecord.severity == severity)
            .count()
        )
        anomaly_counts[severity] = count

    return {
        "dataset_id": dataset_id,
        "dataset_name": dataset.name,
        "status": dataset.status,
        "row_count": dataset.row_count,
        "latest_period": latest_metric.period if latest_metric else None,
        "mrr": float(latest_metric.mrr or 0) if latest_metric else 0,
        "active_customers": latest_metric.active_customers if latest_metric else 0,
        "churn_rate": float(latest_metric.churn_rate or 0) if latest_metric else 0,
        "net_new_mrr": float(latest_metric.net_new_mrr or 0) if latest_metric else 0,
        "anomalies": anomaly_counts,
    }
