"""Ingestion routes — CSV upload, job polling, dataset management."""
import hashlib
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from src.api.schemas import (
    DatasetListResponse, DatasetResponse, IngestResponse, JobStatusResponse,
)
from src.db import Dataset, ETLJob, get_db
from src.logger import get_logger
from src.monitoring.metrics import active_etl_jobs, datasets_ingested_total

logger = get_logger(__name__)
router = APIRouter(prefix="/ingest", tags=["ingestion"])

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv"}
MAX_FILE_MB = 100


@router.post("/csv", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_csv(
    file: UploadFile = File(...),
    dataset_name: str = Form(...),
    domain: str = Form(default="saas"),
    db: Session = Depends(get_db),
) -> IngestResponse:
    """Upload a CSV/Excel file for ETL processing."""
    ext = Path(file.filename or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Unsupported file type '{ext}'. Allowed: {sorted(ALLOWED_EXTENSIONS)}",
        )

    file_bytes = await file.read()
    size_mb = len(file_bytes) / (1024 * 1024)
    if size_mb > MAX_FILE_MB:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large ({size_mb:.1f} MB). Max: {MAX_FILE_MB} MB",
        )

    # Deduplication by content hash + name
    content_hash = hashlib.sha256(file_bytes).hexdigest()
    existing = (
        db.query(Dataset)
        .filter(Dataset.name == dataset_name, Dataset.status == "ready")
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset '{dataset_name}' already exists. Delete it first or use a different name.",
        )

    # Create dataset record
    dataset = Dataset(
        name=dataset_name,
        domain=domain,
        source_type="csv",
        status="pending",
        etl_config=f'{{"filename": "{file.filename}", "hash": "{content_hash[:16]}"}}',
    )
    db.add(dataset)
    db.flush()

    job = ETLJob(dataset_id=dataset.id)
    db.add(job)
    db.commit()
    db.refresh(job)

    # Dispatch Celery task
    from src.workers.tasks import run_etl_task
    task = run_etl_task.apply_async(
        kwargs={
            "job_id": job.id,
            "dataset_id": dataset.id,
            "source_type": "csv",
            "connector_kwargs": {
                "file_bytes": file_bytes.hex(),
                "filename": file.filename or "upload.csv",
            },
            "run_anomaly_detection": True,
        },
        queue="etl",
    )
    job.celery_task_id = task.id
    db.commit()

    active_etl_jobs.inc()
    datasets_ingested_total.labels(source_type="csv").inc()

    logger.info("ingest_queued", dataset_id=dataset.id, job_id=job.id, name=dataset_name)

    return IngestResponse(
        job_id=job.id,
        dataset_id=dataset.id,
        dataset_name=dataset_name,
        source_type="csv",
        status="queued",
        message=f"Dataset '{dataset_name}' queued for ETL. Poll /ingest/jobs/{job.id} for status.",
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str, db: Session = Depends(get_db)) -> JobStatusResponse:
    """Poll ETL job status and progress."""
    job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    if job.status == "completed":
        active_etl_jobs.dec()

    return JobStatusResponse(
        job_id=job.id,
        dataset_id=job.dataset_id,
        status=job.status,
        progress=job.progress,
        stage=job.stage,
        rows_processed=job.rows_processed,
        rows_cleaned=job.rows_cleaned,
        rows_enriched=job.rows_enriched,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error_message=job.error_message,
    )


@router.get("/datasets", response_model=DatasetListResponse)
def list_datasets(
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db),
) -> DatasetListResponse:
    """List all datasets."""
    total = db.query(Dataset).count()
    datasets = db.query(Dataset).offset(skip).limit(limit).all()
    return DatasetListResponse(
        datasets=[
            DatasetResponse(
                id=d.id, name=d.name, domain=d.domain,
                source_type=d.source_type, row_count=d.row_count,
                column_count=d.column_count, status=d.status,
                created_at=d.created_at, error_message=d.error_message,
            )
            for d in datasets
        ],
        total=total,
    )


@router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(dataset_id: str, db: Session = Depends(get_db)) -> None:
    """Delete a dataset and all its processed data."""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    db.delete(dataset)
    db.commit()
    logger.info("dataset_deleted", dataset_id=dataset_id)
