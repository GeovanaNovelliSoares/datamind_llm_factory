"""Celery ETL task — full pipeline in one async job."""
from datetime import datetime

from celery import Task
from celery.utils.log import get_task_logger

from src.workers.celery_app import celery_app

logger = get_task_logger(__name__)


class BaseTask(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f"Task {task_id} failed: {exc}")


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="src.workers.tasks.run_etl_task",
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=600,
    time_limit=900,
)
def run_etl_task(
    self,
    job_id: str,
    dataset_id: str,
    source_type: str,
    connector_kwargs: dict,
    run_anomaly_detection: bool = True,
) -> dict:
    """
    Full ETL pipeline:
    fetch → detect schema → clean → transform → LLM enrich → validate → store → anomaly detect
    """
    from src.db.session import SessionLocal
    from src.db.models import Dataset, ETLJob, SaasSubscription, SaasMetric, AnomalyRecord
    from src.ingestion.factory import build_connector
    from src.ingestion.schema_detector import detect_schema, apply_mapping
    from src.etl.cleaner import clean
    from src.etl.transformer import transform_saas
    from src.etl.validator import validate
    from src.llm.enricher import enrich_saas
    from src.query.anomaly import detect_anomalies
    import json

    db = SessionLocal()

    def _update(status: str, progress: int, stage: str, error: str | None = None):
        job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
        if job:
            job.status = status
            job.progress = progress
            job.stage = stage
            job.error_message = error
            if status == "processing" and not job.started_at:
                job.started_at = datetime.utcnow()
            if status in ("completed", "failed"):
                job.finished_at = datetime.utcnow()
            db.commit()
        self.update_state(state="PROGRESS", meta={"progress": progress, "stage": stage})

    try:
        _update("processing", 5, "fetching")

        # ── 1. Fetch ──────────────────────────────────────────────────────
        connector = build_connector(source_type, **connector_kwargs)
        result = connector.fetch()
        df = result.df
        logger.info(f"Fetched {len(df)} rows from {source_type}")
        _update("processing", 15, "schema_detection")

        # ── 2. Schema detection ───────────────────────────────────────────
        schema = detect_schema(df)
        if schema.column_mapping:
            df = apply_mapping(df, schema.column_mapping)

        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            dataset.row_count = len(df)
            dataset.column_count = len(df.columns)
            db.commit()

        _update("processing", 25, "cleaning")

        # ── 3. Clean ──────────────────────────────────────────────────────
        df, clean_report = clean(df, dataset_name=dataset.name if dataset else "")
        logger.info(f"Cleaned: {clean_report.original_rows} → {clean_report.final_rows} rows")
        _update("processing", 40, "transforming")

        # ── 4. Transform ──────────────────────────────────────────────────
        transform_result = transform_saas(df, dataset_id)
        _update("processing", 55, "llm_enrichment")

        # ── 5. LLM enrichment ─────────────────────────────────────────────
        enriched_df, enrich_report = enrich_saas(transform_result.subscriptions_df)
        _update("processing", 70, "validation")

        # ── 6. Validate ───────────────────────────────────────────────────
        val_result = validate(enriched_df, domain=schema.detected_domain)
        if dataset:
            dataset.validation_report = json.dumps({
                "passed": val_result.passed,
                "pass_rate": val_result.pass_rate,
                "failures": val_result.failures,
                "warnings": val_result.warnings,
            })
            db.commit()

        _update("processing", 80, "storing")

        # ── 7. Store subscriptions ────────────────────────────────────────
        db.query(SaasSubscription).filter(SaasSubscription.dataset_id == dataset_id).delete()
        for _, row in enriched_df.iterrows():
            sub = SaasSubscription(dataset_id=dataset_id)
            for col in enriched_df.columns:
                if hasattr(sub, col):
                    val = row.get(col)
                    import pandas as pd
                    if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                        val = None
                    setattr(sub, col, val)
            db.add(sub)

        # ── 8. Store monthly metrics ──────────────────────────────────────
        db.query(SaasMetric).filter(SaasMetric.dataset_id == dataset_id).delete()
        for _, row in transform_result.metrics_df.iterrows():
            metric = SaasMetric(**{
                col: (None if (hasattr(row[col], '__class__') and str(row[col]) == 'nan') else row[col])
                for col in transform_result.metrics_df.columns
                if hasattr(SaasMetric, col)
            })
            db.add(metric)

        db.commit()
        logger.info(f"Stored {len(enriched_df)} subscriptions, {len(transform_result.metrics_df)} metric periods")

        _update("processing", 90, "anomaly_detection")

        # ── 9. Anomaly detection ──────────────────────────────────────────
        if run_anomaly_detection:
            numeric_df = enriched_df.select_dtypes(include=["number"])
            anomalies = detect_anomalies(numeric_df, dataset_name=dataset.name if dataset else "")

            db.query(AnomalyRecord).filter(AnomalyRecord.dataset_id == dataset_id).delete()
            for a in anomalies:
                import json
                record = AnomalyRecord(
                    dataset_id=dataset_id,
                    column_name=a.column,
                    method=a.method,
                    value=a.value,
                    score=a.score,
                    row_index=a.row_index,
                    row_context=json.dumps(a.row_context, default=str)[:1000],
                    llm_explanation=a.llm_explanation,
                    severity=a.severity,
                )
                db.add(record)
            db.commit()
            logger.info(f"Detected {len(anomalies)} anomalies")

        # ── 10. Finalize ──────────────────────────────────────────────────
        if dataset:
            dataset.status = "ready"
            db.commit()

        job = db.query(ETLJob).filter(ETLJob.id == job_id).first()
        if job:
            job.rows_processed = clean_report.original_rows
            job.rows_cleaned = clean_report.final_rows
            job.rows_enriched = enrich_report.rows_enriched
            db.commit()

        _update("completed", 100, "done")

        return {
            "dataset_id": dataset_id,
            "rows_processed": clean_report.original_rows,
            "rows_final": clean_report.final_rows,
            "anomalies_found": len(anomalies) if run_anomaly_detection else 0,
            "validation_passed": val_result.passed,
            "status": "completed",
        }

    except Exception as exc:
        logger.error(f"ETL failed for {dataset_id}: {exc}")
        _update("failed", 0, "failed", error=str(exc))
        if dataset := db.query(Dataset).filter(Dataset.id == dataset_id).first():
            dataset.status = "failed"
            dataset.error_message = str(exc)
            db.commit()
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc, countdown=30)
        raise
    finally:
        db.close()
