"""NL→SQL query route."""
import time

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.api.schemas import QueryRequest, QueryResponse
from src.db import Dataset, QueryLog, get_db
from src.logger import get_logger
from src.monitoring.metrics import nl_sql_latency_ms, nl_sql_queries_total
from src.query.nl_to_sql import run_nl_to_sql

logger = get_logger(__name__)
router = APIRouter(prefix="/query", tags=["query"])


@router.post("", response_model=QueryResponse)
def query(body: QueryRequest, db: Session = Depends(get_db)) -> QueryResponse:
    """
    Ask a business question in natural language.

    The system converts it to SQL, executes against the clean data,
    and returns both the SQL, raw results, and a narrative answer.
    """
    dataset = db.query(Dataset).filter(Dataset.id == body.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{body.dataset_id}' not found.")
    if dataset.status != "ready":
        raise HTTPException(
            status_code=400,
            detail=f"Dataset is not ready (status: {dataset.status}). Wait for ETL to complete.",
        )

    result = run_nl_to_sql(question=body.question, dataset_id=body.dataset_id)

    # Persist query log
    log = QueryLog(
        question=body.question,
        generated_sql=result.get("sql"),
        result_rows=len(result.get("result", [])),
        answer=result.get("answer"),
        latency_ms=result.get("latency_ms"),
        success=result.get("success", False),
        error_message=result.get("error"),
    )
    db.add(log)
    db.commit()

    # Metrics
    status_label = "success" if result["success"] else "error"
    nl_sql_queries_total.labels(status=status_label).inc()
    nl_sql_latency_ms.observe(result.get("latency_ms", 0))

    return QueryResponse(
        question=body.question,
        sql=result.get("sql", ""),
        answer=result.get("answer", ""),
        result=result.get("result", []),
        row_count=len(result.get("result", [])),
        latency_ms=round(result.get("latency_ms", 0), 2),
        success=result.get("success", False),
        error=result.get("error"),
    )


@router.get("/history")
def query_history(
    limit: int = 20,
    db: Session = Depends(get_db),
) -> list[dict]:
    """Return recent query history."""
    logs = db.query(QueryLog).order_by(QueryLog.created_at.desc()).limit(limit).all()
    return [
        {
            "id": log.id,
            "question": log.question,
            "sql": log.generated_sql,
            "answer": log.answer,
            "row_count": log.result_rows,
            "latency_ms": log.latency_ms,
            "success": log.success,
            "created_at": log.created_at.isoformat(),
        }
        for log in logs
    ]
