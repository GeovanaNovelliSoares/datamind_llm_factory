"""Health and readiness routes."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.api.schemas import HealthResponse
from src.config import get_settings
from src.db import get_db, Dataset

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)) -> HealthResponse:
    settings = get_settings()
    components: dict[str, str] = {}

    # PostgreSQL
    try:
        db.execute(text("SELECT 1"))
        components["postgresql"] = "ok"
    except Exception as e:
        components["postgresql"] = f"error: {e}"

    # Redis
    try:
        import redis
        r = redis.from_url(settings.redis_url, socket_connect_timeout=1)
        r.ping()
        components["redis"] = "ok"
    except Exception as e:
        components["redis"] = f"error: {e}"

    # ChromaDB
    try:
        import chromadb
        client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        components["chromadb"] = "ok"
    except Exception as e:
        components["chromadb"] = f"error: {e}"

    active = db.query(Dataset).filter(Dataset.status == "ready").count()
    overall = "ok" if all(v == "ok" for v in components.values()) else "degraded"

    return HealthResponse(
        status=overall,
        version="0.1.0",
        environment=settings.environment,
        active_datasets=active,
        components=components,
    )


@router.get("/ready")
def readiness() -> dict:
    return {"ready": True}
