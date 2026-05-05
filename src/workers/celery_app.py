"""Celery application factory."""
from celery import Celery
from src.config import get_settings


def create_celery_app() -> Celery:
    settings = get_settings()
    app = Celery(
        "datamind",
        broker=settings.celery_broker_url,
        backend=settings.celery_result_backend,
        include=["src.workers.tasks"],
    )
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_routes={"src.workers.tasks.run_etl_task": {"queue": "etl"}},
    )
    return app


celery_app = create_celery_app()
