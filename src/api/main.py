"""FastAPI application factory."""
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from src.api.middleware import RequestContextMiddleware
from src.api.routes import health, ingest, insights, query
from src.config import get_settings
from src.db.session import init_db
from src.logger import get_logger, setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    setup_logging()
    logger = get_logger(__name__)
    settings = get_settings()
    logger.info("starting_datamind", environment=settings.environment)
    init_db()
    logger.info("database_initialized")
    yield
    logger.info("shutting_down_datamind")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="DataMind — Intelligent Business ETL + LLM",
        description=(
            "Production-grade ETL pipeline with LLM enrichment for SaaS metrics. "
            "Upload data, ask questions in natural language, detect anomalies, generate PDF reports."
        ),
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Prometheus
    if settings.prometheus_enabled:
        Instrumentator(
            should_group_status_codes=True,
            excluded_handlers=["/metrics", "/health", "/ready"],
        ).instrument(app).expose(app, endpoint="/metrics")

    # Routes
    prefix = "/api/v1"
    app.include_router(health.router, prefix=prefix)
    app.include_router(ingest.router, prefix=prefix)
    app.include_router(query.router, prefix=prefix)
    app.include_router(insights.router, prefix=prefix)

    return app


app = create_app()
