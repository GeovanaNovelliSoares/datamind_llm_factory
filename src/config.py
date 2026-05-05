"""Centralized configuration loaded from environment variables."""
from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── LLM ──────────────────────────────────────────
    groq_api_key: str = ""
    llm_provider: str = "groq"
    llm_model: str = "llama-3.1-8b-instant"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "mistral"

    # ── Embeddings ───────────────────────────────────
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_device: str = "cpu"

    # ── Database ─────────────────────────────────────
    database_url: str = "postgresql+psycopg2://datamind:datamind@localhost:5432/datamind"

    # ── ChromaDB ─────────────────────────────────────
    chroma_persist_dir: str = "./data/chroma"
    chroma_collection: str = "datamind"

    # ── Redis ────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    cache_ttl_seconds: int = 3600

    # ── Celery ───────────────────────────────────────
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # ── ETL ──────────────────────────────────────────
    outlier_z_score_threshold: float = 3.0
    null_ratio_threshold: float = 0.5
    llm_enrichment_enabled: bool = True
    llm_enrichment_batch_size: int = 50

    # ── NL→SQL ───────────────────────────────────────
    nl_sql_max_retries: int = 3
    nl_sql_include_schema: bool = True

    # ── Anomaly ──────────────────────────────────────
    anomaly_z_threshold: float = 2.5
    anomaly_iqr_multiplier: float = 1.5
    anomaly_llm_explain: bool = True

    # ── API ──────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    secret_key: str = "change-me-in-production"
    allowed_origins: str = "http://localhost:8501"

    # ── MLflow ───────────────────────────────────────
    mlflow_tracking_uri: str = "./mlflow_tracking"
    mlflow_experiment_name: str = "datamind-etl"

    # ── Monitoring ───────────────────────────────────
    prometheus_enabled: bool = True
    log_level: str = "INFO"
    log_format: str = "json"

    # ── Environment ──────────────────────────────────
    environment: str = "development"

    @field_validator("chroma_persist_dir")
    @classmethod
    def ensure_chroma_dir(cls, v: str) -> str:
        Path(v).mkdir(parents=True, exist_ok=True)
        return v

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
