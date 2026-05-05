"""Shared test configuration."""
import os
import pytest


@pytest.fixture(autouse=True, scope="session")
def configure_test_env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("test_root")
    overrides = {
        "DATABASE_URL": f"sqlite:///{tmp}/test.db",
        "CHROMA_PERSIST_DIR": str(tmp / "chroma"),
        "REDIS_URL": "redis://localhost:6379/9",
        "CELERY_BROKER_URL": "redis://localhost:6379/9",
        "CELERY_RESULT_BACKEND": "redis://localhost:6379/9",
        "LOG_FORMAT": "console",
        "LOG_LEVEL": "WARNING",
        "ENVIRONMENT": "development",
        "GROQ_API_KEY": "test-key",
        "LLM_ENRICHMENT_ENABLED": "false",
        "ANOMALY_LLM_EXPLAIN": "false",
        "PROMETHEUS_ENABLED": "false",
    }
    original = {}
    for k, v in overrides.items():
        original[k] = os.environ.get(k)
        os.environ[k] = v
    yield
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
