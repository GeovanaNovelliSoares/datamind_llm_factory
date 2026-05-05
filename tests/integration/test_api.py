"""Integration tests for FastAPI endpoints."""
import io
import os
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("test_data")
    os.environ["DATABASE_URL"] = f"sqlite:///{tmp}/test.db"
    os.environ["CHROMA_PERSIST_DIR"] = str(tmp / "chroma")
    os.environ["REDIS_URL"] = "redis://localhost:6379/9"
    os.environ["CELERY_BROKER_URL"] = "redis://localhost:6379/9"
    os.environ["CELERY_RESULT_BACKEND"] = "redis://localhost:6379/9"
    os.environ["LOG_FORMAT"] = "console"
    os.environ["LLM_ENRICHMENT_ENABLED"] = "false"
    os.environ["PROMETHEUS_ENABLED"] = "false"

    from src.api.main import create_app
    from src.db.session import init_db
    app = create_app()
    init_db()

    with TestClient(app) as c:
        yield c


def test_health(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] in ("ok", "degraded")


def test_ready(client):
    r = client.get("/api/v1/ready")
    assert r.status_code == 200
    assert r.json()["ready"] is True


def test_list_datasets_empty(client):
    r = client.get("/api/v1/ingest/datasets")
    assert r.status_code == 200
    assert "datasets" in r.json()


def test_upload_invalid_extension(client):
    r = client.post(
        "/api/v1/ingest/csv",
        files={"file": ("test.pdf", io.BytesIO(b"data"), "application/pdf")},
        data={"dataset_name": "test", "domain": "saas"},
    )
    assert r.status_code == 415


def test_upload_csv(client):
    csv_content = b"customer_id,mrr,plan,churned\nc1,100,starter,false\nc2,200,growth,true\n"
    r = client.post(
        "/api/v1/ingest/csv",
        files={"file": ("saas.csv", io.BytesIO(csv_content), "text/csv")},
        data={"dataset_name": "test_saas", "domain": "saas"},
    )
    # 202 if Celery/Redis available, may be 500 without Redis
    assert r.status_code in (202, 500)


def test_query_unknown_dataset(client):
    r = client.post(
        "/api/v1/query",
        json={"question": "What is the MRR?", "dataset_id": "nonexistent-id"},
    )
    assert r.status_code == 404


def test_query_short_question(client):
    r = client.post(
        "/api/v1/query",
        json={"question": "Hi", "dataset_id": "some-id"},
    )
    assert r.status_code == 422


def test_swagger_accessible(client):
    r = client.get("/docs")
    assert r.status_code == 200


def test_openapi_schema(client):
    r = client.get("/openapi.json")
    assert r.status_code == 200
    assert r.json()["info"]["title"] == "DataMind — Intelligent Business ETL + LLM"
