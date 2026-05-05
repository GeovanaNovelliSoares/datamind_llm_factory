"""Unit tests for schema detector."""
import pandas as pd

from src.ingestion.schema_detector import detect_schema, apply_mapping


def test_detect_schema_saas():
    df = pd.DataFrame({
        "customer_id": ["c1", "c2"],
        "mrr": [100, 200],
        "plan": ["starter", "growth"],
        "churned": [False, True],
        "status": ["active", "churned"],
        "start_date": ["2023-01-01", "2023-02-01"],
    })
    schema = detect_schema(df)
    assert schema.detected_domain == "saas"
    assert len(schema.column_mapping) > 0


def test_detect_schema_generic():
    df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
    schema = detect_schema(df)
    assert schema.detected_domain == "generic"


def test_detect_schema_has_dtypes():
    df = pd.DataFrame({"mrr": [100.0, 200.0], "name": ["a", "b"]})
    schema = detect_schema(df)
    assert "mrr" in schema.dtypes
    assert "name" in schema.dtypes


def test_detect_schema_nullable_columns():
    df = pd.DataFrame({"mrr": [100.0, None], "name": ["a", "b"]})
    schema = detect_schema(df)
    assert "mrr" in schema.nullable_columns


def test_apply_mapping():
    df = pd.DataFrame({"monthly_revenue": [100], "client_id": ["c1"]})
    mapping = {"monthly_revenue": "mrr", "client_id": "customer_id"}
    mapped = apply_mapping(df, mapping)
    assert "mrr" in mapped.columns
    assert "customer_id" in mapped.columns
