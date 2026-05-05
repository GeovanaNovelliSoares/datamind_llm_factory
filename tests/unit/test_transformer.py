"""Unit tests for ETL transformer."""
import pandas as pd
import pytest

from src.etl.transformer import transform_saas


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "customer_id": ["c1", "c2", "c3", "c4"],
        "mrr": [100.0, 200.0, 300.0, 400.0],
        "churned": [False, False, True, True],
        "status": ["active", "active", "churned", "churned"],
        "start_date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-01-15", "2023-03-01"]),
        "churn_date": pd.to_datetime([None, None, "2023-06-01", "2023-07-01"]),
        "plan": ["starter", "growth", "starter", "professional"],
        "country": ["US", "BR", "US", "DE"],
    })


def test_transform_returns_result(sample_df):
    result = transform_saas(sample_df, "ds_001")
    assert result.subscriptions_df is not None
    assert result.metrics_df is not None
    assert isinstance(result.summary, dict)


def test_transform_adds_arr(sample_df):
    result = transform_saas(sample_df, "ds_001")
    assert "arr" in result.subscriptions_df.columns
    assert (result.subscriptions_df["arr"] == result.subscriptions_df["mrr"] * 12).all()


def test_transform_adds_lifetime(sample_df):
    result = transform_saas(sample_df, "ds_001")
    assert "lifetime_months" in result.subscriptions_df.columns
    churned = result.subscriptions_df[result.subscriptions_df["churned"] == True]
    assert churned["lifetime_months"].notna().any()


def test_transform_summary_keys(sample_df):
    result = transform_saas(sample_df, "ds_001")
    for key in ["total_customers", "active_customers", "churned_customers", "total_mrr", "churn_rate"]:
        assert key in result.summary


def test_transform_churn_rate(sample_df):
    result = transform_saas(sample_df, "ds_001")
    assert 0 <= result.summary["churn_rate"] <= 1


def test_transform_metrics_has_periods(sample_df):
    result = transform_saas(sample_df, "ds_001")
    assert len(result.metrics_df) >= 1
    assert "mrr" in result.metrics_df.columns
    assert "churn_rate" in result.metrics_df.columns


def test_transform_dataset_id_set(sample_df):
    result = transform_saas(sample_df, "ds_test_123")
    assert (result.subscriptions_df["dataset_id"] == "ds_test_123").all()
