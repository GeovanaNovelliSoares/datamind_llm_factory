"""Unit tests for ETL cleaner."""
import numpy as np
import pandas as pd
import pytest

from src.etl.cleaner import clean


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "customer_id": ["c1", "c2", "c3", "c4", "c5"],
        "mrr": [100.0, 200.0, None, 400.0, 500.0],
        "churned": [False, True, False, True, False],
        "start_date": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01"],
        "country": ["US", "BR", None, "DE", "UK"],
        "junk_col": [None, None, None, None, None],
    })


def test_clean_returns_dataframe(sample_df):
    df, report = clean(sample_df)
    assert isinstance(df, pd.DataFrame)


def test_clean_removes_high_null_columns(sample_df):
    df, report = clean(sample_df)
    assert "junk_col" not in df.columns
    assert "junk_col" in report.columns_dropped


def test_clean_fills_numeric_nulls(sample_df):
    df, report = clean(sample_df)
    assert df["mrr"].isnull().sum() == 0


def test_clean_fills_string_nulls(sample_df):
    df, report = clean(sample_df)
    assert df["country"].isnull().sum() == 0


def test_clean_removes_duplicates():
    df = pd.DataFrame({
        "customer_id": ["c1", "c1", "c2"],
        "mrr": [100, 100, 200],
    })
    cleaned, report = clean(df)
    assert len(cleaned) == 2
    assert report.duplicates_removed == 1


def test_clean_converts_dates(sample_df):
    df, report = clean(sample_df)
    assert pd.api.types.is_datetime64_any_dtype(df["start_date"])


def test_clean_converts_booleans(sample_df):
    df, report = clean(sample_df)
    assert df["churned"].dtype == bool


def test_clean_report_tracks_stats(sample_df):
    _, report = clean(sample_df)
    assert report.original_rows == 5
    assert report.final_rows <= 5
    assert isinstance(report.nulls_filled, dict)


def test_clean_caps_outliers():
    df = pd.DataFrame({
        "mrr": [100, 110, 105, 108, 99, 102, 10000]  # 10000 is outlier
    })
    cleaned, report = clean(df)
    assert "mrr" in report.outliers_capped
    assert cleaned["mrr"].max() < 10000
