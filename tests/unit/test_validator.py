"""Unit tests for data validator."""
import pandas as pd
import pytest

from src.etl.validator import validate


def test_validate_passes_clean_data():
    df = pd.DataFrame({
        "customer_id": ["c1", "c2", "c3"],
        "mrr": [100.0, 200.0, 300.0],
        "churned": [False, False, True],
    })
    result = validate(df, domain="saas")
    assert result.passed is True
    assert result.failed_checks == 0


def test_validate_fails_negative_mrr():
    df = pd.DataFrame({
        "customer_id": ["c1", "c2"],
        "mrr": [-100.0, 200.0],
        "churned": [False, False],
    })
    result = validate(df, domain="saas")
    assert result.passed is False
    assert any("mrr_not_negative" in f["rule"] for f in result.failures)


def test_validate_warns_high_churn():
    df = pd.DataFrame({
        "customer_id": [f"c{i}" for i in range(100)],
        "mrr": [100.0] * 100,
        "churned": [True] * 99 + [False],  # 99% churn — warning
    })
    result = validate(df, domain="saas")
    assert any("churn_rate" in w["rule"] for w in result.warnings)


def test_validate_skips_missing_columns():
    df = pd.DataFrame({"some_col": [1, 2, 3]})
    result = validate(df, domain="saas")
    # Should not crash — missing columns are skipped
    assert result.total_checks >= 0


def test_validate_pass_rate():
    df = pd.DataFrame({
        "customer_id": ["c1", "c2"],
        "mrr": [100.0, 200.0],
        "churned": [False, False],
    })
    result = validate(df, domain="saas")
    assert 0 <= result.pass_rate <= 1
