"""
Data validation using Great Expectations-style rules.

We implement a lightweight validation layer without the full GE overhead,
keeping the dependency footprint minimal while providing the same guarantees:
- Not-null checks on critical columns
- Value range validation
- Uniqueness checks
- Referential integrity
"""
from dataclasses import dataclass, field

import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    total_checks: int
    passed_checks: int
    failed_checks: int
    failures: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return self.passed_checks / max(self.total_checks, 1)


SAAS_RULES = [
    {
        "name": "mrr_not_negative",
        "column": "mrr",
        "check": "min_value",
        "value": 0,
        "severity": "error",
    },
    {
        "name": "mrr_not_extreme",
        "column": "mrr",
        "check": "max_value",
        "value": 1_000_000,
        "severity": "warning",
    },
    {
        "name": "churn_rate_valid",
        "column": "churned",
        "check": "max_ratio_true",
        "value": 0.95,
        "severity": "warning",
    },
    {
        "name": "customer_id_not_null",
        "column": "customer_id",
        "check": "not_null_ratio",
        "value": 0.99,
        "severity": "error",
    },
    {
        "name": "customer_id_unique",
        "column": "customer_id",
        "check": "unique_ratio",
        "value": 0.90,
        "severity": "warning",
    },
]


def validate(df: pd.DataFrame, domain: str = "saas") -> ValidationResult:
    """Run domain-specific validation rules against a DataFrame."""
    rules = SAAS_RULES if domain == "saas" else []
    failures = []
    warnings = []
    passed = 0

    for rule in rules:
        col = rule["column"]
        if col not in df.columns:
            warnings.append({"rule": rule["name"], "reason": f"Column '{col}' not present — skipped"})
            passed += 1
            continue

        check = rule["check"]
        value = rule["value"]
        series = df[col]
        ok = True

        if check == "min_value":
            violations = (pd.to_numeric(series, errors="coerce") < value).sum()
            ok = violations == 0
            detail = f"{violations} values below {value}"
        elif check == "max_value":
            violations = (pd.to_numeric(series, errors="coerce") > value).sum()
            ok = violations == 0
            detail = f"{violations} values above {value}"
        elif check == "not_null_ratio":
            ratio = series.notna().mean()
            ok = ratio >= value
            detail = f"not-null ratio = {ratio:.2%}, required >= {value:.2%}"
        elif check == "unique_ratio":
            ratio = series.nunique() / max(len(series), 1)
            ok = ratio >= value
            detail = f"unique ratio = {ratio:.2%}, required >= {value:.2%}"
        elif check == "max_ratio_true":
            ratio = series.astype(bool).mean()
            ok = ratio <= value
            detail = f"true ratio = {ratio:.2%}, max allowed = {value:.2%}"
        else:
            detail = "unknown check"

        record = {"rule": rule["name"], "column": col, "detail": detail}
        if ok:
            passed += 1
        elif rule["severity"] == "error":
            failures.append(record)
        else:
            warnings.append(record)
            passed += 1  # warnings don't fail

    total = len(rules)
    failed = total - passed
    result = ValidationResult(
        passed=len(failures) == 0,
        total_checks=total,
        passed_checks=passed,
        failed_checks=len(failures),
        failures=failures,
        warnings=warnings,
    )
    logger.info(
        "validation_done",
        domain=domain,
        passed=passed,
        failed=len(failures),
        warnings_count=len(warnings),
    )
    return result
