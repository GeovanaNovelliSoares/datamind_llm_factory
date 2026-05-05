"""Automatic schema detection and type inference."""
from dataclasses import dataclass, field

import pandas as pd

from src.logger import get_logger

logger = get_logger(__name__)

SAAS_COLUMN_HINTS = {
    "customer_id": ["customer_id", "client_id", "account_id", "user_id"],
    "mrr": ["mrr", "monthly_recurring_revenue", "monthly_revenue", "revenue"],
    "plan": ["plan", "tier", "subscription_plan", "package"],
    "status": ["status", "subscription_status", "account_status"],
    "churned": ["churned", "is_churned", "churn", "cancelled"],
    "churn_date": ["churn_date", "cancelled_at", "end_date"],
    "start_date": ["start_date", "created_at", "subscription_start"],
    "churn_reason": ["churn_reason", "cancellation_reason", "reason"],
    "country": ["country", "country_code", "region"],
    "seats": ["seats", "users", "licenses", "quantity"],
}


@dataclass
class SchemaInfo:
    columns: list[str]
    dtypes: dict[str, str]
    nullable_columns: list[str]
    detected_domain: str
    column_mapping: dict[str, str]  # detected_col -> canonical_col
    sample_rows: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def detect_schema(df: pd.DataFrame) -> SchemaInfo:
    """Infer schema and map columns to canonical SaaS field names."""
    dtypes = {col: str(df[col].dtype) for col in df.columns}
    nullable = [col for col in df.columns if df[col].isnull().any()]

    # Try to detect domain
    col_set = set(df.columns)
    saas_score = sum(
        1 for hints in SAAS_COLUMN_HINTS.values()
        if any(h in col_set for h in hints)
    )
    domain = "saas" if saas_score >= 3 else "generic"

    # Map to canonical column names
    mapping: dict[str, str] = {}
    for canonical, hints in SAAS_COLUMN_HINTS.items():
        for col in df.columns:
            if col in hints:
                mapping[col] = canonical
                break

    # Warnings
    warnings = []
    for col in df.columns:
        null_ratio = df[col].isnull().mean()
        if null_ratio > 0.5:
            warnings.append(f"Column '{col}' has {null_ratio:.0%} nulls — consider dropping")
        if df[col].dtype == "object" and df[col].nunique() == len(df):
            warnings.append(f"Column '{col}' may be an ID column (all unique)")

    sample = df.head(3).to_dict(orient="records")

    logger.info(
        "schema_detected",
        domain=domain,
        columns=len(df.columns),
        mapped=len(mapping),
        warnings=len(warnings),
    )

    return SchemaInfo(
        columns=list(df.columns),
        dtypes=dtypes,
        nullable_columns=nullable,
        detected_domain=domain,
        column_mapping=mapping,
        sample_rows=sample,
        warnings=warnings,
    )


def apply_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename columns to canonical names and drop unmapped ones."""
    return df.rename(columns=mapping)
