"""
ETL Cleaner — handles nulls, types, outliers, duplicates.

Design decision: cleaning is deterministic and rule-based.
No LLM here — LLM comes in the enrichment step only.
This keeps the pipeline fast and auditable.
"""
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.config import get_settings
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class CleanReport:
    original_rows: int
    final_rows: int
    duplicates_removed: int
    nulls_filled: dict[str, int]
    columns_dropped: list[str]
    type_conversions: dict[str, str]
    outliers_capped: dict[str, int]
    warnings: list[str] = field(default_factory=list)


DATE_COLUMNS = {"start_date", "churn_date", "created_at", "updated_at", "cancelled_at", "end_date"}
BOOL_COLUMNS = {"churned", "is_churned", "churn", "cancelled", "active"}
NUMERIC_COLUMNS = {"mrr", "arr", "seats", "users", "licenses", "revenue", "amount", "price"}


def clean(df: pd.DataFrame, dataset_name: str = "") -> tuple[pd.DataFrame, CleanReport]:
    """
    Full cleaning pipeline:
    1. Drop columns with too many nulls
    2. Remove duplicates
    3. Infer and cast types
    4. Fill remaining nulls
    5. Cap outliers
    """
    settings = get_settings()
    original_rows = len(df)
    report = CleanReport(
        original_rows=original_rows,
        final_rows=0,
        duplicates_removed=0,
        nulls_filled={},
        columns_dropped=[],
        type_conversions={},
        outliers_capped={},
    )

    df = df.copy()

    # ── 1. Drop high-null columns ─────────────────────────────────────────
    for col in df.columns:
        null_ratio = df[col].isnull().mean()
        if null_ratio > settings.null_ratio_threshold:
            df.drop(columns=[col], inplace=True)
            report.columns_dropped.append(col)
            logger.info("column_dropped_high_null", col=col, null_ratio=f"{null_ratio:.0%}")

    # ── 2. Remove duplicates ──────────────────────────────────────────────
    before = len(df)
    df.drop_duplicates(inplace=True)
    report.duplicates_removed = before - len(df)

    # ── 3. Type inference & casting ───────────────────────────────────────
    for col in df.columns:
        col_lower = col.lower()

        # Date columns
        if col_lower in DATE_COLUMNS or col_lower.endswith("_date") or col_lower.endswith("_at"):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                report.type_conversions[col] = "datetime"
            except Exception:
                pass

        # Boolean columns
        elif col_lower in BOOL_COLUMNS:
            df[col] = df[col].map(
                {True: True, False: False, 1: True, 0: False,
                 "true": True, "false": False, "yes": True, "no": False,
                 "1": True, "0": False}
            ).fillna(False).astype(bool)
            report.type_conversions[col] = "bool"

        # Numeric columns
        elif col_lower in NUMERIC_COLUMNS or col_lower.startswith("mrr") or col_lower.startswith("arr"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            report.type_conversions[col] = "float"

        # Try numeric for object columns
        elif df[col].dtype == "object":
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().mean() > 0.8:
                df[col] = converted
                report.type_conversions[col] = "float_inferred"

    # ── 4. Fill nulls ─────────────────────────────────────────────────────
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count == 0:
            continue

        if pd.api.types.is_numeric_dtype(df[col]):
            fill_val = df[col].median()
            df[col].fillna(fill_val, inplace=True)
            report.nulls_filled[col] = int(null_count)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            pass  # keep NaT for date columns
        else:
            df[col].fillna("unknown", inplace=True)
            report.nulls_filled[col] = int(null_count)

    # ── 5. Cap outliers (IQR) for numeric cols ────────────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col.lower() in {"id", "row_id", "index"}:
            continue
        q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 3 * iqr
        upper = q3 + 3 * iqr
        outliers = ((df[col] < lower) | (df[col] > upper)).sum()
        if outliers > 0:
            df[col] = df[col].clip(lower=lower, upper=upper)
            report.outliers_capped[col] = int(outliers)

    report.final_rows = len(df)
    logger.info(
        "cleaning_done",
        dataset=dataset_name,
        original=original_rows,
        final=report.final_rows,
        duplicates=report.duplicates_removed,
        dropped_cols=len(report.columns_dropped),
    )
    return df, report
