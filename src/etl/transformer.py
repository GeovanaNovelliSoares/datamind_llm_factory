"""
ETL Transformer — business logic transformations for SaaS domain.

Computes derived metrics: ARR, lifetime months, churn rate, ARPU, NRR.
Groups data into monthly MRR snapshots for time-series analysis.
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass

from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TransformResult:
    subscriptions_df: pd.DataFrame
    metrics_df: pd.DataFrame  # monthly KPI snapshots
    summary: dict


def transform_saas(df: pd.DataFrame, dataset_id: str) -> TransformResult:
    """
    Transform raw SaaS subscription data into clean analytics tables.

    Input: cleaned DataFrame with canonical column names
    Output: subscriptions table + monthly metrics table
    """
    df = df.copy()

    # ── Derive ARR ────────────────────────────────────────────────────────
    if "mrr" in df.columns:
        df["arr"] = df["mrr"] * 12
    else:
        df["mrr"] = 0.0
        df["arr"] = 0.0

    # ── Derive lifetime_months ────────────────────────────────────────────
    if "start_date" in df.columns and "churn_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["churn_date"] = pd.to_datetime(df["churn_date"], errors="coerce")
        mask = df["churn_date"].notna() & df["start_date"].notna()
        df.loc[mask, "lifetime_months"] = (
            (df.loc[mask, "churn_date"] - df.loc[mask, "start_date"])
            .dt.days / 30.44
        ).round(1)

    # ── Normalize churned boolean ─────────────────────────────────────────
    if "churned" not in df.columns:
        if "status" in df.columns:
            df["churned"] = df["status"].str.lower().isin(["churned", "cancelled", "canceled"])
        else:
            df["churned"] = False

    df["dataset_id"] = dataset_id

    # ── Build monthly metrics snapshot ────────────────────────────────────
    metrics_df = _build_monthly_metrics(df, dataset_id)

    summary = {
        "total_customers": len(df),
        "active_customers": int((~df["churned"]).sum()),
        "churned_customers": int(df["churned"].sum()),
        "total_mrr": float(df.loc[~df["churned"], "mrr"].sum()),
        "total_arr": float(df.loc[~df["churned"], "arr"].sum()),
        "churn_rate": float(df["churned"].mean()),
        "avg_mrr_per_customer": float(df.loc[~df["churned"], "mrr"].mean()) if (~df["churned"]).any() else 0,
        "monthly_snapshots": len(metrics_df),
    }

    logger.info("transform_done", dataset_id=dataset_id, **summary)
    return TransformResult(subscriptions_df=df, metrics_df=metrics_df, summary=summary)


def _build_monthly_metrics(df: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    """Aggregate subscriptions into monthly MRR/churn snapshots."""
    if "start_date" not in df.columns or df["start_date"].isna().all():
        # No date info — return single snapshot
        active = df[~df["churned"]]
        return pd.DataFrame([{
            "dataset_id": dataset_id,
            "period": pd.Timestamp.now().strftime("%Y-%m"),
            "mrr": float(active["mrr"].sum()),
            "new_mrr": float(active["mrr"].sum()),
            "expansion_mrr": 0.0,
            "churned_mrr": float(df.loc[df["churned"], "mrr"].sum()),
            "net_new_mrr": float(active["mrr"].sum()) - float(df.loc[df["churned"], "mrr"].sum()),
            "active_customers": int((~df["churned"]).sum()),
            "new_customers": len(df),
            "churned_customers": int(df["churned"].sum()),
            "churn_rate": float(df["churned"].mean()),
            "arpu": float(active["mrr"].mean()) if len(active) > 0 else 0,
        }])

    df["period"] = df["start_date"].dt.to_period("M").astype(str)
    periods = sorted(df["period"].dropna().unique())

    rows = []
    for period in periods:
        period_start = pd.Period(period, "M").start_time
        period_end = pd.Period(period, "M").end_time

        # Active at end of period
        active_mask = (
            (df["start_date"] <= period_end) &
            (df["churn_date"].isna() | (df["churn_date"] > period_end))
        )
        active_df = df[active_mask]

        # New in period
        new_mask = (df["start_date"] >= period_start) & (df["start_date"] <= period_end)
        new_df = df[new_mask]

        # Churned in period
        churned_mask = (
            df["churn_date"].notna() &
            (df["churn_date"] >= period_start) & (df["churn_date"] <= period_end)
        )
        churned_df = df[churned_mask]

        mrr = float(active_df["mrr"].sum())
        churned_mrr = float(churned_df["mrr"].sum())
        new_mrr = float(new_df["mrr"].sum())
        active_count = len(active_df)
        churn_rate = len(churned_df) / max(active_count, 1)

        rows.append({
            "dataset_id": dataset_id,
            "period": period,
            "mrr": mrr,
            "new_mrr": new_mrr,
            "expansion_mrr": 0.0,
            "churned_mrr": churned_mrr,
            "net_new_mrr": new_mrr - churned_mrr,
            "active_customers": active_count,
            "new_customers": len(new_df),
            "churned_customers": len(churned_df),
            "churn_rate": round(churn_rate, 4),
            "arpu": round(mrr / max(active_count, 1), 2),
            "nrr": round((mrr / max(mrr - new_mrr + churned_mrr, 1)) * 100, 2),
        })

    return pd.DataFrame(rows)
