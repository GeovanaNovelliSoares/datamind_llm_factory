"""
LLM Enricher — adds business intelligence to cleaned data.

This is the key differentiator: instead of just cleaning data,
we use LLM to classify, score, and categorize fields that
rule-based systems cannot handle (free-text reasons, plan names, industries).

Runs in batches to respect rate limits and minimize cost.
"""
import json
from dataclasses import dataclass

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import get_settings
from src.llm.client import call_llm
from src.llm.prompts import (
    ENRICH_CHURN_REASON,
    ENRICH_CHURN_RISK,
    ENRICH_INDUSTRY,
    ENRICH_PLAN_CATEGORY,
)
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class EnrichmentReport:
    rows_enriched: int
    fields_enriched: list[str]
    tokens_used: int
    errors: int


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5))
def _safe_llm_json(prompt: str, fallback: dict) -> dict:
    """Call LLM expecting JSON response. Returns fallback on failure."""
    try:
        text, _, _ = call_llm(prompt, json_mode=True, max_tokens=256)
        text = text.strip().strip("```json").strip("```").strip()
        return json.loads(text)
    except Exception as e:
        logger.warning("llm_json_failed", error=str(e))
        return fallback


def enrich_saas(df: pd.DataFrame) -> tuple[pd.DataFrame, EnrichmentReport]:
    """
    Enrich SaaS subscription DataFrame with LLM-derived fields:
    - plan_category: normalized plan tier
    - churn_reason_category: classified churn reason
    - churn_risk_score: predicted churn risk 0-1
    - industry_category: company industry classification
    """
    settings = get_settings()
    if not settings.llm_enrichment_enabled:
        logger.info("llm_enrichment_disabled")
        return df, EnrichmentReport(0, [], 0, 0)

    df = df.copy()
    batch_size = settings.llm_enrichment_batch_size
    tokens_total = 0
    errors = 0
    enriched_fields = []

    total = min(len(df), batch_size)
    logger.info("enrichment_started", rows=total)

    for i, (idx, row) in enumerate(df.head(batch_size).iterrows()):
        if i % 10 == 0:
            logger.info("enrichment_progress", done=i, total=total)

        # ── Plan category ─────────────────────────────────────────────
        if "plan" in df.columns and pd.notna(row.get("plan")):
            result = _safe_llm_json(
                ENRICH_PLAN_CATEGORY.format(
                    plan=row.get("plan", ""),
                    mrr=row.get("mrr", 0),
                    seats=row.get("seats", "unknown"),
                ),
                {"plan_category": "unknown"},
            )
            df.at[idx, "plan_category"] = result.get("plan_category", "unknown")
            if "plan_category" not in enriched_fields:
                enriched_fields.append("plan_category")

        # ── Churn reason category ─────────────────────────────────────
        if "churn_reason" in df.columns and pd.notna(row.get("churn_reason")):
            reason = str(row.get("churn_reason", ""))
            if reason and reason != "unknown" and len(reason) > 3:
                result = _safe_llm_json(
                    ENRICH_CHURN_REASON.format(reason=reason),
                    {"category": "other"},
                )
                df.at[idx, "churn_reason_category"] = result.get("category", "other")
                if "churn_reason_category" not in enriched_fields:
                    enriched_fields.append("churn_reason_category")

        # ── Churn risk score ──────────────────────────────────────────
        if "mrr" in df.columns:
            result = _safe_llm_json(
                ENRICH_CHURN_RISK.format(
                    plan=row.get("plan", "unknown"),
                    mrr=row.get("mrr", 0),
                    lifetime=row.get("lifetime_months", "unknown"),
                    country=row.get("country", "unknown"),
                    seats=row.get("seats", "unknown"),
                    status=row.get("status", "unknown"),
                ),
                {"churn_risk_score": 0.5},
            )
            df.at[idx, "churn_risk_score"] = float(result.get("churn_risk_score", 0.5))
            if "churn_risk_score" not in enriched_fields:
                enriched_fields.append("churn_risk_score")

        # ── Industry category ─────────────────────────────────────────
        if "customer_name" in df.columns and pd.notna(row.get("customer_name")):
            result = _safe_llm_json(
                ENRICH_INDUSTRY.format(
                    customer_name=row.get("customer_name", ""),
                    plan=row.get("plan", ""),
                    country=row.get("country", ""),
                ),
                {"industry_category": "other"},
            )
            df.at[idx, "industry_category"] = result.get("industry_category", "other")
            if "industry_category" not in enriched_fields:
                enriched_fields.append("industry_category")

    report = EnrichmentReport(
        rows_enriched=total,
        fields_enriched=enriched_fields,
        tokens_used=tokens_total,
        errors=errors,
    )
    logger.info("enrichment_done", rows=total, fields=enriched_fields, errors=errors)
    return df, report
