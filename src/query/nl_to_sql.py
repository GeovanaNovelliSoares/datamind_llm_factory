"""
NL→SQL Agent — converts natural language questions to PostgreSQL queries.

Architecture decision (ADR-002):
We use LLM directly for NL→SQL rather than full RAG because:
- The schema is structured and finite (can fit in context)
- SQL is deterministic — same schema + question = same query
- RAG adds latency without benefit for structured queries

Safety: queries are READ-ONLY (no INSERT/UPDATE/DELETE allowed).
"""
import re
import time

import pandas as pd
from sqlalchemy import text

from src.db.session import engine
from src.llm.client import call_llm
from src.llm.prompts import NL_TO_SQL_ANSWER, NL_TO_SQL_SYSTEM
from src.logger import get_logger

logger = get_logger(__name__)

SCHEMA_DESCRIPTION = """
TABLE: saas_subscriptions
  - id (text): primary key
  - dataset_id (text): dataset identifier
  - customer_id (text): unique customer identifier
  - customer_name (text): company name
  - plan (text): subscription plan name
  - plan_category (text): starter|growth|professional|enterprise|custom
  - mrr (numeric): monthly recurring revenue in USD
  - arr (numeric): annual recurring revenue in USD
  - status (text): active|churned|trial
  - churned (boolean): true if customer has churned
  - churn_reason (text): free text churn reason
  - churn_reason_category (text): price|product|competition|support|usage|other
  - churn_risk_score (float): 0.0-1.0 predicted churn risk
  - start_date (timestamp): subscription start date
  - churn_date (timestamp): churn date (null if active)
  - lifetime_months (float): months as customer
  - country (text): customer country
  - industry_category (text): tech|finance|healthcare|retail|education|manufacturing|media|other
  - seats (integer): number of seats/licenses

TABLE: saas_metrics (monthly snapshots)
  - dataset_id (text)
  - period (text): YYYY-MM format
  - mrr (numeric): total MRR at end of period
  - new_mrr (numeric): MRR from new customers
  - churned_mrr (numeric): MRR lost to churn
  - net_new_mrr (numeric): net MRR change
  - active_customers (integer)
  - new_customers (integer)
  - churned_customers (integer)
  - churn_rate (float): monthly churn rate 0-1
  - nrr (float): net revenue retention %
  - arpu (numeric): average revenue per user

TABLE: anomaly_records
  - dataset_id (text)
  - column_name (text): which column has the anomaly
  - method (text): zscore|iqr|zscore+iqr
  - value (float): the anomalous value
  - score (float): anomaly score
  - llm_explanation (text): business explanation
  - severity (text): low|medium|high
"""

BLOCKED_KEYWORDS = ["insert", "update", "delete", "drop", "truncate", "alter", "create", "grant"]


def _is_safe_sql(sql: str) -> bool:
    """Reject any non-SELECT SQL."""
    sql_lower = sql.lower().strip()
    if not sql_lower.startswith("select"):
        return False
    return not any(kw in sql_lower for kw in BLOCKED_KEYWORDS)


def _clean_sql(raw: str) -> str:
    """Strip markdown fences and extra whitespace."""
    cleaned = re.sub(r"```sql|```", "", raw, flags=re.IGNORECASE).strip()
    return cleaned


def run_nl_to_sql(question: str, dataset_id: str) -> dict:
    """
    Convert natural language question to SQL, execute, and generate answer.

    Returns:
        {
            "question": str,
            "sql": str,
            "result": list[dict],
            "answer": str,
            "latency_ms": float,
            "success": bool,
            "error": str | None
        }
    """
    start = time.perf_counter()

    # ── Step 1: Generate SQL ──────────────────────────────────────────────
    system = NL_TO_SQL_SYSTEM.format(schema=SCHEMA_DESCRIPTION, dataset_id=dataset_id)
    settings_retries = 3
    sql = None

    for attempt in range(settings_retries):
        try:
            raw_sql, _, _ = call_llm(question, system=system, temperature=0.0, max_tokens=512)
            sql = _clean_sql(raw_sql)
            if _is_safe_sql(sql):
                break
            else:
                logger.warning("unsafe_sql_generated", attempt=attempt, sql=sql[:100])
                sql = None
        except Exception as e:
            logger.warning("sql_generation_failed", attempt=attempt, error=str(e))

    if not sql:
        return {
            "question": question,
            "sql": "",
            "result": [],
            "answer": "Could not generate a safe SQL query for this question.",
            "latency_ms": (time.perf_counter() - start) * 1000,
            "success": False,
            "error": "SQL generation failed or produced unsafe query",
        }

    # ── Step 2: Execute SQL ───────────────────────────────────────────────
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = [dict(r._mapping) for r in result.fetchmany(100)]
    except Exception as e:
        logger.error("sql_execution_failed", sql=sql[:200], error=str(e))
        return {
            "question": question,
            "sql": sql,
            "result": [],
            "answer": f"SQL executed but returned an error: {e}",
            "latency_ms": (time.perf_counter() - start) * 1000,
            "success": False,
            "error": str(e),
        }

    # ── Step 3: Generate natural language answer ──────────────────────────
    result_sample = str(rows[:5]) if rows else "No rows returned"
    answer_prompt = NL_TO_SQL_ANSWER.format(
        question=question,
        sql=sql,
        rows=len(rows),
        result_sample=result_sample,
    )
    try:
        answer, _, _ = call_llm(answer_prompt, max_tokens=512)
    except Exception:
        answer = f"Query returned {len(rows)} rows. See the data table for details."

    latency_ms = (time.perf_counter() - start) * 1000
    logger.info("nl_to_sql_done", question=question[:80], rows=len(rows), latency_ms=round(latency_ms))

    return {
        "question": question,
        "sql": sql,
        "result": rows,
        "answer": answer.strip(),
        "latency_ms": latency_ms,
        "success": True,
        "error": None,
    }
