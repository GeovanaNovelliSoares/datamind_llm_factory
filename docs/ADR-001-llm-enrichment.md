# ADR-001: LLM enrichment inside the ETL pipeline

**Status:** Accepted | **Date:** 2024-06

## Context

Raw SaaS data contains free-text fields (churn_reason, plan names, company names) that rule-based ETL cannot classify reliably. Options:

| Option | Quality | Cost | Latency |
|---|---|---|---|
| Rule-based regex | Low | Free | <1ms |
| ML classifier (trained) | Medium | Free (after training) | <10ms |
| **LLM in-pipeline** | High | Groq free tier | ~200ms/row |
| Post-hoc LLM | High | Same | Same |

## Decision

LLM enrichment **inside** the ETL pipeline (not post-hoc), limited to `batch_size=50` rows per run.

## Rationale

1. Enriched fields (`plan_category`, `churn_reason_category`, `churn_risk_score`) feed directly into analytics and the NL→SQL layer — they must be available at query time.
2. Running in-pipeline ensures data consistency: every row in the DB was enriched with the same model version.
3. Batch size of 50 keeps Groq free-tier usage under 14.4k/day even with frequent uploads.
4. `LLM_ENRICHMENT_ENABLED=false` flag allows disabling for CI and cost-sensitive runs.

## Consequences

- **Positive:** Analytics quality significantly higher than rule-based. `plan_category` enables cross-plan comparisons that raw plan names cannot.
- **Negative:** ETL pipeline is ~3-5x slower with enrichment. Mitigated by async Celery worker.
- **Revisit:** If enrichment batch consistently >1000 rows, evaluate fine-tuned smaller model.
