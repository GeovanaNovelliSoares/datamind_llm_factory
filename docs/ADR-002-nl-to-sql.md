# ADR-002: NL→SQL over full RAG for structured data queries

**Status:** Accepted | **Date:** 2024-06

## Context

Users need to query structured SaaS data (PostgreSQL tables) in natural language. Two approaches:

| Approach | Accuracy | Latency | Complexity |
|---|---|---|---|
| Full RAG (embed rows → retrieve → answer) | Medium | High | High |
| **NL→SQL (schema in context → generate SQL → execute)** | High | Low | Medium |
| Pre-built dashboards only | Low | Instant | Low |

## Decision

**NL→SQL** with the full schema in the LLM context window.

## Rationale

1. **Structured data = SQL is the right abstraction.** RAG was designed for unstructured text. Embedding tabular rows loses relational structure (JOINs, aggregations, filters).
2. **Schema fits in context.** Our schema (3 tables, ~30 columns) fits in <1000 tokens — no retrieval needed.
3. **Deterministic and auditable.** Generated SQL is shown to the user, making the answer verifiable.
4. **Safety.** All generated queries are validated to be SELECT-only before execution.

## Consequences

- **Positive:** Complex aggregations ("average MRR by plan for active customers") work correctly where RAG would hallucinate.
- **Negative:** Breaks on very complex multi-step analytical questions. Mitigation: retry loop (max 3 attempts) with error feedback to LLM.
- **Negative:** Schema changes require updating `SCHEMA_DESCRIPTION` in `nl_to_sql.py`. Mitigation: auto-generate from SQLAlchemy metadata in future iteration.

## Revisit trigger

If >20% of NL→SQL queries fail after 3 retries, add intermediate reasoning step (chain-of-thought before SQL generation).
