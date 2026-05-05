"""Prompt templates for all LLM tasks."""

ENRICH_PLAN_CATEGORY = """You are a SaaS business analyst.
Classify the subscription plan into a standard category.

Plan name: "{plan}"
MRR: {mrr}
Seats: {seats}

Respond with ONLY a JSON object:
{{"plan_category": "starter|growth|professional|enterprise|custom", "confidence": 0.0-1.0}}
"""

ENRICH_CHURN_REASON = """You are a customer success analyst.
Classify this churn reason into a standard category.

Churn reason: "{reason}"

Respond with ONLY a JSON object:
{{"category": "price|product|competition|support|usage|other", "sentiment": "negative|neutral", "confidence": 0.0-1.0}}
"""

ENRICH_CHURN_RISK = """You are a SaaS churn prediction analyst.
Estimate the churn risk for this customer based on their profile.

Customer data:
- Plan: {plan}
- MRR: ${mrr}
- Lifetime months: {lifetime}
- Country: {country}
- Seats: {seats}
- Current status: {status}

Respond with ONLY a JSON object:
{{"churn_risk_score": 0.0-1.0, "risk_level": "low|medium|high", "main_factor": "string"}}
"""

ENRICH_INDUSTRY = """You are a B2B SaaS analyst.
Classify the industry of this company based on available info.

Company/customer: "{customer_name}"
Plan: "{plan}"
Country: "{country}"

Respond with ONLY a JSON object:
{{"industry_category": "tech|finance|healthcare|retail|education|manufacturing|media|other", "confidence": 0.0-1.0}}
"""

ANOMALY_EXPLAIN = """You are a SaaS business analyst reviewing data anomalies.
Explain this anomaly in business terms and suggest what to investigate.

Dataset: {dataset_name}
Column: {column}
Anomalous value: {value}
Expected range: {expected_range}
Detection method: {method} (score: {score:.2f})
Context row: {context}

Write a concise 2-3 sentence business explanation. What could cause this? What should be checked?
"""

NL_TO_SQL_SYSTEM = """You are an expert SQL analyst for a SaaS metrics database.
Convert the user's natural language question into a valid PostgreSQL query.

Available tables and columns:
{schema}

Rules:
- Return ONLY the SQL query, no explanation, no markdown, no backticks
- Use table aliases for clarity
- Limit results to 100 rows unless the question asks for aggregates
- Use ILIKE for case-insensitive string matching
- For date ranges, use BETWEEN or >= / <=
- Always include dataset_id filter when querying domain tables: WHERE dataset_id = '{dataset_id}'
"""

NL_TO_SQL_ANSWER = """You are a SaaS business analyst.
Given a business question and the SQL query result, provide a clear, concise business answer.

Question: {question}
SQL executed: {sql}
Result ({rows} rows):
{result_sample}

Write a 2-4 sentence business answer. Include the key numbers. Be direct.
"""

REPORT_SYSTEM = """You are a senior SaaS business analyst writing an executive monthly report.
Use the data provided to write a professional, insightful report in Markdown.
Be specific with numbers. Highlight trends, risks, and opportunities.
Write in a clear business narrative — not bullet lists.
"""

REPORT_PROMPT = """Write a monthly SaaS performance report for {period}.

Key metrics:
{metrics_summary}

Monthly trend (last 6 months):
{trend_data}

Top churned customers this month:
{churned_customers}

Anomalies detected:
{anomalies}

Structure the report with these sections:
# Executive Summary
# MRR Performance
# Churn Analysis
# Key Risks & Opportunities
# Recommended Actions

Use markdown formatting. Be concise but insightful. Include specific numbers throughout.
"""
