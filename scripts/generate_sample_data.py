"""
Generate realistic SaaS subscription sample data for testing.

Usage:
    python scripts/generate_sample_data.py --rows 500 --output data/saas_sample.csv
"""
import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

PLANS = ["starter", "growth", "professional", "enterprise", "custom"]
PLAN_MRR = {"starter": (49, 99), "growth": (199, 499), "professional": (499, 999),
             "enterprise": (999, 4999), "custom": (2000, 15000)}
COUNTRIES = ["US", "BR", "UK", "DE", "FR", "CA", "AU", "IN", "MX", "NL"]
INDUSTRIES = ["tech", "finance", "healthcare", "retail", "education", "manufacturing", "media", "other"]
CHURN_REASONS = [
    "Too expensive for our budget",
    "Switched to competitor",
    "Missing key features",
    "Poor customer support experience",
    "Company went out of business",
    "Not using the product enough",
    "Found a cheaper alternative",
    "Product doesn't fit our workflow",
    None, None, None,  # active customers have no reason
]
COMPANY_PREFIXES = ["Acme", "Nova", "Apex", "Blue", "Green", "Fast", "Smart", "Cloud", "Data", "Tech"]
COMPANY_SUFFIXES = ["Corp", "Inc", "Ltd", "Solutions", "Systems", "Labs", "Group", "Co", "HQ", "Works"]


def random_company():
    return f"{random.choice(COMPANY_PREFIXES)} {random.choice(COMPANY_SUFFIXES)}"


def generate_saas_data(n_rows: int = 500) -> pd.DataFrame:
    rows = []
    start_base = datetime(2022, 1, 1)

    for i in range(n_rows):
        plan = random.choice(PLANS)
        mrr_min, mrr_max = PLAN_MRR[plan]
        mrr = round(random.uniform(mrr_min, mrr_max), 2)

        start_date = start_base + timedelta(days=random.randint(0, 700))
        churned = random.random() < 0.28  # ~28% churn rate

        churn_date = None
        churn_reason = None
        if churned:
            days_active = random.randint(30, 540)
            churn_date = start_date + timedelta(days=days_active)
            churn_reason = random.choice([r for r in CHURN_REASONS if r is not None])

        # Inject some anomalies (~3%)
        if random.random() < 0.03:
            mrr = mrr * random.choice([10, 0.01, 50])

        rows.append({
            "customer_id": f"cust_{i+1:04d}",
            "customer_name": random_company(),
            "plan": plan,
            "mrr": mrr,
            "status": "churned" if churned else random.choice(["active", "active", "active", "trial"]),
            "churned": churned,
            "churn_reason": churn_reason,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "churn_date": churn_date.strftime("%Y-%m-%d") if churn_date else None,
            "country": random.choice(COUNTRIES),
            "industry": random.choice(INDUSTRIES),
            "seats": random.randint(1, 500) if plan in ("enterprise", "custom") else random.randint(1, 50),
        })

    df = pd.DataFrame(rows)

    # Inject nulls (~5%) to test cleaning
    for col in ["churn_reason", "country", "seats"]:
        mask = np.random.random(len(df)) < 0.05
        df.loc[mask, col] = None

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--output", default="data/saas_sample.csv")
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df = generate_saas_data(args.rows)
    df.to_csv(args.output, index=False)

    print(f"✓ Generated {len(df)} rows → {args.output}")
    print(f"  Active: {(~df['churned']).sum()} | Churned: {df['churned'].sum()}")
    print(f"  MRR total: ${df.loc[~df['churned'], 'mrr'].sum():,.0f}")
    print(f"  Plans: {df['plan'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
