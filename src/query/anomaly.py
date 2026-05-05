"""
Anomaly detection: Z-score + IQR with LLM explanation.

Two-stage approach:
1. Statistical detection (Z-score and IQR) — fast, deterministic
2. LLM explanation — converts statistical anomaly into business insight
"""
import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.config import get_settings
from src.llm.client import call_llm
from src.llm.prompts import ANOMALY_EXPLAIN
from src.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Anomaly:
    column: str
    method: str
    value: float
    score: float
    row_index: int | None
    row_context: dict
    llm_explanation: str | None
    severity: str  # low | medium | high


def detect_anomalies(
    df: pd.DataFrame,
    dataset_name: str = "",
    explain_with_llm: bool = True,
) -> list[Anomaly]:
    """
    Detect anomalies in all numeric columns using Z-score and IQR.
    Optionally explains each anomaly with LLM.
    """
    settings = get_settings()
    anomalies: list[Anomaly] = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # Skip ID-like columns
    numeric_cols = [c for c in numeric_cols if not any(
        skip in c.lower() for skip in ["id", "index", "row"]
    )]

    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) < 10:
            continue

        # Z-score
        mean, std = series.mean(), series.std()
        if std == 0:
            continue
        z_scores = ((df[col] - mean) / std).abs()
        z_mask = z_scores > settings.anomaly_z_threshold

        # IQR
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - settings.anomaly_iqr_multiplier * iqr
        upper = q3 + settings.anomaly_iqr_multiplier * iqr
        iqr_mask = (df[col] < lower) | (df[col] > upper)

        combined_mask = z_mask | iqr_mask

        for idx in df[combined_mask].index:
            val = float(df.at[idx, col])
            z_score = float(z_scores.at[idx]) if idx in z_scores.index else 0
            method = "zscore+iqr" if (z_mask.at[idx] and iqr_mask.at[idx]) else ("zscore" if z_mask.at[idx] else "iqr")

            severity = "high" if z_score > 4 else ("medium" if z_score > 3 else "low")
            context = df.loc[idx].to_dict()

            explanation = None
            if explain_with_llm and settings.anomaly_llm_explain:
                explanation = _explain_anomaly(
                    dataset_name=dataset_name,
                    column=col,
                    value=val,
                    expected_range=f"{lower:.2f} – {upper:.2f}",
                    method=method,
                    score=z_score,
                    context=context,
                )

            anomalies.append(Anomaly(
                column=col,
                method=method,
                value=val,
                score=z_score,
                row_index=int(idx),
                row_context=context,
                llm_explanation=explanation,
                severity=severity,
            ))

    logger.info("anomaly_detection_done", dataset=dataset_name, count=len(anomalies))
    return anomalies


def _explain_anomaly(
    dataset_name: str,
    column: str,
    value: float,
    expected_range: str,
    method: str,
    score: float,
    context: dict,
) -> str | None:
    """Use LLM to generate a business explanation for an anomaly."""
    # Limit context to avoid huge prompts
    safe_context = {k: v for k, v in list(context.items())[:8]}
    prompt = ANOMALY_EXPLAIN.format(
        dataset_name=dataset_name,
        column=column,
        value=value,
        expected_range=expected_range,
        method=method,
        score=score,
        context=json.dumps(safe_context, default=str),
    )
    try:
        text, _, _ = call_llm(prompt, max_tokens=300)
        return text.strip()
    except Exception as e:
        logger.warning("anomaly_explain_failed", error=str(e))
        return None
