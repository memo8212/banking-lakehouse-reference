"""Generate a Databricks AI/BI Genie space configuration from the semantic model.

The config lists trusted tables, business instructions, sample questions and the
per-metric SQL — everything a Genie space needs to answer questions accurately.
"""
from __future__ import annotations

from semantic.model import Model, render_sql

INSTRUCTIONS = (
    "You are a banking analytics assistant. Answer questions using ONLY the "
    "trusted tables and the defined metrics. Prefer the provided SQL snippets. "
    "Group by a metric's allowed dimensions. Round money to 2 decimals. "
    "If a question is ambiguous, ask which segment / product / period is meant."
)


def genie_space(model: Model, name: str = "bank_gold_genie") -> dict:
    metrics = list(model.metrics.values())
    return {
        "name": name,
        "catalog": model.catalog,
        "trusted_tables": sorted({m.full_table(model.catalog) for m in metrics}),
        "instructions": INSTRUCTIONS,
        "sample_questions": [m.question for m in metrics if m.question],
        "metrics": [
            {"name": m.name, "description": m.description,
             "synonyms": m.synonyms, "dimensions": m.dimensions,
             "table": m.full_table(model.catalog)}
            for m in metrics
        ],
        "sql": {m.name: render_sql(model, m) for m in metrics},
    }
