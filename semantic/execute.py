"""Execute a metric over the local gold marts with pandas (no Spark needed)."""
from __future__ import annotations

import re

import pandas as pd

from lakehouse.read import read_gold
from semantic.model import Metric, Model

_RE_MEASURE = re.compile(r"^(SUM|AVG|COUNT)\((.+)\)$")


def _measure(measure: str) -> tuple[str, str]:
    m = _RE_MEASURE.match(measure.strip())
    if not m:
        raise ValueError(f"unsupported measure: {measure}")
    return m.group(1), m.group(2)


def execute(storage: str, model: Model, metric: Metric,
            group_by: str | None = None) -> pd.DataFrame:
    table = metric.table.split(".")[-1]
    df = read_gold(storage, table)
    if df.empty:
        return df
    if metric.filter:
        df = df.query(metric.filter)
    fn, col = _measure(metric.measure)

    if group_by:
        g = df.groupby(group_by)
        if fn == "SUM":
            out = g[col].sum()
        elif fn == "AVG":
            out = g[col].mean()
        else:
            out = g[col].count()
        out = out.reset_index(name=metric.name).sort_values(metric.name, ascending=False)
        return out.reset_index(drop=True)

    if fn == "SUM":
        val = df[col].sum()
    elif fn == "AVG":
        val = df[col].mean()
    else:
        val = len(df) if col == "*" else df[col].count()
    return pd.DataFrame({metric.name: [val]})
