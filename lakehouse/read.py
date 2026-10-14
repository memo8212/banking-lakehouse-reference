"""Read gold marts from the local Delta store (Parquet parts) without Spark.

Handy for notebooks, tests and the Streamlit explorer. On Databricks you'd read
the UC tables directly (`spark.table("bank_dev.gold.<mart>")`).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def gold_dir(storage: str) -> Path:
    return Path(storage) / "gold"


def list_gold(storage: str) -> list[str]:
    d = gold_dir(storage)
    if not d.exists():
        return []
    return sorted(p.name for p in d.iterdir() if p.is_dir())


def read_gold(storage: str, table: str) -> pd.DataFrame:
    d = gold_dir(storage) / table
    if not d.exists():
        return pd.DataFrame()
    files = sorted(d.glob("part-*.parquet")) or sorted(d.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
