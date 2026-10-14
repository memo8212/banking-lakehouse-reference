"""Tests for the gold-mart reader (used by the Streamlit explorer)."""
import pandas as pd

from lakehouse.read import list_gold, read_gold


def test_list_and_read(tmp_path):
    d = tmp_path / "gold" / "demo"
    d.mkdir(parents=True)
    pd.DataFrame({"segment": ["RETAIL", "CORP"], "value": [10.0, 20.0]}).to_parquet(
        d / "part-00000.parquet")
    assert list_gold(str(tmp_path)) == ["demo"]
    df = read_gold(str(tmp_path), "demo")
    assert len(df) == 2
    assert list(df.columns) == ["segment", "value"]


def test_missing_is_empty(tmp_path):
    assert list_gold(str(tmp_path)) == []
    assert read_gold(str(tmp_path), "nope").empty
