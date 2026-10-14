"""Tests for the semantic layer + Genie space."""
import pandas as pd
import pytest

from semantic.ask import answer, match
from semantic.execute import execute
from semantic.genie import genie_space
from semantic.model import load_model, render_sql


def test_load_model():
    model = load_model()
    assert model.catalog == "bank_dev"
    assert len(model.metrics) >= 10
    m = model.get("total_card_spend")
    assert m.measure == "SUM(spend)"
    assert "segment" in m.dimensions
    assert "card spend" in m.synonyms


def test_render_sql():
    model = load_model()
    sql = render_sql(model, model.get("total_card_spend"), group_by="segment")
    assert "SUM(spend) AS total_card_spend" in sql
    assert "bank_dev.gold.card_spend_by_segment" in sql
    assert "GROUP  BY segment" in sql

    with pytest.raises(ValueError):
        render_sql(model, model.get("total_card_spend"), group_by="rating")


def test_genie_space():
    space = genie_space(load_model())
    assert space["name"] == "bank_gold_genie"
    assert "bank_dev.gold.customer_360" in space["trusted_tables"]
    assert len(space["sample_questions"]) >= 10
    assert "total_card_spend" in space["sql"]


def test_ask_matches_metrics():
    model = load_model()
    r = answer("what is card spend by segment?", model)
    assert r["metric"] == "total_card_spend"
    assert r["group_by"] == "segment"
    assert "GROUP  BY segment" in r["sql"]

    assert answer("how many high risk customers are there", model)["metric"] == "high_risk_customers"
    assert answer("expected credit loss by rating", model)["metric"] == "expected_loss"
    assert match("totally unrelated gibberish zzz", model)[1] == 0.0


def test_execute_over_local_gold(tmp_path):
    d = tmp_path / "gold" / "card_spend_by_segment"
    d.mkdir(parents=True)
    pd.DataFrame({"segment": ["RETAIL", "RETAIL", "CORP"],
                  "month": ["2026-01", "2026-02", "2026-01"],
                  "spend": [10.0, 20.0, 5.0]}).to_parquet(d / "part-00000.parquet")
    model = load_model()
    out = execute(str(tmp_path), model, model.get("total_card_spend"), group_by="segment")
    assert out.set_index("segment").loc["RETAIL", "total_card_spend"] == 30.0
    scalar = execute(str(tmp_path), model, model.get("total_card_spend"))
    assert scalar.iloc[0, 0] == 35.0
