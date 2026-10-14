"""Tests for data contracts."""
from lakehouse.contracts import load_contracts, validate


def test_load_contracts():
    contracts = load_contracts()
    assert {"customer", "cards_txn", "payments", "loans", "accounts"} <= set(contracts)
    c = contracts["customer"]
    assert c.primary_key == "customer_id"
    assert c.pii["email"] == "tokenize"
    assert c.pii["full_name"] == "mask"
    assert "segment" in c.allowed
    assert c.columns["onboard_date"] == "date"

    t = contracts["cards_txn"]
    assert t.primary_key == "txn_id"
    assert "status" in t.allowed
    assert t.pii["account_no"] == "tokenize"

    p = contracts["payments"]
    assert p.primary_key == "payment_id"
    assert "direction" in p.allowed and "channel" in p.allowed
    assert p.pii["counterparty"] == "mask"

    ln = contracts["loans"]
    assert ln.primary_key == "loan_id"
    assert "product_type" in ln.allowed and "status" in ln.allowed
    assert ln.pii["account_no"] == "tokenize"

    a = contracts["accounts"]
    assert a.primary_key == "account_id"
    assert "account_type" in a.allowed and "status" in a.allowed
    assert a.pii["account_no"] == "tokenize"


def test_validate_detects_missing_and_bad_values(spark):
    c = load_contracts()["customer"]
    cols = list(c.columns)
    rows = [
        ("C1", "P1", "A***", "1980-01-01", "tok", "***", "RETAIL", "CA", "VERIFIED", "2020-01-01"),
        ("C2", "P2", "B***", "1980-01-01", "tok", "***", "UNKNOWN", "CA", "VERIFIED", "2020-01-01"),
    ]
    df = spark.createDataFrame(rows, cols)
    rep = validate(df, c)
    assert rep["missing_columns"] == []
    assert rep["bad_values"].get("segment") == 1     # UNKNOWN not allowed
    assert rep["passed"] is False
