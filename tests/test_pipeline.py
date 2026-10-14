"""Integration test: run the medallion end-to-end and check reconciliation."""
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytest.importorskip("pyspark")
pytest.importorskip("delta")


def test_medallion_end_to_end(tmp_path, spark):
    from lakehouse.config import Env
    from lakehouse.pipeline import run

    subprocess.run([sys.executable, "data/generate_sample_data.py"], cwd=str(ROOT), check=True)
    env = Env(storage=str(tmp_path / "store"), source_dir=str(ROOT / "data"))
    result = run(spark, env)

    assert result["bronze"]["customer"] > 0
    assert result["bronze"]["payments"] > 0
    assert result["bronze"]["loans"] > 0
    assert result["bronze"]["accounts"] > 0
    assert result["bronze"]["risk_exposure"] > 0
    assert result["bronze"]["aml_alerts"] > 0
    assert result["bronze"]["regulatory_return"] > 0
    assert result["silver"]["customer"]["good_rows"] > 0
    assert result["silver"]["payments"]["good_rows"] > 0
    assert result["silver"]["loans"]["good_rows"] > 0
    assert result["silver"]["accounts"]["good_rows"] > 0
    assert result["silver"]["risk_exposure"]["good_rows"] > 0
    assert result["silver"]["aml_alerts"]["good_rows"] > 0
    assert result["silver"]["regulatory_return"]["good_rows"] > 0
    assert result["gold"]["customer_360"] > 0
    assert result["gold"]["payments_flow"] > 0
    assert result["gold"]["loan_portfolio"] > 0
    assert result["gold"]["account_balances"] > 0
    assert result["gold"]["risk_exposure_by_rating"] > 0
    assert result["gold"]["aml_alerts_by_rule"] > 0
    assert result["gold"]["regulatory_by_report"] > 0
    assert all(r["status"] == "PASSED" for r in result["recon"]), result["recon"]
    # quarantine works: some bad rows were rejected
    assert result["silver"]["customer"]["rejected_rows"] > 0
    assert result["silver"]["payments"]["rejected_rows"] > 0
    assert result["silver"]["loans"]["rejected_rows"] > 0
    assert result["silver"]["accounts"]["rejected_rows"] > 0
    assert result["silver"]["risk_exposure"]["rejected_rows"] > 0
    assert result["silver"]["aml_alerts"]["rejected_rows"] > 0
    assert result["silver"]["regulatory_return"]["rejected_rows"] > 0
