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
    assert result["silver"]["customer"]["good_rows"] > 0
    assert result["gold"]["customer_360"] > 0
    assert all(r["status"] == "PASSED" for r in result["recon"]), result["recon"]
    # quarantine works: some bad rows were rejected
    assert result["silver"]["customer"]["rejected_rows"] > 0
