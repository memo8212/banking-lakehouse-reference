"""End-to-end medallion pipeline: bronze → silver → gold → reconcile."""
from __future__ import annotations

from lakehouse.bronze import ingest
from lakehouse.config import Env
from lakehouse.contracts import load_contracts
from lakehouse.gold import build
from lakehouse.reconcile import reconcile
from lakehouse.silver import conform


def run(spark, env: Env | None = None, contracts_dir=None) -> dict:
    env = env or Env()
    contracts = load_contracts(contracts_dir) if contracts_dir else load_contracts()
    bronze = ingest(spark, env)
    silver = conform(spark, env, contracts)
    gold = build(spark, env)
    recon = reconcile(spark, env)
    return {"bronze": bronze, "silver": silver, "gold": gold, "recon": recon}
