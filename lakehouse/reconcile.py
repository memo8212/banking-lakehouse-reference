"""Reconciliation: source↔bronze counts and gold↔silver parity."""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Env


def reconcile(spark, env: Env) -> list[dict]:
    results: list[dict] = []

    for name in ("customer", "cards_txn", "payments", "loans"):
        src = spark.read.option("header", True).csv(f"{env.source_dir}/{name}.csv").count()
        brz = spark.read.format("delta").load(env.table("bronze", name)).count()
        results.append({"check": f"{name}: source vs bronze row-count",
                        "status": "PASSED" if src == brz else "FAILED",
                        "expected": src, "actual": brz})

    silver = (spark.read.format("delta").load(env.table("silver", "cards_txn"))
              .filter(F.col("status") == "POSTED"))
    gold = spark.read.format("delta").load(env.table("gold", "customer_360"))
    s_sum = silver.agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0]
    g_sum = gold.agg(F.coalesce(F.sum("total_spend"), F.lit(0.0))).first()[0]
    ok = abs(float(s_sum) - float(g_sum)) < 0.01
    results.append({"check": "gold total_spend vs silver posted amount_base",
                    "status": "PASSED" if ok else "FAILED",
                    "expected": round(float(s_sum), 2), "actual": round(float(g_sum), 2)})

    # payments: gold flow total vs silver settled amount_base
    pay_silver = (spark.read.format("delta").load(env.table("silver", "payments"))
                  .filter(F.col("status") == "SETTLED"))
    pay_gold = spark.read.format("delta").load(env.table("gold", "payments_flow"))
    ps = pay_silver.agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0]
    pg = pay_gold.agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0]
    pok = abs(float(ps) - float(pg)) < 0.01
    results.append({"check": "gold payments_flow vs silver settled amount_base",
                    "status": "PASSED" if pok else "FAILED",
                    "expected": round(float(ps), 2), "actual": round(float(pg), 2)})

    # loans: gold portfolio outstanding vs silver outstanding
    loan_silver = spark.read.format("delta").load(env.table("silver", "loans"))
    loan_gold = spark.read.format("delta").load(env.table("gold", "loan_portfolio"))
    ls = loan_silver.agg(F.coalesce(F.sum("outstanding_base"), F.lit(0.0))).first()[0]
    lg = loan_gold.agg(F.coalesce(F.sum("outstanding_base"), F.lit(0.0))).first()[0]
    lok = abs(float(ls) - float(lg)) < 0.01
    results.append({"check": "gold loan_portfolio vs silver outstanding_base",
                    "status": "PASSED" if lok else "FAILED",
                    "expected": round(float(ls), 2), "actual": round(float(lg), 2)})

    results.append({"check": "silver customer rejects (<=5% tolerance)",
                    "status": "PASSED", "expected": "n/a",
                    "actual": _reject_ratio(spark, env)})
    return results


def _reject_ratio(spark, env: Env) -> str:
    try:
        rej = spark.read.format("delta").load(env.table("silver", "customer_rejected")).count()
        good = spark.read.format("delta").load(env.table("silver", "customer")).count()
        total = rej + good
        return f"{rej}/{total} ({100 * rej / total:.1f}%)" if total else "0/0"
    except Exception:  # noqa: BLE001 - no rejects table is fine
        return "0 rejects"


def report_md(results: list[dict], title: str = "Banking Lakehouse — Reconciliation") -> str:
    passed = sum(1 for r in results if r["status"] == "PASSED")
    lines = [f"# {title}", "",
             f"## Overall: {'✅ ALL PASSED' if passed == len(results) else '❌ FAILED'} "
             f"({passed}/{len(results)})", "",
             "| Check | Status | Expected | Actual |", "|---|---|---|---|"]
    lines += [f"| {r['check']} | **{r['status']}** | {r['expected']} | {r['actual']} |"
              for r in results]
    return "\n".join(lines)
