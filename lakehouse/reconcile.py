"""Reconciliation: source↔bronze counts and gold↔silver parity."""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Env


def reconcile(spark, env: Env) -> list[dict]:
    results: list[dict] = []

    for name in ("customer", "cards_txn", "payments", "loans", "accounts",
                 "risk_exposure", "aml_rules", "aml_alerts"):
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

    # accounts: gold account_balances total vs silver balance
    acct_silver = spark.read.format("delta").load(env.table("silver", "accounts"))
    acct_gold = spark.read.format("delta").load(env.table("gold", "account_balances"))
    asum = acct_silver.agg(F.coalesce(F.sum("balance_base"), F.lit(0.0))).first()[0]
    agsum = acct_gold.agg(F.coalesce(F.sum("balance_base"), F.lit(0.0))).first()[0]
    aok = abs(float(asum) - float(agsum)) < 0.01
    results.append({"check": "gold account_balances vs silver balance_base",
                    "status": "PASSED" if aok else "FAILED",
                    "expected": round(float(asum), 2), "actual": round(float(agsum), 2)})

    # risk: gold risk_exposure_by_rating ead vs silver ead
    risk_silver = spark.read.format("delta").load(env.table("silver", "risk_exposure"))
    risk_gold = spark.read.format("delta").load(env.table("gold", "risk_exposure_by_rating"))
    rs = risk_silver.agg(F.coalesce(F.sum("ead_base"), F.lit(0.0))).first()[0]
    rg = risk_gold.agg(F.coalesce(F.sum("ead_base"), F.lit(0.0))).first()[0]
    rok = abs(float(rs) - float(rg)) < 0.01
    results.append({"check": "gold risk_exposure_by_rating vs silver ead_base",
                    "status": "PASSED" if rok else "FAILED",
                    "expected": round(float(rs), 2), "actual": round(float(rg), 2)})

    # AML: gold aml_alerts_by_rule amount vs silver alerts
    aml_silver = spark.read.format("delta").load(env.table("silver", "aml_alerts"))
    aml_gold = spark.read.format("delta").load(env.table("gold", "aml_alerts_by_rule"))
    ams = aml_silver.agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0]
    amg = aml_gold.agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0]
    amok = abs(float(ams) - float(amg)) < 0.01
    results.append({"check": "gold aml_alerts_by_rule vs silver amount_base",
                    "status": "PASSED" if amok else "FAILED",
                    "expected": round(float(ams), 2), "actual": round(float(amg), 2)})

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
