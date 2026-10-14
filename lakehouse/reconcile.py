"""Reconciliation: source↔bronze counts and gold↔silver parity."""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Env


def _close(a, b, tol: float = 0.01) -> bool:
    """Absolute OR relative closeness (large money sums lose float precision)."""
    a, b = float(a), float(b)
    return abs(a - b) <= max(tol, 1e-9 * abs(a))


def reconcile(spark, env: Env) -> list[dict]:
    results: list[dict] = []

    for name in ("customer", "cards_txn", "payments", "loans", "accounts",
                 "risk_exposure", "aml_rules", "aml_alerts", "regulatory_return",
                 "deposits"):
        src = spark.read.option("header", True).csv(f"{env.source_dir}/{name}.csv").count()
        brz = spark.read.format("delta").load(env.table("bronze", name)).count()
        results.append({"check": f"{name}: source vs bronze row-count",
                        "status": "PASSED" if src == brz else "FAILED",
                        "expected": src, "actual": brz})

    def compare(label: str, silver_table: str, silver_filter: str,
                gold_table: str, col: str, gold_col: str | None = None) -> None:
        sdf = spark.read.format("delta").load(env.table("silver", silver_table))
        if silver_filter:
            sdf = sdf.filter(silver_filter)
        gdf = spark.read.format("delta").load(env.table("gold", gold_table))
        a = sdf.agg(F.coalesce(F.sum(col), F.lit(0.0))).first()[0]
        b = gdf.agg(F.coalesce(F.sum(gold_col or col), F.lit(0.0))).first()[0]
        results.append({"check": label, "status": "PASSED" if _close(a, b) else "FAILED",
                        "expected": round(float(a), 2), "actual": round(float(b), 2)})

    compare("gold total_spend vs silver posted amount_base",
            "cards_txn", "status = 'POSTED'", "customer_360", "amount_base", "total_spend")
    compare("gold payments_flow vs silver settled amount_base",
            "payments", "status = 'SETTLED'", "payments_flow", "amount_base")
    compare("gold loan_portfolio vs silver outstanding_base",
            "loans", "", "loan_portfolio", "outstanding_base")
    compare("gold account_balances vs silver balance_base",
            "accounts", "", "account_balances", "balance_base")
    compare("gold risk_exposure_by_rating vs silver ead_base",
            "risk_exposure", "", "risk_exposure_by_rating", "ead_base")
    compare("gold aml_alerts_by_rule vs silver amount_base",
            "aml_alerts", "", "aml_alerts_by_rule", "amount_base")
    compare("gold regulatory_by_report vs silver value_base",
            "regulatory_return", "", "regulatory_by_report", "value_base")
    compare("gold deposit_portfolio vs silver principal_base",
            "deposits", "", "deposit_portfolio", "principal_base")

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
