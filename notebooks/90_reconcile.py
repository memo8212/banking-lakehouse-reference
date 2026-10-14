# Databricks notebook source
# MAGIC %md
# MAGIC # 90 — Reconcile (source ↔ bronze, gold ↔ silver)
# MAGIC Fails the task on any mismatch.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")

results = []
for feed in ("customer", "cards_txn", "payments", "loans", "accounts", "risk_exposure", "aml_rules", "aml_alerts", "regulatory_return"):
    src = spark.read.option("header", True).csv(f"/Volumes/{catalog}/landing/feeds/{feed}").count()
    brz = spark.table(f"{catalog}.bronze.{feed}").count()
    results.append((f"{feed} source vs bronze", src == brz, src, brz))

s_sum = (spark.table(f"{catalog}.silver.cards_txn").filter("status = 'POSTED'")
         .agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0])
g_sum = (spark.table(f"{catalog}.gold.customer_360")
         .agg(F.coalesce(F.sum("total_spend"), F.lit(0.0))).first()[0])
results.append(("gold total_spend vs silver posted",
                abs(float(s_sum) - float(g_sum)) < 0.01, round(float(s_sum), 2), round(float(g_sum), 2)))

ps = (spark.table(f"{catalog}.silver.payments").filter("status = 'SETTLED'")
      .agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0])
pg = (spark.table(f"{catalog}.gold.payments_flow")
      .agg(F.coalesce(F.sum("amount_base"), F.lit(0.0))).first()[0])
results.append(("gold payments_flow vs silver settled",
                abs(float(ps) - float(pg)) < 0.01, round(float(ps), 2), round(float(pg), 2)))

failed = [r for r in results if not r[1]]
for name, ok, exp, act in results:
    print(f"{'PASS' if ok else 'FAIL'}  {name}: expected={exp} actual={act}")
assert not failed, f"reconciliation failed: {failed}"
print("reconciliation PASSED")
