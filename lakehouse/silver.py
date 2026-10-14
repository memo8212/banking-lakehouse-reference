"""Silver layer: conform, mask PII, dedupe, quality-gate (quarantine bad rows)."""
from __future__ import annotations

from pyspark.sql import Window, functions as F

from lakehouse.config import Env
from lakehouse.contracts import Contract, validate

AUDIT = ["_ingested_at", "_source_file"]


def _masked(col):
    return F.concat(F.substring(F.col(col), 1, 1), F.lit("***"))


def _token(col):
    return F.substring(F.sha2(F.col(col), 256), 1, 16)


def _apply_pii(df, contract: Contract):
    for col, how in contract.pii.items():
        if col not in df.columns:
            continue
        if how == "mask":
            df = df.withColumn(col, _masked(col))
        elif how == "tokenize":
            df = df.withColumn(col, _token(col))
    return df


def _dedupe(df, key: str):
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")


def _split_and_write(spark, env: Env, df, contract: Contract, report: dict) -> None:
    df = df.drop(*AUDIT) if all(c in df.columns for c in AUDIT) else df
    rep = validate(df, contract)

    cond = F.lit(True)
    for col in contract.not_null:
        cond = cond & F.col(col).isNotNull()
    for col, allowed in contract.allowed.items():
        cond = cond & F.col(col).isin(*allowed)

    good = df.filter(cond)
    bad = df.filter(~cond)
    good_count = good.count()
    bad_count = bad.count()
    rep["good_rows"] = good_count
    rep["rejected_rows"] = bad_count
    report[contract.name] = rep

    good.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("silver", contract.target_table))
    if bad_count:
        bad.write.mode("overwrite").option("overwriteSchema", "true") \
           .format("delta").save(env.table("silver", f"{contract.target_table}_rejected"))


def conform(spark, env: Env, contracts: dict) -> dict:
    report: dict = {}

    # ---- customer ----
    c = contracts["customer"]
    cust = (spark.read.format("delta").load(env.table("bronze", "customer"))
            .withColumn("dob", F.to_date("dob"))
            .withColumn("onboard_date", F.to_date("onboard_date"))
            .withColumn("segment", F.upper(F.trim("segment")))
            .withColumn("country", F.upper(F.trim("country"))))
    cust = _apply_pii(cust, c)
    cust = _dedupe(cust, c.primary_key)
    _split_and_write(spark, env, cust, c, report)

    # ---- cards_txn ----
    t = contracts["cards_txn"]
    fx = spark.read.format("delta").load(env.table("bronze", "fx_rates"))
    cards = (spark.read.format("delta").load(env.table("bronze", "cards_txn"))
             .withColumn("amount", F.col("amount").cast("double"))
             .withColumn("txn_ts", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))
             .withColumn("status", F.upper(F.trim("status")))
             .withColumn("ccy", F.upper(F.trim("ccy"))))
    cards = _apply_pii(cards, t)
    cards = cards.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
                 .withColumn("amount_base", F.round(F.col("amount") * F.col("fx_rate"), 2))
    cards = _dedupe(cards, t.primary_key)
    _split_and_write(spark, env, cards, t, report)

    # ---- payments ----
    p = contracts["payments"]
    pay = (spark.read.format("delta").load(env.table("bronze", "payments"))
           .withColumn("amount", F.col("amount").cast("double"))
           .withColumn("value_date", F.to_date("value_date"))
           .withColumn("created_ts", F.to_timestamp("created_ts", "yyyy-MM-dd HH:mm:ss"))
           .withColumn("status", F.upper(F.trim("status")))
           .withColumn("direction", F.upper(F.trim("direction")))
           .withColumn("channel", F.upper(F.trim("channel")))
           .withColumn("ccy", F.upper(F.trim("ccy"))))
    pay = _apply_pii(pay, p)
    pay = pay.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
             .withColumn("amount_base", F.round(F.col("amount") * F.col("fx_rate"), 2))
    pay = _dedupe(pay, p.primary_key)
    _split_and_write(spark, env, pay, p, report)

    # ---- loans ----
    ln = contracts["loans"]
    loans = (spark.read.format("delta").load(env.table("bronze", "loans"))
             .withColumn("principal", F.col("principal").cast("double"))
             .withColumn("outstanding_balance", F.col("outstanding_balance").cast("double"))
             .withColumn("interest_rate", F.col("interest_rate").cast("double"))
             .withColumn("term_months", F.col("term_months").cast("int"))
             .withColumn("days_past_due", F.col("days_past_due").cast("int"))
             .withColumn("ltv", F.col("ltv").cast("double"))
             .withColumn("origination_date", F.to_date("origination_date"))
             .withColumn("maturity_date", F.to_date("maturity_date"))
             .withColumn("status", F.upper(F.trim("status")))
             .withColumn("product_type", F.upper(F.trim("product_type")))
             .withColumn("ccy", F.upper(F.trim("ccy"))))
    loans = _apply_pii(loans, ln)
    loans = loans.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
                 .withColumn("principal_base", F.round(F.col("principal") * F.col("fx_rate"), 2)) \
                 .withColumn("outstanding_base", F.round(F.col("outstanding_balance") * F.col("fx_rate"), 2))
    loans = _dedupe(loans, ln.primary_key)
    _split_and_write(spark, env, loans, ln, report)

    # ---- accounts ----
    ac = contracts["accounts"]
    accounts = (spark.read.format("delta").load(env.table("bronze", "accounts"))
                .withColumn("balance", F.col("balance").cast("double"))
                .withColumn("available_balance", F.col("available_balance").cast("double"))
                .withColumn("interest_rate", F.col("interest_rate").cast("double"))
                .withColumn("opened_date", F.to_date("opened_date"))
                .withColumn("status", F.upper(F.trim("status")))
                .withColumn("account_type", F.upper(F.trim("account_type")))
                .withColumn("ccy", F.upper(F.trim("ccy"))))
    accounts = _apply_pii(accounts, ac)
    accounts = accounts.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
                       .withColumn("balance_base", F.round(F.col("balance") * F.col("fx_rate"), 2)) \
                       .withColumn("available_base", F.round(F.col("available_balance") * F.col("fx_rate"), 2))
    accounts = _dedupe(accounts, ac.primary_key)
    _split_and_write(spark, env, accounts, ac, report)

    # ---- risk_exposure ----
    rx = contracts["risk_exposure"]
    risk = (spark.read.format("delta").load(env.table("bronze", "risk_exposure"))
            .withColumn("ead", F.col("ead").cast("double"))
            .withColumn("pd", F.col("pd").cast("double"))
            .withColumn("lgd", F.col("lgd").cast("double"))
            .withColumn("risk_weight", F.col("risk_weight").cast("double"))
            .withColumn("limit_amount", F.col("limit_amount").cast("double"))
            .withColumn("as_of_date", F.to_date("as_of_date"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("exposure_type", F.upper(F.trim("exposure_type")))
            .withColumn("rating", F.upper(F.trim("rating")))
            .withColumn("ccy", F.upper(F.trim("ccy"))))
    risk = _apply_pii(risk, rx)
    risk = risk.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
               .withColumn("ead_base", F.round(F.col("ead") * F.col("fx_rate"), 2)) \
               .withColumn("limit_base", F.round(F.col("limit_amount") * F.col("fx_rate"), 2))
    risk = _dedupe(risk, rx.primary_key)
    _split_and_write(spark, env, risk, rx, report)

    # ---- aml_rules ----
    ar = contracts["aml_rules"]
    rules = (spark.read.format("delta").load(env.table("bronze", "aml_rules"))
             .withColumn("threshold", F.col("threshold").cast("double"))
             .withColumn("rule_type", F.upper(F.trim("rule_type")))
             .withColumn("severity", F.upper(F.trim("severity")))
             .withColumn("enabled", F.upper(F.trim("enabled"))))
    rules = _dedupe(rules, ar.primary_key)
    _split_and_write(spark, env, rules, ar, report)

    # ---- aml_alerts ----
    aa = contracts["aml_alerts"]
    alerts = (spark.read.format("delta").load(env.table("bronze", "aml_alerts"))
              .withColumn("amount", F.col("amount").cast("double"))
              .withColumn("score", F.col("score").cast("double"))
              .withColumn("alert_date", F.to_date("alert_date"))
              .withColumn("source_system", F.upper(F.trim("source_system")))
              .withColumn("status", F.upper(F.trim("status")))
              .withColumn("ccy", F.upper(F.trim("ccy"))))
    alerts = (alerts
              .join(rules.select("rule_id", "rule_name", "severity"), "rule_id", "left")
              .join(fx.select("ccy", "fx_rate"), "ccy", "left")
              .withColumn("amount_base", F.round(F.col("amount") * F.col("fx_rate"), 2)))
    alerts = _dedupe(alerts, aa.primary_key)
    _split_and_write(spark, env, alerts, aa, report)

    # ---- regulatory_return ----
    rg = contracts["regulatory_return"]
    reg = (spark.read.format("delta").load(env.table("bronze", "regulatory_return"))
           .withColumn("value", F.col("value").cast("double"))
           .withColumn("version", F.col("version").cast("int"))
           .withColumn("reporting_date", F.to_date("reporting_date"))
           .withColumn("submitted_date", F.to_date("submitted_date"))
           .withColumn("report_code", F.upper(F.trim("report_code")))
           .withColumn("metric_name", F.upper(F.trim("metric_name")))
           .withColumn("status", F.upper(F.trim("status")))
           .withColumn("ccy", F.upper(F.trim("ccy"))))
    reg = reg.join(fx.select("ccy", "fx_rate"), on="ccy", how="left") \
             .withColumn("value_base", F.round(F.col("value") * F.col("fx_rate"), 2))
    reg = _dedupe(reg, rg.primary_key)
    _split_and_write(spark, env, reg, rg, report)
    return report
