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
    return report
