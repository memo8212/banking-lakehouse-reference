"""Gold layer: domain marts for consumers (customer_360, card spend)."""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Env


def build(spark, env: Env) -> dict:
    cust = spark.read.format("delta").load(env.table("silver", "customer"))
    cards = spark.read.format("delta").load(env.table("silver", "cards_txn"))
    accounts = spark.read.format("delta").load(env.table("silver", "accounts"))

    posted = cards.filter(F.col("status") == "POSTED")

    # customer 360: profile + card spend + total account balance
    spend = (posted.groupBy("customer_id")
             .agg(F.round(F.sum("amount_base"), 2).alias("total_spend"),
                  F.count("txn_id").alias("txn_count"),
                  F.max("txn_ts").alias("last_txn_ts")))
    balances = (accounts.filter(F.col("status") == "ACTIVE")
                .groupBy("customer_id")
                .agg(F.round(F.sum("balance_base"), 2).alias("total_balance")))
    c360 = (cust.select("customer_id", "segment", "country", "kyc_status")
                .join(spend, "customer_id", "left")
                .join(balances, "customer_id", "left")
                .fillna({"total_spend": 0.0, "txn_count": 0, "total_balance": 0.0})
                .withColumn("total_spend", F.round("total_spend", 2))
                .withColumn("total_balance", F.round("total_balance", 2)))
    c360.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "customer_360"))

    # card spend by segment x month
    seg = (posted.join(cust.select("customer_id", "segment"), "customer_id", "left")
                 .withColumn("month", F.date_format("txn_ts", "yyyy-MM"))
                 .groupBy("segment", "month")
                 .agg(F.round(F.sum("amount_base"), 2).alias("spend"),
                      F.count("txn_id").alias("txns"))
                 .orderBy("segment", "month"))
    seg.write.mode("overwrite").option("overwriteSchema", "true") \
       .format("delta").save(env.table("gold", "card_spend_by_segment"))

    # ---- payments marts ----
    payments = spark.read.format("delta").load(env.table("silver", "payments"))
    settled = payments.filter(F.col("status") == "SETTLED")

    flow = (settled
            .withColumn("month", F.date_format("created_ts", "yyyy-MM"))
            .groupBy("channel", "direction", "month")
            .agg(F.round(F.sum("amount_base"), 2).alias("amount_base"),
                 F.count("payment_id").alias("payments"))
            .orderBy("channel", "direction", "month"))
    flow.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "payments_flow"))

    by_segment = (settled.join(cust.select("customer_id", "segment"), "customer_id", "left")
                  .groupBy("segment", "payment_type")
                  .agg(F.round(F.sum("amount_base"), 2).alias("amount_base"),
                       F.count("payment_id").alias("payments"))
                  .orderBy("segment", "payment_type"))
    by_segment.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "payments_by_segment"))

    # ---- loans marts ----
    loans = spark.read.format("delta").load(env.table("silver", "loans"))

    portfolio = (loans.groupBy("product_type", "status")
                 .agg(F.count("loan_id").alias("loans"),
                      F.round(F.sum("principal_base"), 2).alias("principal_base"),
                      F.round(F.sum("outstanding_base"), 2).alias("outstanding_base"),
                      F.round(F.avg("interest_rate"), 4).alias("avg_rate"),
                      F.round(F.avg("days_past_due"), 1).alias("avg_dpd"))
                 .orderBy("product_type", "status"))
    portfolio.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "loan_portfolio"))

    loan_risk = (loans.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment")
            .agg(F.count("loan_id").alias("loans"),
                 F.round(F.sum("outstanding_base"), 2).alias("outstanding_base"),
                 F.sum(F.when(F.col("days_past_due") > 0, 1).otherwise(0)).alias("delinquent"),
                 F.round(F.sum(F.when(F.col("days_past_due") > 0, 1).otherwise(0))
                         / F.count("loan_id"), 4).alias("delinquency_rate"))
            .orderBy("segment"))
    loan_risk.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "loan_risk_by_segment"))

    # ---- accounts marts ----
    acct_bal = (accounts.groupBy("account_type", "status")
                .agg(F.count("account_id").alias("accounts"),
                     F.round(F.sum("balance_base"), 2).alias("balance_base"),
                     F.round(F.sum("available_base"), 2).alias("available_base"),
                     F.round(F.avg("balance_base"), 2).alias("avg_balance"))
                .orderBy("account_type", "status"))
    acct_bal.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "account_balances"))

    deposits = (accounts.filter(F.col("status") == "ACTIVE")
                .join(cust.select("customer_id", "segment"), "customer_id", "left")
                .groupBy("segment", "account_type")
                .agg(F.count("account_id").alias("accounts"),
                     F.round(F.sum("balance_base"), 2).alias("balance_base"))
                .orderBy("segment", "account_type"))
    deposits.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "deposits_by_segment"))

    # ---- risk marts ----
    risk = spark.read.format("delta").load(env.table("silver", "risk_exposure"))
    el = F.round(F.col("pd") * F.col("lgd") * F.col("ead_base"), 2)

    by_rating = (risk.groupBy("rating")
                 .agg(F.count("exposure_id").alias("exposures"),
                      F.round(F.sum("ead_base"), 2).alias("ead_base"),
                      F.round(F.sum(el), 2).alias("expected_loss"),
                      F.round(F.avg("pd"), 4).alias("avg_pd"))
                 .orderBy("rating"))
    by_rating.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "risk_exposure_by_rating"))

    limits = (risk.groupBy("exposure_type")
              .agg(F.round(F.sum("limit_base"), 2).alias("limit_base"),
                   F.round(F.sum("ead_base"), 2).alias("ead_base"))
              .withColumn("utilisation", F.round(F.col("ead_base") / F.col("limit_base"), 4))
              .orderBy("exposure_type"))
    limits.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "risk_limit_utilisation"))

    risk_seg = (risk.join(cust.select("customer_id", "segment"), "customer_id", "left")
                .groupBy("segment")
                .agg(F.round(F.sum("ead_base"), 2).alias("ead_base"),
                     F.round(F.sum(el), 2).alias("expected_loss"),
                     F.sum(F.when(F.col("status") == "DEFAULT", 1).otherwise(0)).alias("defaults"))
                .orderBy("segment"))
    risk_seg.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "risk_by_segment"))

    # ---- AML marts ----
    alerts = spark.read.format("delta").load(env.table("silver", "aml_alerts"))

    by_rule = (alerts.groupBy("rule_id", "rule_name", "severity")
               .agg(F.count("alert_id").alias("alerts"),
                    F.round(F.sum("amount_base"), 2).alias("amount_base"),
                    F.round(F.avg("score"), 3).alias("avg_score"))
               .orderBy("rule_id"))
    by_rule.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "aml_alerts_by_rule"))

    by_status = (alerts.groupBy("status")
                 .agg(F.count("alert_id").alias("alerts"),
                      F.round(F.sum("amount_base"), 2).alias("amount_base"),
                      F.round(F.avg("score"), 3).alias("avg_score"))
                 .orderBy("status"))
    by_status.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "aml_alert_summary"))

    high_risk = (alerts.groupBy("customer_id")
                 .agg(F.count("alert_id").alias("alerts"),
                      F.countDistinct("rule_id").alias("rules"),
                      F.round(F.sum("amount_base"), 2).alias("amount_base"),
                      F.round(F.max("score"), 3).alias("max_score"))
                 .orderBy(F.col("alerts").desc(), F.col("max_score").desc()))
    high_risk.write.mode("overwrite").option("overwriteSchema", "true") \
        .format("delta").save(env.table("gold", "aml_high_risk_customers"))

    return {"customer_360": c360.count(),
            "card_spend_by_segment": seg.count(),
            "payments_flow": flow.count(),
            "payments_by_segment": by_segment.count(),
            "loan_portfolio": portfolio.count(),
            "loan_risk_by_segment": loan_risk.count(),
            "account_balances": acct_bal.count(),
            "deposits_by_segment": deposits.count(),
            "risk_exposure_by_rating": by_rating.count(),
            "risk_limit_utilisation": limits.count(),
            "aml_alerts_by_rule": by_rule.count(),
            "aml_alert_summary": by_status.count(),
            "aml_high_risk_customers": high_risk.count()}
