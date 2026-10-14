# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Gold marts (customer_360, card spend)
# MAGIC Consumer-ready marts. Analysts read Gold only.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")

cust = spark.table(f"{catalog}.silver.customer")
posted = spark.table(f"{catalog}.silver.cards_txn").filter(F.col("status") == "POSTED")

# COMMAND ----------

# MAGIC %md ## customer_360
spend = (posted.groupBy("customer_id")
         .agg(F.round(F.sum("amount_base"), 2).alias("total_spend"),
              F.count("txn_id").alias("txn_count"),
              F.max("txn_ts").alias("last_txn_ts")))
(cust.select("customer_id", "segment", "country", "kyc_status")
     .join(spend, "customer_id", "left")
     .fillna({"total_spend": 0.0, "txn_count": 0})
     .write.mode("overwrite").saveAsTable(f"{catalog}.gold.customer_360"))

# COMMAND ----------

# MAGIC %md ## card_spend_by_segment (segment x month)
(posted.join(cust.select("customer_id", "segment"), "customer_id", "left")
       .withColumn("month", F.date_format("txn_ts", "yyyy-MM"))
       .groupBy("segment", "month")
       .agg(F.round(F.sum("amount_base"), 2).alias("spend"), F.count("txn_id").alias("txns"))
       .orderBy("segment", "month")
       .write.mode("overwrite").saveAsTable(f"{catalog}.gold.card_spend_by_segment"))

# COMMAND ----------

# MAGIC %md ## payments marts (flow + by segment)
pay = spark.table(f"{catalog}.silver.payments").filter(F.col("status") == "SETTLED")

(pay.withColumn("month", F.date_format("created_ts", "yyyy-MM"))
    .groupBy("channel", "direction", "month")
    .agg(F.round(F.sum("amount_base"), 2).alias("amount_base"),
         F.count("payment_id").alias("payments"))
    .orderBy("channel", "direction", "month")
    .write.mode("overwrite").saveAsTable(f"{catalog}.gold.payments_flow"))

(pay.join(cust.select("customer_id", "segment"), "customer_id", "left")
    .groupBy("segment", "payment_type")
    .agg(F.round(F.sum("amount_base"), 2).alias("amount_base"),
         F.count("payment_id").alias("payments"))
    .orderBy("segment", "payment_type")
    .write.mode("overwrite").saveAsTable(f"{catalog}.gold.payments_by_segment"))

# COMMAND ----------

# MAGIC %md ## loans marts (portfolio + risk by segment)
loans = spark.table(f"{catalog}.silver.loans")

(loans.groupBy("product_type", "status")
      .agg(F.count("loan_id").alias("loans"),
           F.round(F.sum("principal_base"), 2).alias("principal_base"),
           F.round(F.sum("outstanding_base"), 2).alias("outstanding_base"),
           F.round(F.avg("interest_rate"), 4).alias("avg_rate"),
           F.round(F.avg("days_past_due"), 1).alias("avg_dpd"))
      .orderBy("product_type", "status")
      .write.mode("overwrite").saveAsTable(f"{catalog}.gold.loan_portfolio"))

(loans.join(cust.select("customer_id", "segment"), "customer_id", "left")
      .groupBy("segment")
      .agg(F.count("loan_id").alias("loans"),
           F.round(F.sum("outstanding_base"), 2).alias("outstanding_base"),
           F.sum(F.when(F.col("days_past_due") > 0, 1).otherwise(0)).alias("delinquent"),
           F.round(F.sum(F.when(F.col("days_past_due") > 0, 1).otherwise(0))
                   / F.count("loan_id"), 4).alias("delinquency_rate"))
      .orderBy("segment")
      .write.mode("overwrite").saveAsTable(f"{catalog}.gold.loan_risk_by_segment"))

# COMMAND ----------

# MAGIC %md ## accounts marts (balances + deposits by segment)
accounts = spark.table(f"{catalog}.silver.accounts")

(accounts.groupBy("account_type", "status")
         .agg(F.count("account_id").alias("accounts"),
              F.round(F.sum("balance_base"), 2).alias("balance_base"),
              F.round(F.sum("available_base"), 2).alias("available_base"),
              F.round(F.avg("balance_base"), 2).alias("avg_balance"))
         .orderBy("account_type", "status")
         .write.mode("overwrite").saveAsTable(f"{catalog}.gold.account_balances"))

(accounts.filter(F.col("status") == "ACTIVE")
         .join(cust.select("customer_id", "segment"), "customer_id", "left")
         .groupBy("segment", "account_type")
         .agg(F.count("account_id").alias("accounts"),
              F.round(F.sum("balance_base"), 2).alias("balance_base"))
         .orderBy("segment", "account_type")
         .write.mode("overwrite").saveAsTable(f"{catalog}.gold.deposits_by_segment"))

# COMMAND ----------

# MAGIC %md ## risk marts (exposure by rating, limit utilisation, by segment)
risk = spark.table(f"{catalog}.silver.risk_exposure")
el = F.round(F.col("pd") * F.col("lgd") * F.col("ead_base"), 2)

(risk.groupBy("rating")
     .agg(F.count("exposure_id").alias("exposures"),
          F.round(F.sum("ead_base"), 2).alias("ead_base"),
          F.round(F.sum(el), 2).alias("expected_loss"),
          F.round(F.avg("pd"), 4).alias("avg_pd"))
     .orderBy("rating")
     .write.mode("overwrite").saveAsTable(f"{catalog}.gold.risk_exposure_by_rating"))

(risk.groupBy("exposure_type")
     .agg(F.round(F.sum("limit_base"), 2).alias("limit_base"),
          F.round(F.sum("ead_base"), 2).alias("ead_base"))
     .withColumn("utilisation", F.round(F.col("ead_base") / F.col("limit_base"), 4))
     .orderBy("exposure_type")
     .write.mode("overwrite").saveAsTable(f"{catalog}.gold.risk_limit_utilisation"))

(risk.join(cust.select("customer_id", "segment"), "customer_id", "left")
     .groupBy("segment")
     .agg(F.round(F.sum("ead_base"), 2).alias("ead_base"),
          F.round(F.sum(el), 2).alias("expected_loss"),
          F.sum(F.when(F.col("status") == "DEFAULT", 1).otherwise(0)).alias("defaults"))
     .orderBy("segment")
     .write.mode("overwrite").saveAsTable(f"{catalog}.gold.risk_by_segment"))

# COMMAND ----------

# MAGIC %md ## AML marts (by rule, by status, high-risk customers)
alerts = spark.table(f"{catalog}.silver.aml_alerts")

(alerts.groupBy("rule_id", "rule_name", "severity")
       .agg(F.count("alert_id").alias("alerts"),
            F.round(F.sum("amount_base"), 2).alias("amount_base"),
            F.round(F.avg("score"), 3).alias("avg_score"))
       .orderBy("rule_id")
       .write.mode("overwrite").saveAsTable(f"{catalog}.gold.aml_alerts_by_rule"))

(alerts.groupBy("status")
       .agg(F.count("alert_id").alias("alerts"),
            F.round(F.sum("amount_base"), 2).alias("amount_base"),
            F.round(F.avg("score"), 3).alias("avg_score"))
       .orderBy("status")
       .write.mode("overwrite").saveAsTable(f"{catalog}.gold.aml_alert_summary"))

(alerts.groupBy("customer_id")
       .agg(F.count("alert_id").alias("alerts"),
            F.countDistinct("rule_id").alias("rules"),
            F.round(F.sum("amount_base"), 2).alias("amount_base"),
            F.round(F.max("score"), 3).alias("max_score"))
       .orderBy(F.col("alerts").desc(), F.col("max_score").desc())
       .write.mode("overwrite").saveAsTable(f"{catalog}.gold.aml_high_risk_customers"))

# COMMAND ----------

# MAGIC %md ## regulatory marts (by report, by status, by metric)
reg = spark.table(f"{catalog}.silver.regulatory_return")

(reg.groupBy("report_code", "entity")
    .agg(F.count("return_id").alias("returns"),
         F.round(F.sum("value_base"), 2).alias("value_base"),
         F.sum(F.when(F.col("status") == "ACCEPTED", 1).otherwise(0)).alias("accepted"),
         F.round(F.sum(F.when(F.col("status") == "ACCEPTED", 1).otherwise(0))
                 / F.count("return_id"), 4).alias("acceptance_rate"))
    .orderBy("report_code", "entity")
    .write.mode("overwrite").saveAsTable(f"{catalog}.gold.regulatory_by_report"))

(reg.groupBy("status")
    .agg(F.count("return_id").alias("submissions"),
         F.round(F.sum("value_base"), 2).alias("value_base"))
    .orderBy("status")
    .write.mode("overwrite").saveAsTable(f"{catalog}.gold.regulatory_submission_status"))

(reg.groupBy("report_code", "metric_name")
    .agg(F.count("return_id").alias("submissions"),
         F.round(F.sum("value_base"), 2).alias("value_base"),
         F.round(F.avg("value_base"), 2).alias("avg_value"))
    .orderBy("report_code", "metric_name")
    .write.mode("overwrite").saveAsTable(f"{catalog}.gold.regulatory_by_metric"))
display(spark.table(f"{catalog}.gold.regulatory_by_report"))
