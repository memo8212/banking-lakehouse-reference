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
display(spark.table(f"{catalog}.gold.loan_portfolio"))
