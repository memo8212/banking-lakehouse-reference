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
display(spark.table(f"{catalog}.gold.card_spend_by_segment"))
