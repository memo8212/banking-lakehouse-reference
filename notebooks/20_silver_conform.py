# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Silver conform (contracts + PII masks + quarantine)
# MAGIC Applies the data contract: types, masks, dedupe, quality gate. Bad rows go
# MAGIC to `<table>_rejected`.

# COMMAND ----------

from pyspark.sql import Window, functions as F

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")

def masked(c): return F.concat(F.substring(F.col(c), 1, 1), F.lit("***"))
def token(c):  return F.substring(F.sha2(F.col(c), 256), 1, 16)

# COMMAND ----------

# MAGIC %md ## customer (mask name, tokenize email)
cust = (spark.table(f"{catalog}.bronze.customer")
        .withColumn("dob", F.to_date("dob"))
        .withColumn("onboard_date", F.to_date("onboard_date"))
        .withColumn("segment", F.upper(F.trim("segment")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("full_name", masked("full_name"))
        .withColumn("email", token("email"))
        .withColumn("phone", F.lit("***"))
        .drop("_ingested_at", "_source_file"))
w = Window.partitionBy("customer_id").orderBy(F.monotonic_increasing_id())
cust = cust.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")
good = cust.filter(F.col("customer_id").isNotNull()
                   & F.col("segment").isin("RETAIL", "MASS", "AFFLUENT", "SME", "CORP")
                   & F.col("country").isNotNull() & (F.col("country") != ""))
good.write.mode("overwrite").saveAsTable(f"{catalog}.silver.customer")
cust.subtract(good).write.mode("overwrite").saveAsTable(f"{catalog}.silver.customer_rejected")

# COMMAND ----------

# MAGIC %md ## cards_txn (tokenize account, FX to base, quality gate)
fx = spark.table(f"{catalog}.bronze.fx_rates")
cards = (spark.table(f"{catalog}.bronze.cards_txn")
         .withColumn("amount", F.col("amount").cast("double"))
         .withColumn("txn_ts", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))
         .withColumn("status", F.upper(F.trim("status")))
         .withColumn("ccy", F.upper(F.trim("ccy")))
         .withColumn("account_no", token("account_no"))
         .drop("_ingested_at", "_source_file")
         .join(fx.select("ccy", "fx_rate"), "ccy", "left")
         .withColumn("amount_base", F.round(F.col("amount") * F.col("fx_rate"), 2)))
wc = Window.partitionBy("txn_id").orderBy(F.monotonic_increasing_id())
cards = cards.withColumn("__rn", F.row_number().over(wc)).filter("__rn = 1").drop("__rn")
goodc = cards.filter(F.col("txn_id").isNotNull() & F.col("customer_id").isNotNull()
                     & F.col("amount").isNotNull()
                     & F.col("status").isin("POSTED", "REVERSAL", "PENDING", "DECLINED"))
goodc.write.mode("overwrite").saveAsTable(f"{catalog}.silver.cards_txn")
cards.subtract(goodc).write.mode("overwrite").saveAsTable(f"{catalog}.silver.cards_txn_rejected")
print("silver:", good.count(), "customers,", goodc.count(), "txns")
