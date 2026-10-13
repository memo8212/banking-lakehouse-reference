# Databricks notebook source
# MAGIC %md
# MAGIC # Banking lakehouse — Lakeflow Declarative Pipeline
# MAGIC Declares the medallion (Bronze → Silver → Gold) with expectations and
# MAGIC PII masking. Runs inside a Lakeflow Declarative Pipeline (LDP/DLT).

# COMMAND ----------

try:
    from pyspark import pipelines as dp  # DBR 16.3+
except ImportError:  # pragma: no cover
    import dlt as dp  # classic name

from pyspark.sql import functions as F

LANDING = spark.conf.get("bank.landing", "/Volumes/bank_dev/landing/feeds")
CATALOG = spark.conf.get("bank.catalog", "bank_dev")

# COMMAND ----------

# MAGIC %md ## Bronze — streaming tables (raw + audit)
@dp.table(name="bronze_customer", comment="raw customer feed")
def bronze_customer():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/customer")
            .load(f"{LANDING}/customer")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("customer.csv")))

@dp.table(name="bronze_cards_txn", comment="raw card transaction feed")
def bronze_cards_txn():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/cards_txn")
            .load(f"{LANDING}/cards_txn")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("cards_txn.csv")))

# COMMAND ----------

# MAGIC %md ## Silver — conformed + masked + quality expectations
@dp.table(name="silver_customer", comment="conformed customer (PII masked)")
@dp.expect_or_drop("valid_segment", "segment IN ('RETAIL','MASS','AFFLUENT','SME','CORP')")
@dp.expect("has_id", "customer_id IS NOT NULL")
def silver_customer():
    return (dp.read_stream("bronze_customer")
            .withColumn("segment", F.upper(F.trim("segment")))
            .withColumn("country", F.upper(F.trim("country")))
            .withColumn("full_name", F.concat(F.substring("full_name", 1, 1), F.lit("***")))
            .withColumn("email", F.substring(F.sha2("email", 256), 1, 16))
            .withColumn("phone", F.lit("***")))

@dp.table(name="silver_cards_txn", comment="conformed card transactions")
@dp.expect_or_drop("valid_status", "status IN ('POSTED','REVERSAL','PENDING','DECLINED')")
@dp.expect("has_amount", "amount IS NOT NULL")
def silver_cards_txn():
    return (dp.read_stream("bronze_cards_txn")
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("account_no", F.substring(F.sha2("account_no", 256), 1, 16)))

# COMMAND ----------

# MAGIC %md ## Gold — materialized views (marts)
@dp.materialized_view(name="gold_customer_360", comment="customer 360")
def gold_customer_360():
    cust = dp.read("silver_customer")
    posted = dp.read("silver_cards_txn").filter(F.col("status") == "POSTED")
    spend = posted.groupBy("customer_id").agg(
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.count("txn_id").alias("txn_count"))
    return cust.select("customer_id", "segment", "country", "kyc_status") \
               .join(spend, "customer_id", "left").fillna({"total_spend": 0.0, "txn_count": 0})

@dp.materialized_view(name="gold_card_spend_by_segment", comment="spend by segment")
def gold_card_spend_by_segment():
    cust = dp.read("silver_customer")
    posted = dp.read("silver_cards_txn").filter(F.col("status") == "POSTED")
    return (posted.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment").agg(F.round(F.sum("amount"), 2).alias("spend"),
                                    F.count("txn_id").alias("txns")))
