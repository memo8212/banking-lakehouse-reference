# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bootstrap the banking lakehouse
# MAGIC Creates the catalogs/schemas/volume and the medallion tables. Run once
# MAGIC (or from CI) before the pipeline.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md ## Schemas (one per medallion layer)
for schema in ("landing", "bronze", "silver", "gold", "sandbox", "monitoring"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Landing volume (write-once raw files)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.feeds")
print("bootstrap complete for", catalog)
