# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Bronze ingest (Auto Loader) + file registry
# MAGIC Lands every source feed into Bronze with audit columns, and records a file
# MAGIC registry for file-level lineage.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")
landing = f"/Volumes/{catalog}/landing/feeds"

# COMMAND ----------

# MAGIC %md ## Ingest each feed (idempotent, append to Bronze)
for feed in ("customer", "cards_txn", "payments", "fx_rates"):
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{landing}/_schema/{feed}")
        .load(f"{landing}/{feed}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{feed}.csv"))
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{landing}/_ckpt/{feed}")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{feed}"))

# COMMAND ----------

# MAGIC %md ## File registry (file-level lineage)
rows = []
for f in dbutils.fs.ls(landing):
    if f.isFile():
        rows.append((f.name, f.path, f.size))
reg = spark.createDataFrame(rows, ["file_name", "path", "size"])
reg.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.file_registry")
reg.show(truncate=False)
