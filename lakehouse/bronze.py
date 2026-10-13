"""Bronze layer: raw ingest with audit columns + a file registry."""
from __future__ import annotations

import hashlib
from pathlib import Path

from pyspark.sql import functions as F

from lakehouse.config import Env

SOURCES = ["customer", "cards_txn", "fx_rates"]


def file_registry(spark, source_dir: str):
    rows = []
    for p in sorted(Path(source_dir).glob("*.csv")):
        data = p.read_bytes()
        rows.append((p.name, str(p), len(data), hashlib.md5(data).hexdigest(),
                     data.count(b"\n")))
    return spark.createDataFrame(rows, ["file_name", "path", "size", "md5", "lines"])


def ingest(spark, env: Env) -> dict:
    out = {}
    for name in SOURCES:
        df = (spark.read.option("header", True).option("inferSchema", True)
              .csv(f"{env.source_dir}/{name}.csv")
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(f"{name}.csv")))
        df.write.mode("overwrite").option("overwriteSchema", "true") \
          .format("delta").save(env.table("bronze", name))
        out[name] = df.count()
    reg = file_registry(spark, env.source_dir)
    reg.write.mode("overwrite").format("delta").save(env.table("bronze", "file_registry"))
    return out
