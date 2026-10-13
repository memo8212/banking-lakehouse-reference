"""Gold layer: domain marts for consumers (customer_360, card spend)."""
from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.config import Env


def build(spark, env: Env) -> dict:
    cust = spark.read.format("delta").load(env.table("silver", "customer"))
    cards = spark.read.format("delta").load(env.table("silver", "cards_txn"))

    posted = cards.filter(F.col("status") == "POSTED")

    # customer 360: profile + card spend
    spend = (posted.groupBy("customer_id")
             .agg(F.round(F.sum("amount_base"), 2).alias("total_spend"),
                  F.count("txn_id").alias("txn_count"),
                  F.max("txn_ts").alias("last_txn_ts")))
    c360 = (cust.select("customer_id", "segment", "country", "kyc_status")
                .join(spend, "customer_id", "left")
                .fillna({"total_spend": 0.0, "txn_count": 0})
                .withColumn("total_spend", F.round("total_spend", 2)))
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

    return {"customer_360": c360.count(), "card_spend_by_segment": seg.count()}
