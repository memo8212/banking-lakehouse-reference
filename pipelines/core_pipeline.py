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

@dp.table(name="bronze_payments", comment="raw payments feed")
def bronze_payments():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/payments")
            .load(f"{LANDING}/payments")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("payments.csv")))

@dp.table(name="bronze_loans", comment="raw loans feed")
def bronze_loans():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/loans")
            .load(f"{LANDING}/loans")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("loans.csv")))

@dp.table(name="bronze_accounts", comment="raw accounts feed")
def bronze_accounts():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/accounts")
            .load(f"{LANDING}/accounts")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("accounts.csv")))

@dp.table(name="bronze_risk_exposure", comment="raw risk exposure feed")
def bronze_risk_exposure():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/risk_exposure")
            .load(f"{LANDING}/risk_exposure")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("risk_exposure.csv")))

@dp.table(name="bronze_aml_rules", comment="raw AML rules")
def bronze_aml_rules():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/aml_rules")
            .load(f"{LANDING}/aml_rules")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("aml_rules.csv")))

@dp.table(name="bronze_aml_alerts", comment="raw AML alerts")
def bronze_aml_alerts():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/aml_alerts")
            .load(f"{LANDING}/aml_alerts")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("aml_alerts.csv")))

@dp.table(name="bronze_regulatory_return", comment="raw regulatory returns")
def bronze_regulatory_return():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/regulatory_return")
            .load(f"{LANDING}/regulatory_return")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("regulatory_return.csv")))

@dp.table(name="bronze_deposits", comment="raw deposit feed")
def bronze_deposits():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.schemaLocation", f"{LANDING}/_schema/deposits")
            .load(f"{LANDING}/deposits")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit("deposits.csv")))

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

@dp.table(name="silver_payments", comment="conformed payments")
@dp.expect_or_drop("valid_status", "status IN ('SETTLED','PENDING','REJECTED','RETURNED')")
@dp.expect_or_drop("valid_direction", "direction IN ('DEBIT','CREDIT')")
def silver_payments():
    return (dp.read_stream("bronze_payments")
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("direction", F.upper(F.trim("direction")))
            .withColumn("account_no", F.substring(F.sha2("account_no", 256), 1, 16))
            .withColumn("counterparty", F.concat(F.substring("counterparty", 1, 1), F.lit("***"))))

@dp.table(name="silver_loans", comment="conformed loans")
@dp.expect_or_drop("valid_status", "status IN ('ACTIVE','CLOSED','DELINQUENT','WRITTEN_OFF')")
def silver_loans():
    return (dp.read_stream("bronze_loans")
            .withColumn("principal", F.col("principal").cast("double"))
            .withColumn("outstanding_balance", F.col("outstanding_balance").cast("double"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("account_no", F.substring(F.sha2("account_no", 256), 1, 16)))

@dp.table(name="silver_accounts", comment="conformed accounts")
@dp.expect_or_drop("valid_status", "status IN ('ACTIVE','DORMANT','CLOSED','FROZEN')")
def silver_accounts():
    return (dp.read_stream("bronze_accounts")
            .withColumn("balance", F.col("balance").cast("double"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("account_type", F.upper(F.trim("account_type")))
            .withColumn("account_no", F.substring(F.sha2("account_no", 256), 1, 16)))

@dp.table(name="silver_risk_exposure", comment="conformed risk exposures")
@dp.expect_or_drop("valid_status", "status IN ('ACTIVE','MATURED','DEFAULT')")
def silver_risk_exposure():
    return (dp.read_stream("bronze_risk_exposure")
            .withColumn("ead", F.col("ead").cast("double"))
            .withColumn("pd", F.col("pd").cast("double"))
            .withColumn("lgd", F.col("lgd").cast("double"))
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("rating", F.upper(F.trim("rating"))))

@dp.table(name="silver_aml_rules", comment="conformed AML rules")
def silver_aml_rules():
    return (dp.read_stream("bronze_aml_rules")
            .withColumn("rule_type", F.upper(F.trim("rule_type")))
            .withColumn("severity", F.upper(F.trim("severity"))))

@dp.table(name="silver_aml_alerts", comment="conformed AML alerts")
@dp.expect_or_drop("valid_status",
                   "status IN ('OPEN','IN_REVIEW','ESCALATED','CLOSED','DISMISSED')")
def silver_aml_alerts():
    return (dp.read_stream("bronze_aml_alerts")
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("score", F.col("score").cast("double"))
            .withColumn("status", F.upper(F.trim("status"))))

@dp.table(name="silver_regulatory_return", comment="conformed regulatory returns")
@dp.expect_or_drop("valid_status", "status IN ('DRAFT','SUBMITTED','ACCEPTED','REJECTED')")
def silver_regulatory_return():
    return (dp.read_stream("bronze_regulatory_return")
            .withColumn("value", F.col("value").cast("double"))
            .withColumn("report_code", F.upper(F.trim("report_code")))
            .withColumn("status", F.upper(F.trim("status"))))

@dp.table(name="silver_deposits", comment="conformed deposits")
@dp.expect_or_drop("valid_status", "status IN ('ACTIVE','MATURED','WITHDRAWN','ROLLED')")
def silver_deposits():
    return (dp.read_stream("bronze_deposits")
            .withColumn("principal", F.col("principal").cast("double"))
            .withColumn("accrued_interest", F.col("accrued_interest").cast("double"))
            .withColumn("interest_rate", F.col("interest_rate").cast("double"))
            .withColumn("term_months", F.col("term_months").cast("int"))
            .withColumn("deposit_type", F.upper(F.trim("deposit_type")))
            .withColumn("status", F.upper(F.trim("status"))))

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

@dp.materialized_view(name="gold_payments_flow", comment="payments flow by channel/direction")
def gold_payments_flow():
    settled = dp.read("silver_payments").filter(F.col("status") == "SETTLED")
    return (settled.withColumn("month", F.date_format(F.current_date(), "yyyy-MM"))
            .groupBy("channel", "direction", "month")
            .agg(F.round(F.sum("amount"), 2).alias("amount_base"),
                 F.count("payment_id").alias("payments")))

@dp.materialized_view(name="gold_payments_by_segment", comment="payments by segment")
def gold_payments_by_segment():
    cust = dp.read("silver_customer")
    settled = dp.read("silver_payments").filter(F.col("status") == "SETTLED")
    return (settled.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment", "payment_type")
            .agg(F.round(F.sum("amount"), 2).alias("amount_base"),
                 F.count("payment_id").alias("payments")))

@dp.materialized_view(name="gold_loan_portfolio", comment="loan portfolio by product/status")
def gold_loan_portfolio():
    loans = dp.read("silver_loans")
    return (loans.groupBy("product_type", "status")
            .agg(F.count("loan_id").alias("loans"),
                 F.round(F.sum("outstanding_balance"), 2).alias("outstanding_base"),
                 F.round(F.avg("interest_rate"), 4).alias("avg_rate")))

@dp.materialized_view(name="gold_loan_risk_by_segment", comment="loan risk by segment")
def gold_loan_risk_by_segment():
    cust = dp.read("silver_customer")
    loans = dp.read("silver_loans")
    return (loans.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment")
            .agg(F.count("loan_id").alias("loans"),
                 F.round(F.sum("outstanding_balance"), 2).alias("outstanding_base"),
                 F.sum(F.when(F.col("days_past_due") > 0, 1).otherwise(0)).alias("delinquent")))

@dp.materialized_view(name="gold_account_balances", comment="balances by account type/status")
def gold_account_balances():
    accounts = dp.read("silver_accounts")
    return (accounts.groupBy("account_type", "status")
            .agg(F.count("account_id").alias("accounts"),
                 F.round(F.sum("balance"), 2).alias("balance_base"),
                 F.round(F.avg("balance"), 2).alias("avg_balance")))

@dp.materialized_view(name="gold_deposits_by_segment", comment="deposits by segment")
def gold_deposits_by_segment():
    cust = dp.read("silver_customer")
    accounts = dp.read("silver_accounts").filter(F.col("status") == "ACTIVE")
    return (accounts.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment", "account_type")
            .agg(F.count("account_id").alias("accounts"),
                 F.round(F.sum("balance"), 2).alias("balance_base")))

@dp.materialized_view(name="gold_risk_exposure_by_rating", comment="exposure by rating")
def gold_risk_exposure_by_rating():
    risk = dp.read("silver_risk_exposure")
    el = F.round(F.col("pd") * F.col("lgd") * F.col("ead"), 2)
    return (risk.groupBy("rating")
            .agg(F.count("exposure_id").alias("exposures"),
                 F.round(F.sum("ead"), 2).alias("ead_base"),
                 F.round(F.sum(el), 2).alias("expected_loss"),
                 F.round(F.avg("pd"), 4).alias("avg_pd")))

@dp.materialized_view(name="gold_risk_by_segment", comment="exposure by segment")
def gold_risk_by_segment():
    cust = dp.read("silver_customer")
    risk = dp.read("silver_risk_exposure")
    el = F.round(F.col("pd") * F.col("lgd") * F.col("ead"), 2)
    return (risk.join(cust.select("customer_id", "segment"), "customer_id", "left")
            .groupBy("segment")
            .agg(F.round(F.sum("ead"), 2).alias("ead_base"),
                 F.round(F.sum(el), 2).alias("expected_loss"),
                 F.sum(F.when(F.col("status") == "DEFAULT", 1).otherwise(0)).alias("defaults")))

@dp.materialized_view(name="gold_aml_alerts_by_rule", comment="AML alerts by rule")
def gold_aml_alerts_by_rule():
    alerts = dp.read("silver_aml_alerts")
    return (alerts.groupBy("rule_id")
            .agg(F.count("alert_id").alias("alerts"),
                 F.round(F.sum("amount"), 2).alias("amount_base"),
                 F.round(F.avg("score"), 3).alias("avg_score")))

@dp.materialized_view(name="gold_aml_alert_summary", comment="AML alerts by status")
def gold_aml_alert_summary():
    alerts = dp.read("silver_aml_alerts")
    return (alerts.groupBy("status")
            .agg(F.count("alert_id").alias("alerts"),
                 F.round(F.sum("amount"), 2).alias("amount_base")))

@dp.materialized_view(name="gold_aml_high_risk_customers", comment="customers by alert volume")
def gold_aml_high_risk_customers():
    alerts = dp.read("silver_aml_alerts")
    return (alerts.groupBy("customer_id")
            .agg(F.count("alert_id").alias("alerts"),
                 F.countDistinct("rule_id").alias("rules"),
                 F.round(F.sum("amount"), 2).alias("amount_base"),
                 F.round(F.max("score"), 3).alias("max_score")))

@dp.materialized_view(name="gold_regulatory_by_report", comment="returns by report/entity")
def gold_regulatory_by_report():
    reg = dp.read("silver_regulatory_return")
    return (reg.groupBy("report_code", "entity")
            .agg(F.count("return_id").alias("returns"),
                 F.round(F.sum("value_base"), 2).alias("value_base"),
                 F.sum(F.when(F.col("status") == "ACCEPTED", 1).otherwise(0)).alias("accepted"),
                 F.round(F.sum(F.when(F.col("status") == "ACCEPTED", 1).otherwise(0))
                         / F.count("return_id"), 4).alias("acceptance_rate")))

@dp.materialized_view(name="gold_regulatory_submission_status", comment="submissions by status")
def gold_regulatory_submission_status():
    reg = dp.read("silver_regulatory_return")
    return (reg.groupBy("status")
            .agg(F.count("return_id").alias("submissions"),
                 F.round(F.sum("value_base"), 2).alias("value_base")))

@dp.materialized_view(name="gold_regulatory_by_metric", comment="returns by report/metric")
def gold_regulatory_by_metric():
    reg = dp.read("silver_regulatory_return")
    return (reg.groupBy("report_code", "metric_name")
            .agg(F.count("return_id").alias("submissions"),
                 F.round(F.sum("value_base"), 2).alias("value_base"),
                 F.round(F.avg("value_base"), 2).alias("avg_value")))

@dp.materialized_view(name="gold_deposit_portfolio", comment="deposit portfolio by type/status")
def gold_deposit_portfolio():
    dep = dp.read("silver_deposits")
    return (dep.groupBy("deposit_type", "status")
            .agg(F.count("deposit_id").alias("deposits"),
                 F.round(F.sum("principal_base"), 2).alias("principal_base"),
                 F.round(F.sum("accrued_interest_base"), 2).alias("accrued_interest_base"),
                 F.round(F.avg("interest_rate"), 4).alias("avg_rate")))

@dp.materialized_view(name="gold_deposit_maturity_ladder", comment="active deposits by maturity month")
def gold_deposit_maturity_ladder():
    dep = dp.read("silver_deposits").filter(F.col("status") == "ACTIVE").withColumn(
        "maturity_month", F.date_format("maturity_date", "yyyy-MM"))
    return (dep.groupBy("maturity_month")
            .agg(F.count("deposit_id").alias("deposits"),
                 F.round(F.sum("principal_base"), 2).alias("principal_base")))
