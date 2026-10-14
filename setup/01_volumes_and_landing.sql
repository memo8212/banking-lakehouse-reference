-- ============================================================================
-- Landing zone (governed file storage) + Bronze/Silver/Gold target tables.
-- ============================================================================

CREATE VOLUME IF NOT EXISTS bank_dev.landing.feeds
  COMMENT 'Department / source feed landing (write-once, immutable)';

-- Bronze (raw + audit)
CREATE TABLE IF NOT EXISTS bank_dev.bronze.customer (
  customer_id STRING, party_id STRING, full_name STRING, dob STRING, email STRING,
  phone STRING, segment STRING, country STRING, kyc_status STRING, onboard_date STRING,
  _ingested_at TIMESTAMP, _source_file STRING);

CREATE TABLE IF NOT EXISTS bank_dev.bronze.cards_txn (
  txn_id STRING, card_id STRING, customer_id STRING, account_no STRING, amount STRING,
  ccy STRING, merchant STRING, mcc STRING, txn_ts STRING, status STRING,
  _ingested_at TIMESTAMP, _source_file STRING);

CREATE TABLE IF NOT EXISTS bank_dev.bronze.fx_rates (ccy STRING, fx_rate DOUBLE);

CREATE TABLE IF NOT EXISTS bank_dev.bronze.payments (
  payment_id STRING, customer_id STRING, account_no STRING, direction STRING,
  amount STRING, ccy STRING, payment_type STRING, channel STRING, counterparty STRING,
  beneficiary_country STRING, status STRING, value_date STRING, created_ts STRING,
  _ingested_at TIMESTAMP, _source_file STRING);

CREATE TABLE IF NOT EXISTS bank_dev.bronze.loans (
  loan_id STRING, customer_id STRING, account_no STRING, product_type STRING,
  principal STRING, ccy STRING, interest_rate STRING, term_months STRING,
  origination_date STRING, maturity_date STRING, status STRING,
  outstanding_balance STRING, days_past_due STRING, ltv STRING, collateral_type STRING,
  _ingested_at TIMESTAMP, _source_file STRING);

CREATE TABLE IF NOT EXISTS bank_dev.bronze.accounts (
  account_id STRING, customer_id STRING, account_no STRING, account_type STRING,
  product_code STRING, ccy STRING, balance STRING, available_balance STRING,
  opened_date STRING, status STRING, branch STRING, interest_rate STRING,
  _ingested_at TIMESTAMP, _source_file STRING);

-- File registry (file-level lineage)
CREATE TABLE IF NOT EXISTS bank_dev.bronze.file_registry (
  file_name STRING, path STRING, size BIGINT, md5 STRING, lines BIGINT);

-- Silver (conformed, masked)
CREATE TABLE IF NOT EXISTS bank_dev.silver.customer (
  customer_id STRING NOT NULL, party_id STRING, full_name STRING, dob DATE, email STRING,
  phone STRING, segment STRING, country STRING, kyc_status STRING, onboard_date DATE,
  CONSTRAINT pk_customer PRIMARY KEY (customer_id))
  COMMENT 'PII masked/tokenized; contract customer.contract.yaml';

CREATE TABLE IF NOT EXISTS bank_dev.silver.cards_txn (
  txn_id STRING NOT NULL, card_id STRING, customer_id STRING, account_no STRING,
  amount DOUBLE, ccy STRING, merchant STRING, mcc STRING, txn_ts TIMESTAMP,
  status STRING, fx_rate DOUBLE, amount_base DOUBLE,
  CONSTRAINT pk_cards_txn PRIMARY KEY (txn_id))
  COMMENT 'contract cards_txn.contract.yaml';

CREATE TABLE IF NOT EXISTS bank_dev.silver.payments (
  payment_id STRING NOT NULL, customer_id STRING, account_no STRING, direction STRING,
  amount DOUBLE, ccy STRING, payment_type STRING, channel STRING, counterparty STRING,
  beneficiary_country STRING, status STRING, value_date DATE, created_ts TIMESTAMP,
  fx_rate DOUBLE, amount_base DOUBLE,
  CONSTRAINT pk_payments PRIMARY KEY (payment_id))
  COMMENT 'contract payments.contract.yaml';

CREATE TABLE IF NOT EXISTS bank_dev.silver.loans (
  loan_id STRING NOT NULL, customer_id STRING, account_no STRING, product_type STRING,
  principal DOUBLE, ccy STRING, interest_rate DOUBLE, term_months INT,
  origination_date DATE, maturity_date DATE, status STRING, outstanding_balance DOUBLE,
  days_past_due INT, ltv DOUBLE, collateral_type STRING, fx_rate DOUBLE,
  principal_base DOUBLE, outstanding_base DOUBLE,
  CONSTRAINT pk_loans PRIMARY KEY (loan_id))
  COMMENT 'contract loans.contract.yaml';

CREATE TABLE IF NOT EXISTS bank_dev.silver.accounts (
  account_id STRING NOT NULL, customer_id STRING, account_no STRING, account_type STRING,
  product_code STRING, ccy STRING, balance DOUBLE, available_balance DOUBLE,
  opened_date DATE, status STRING, branch STRING, interest_rate DOUBLE,
  fx_rate DOUBLE, balance_base DOUBLE, available_base DOUBLE,
  CONSTRAINT pk_accounts PRIMARY KEY (account_id))
  COMMENT 'contract accounts.contract.yaml';

-- Gold (domain marts)
CREATE TABLE IF NOT EXISTS bank_dev.gold.customer_360 (
  customer_id STRING, segment STRING, country STRING, kyc_status STRING,
  total_spend DECIMAL(18,2), txn_count BIGINT, last_txn_ts TIMESTAMP,
  total_balance DECIMAL(18,2))
  CLUSTER BY (segment);

CREATE TABLE IF NOT EXISTS bank_dev.gold.card_spend_by_segment (
  segment STRING, month STRING, spend DECIMAL(18,2), txns BIGINT)
  CLUSTER BY (segment, month);

CREATE TABLE IF NOT EXISTS bank_dev.gold.payments_flow (
  channel STRING, direction STRING, month STRING, amount_base DECIMAL(18,2), payments BIGINT)
  CLUSTER BY (channel, month);

CREATE TABLE IF NOT EXISTS bank_dev.gold.payments_by_segment (
  segment STRING, payment_type STRING, amount_base DECIMAL(18,2), payments BIGINT)
  CLUSTER BY (segment);

CREATE TABLE IF NOT EXISTS bank_dev.gold.loan_portfolio (
  product_type STRING, status STRING, loans BIGINT, principal_base DECIMAL(18,2),
  outstanding_base DECIMAL(18,2), avg_rate DOUBLE, avg_dpd DOUBLE)
  CLUSTER BY (product_type);

CREATE TABLE IF NOT EXISTS bank_dev.gold.loan_risk_by_segment (
  segment STRING, loans BIGINT, outstanding_base DECIMAL(18,2), delinquent BIGINT,
  delinquency_rate DOUBLE)
  CLUSTER BY (segment);

CREATE TABLE IF NOT EXISTS bank_dev.gold.account_balances (
  account_type STRING, status STRING, accounts BIGINT, balance_base DECIMAL(18,2),
  available_base DECIMAL(18,2), avg_balance DECIMAL(18,2))
  CLUSTER BY (account_type);

CREATE TABLE IF NOT EXISTS bank_dev.gold.deposits_by_segment (
  segment STRING, account_type STRING, accounts BIGINT, balance_base DECIMAL(18,2))
  CLUSTER BY (segment);

SELECT 'landing + tables ready' AS status;
