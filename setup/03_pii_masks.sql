-- ============================================================================
-- PII governance: column masks + row filters (Unity Catalog).
-- Raw PII is restricted; consumers see masked/tokenized values.
-- ============================================================================

CREATE FUNCTION IF NOT EXISTS bank_dev.silver.mask_name(name STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('pii_readers') THEN name
            ELSE concat(substring(name, 1, 1), '***') END;

CREATE FUNCTION IF NOT EXISTS bank_dev.silver.tokenize_id(x STRING)
RETURNS STRING
RETURN sha2(x, 256);

-- Apply masks to the Silver columns.
ALTER TABLE bank_dev.silver.customer ALTER COLUMN full_name SET MASK bank_dev.silver.mask_name;
ALTER TABLE bank_dev.silver.customer ALTER COLUMN email     SET MASK bank_dev.silver.tokenize_id;
ALTER TABLE bank_dev.silver.cards_txn ALTER COLUMN account_no SET MASK bank_dev.silver.tokenize_id;
ALTER TABLE bank_dev.silver.payments  ALTER COLUMN account_no SET MASK bank_dev.silver.tokenize_id;
ALTER TABLE bank_dev.silver.payments  ALTER COLUMN counterparty SET MASK bank_dev.silver.mask_name;
ALTER TABLE bank_dev.silver.loans     ALTER COLUMN account_no SET MASK bank_dev.silver.tokenize_id;

-- Row filter example: analysts in 'cards_only' see only their segment's data.
CREATE FUNCTION IF NOT EXISTS bank_dev.silver.segment_filter(segment STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('bank_all') OR segment = 'RETAIL';

-- ALTER TABLE bank_dev.silver.customer SET ROW FILTER bank_dev.silver.segment_filter ON (segment);

-- Retention: 7-year regulatory.
-- ALTER TABLE bank_dev.bronze.customer SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 2555 days');

-- Least privilege.
-- GRANT USAGE ON CATALOG bank_dev TO `analysts`;
-- GRANT SELECT ON TABLE bank_dev.gold.customer_360 TO `analysts`;

SELECT 'PII masks applied' AS status;
