-- ============================================================================
-- Banking Lakehouse — catalogs & schemas (Unity Catalog).
-- One catalog per environment; one schema per medallion layer (+ sandbox, monitoring).
-- ============================================================================

CREATE CATALOG IF NOT EXISTS bank_dev;
CREATE SCHEMA IF NOT EXISTS bank_dev.landing    COMMENT 'raw files, write-once';
CREATE SCHEMA IF NOT EXISTS bank_dev.bronze     COMMENT 'raw + audit, append-only';
CREATE SCHEMA IF NOT EXISTS bank_dev.silver     COMMENT 'conformed, masked, deduped';
CREATE SCHEMA IF NOT EXISTS bank_dev.gold       COMMENT 'domain marts / KPIs';
CREATE SCHEMA IF NOT EXISTS bank_dev.sandbox    COMMENT 'analyst scratch';
CREATE SCHEMA IF NOT EXISTS bank_dev.monitoring COMMENT 'profile/drift/DQ metrics';

-- In prod, repeat for bank_uat / bank_prod and promote via Asset Bundles.
SELECT 'bank_dev catalogs/schemas ready' AS status;
