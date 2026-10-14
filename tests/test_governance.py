"""Tests for governance-as-code (access + Delta Sharing)."""
from governance.access import load_access, render_access_sql
from governance.sharing import load_sharing, render_sharing_sql, share_manifest


def test_access_sql_renders_grants_tags_retention_masks():
    sql = render_access_sql(load_access())
    assert "GRANT USAGE ON CATALOG bank_dev TO `analysts`;" in sql
    assert "GRANT SELECT ON TABLE bank_dev.gold.customer_360 TO `analysts`;" in sql
    assert "GRANT SELECT ON TABLE bank_dev.silver.customer TO `pii_readers`;" in sql
    assert "SET TAGS ('domain' = 'loans', 'layer' = 'gold')" in sql
    assert "deletedFileRetentionDuration" in sql
    assert "ALTER COLUMN full_name SET MASK bank_dev.silver.mask_name;" in sql
    assert "ALTER COLUMN account_no SET MASK bank_dev.silver.tokenize_id;" in sql


def test_sharing_sql_and_manifest():
    sharing = load_sharing()
    sql = render_sharing_sql(sharing)
    assert "CREATE SHARE IF NOT EXISTS bank_gold_share;" in sql
    assert "ALTER SHARE bank_gold_share ADD TABLE bank_dev.gold.loan_portfolio;" in sql
    assert "TO RECIPIENT regulator_acme;" in sql

    m = share_manifest(sharing)
    assert m["share"] == "bank_gold_share"
    assert m["recipient"] == "regulator_acme"
    assert m["table_count"] == 8
    assert m["content_hash"] == share_manifest(load_sharing())["content_hash"]  # deterministic
