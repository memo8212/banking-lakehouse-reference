"""Create/refresh Lakehouse Monitors for the banking lakehouse.

    python monitoring/create_monitors.py --catalog bank_dev

Snapshot monitors on the Gold marts (profile + drift). Requires a UC-enabled
workspace and the Databricks SDK.
"""
from __future__ import annotations

import argparse


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="bank_dev")
    ap.add_argument("--assets-dir", default=None)
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.catalog import (
        MonitorCronSchedule, MonitorSnapshot,
    )

    w = WorkspaceClient()
    assets = args.assets_dir or f"/Volumes/{args.catalog}/landing/_monitoring"
    tables = [
        f"{args.catalog}.gold.customer_360",
        f"{args.catalog}.gold.card_spend_by_segment",
        f"{args.catalog}.gold.payments_flow",
        f"{args.catalog}.gold.payments_by_segment",
        f"{args.catalog}.gold.loan_portfolio",
        f"{args.catalog}.gold.loan_risk_by_segment",
        f"{args.catalog}.gold.account_balances",
        f"{args.catalog}.gold.deposits_by_segment",
        f"{args.catalog}.gold.risk_exposure_by_rating",
        f"{args.catalog}.gold.risk_limit_utilisation",
        f"{args.catalog}.gold.risk_by_segment",
        f"{args.catalog}.gold.aml_alerts_by_rule",
        f"{args.catalog}.gold.aml_alert_summary",
        f"{args.catalog}.gold.aml_high_risk_customers",
        f"{args.catalog}.gold.regulatory_by_report",
        f"{args.catalog}.gold.regulatory_submission_status",
        f"{args.catalog}.gold.regulatory_by_metric",
    ]

    for table in tables:
        try:
            w.quality_monitors.get(table_name=table)
            w.quality_monitors.run_refresh(table_name=table)
            print(f"[monitor] refreshed {table}")
            continue
        except NotFound:
            pass
        w.quality_monitors.create(
            table_name=table,
            assets_dir=assets,
            output_schema_name=f"{args.catalog}.monitoring",
            snapshot=MonitorSnapshot(),
            schedule=MonitorCronSchedule(quartz_cron_expression="0 0 6 * * ?",
                                         timezone_id="UTC"),
        )
        print(f"[monitor] created {table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
