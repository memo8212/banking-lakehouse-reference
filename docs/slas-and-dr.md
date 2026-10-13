# SLAs, DR & Operations

## Service levels (targets)
| Domain | Freshness | Availability | Notes |
|---|---|---|---|
| Cards / Payments | ≤ 15 min (streaming) | 99.9% | near-real-time |
| Customer / Accounts | nightly (T+1) | 99.9% | batch |
| Regulatory returns | by deadline (T+1) | 99.95% | reconciled, audit trail |

## Operations
- **Scheduling:** Workflows (cron + file-arrival triggers); retries + timeouts.
- **Alerting:** job notifications + SQL alerts on drift/reconciliation failures.
- **Backfill:** parameterized by `run_date`; Bronze is append-only so re-runs are safe.
- **Runbook:** on failure, check the reconciliation report and quarantine tables,
  re-run the failing task (idempotent).

## DR / BCP
- Storage in geo-redundant object storage; multi-region replication for prod.
- Recovery runbooks; RPO ≤ 15 min, RTO ≤ 4 h (tune per domain).
- Restore by re-running Bronze ingestion from landing (source of truth).

## Cost
- Liquid Clustering + `OPTIMIZE` to avoid small files; serverless SQL for BI.
- Budgets + resource tags per domain; alert on spend thresholds.

## Change management
- All changes via PR → CI (`databricks bundle validate` + tests) → deploy.
- `dev` → `uat` → `prod` promotion of the **same bundle**.
