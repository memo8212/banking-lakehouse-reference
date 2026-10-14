# CI/CD & Environments

Two workflows and one bundle promote the lakehouse across environments.

## Environments
| Target | Catalog | Workspace | Mode | Promotion |
|---|---|---|---|---|
| `dev` | `bank_dev` | dev workspace | `development` | auto on merge to `main` |
| `uat` | `bank_uat` | uat workspace | `production` | manual, gated |
| `prod` | `bank_prod` | prod workspace | `production` (`run_as` SP) | manual, gated |

All three use the **same `databricks.yml`**; only the target changes. No copy-paste
between workspaces.

## Workflows
- **`.github/workflows/ci.yml`** — on every PR/push: run the medallion locally
  (`python -m lakehouse run`), the tests, and render governance.
- **`.github/workflows/cd.yml`** — on merge to `main`: deploy to **dev**.
  Manual `workflow_dispatch` promotes to **uat**/**prod** (uses GitHub
  *Environments* for required-reviewer gates).

## Promotion (local / CLI)
```bash
./scripts/promote.sh dev
./scripts/promote.sh uat
./scripts/promote.sh prod
```

## Secrets (GitHub → repository/Environment secrets)
| Secret | Used by |
|---|---|
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | dev deploy |
| `DATABRICKS_HOST_UAT`, `DATABRICKS_TOKEN_UAT` | uat promote |
| `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD` | prod promote |
| `DATABRICKS_SP_NAME` | `run_as` service principal (uat/prod) |

Prefer **OAuth / service principals** over personal tokens in prod.

## Deploy order (per environment)
```
databricks bundle validate -t <env>
databricks bundle deploy   -t <env>
# then, per the environment's setup SQL (once):
#   setup/00_catalogs_schemas.sql, 01_volumes_and_landing.sql, 03_pii_masks.sql
# governance as code:
python -m governance access --execute
python -m governance share  --execute
databricks bundle run bank_daily_load -t <env>
```

## Guardrails
- PRs run CI (tests + pipeline); merges deploy to dev only.
- uat/prod require manual approval (GitHub Environment protection).
- `run_as` a service principal; no personal credentials in prod.
- Schema/table DDL and governance are applied **before** the pipeline runs.
