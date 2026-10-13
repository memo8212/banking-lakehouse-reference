# Naming & Conventions

## Namespace (Unity Catalog)
```
bank_<env>.{landing, bronze, silver, gold, sandbox, monitoring}
   │              │
   │              └─ one schema per medallion layer
   └─ one catalog per environment: bank_dev / bank_uat / bank_prod
```
- **Tables:** `<domain>_<entity>` (e.g., `cards_txn`, `customer_360`,
  `loan_portfolio`). Domain prefixes keep layers scannable.
- **Rejects:** `<table>_rejected` (quarantine).
- **Volumes:** `bank_<env>.landing.feeds` (+ per-feed folders).

## Layers
| Prefix | Meaning |
|---|---|
| `bronze_` | raw + audit |
| `silver_` | conformed, masked |
| `gold_` | mart / KPI |

## Columns
- `snake_case`, lowercase.
- Audit columns start with `_` (`_ingested_at`, `_source_file`, `_batch_id`).
- Dates `DATE`, timestamps `TIMESTAMP`, money `DECIMAL(18,2)` (or `DOUBLE` for
  analytics), currency codes ISO-4217 (`ccy`).

## Contracts
- One file per feed: `contracts/<feed>.contract.yaml`.
- Declares: `columns` (name+type), `primary_key`, `not_null`, `allowed`, `pii`.
- Drives DDL, expectations and masking — the single source of truth for a feed.

## Jobs & bundles
- One **Asset Bundle** per repo; targets `dev`/`prod`.
- Job names `bank_<purpose>` (e.g., `bank_daily_load`).
- Standard task DAG: `bootstrap → bronze → silver → gold → reconcile`.

## Environments
| Env | Catalog | Purpose |
|---|---|---|
| dev | `bank_dev` | iteration (dev mode bundle) |
| uat | `bank_uat` | business validation |
| prod | `bank_prod` | production (`run_as` service principal) |

Promotion = run the **same bundle** against the next target. No copy-paste.
