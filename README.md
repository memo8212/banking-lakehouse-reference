# Banking Lakehouse — Reference

A production-shaped **bank lakehouse on Databricks**: Unity Catalog + Delta +
Workflows, organized in **medallion** layers, with **data contracts**, **quality
gates**, **PII governance** and **reconciliation** — the target that all the
source-migration demos in this workspace land in.

It ships a **runnable MVP vertical slice** (customer + cards): contract → bronze
→ silver → gold → reconcile, plus the Databricks artifacts (notebooks, Lakeflow
Declarative Pipeline, Asset Bundle) and the design docs.

```
 Sources                 Ingestion              Medallion (Delta + Unity Catalog)          Serving
 dept files  ─┐          Auto Loader /          landing → bronze → silver → gold      ┌─ Databricks SQL / BI
 core DB     ─┼─▶        Lakeflow Connect ─▶    (raw)     (clean)  (conformed)(marts) ├─ Feature Store / ML
 cards/pay   ─┤          JDBC / Kafka / CDC                                             ├─ Delta Sharing → regulator
 mainframe   ─┘                                                                         └─ reverse-ETL / APIs
```

## Quick start (local)

```bash
pip install -r requirements.txt
python data/generate_sample_data.py
python -m lakehouse run          # bronze → silver → gold → reconcile
python -m lakehouse reconcile    # reconciliation only
pytest -q tests/
```

Example output:
```
Silver: {'customer': (2000, 30), 'cards_txn': (19980, 20),
         'payments': (14985, 25), 'loans': (7990, 20)}
Gold:   {'customer_360': 2000, 'card_spend_by_segment': 5,
         'payments_flow': 10, 'payments_by_segment': 25,
         'loan_portfolio': 20, 'loan_risk_by_segment': 5}
Reconcile: 8/8 passed
```

Domains shipped end-to-end: **customer, cards, payments, loans**.

## What's inside

```
contracts/        data contracts (YAML): schema, quality rules, PII per feed
lakehouse/        runnable medallion (Spark + Delta)
  config.py       Env — the only place names/paths live
  contracts.py    load + validate contracts
  bronze.py       raw ingest + file registry
  silver.py       conform + PII masks + dedupe + quarantine
  gold.py         domain marts (customer_360, card spend)
  reconcile.py    source↔bronze counts, gold↔silver parity
  pipeline.py     run() the whole thing
  cli.py          `python -m lakehouse run|reconcile`
notebooks/        Databricks notebooks (bootstrap → bronze → silver → gold → reconcile)
pipelines/        Lakeflow Declarative Pipeline (streaming tables + MVs + expectations)
resources/        Asset Bundle job + pipeline
setup/            UC catalogs/schemas/volume + PII masks (SQL)
monitoring/       Lakehouse Monitoring setup (profile/drift)
governance/       governance as code: access.yaml + sharing.yaml -> SQL + manifest
docs/             architecture · naming · data-model · governance · slas-and-dr
tests/            contracts + end-to-end medallion + governance
```

## Governance as code + Delta Sharing

```bash
python -m governance access           # grants/tags/retention/masks -> SQL
python -m governance share            # Delta Sharing SQL + manifest (no data copy)
```

`governance/access.yaml` declares who can do what (reviewed in PRs, applied by
CI); `governance/sharing.yaml` exposes curated Gold to a regulator via **Delta
Sharing**. See `docs/governance.md`.

## Deploy to Databricks (Asset Bundle)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run bank_daily_load -t dev      # bootstrap → bronze → silver → gold → reconcile
databricks bundle run core_medallion  -t dev      # the LDP pipeline
```

`setup/00_catalogs_schemas.sql` + `setup/01_volumes_and_landing.sql` create the
catalogs/schemas/volume and tables; `setup/03_pii_masks.sql` applies the masks.

## Conventions (short version)
- `bank_<env>.{landing,bronze,silver,gold,sandbox,monitoring}`; tables `<domain>_<entity>`.
- Audit cols `_ingested_at`, `_source_file`; money `DECIMAL(18,2)`; ISO-4217 `ccy`.
- Contracts are the source of truth for schema, quality and PII.
- Standard DAG: `bootstrap → bronze → silver → gold → reconcile`.

Full detail in `docs/`.

## How it connects to the rest of the family
- Source migrations (`sas2databricks`, `informatica2databricks`,
  `mainframe2databricks`, `elasticsearch2databricks`) produce **Bronze** inputs.
- **`lineage-migration-planner`** maps a legacy estate into this target and plans
  migration waves.
- Contracts → **`bank-data-contracts`** (planned) generate expectations + DDL.
- Monitoring/observability generalizes the registry + reconciliation patterns.

## Roadmap
See the design docs and the family `migration-demos` index. Domains done:
**customer, cards, payments, loans**. Next: **accounts, deposits, wealth, risk,
AML**. Governance-as-code and Delta Sharing are implemented in `governance/`.

## License
MIT — see `LICENSE`.
