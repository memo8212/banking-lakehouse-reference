# Architecture

A production-shaped bank lakehouse on Databricks: **Unity Catalog + Delta +
Workflows**, organized in medallion layers, with contracts, quality gates, PII
governance and reconciliation.

```
 Sources                     Ingestion                 Medallion (Delta + Unity Catalog)              Serving
 ───────                     ─────────                 ──────────────────────────────────            ───────
 dept files (many)  ─┐       Auto Loader /             landing → bronze → silver → gold        ┌─ Databricks SQL / BI
 core banking DB    ─┼─▶     Lakeflow Connect   ─▶     (raw)     (clean)  (conformed)(marts)   ├─ Genie / semantic layer
 payments / cards   ─┤       COPY INTO / JDBC                                                     ├─ Feature Store / ML
 mainframe (VSAM)   ─┤       Kafka / CDC (CDF)                                                    ├─ Delta Sharing → regulator
 SaaS (CRM, market) ─┘                                                                          └─ reverse-ETL / APIs

 governance: Unity Catalog · contracts · PII masks · retention · lineage · audit
 quality: expectations · quarantine · monitoring · reconciliation
 orchestration: Workflows / Asset Bundles · dev→uat→prod
```

## Layers
| Layer | Holds | Rules |
|---|---|---|
| **Landing** | raw files exactly as received (immutable, hashed) | write-once, no edits, file registry |
| **Bronze** | append-only raw + audit cols (`_ingested_at`, `_source_file`) | schema evolution on; no business logic |
| **Silver** | typed, deduped, conformed, PII-masked, quality-gated | idempotent; quarantine rejects |
| **Gold** | domain marts / KPIs | one per use case; documented; granted |
| **Sandbox** | analyst scratch | no prod dependencies |
| **Monitoring** | profile/drift/DQ metrics | Lakehouse Monitoring output |

## Flows in this repo
1. **Local medallion** (`lakehouse/`) — runnable with Spark + Delta:
   `data → bronze → silver → gold → reconcile` (`python -m lakehouse run`).
2. **Notebooks** (`notebooks/`) — the same pipeline for Databricks.
3. **Lakeflow Declarative Pipeline** (`pipelines/core_pipeline.py`) — streaming
   tables + materialized views with expectations.
4. **Asset Bundle** (`databricks.yml` + `resources/`) — deploys the job + pipeline.

## Design principles
- **Contracts first** — every feed has a contract (`contracts/*.contract.yaml`)
  that drives schema, quality rules and PII handling.
- **Boundaries are explicit** — external sources and terminal outputs are
  classified (see the assistant in `lineage-migration-planner`).
- **Idempotent** — re-running yields the same result; Bronze is append-only.
- **Reconciled** — every load proves source↔target parity.
- **Governed** — PII masked/tokenized; least-privilege grants; 7-year retention.

See also: `naming-and-conventions.md`, `data-model.md`, `governance.md`, `slas-and-dr.md`.
