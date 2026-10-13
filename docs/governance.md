# Governance

## Unity Catalog
- One catalog per environment (`bank_dev`/`bank_uat`/`bank_prod`), schemas per layer.
- **Lineage** is automatic (notebooks/pipelines) — combined with the file
  registry for file→column lineage.
- **Audit** via Delta `DESCRIBE HISTORY` + a run/audit table.

## PII
- Raw PII is isolated; consumers see **masked** or **tokenized** values.
- Masks: `mask_name` (first initial + `***`), `tokenize_id` (`sha2`), applied via
  `ALTER TABLE … ALTER COLUMN … SET MASK` (see `setup/03_pii_masks.sql`).
- Tokenization is deterministic → joinable without exposing the value.
- Row filters restrict by segment/group.

## Access (least privilege)
| Group | Access |
|---|---|
| `analysts` | `USAGE` catalog + `SELECT` on **Gold** |
| `dataops` | write on Bronze/Silver |
| `pii_readers` | unmasked PII (small, audited group) |
| `regulators` | **Delta Sharing** on specific Gold tables |

## Retention & residency
- **7-year** regulatory retention: Delta `deletedFileRetentionDuration` +
  scheduled `VACUUM` (ILM equivalent).
- Data residency via external locations / regional catalogs.

## Quality & observability
- **Expectations** (LDP) drop/fail bad rows inline.
- **Quarantine** tables (`*_rejected`) hold rows failing contracts.
- **Lakehouse Monitoring** profiles Silver/Gold and detects drift.
- **Reconciliation** proves source↔target parity each load.

## Secrets
- Secret scopes for credentials; never in code. CI uses OAuth / service
  principals, not personal tokens.
