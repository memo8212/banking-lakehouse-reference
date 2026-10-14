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

## Governance as code
`governance/access.yaml` declares grants, tags, retention and masks; a renderer
turns it into SQL (deterministic), so access is reviewed in PRs and applied by CI
instead of by hand.

```bash
python -m governance access            # -> governance/generated/access.sql
python -m governance access --execute  # apply on a workspace
```
It renders `GRANT … TO \`group\``, `SET TAGS`, `deletedFileRetentionDuration`
(7-year retention) and `ALTER COLUMN … SET MASK`.

## Delta Sharing
`governance/sharing.yaml` declares a share + recipient + tables. The renderer
emits the sharing SQL and a **manifest** (deterministic content hash) — no data
copy, the recipient reads from your storage.

```bash
python -m governance share             # -> governance/generated/sharing.sql + manifest
python -m governance share --execute   # apply on a workspace
```
Recipients read with a profile file: `SELECT * FROM bank_gold_share.bank_dev.gold.customer_360;`.
Revoke with `REVOKE SELECT ON SHARE … FROM RECIPIENT …` when the engagement ends.

