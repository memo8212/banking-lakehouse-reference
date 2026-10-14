#!/usr/bin/env bash
# Promote the banking lakehouse bundle across environments: dev -> uat -> prod.
# Runs the SAME bundle against a different target (no copy-paste).
#
#   ./scripts/promote.sh dev      # validate + deploy to dev
#   ./scripts/promote.sh uat      # promote to uat (needs DATABRICKS_HOST_UAT)
#   ./scripts/promote.sh prod     # promote to prod (needs DATABRICKS_HOST_PROD)
set -euo pipefail

TARGET="${1:-dev}"
cd "$(dirname "$0")/.."

echo "== validate ($TARGET) =="
databricks bundle validate -t "$TARGET"

echo "== deploy ($TARGET) =="
databricks bundle deploy -t "$TARGET"

echo "== run the daily load ($TARGET) =="
databricks bundle run bank_daily_load -t "$TARGET"

echo "done: $TARGET"
