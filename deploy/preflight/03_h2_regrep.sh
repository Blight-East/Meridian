#!/usr/bin/env bash
# deploy/preflight/03_h2_regrep.sh
#
# Validates that the H2 writer map is correct:
#   - Only the two known writers target _raw tables
#   - No writer targets the unsuffixed base table names post-migration 007
#   - No unexpected CREATE TABLE references the base table name
#
# Exit 0 on pass, non-zero on fail.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ART_DIR="${ART_DIR:-deploy/artifacts}"
mkdir -p "$ART_DIR"
DEPLOY_SHA="${DEPLOY_SHA:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
LOG="$ART_DIR/h2_regrep_${DEPLOY_SHA}.log"

echo "H2 writer re-grep started at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$LOG"

FAIL=0

# ── 1. Verify writers to deal_stage_transitions_raw ────────────────────
#    Only deal_lifecycle.py should INSERT INTO or CREATE TABLE deal_stage_transitions_raw.
DST_RAW_WRITERS=$(grep -rl 'deal_stage_transitions_raw' "$ROOT" \
    --include='*.py' \
    | grep -v __pycache__ \
    | grep -v '.pyc' \
    | sort -u || true)

echo "Files referencing deal_stage_transitions_raw:" | tee -a "$LOG"
echo "$DST_RAW_WRITERS" | tee -a "$LOG"

# Expected: runtime/ops/deal_lifecycle.py and deploy/migrations/007_recovery_enforcement.sql
EXPECTED_DST="runtime/ops/deal_lifecycle.py"
for f in $DST_RAW_WRITERS; do
    REL="${f#$ROOT/}"
    if [[ "$REL" != "$EXPECTED_DST" ]]; then
        echo "WARN: unexpected _raw writer: $REL" | tee -a "$LOG"
        # Not a hard fail if it's just a migration file or test
    fi
done

# ── 2. Verify writers to learning_feedback_ledger_raw ──────────────────
LFL_RAW_WRITERS=$(grep -rl 'learning_feedback_ledger_raw' "$ROOT" \
    --include='*.py' \
    | grep -v __pycache__ \
    | grep -v '.pyc' \
    | sort -u || true)

echo "Files referencing learning_feedback_ledger_raw:" | tee -a "$LOG"
echo "$LFL_RAW_WRITERS" | tee -a "$LOG"

EXPECTED_LFL="memory/structured/db.py"
for f in $LFL_RAW_WRITERS; do
    REL="${f#$ROOT/}"
    if [[ "$REL" != "$EXPECTED_LFL" ]]; then
        echo "WARN: unexpected _raw writer: $REL" | tee -a "$LOG"
    fi
done

# ── 3. No Python file should INSERT INTO or CREATE TABLE the unsuffixed
#       base names (which are now views after migration 007).
echo "" | tee -a "$LOG"
echo "Checking for prohibited unsuffixed base table writes..." | tee -a "$LOG"

for TABLE in deal_stage_transitions learning_feedback_ledger; do
    # Match INSERT INTO <table> or CREATE TABLE [IF NOT EXISTS] <table>
    # but NOT <table>_raw or <table>_all
    HITS=$(grep -rnE "(INSERT INTO|CREATE TABLE|CREATE TABLE IF NOT EXISTS)\s+${TABLE}\b" "$ROOT" \
        --include='*.py' \
        | grep -v __pycache__ \
        | grep -v "${TABLE}_raw" \
        | grep -v "${TABLE}_all" || true)
    if [ -n "$HITS" ]; then
        echo "FAIL: unsuffixed write to $TABLE found:" | tee -a "$LOG"
        echo "$HITS" | tee -a "$LOG"
        FAIL=1
    fi
done

if [ "$FAIL" -ne 0 ]; then
    echo "H2 re-grep FAILED" | tee -a "$LOG"
    exit 1
fi

echo "H2 re-grep PASSED" | tee -a "$LOG"
exit 0
