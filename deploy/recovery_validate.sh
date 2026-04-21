#!/usr/bin/env bash
# deploy/recovery_validate.sh
#
# V1–V8 validation suite. Must be run AFTER migration 007 and all code patches.
# Run against production Postgres/Redis. Returns exit 0 only if all pass.
#
# Usage:
#   export DATABASE_URL=postgresql://postgres@127.0.0.1/agent_flux
#   bash deploy/recovery_validate.sh

set -euo pipefail

DB="${DATABASE_URL:-postgresql://postgres@127.0.0.1/agent_flux}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

PASS=0
FAIL=0

check() {
  local name="$1" result="$2"
  if [ "$result" = "PASS" ]; then
    echo "  ✅ $name"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $name ($result)"
    FAIL=$((FAIL + 1))
  fi
}

echo "PayFlux Recovery Validation Suite — $(ts)"
echo "================================================================"

# V1: system_mode table exists and recovery is set
V1=$(psql "$DB" -tAc "SELECT value FROM system_mode WHERE key='mode'" 2>/dev/null || echo "MISSING")
[ "$V1" = "recovery" ] && check "V1: system_mode=recovery (PG)" "PASS" || check "V1: system_mode=recovery (PG)" "got=$V1"

# V1b: Redis agrees
V1R=$($REDIS_CLI GET agent_flux:system_mode 2>/dev/null || echo "MISSING")
[ "$V1R" = "recovery" ] && check "V1b: system_mode=recovery (Redis)" "PASS" || check "V1b: system_mode=recovery (Redis)" "got=$V1R"

# V2: processed_stripe_events table exists
V2=$(psql "$DB" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='processed_stripe_events'" 2>/dev/null || echo "0")
[ "$V2" = "1" ] && check "V2: processed_stripe_events exists" "PASS" || check "V2: processed_stripe_events exists" "FAIL"

# V3: deal_stage_transitions_raw exists as base table
V3=$(psql "$DB" -tAc "SELECT table_type FROM information_schema.tables WHERE table_name='deal_stage_transitions_raw'" 2>/dev/null || echo "MISSING")
[ "$V3" = "BASE TABLE" ] && check "V3: deal_stage_transitions_raw is BASE TABLE" "PASS" || check "V3: deal_stage_transitions_raw is BASE TABLE" "got=$V3"

# V3b: deal_stage_transitions is now a view
V3B=$(psql "$DB" -tAc "SELECT table_type FROM information_schema.tables WHERE table_name='deal_stage_transitions'" 2>/dev/null || echo "MISSING")
[ "$V3B" = "VIEW" ] && check "V3b: deal_stage_transitions is VIEW" "PASS" || check "V3b: deal_stage_transitions is VIEW" "got=$V3B"

# V4: learning_feedback_ledger_raw exists as base table
V4=$(psql "$DB" -tAc "SELECT table_type FROM information_schema.tables WHERE table_name='learning_feedback_ledger_raw'" 2>/dev/null || echo "MISSING")
[ "$V4" = "BASE TABLE" ] && check "V4: learning_feedback_ledger_raw is BASE TABLE" "PASS" || check "V4: learning_feedback_ledger_raw is BASE TABLE" "got=$V4"

# V4b: learning_feedback_ledger is now a view
V4B=$(psql "$DB" -tAc "SELECT table_type FROM information_schema.tables WHERE table_name='learning_feedback_ledger'" 2>/dev/null || echo "MISSING")
[ "$V4B" = "VIEW" ] && check "V4b: learning_feedback_ledger is VIEW" "PASS" || check "V4b: learning_feedback_ledger is VIEW" "got=$V4B"

# V5: events table has payload_signature column
V5=$(psql "$DB" -tAc "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='events' AND column_name='payload_signature'" 2>/dev/null || echo "0")
[ "$V5" = "1" ] && check "V5: events.payload_signature column exists" "PASS" || check "V5: events.payload_signature column exists" "FAIL"

# V6: learning_enabled is off
V6=$(psql "$DB" -tAc "SELECT value FROM system_mode WHERE key='learning_enabled'" 2>/dev/null || echo "MISSING")
[ "$V6" = "false" ] && check "V6: learning_enabled=false" "PASS" || check "V6: learning_enabled=false" "got=$V6"

# V7: agent_tasks_inflight key exists or is empty (just verify Redis is alive)
V7=$($REDIS_CLI LLEN agent_tasks_inflight 2>/dev/null || echo "ERR")
if [[ "$V7" =~ ^[0-9]+$ ]]; then
  check "V7: agent_tasks_inflight accessible (len=$V7)" "PASS"
else
  check "V7: agent_tasks_inflight accessible" "redis_err=$V7"
fi

# V8: No Python writer targets unsuffixed base table
V8=0
for TABLE in deal_stage_transitions learning_feedback_ledger; do
  HITS=$(grep -rnE "(INSERT INTO|CREATE TABLE)\s+${TABLE}\b" . \
      --include='*.py' \
      | grep -v __pycache__ \
      | grep -v "${TABLE}_raw" \
      | grep -v "${TABLE}_all" || true)
  if [ -n "$HITS" ]; then
    echo "  V8 FAIL: unsuffixed write found: $HITS"
    V8=1
  fi
done
[ "$V8" = "0" ] && check "V8: No unsuffixed base-table writers" "PASS" || check "V8: No unsuffixed base-table writers" "FAIL"

echo ""
echo "================================================================"
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "⛔ DO NOT exit recovery mode until all checks pass."
  exit 1
fi
echo "✅ All validations passed. Safe to transition to normal mode."
exit 0
