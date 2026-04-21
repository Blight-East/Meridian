#!/usr/bin/env bash
# deploy/fingerprint/verify_config_fingerprint.sh
#
# Compares the LIVE config fingerprint (recomputed from the host's
# environment) against the EXPECTED config fingerprint that was
# captured at deploy time. Per-source granularity — the verifier
# tells you exactly which layer drifted.
#
# Inputs:
#   FINGERPRINT_CONFIG_EXPECTED_PATH   default: deploy/artifacts/expected_config_fingerprint.json
#   ROLE                               required label for metric/log
#   plus everything build_config_fingerprint.sh accepts
#
# Optional:
#   FINGERPRINT_KILL_ON_MISMATCH       0|1 (default 0) — exit non-zero so
#                                       the supervising process tears down
#   FINGERPRINT_METRICS_DIR            default: /var/lib/payflux/metrics
#   FINGERPRINT_LOG_PATH               optional explicit log file
#   FINGERPRINT_CONFIG_REQUIRED_SOURCES default: "env,db_flags,redis_flags,nginx"
#                                       sources expected to be present:true.
#                                       Anything missing → mismatch.
#
# Exit codes:
#   0   ok (composite matches AND every required source present:true with matching hash)
#   20  composite mismatch (one or more sources drifted)
#   21  required source unavailable on host (e.g. nginx not on PATH)
#   22  expected fingerprint missing
#   23  recomputation failed
#   24  config error (missing ROLE)
#   25  baseline incomplete (expected file does not declare a required source)

set -euo pipefail

ROLE="${ROLE:-unknown}"
EXPECTED_PATH="${FINGERPRINT_CONFIG_EXPECTED_PATH:-deploy/artifacts/expected_config_fingerprint.json}"
METRICS_DIR="${FINGERPRINT_METRICS_DIR:-/var/lib/payflux/metrics}"
LOG_PATH="${FINGERPRINT_LOG_PATH:-}"
KILL="${FINGERPRINT_KILL_ON_MISMATCH:-0}"
REQUIRED_SOURCES="${FINGERPRINT_CONFIG_REQUIRED_SOURCES:-env,db_flags,redis_flags,nginx}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() {
  local level="$1"; shift
  local line="$(ts) [$level] [config-fingerprint] role=$ROLE $*"
  echo "$line" >&2
  if [ -n "$LOG_PATH" ]; then
    mkdir -p "$(dirname "$LOG_PATH")" 2>/dev/null || true
    printf '%s\n' "$line" >> "$LOG_PATH" 2>/dev/null || true
  fi
}

write_metrics() {
  # Per-source mismatch + composite mismatch.
  # Args: composite_mismatch env_mismatch db_flags_mismatch redis_flags_mismatch nginx_mismatch
  local composite="$1" envm="$2" dbm="$3" redm="$4" ngm="$5"
  if ! mkdir -p "$METRICS_DIR" 2>/dev/null; then
    log WARN "metrics dir not writable: $METRICS_DIR"
    return 0
  fi
  local out="$METRICS_DIR/config_fingerprint.prom"
  local tmp="${out}.tmp.$$"
  {
    echo "# HELP config_fingerprint_mismatch 1 if the composite config fingerprint differs from expected"
    echo "# TYPE config_fingerprint_mismatch gauge"
    echo "config_fingerprint_mismatch{component=\"$ROLE\",source=\"composite\"} $composite"
    echo "config_fingerprint_mismatch{component=\"$ROLE\",source=\"env\"} $envm"
    echo "config_fingerprint_mismatch{component=\"$ROLE\",source=\"db_flags\"} $dbm"
    echo "config_fingerprint_mismatch{component=\"$ROLE\",source=\"redis_flags\"} $redm"
    echo "config_fingerprint_mismatch{component=\"$ROLE\",source=\"nginx\"} $ngm"
    echo "# HELP config_fingerprint_last_check_unixtime epoch seconds of last verifier run"
    echo "# TYPE config_fingerprint_last_check_unixtime gauge"
    echo "config_fingerprint_last_check_unixtime{component=\"$ROLE\"} $(date -u +%s)"
  } > "$tmp" && mv -f "$tmp" "$out"
}

if [ "$ROLE" = "unknown" ]; then
  log CRITICAL "ROLE env var is required"
  exit 24
fi

if [ ! -s "$EXPECTED_PATH" ]; then
  log CRITICAL "expected config fingerprint missing: $EXPECTED_PATH"
  write_metrics 1 1 1 1 1
  exit 22
fi

if ! command -v jq >/dev/null 2>&1; then
  log CRITICAL "jq unavailable"
  exit 23
fi

# --- Recompute live config fingerprint ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILDER="$SCRIPT_DIR/build_config_fingerprint.sh"
if [ ! -r "$BUILDER" ]; then
  log CRITICAL "missing builder script: $BUILDER"
  exit 23
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
LIVE_PATH="$WORK_DIR/live_config_fingerprint.json"
if ! ART_DIR="$WORK_DIR" bash "$BUILDER" "$LIVE_PATH" >/dev/null 2>"$WORK_DIR/build.err"; then
  log CRITICAL "config recomputation failed: $(tail -n5 "$WORK_DIR/build.err" | tr '\n' ' ')"
  write_metrics 1 1 1 1 1
  exit 23
fi

# --- Extract values ---
EXPECTED_COMP="$(jq -r '.fingerprint' "$EXPECTED_PATH")"
LIVE_COMP="$(jq -r '.fingerprint' "$LIVE_PATH")"

read_src() {
  local file="$1" src="$2" field="$3"
  jq -r ".sources.\"$src\".\"$field\"" "$file"
}

cmp_source() {
  local src="$1"
  local exp_h liv_h liv_present exp_present
  exp_h="$(read_src "$EXPECTED_PATH" "$src" sha256)"
  liv_h="$(read_src "$LIVE_PATH"     "$src" sha256)"
  liv_present="$(read_src "$LIVE_PATH" "$src" present)"
  exp_present="$(read_src "$EXPECTED_PATH" "$src" present)"

  # Baseline integrity: the EXPECTED file must declare every required
  # source with present:true. A baseline that was captured without
  # STRICT=1 (e.g. from a degraded host) is treated as fail-closed —
  # we will not silently accept a baseline that itself does not assert
  # the required surface.
  if printf ',%s,' "$REQUIRED_SOURCES" | grep -q ",$src,"; then
    if [ "$exp_h" = "null" ] || [ -z "$exp_h" ]; then
      log CRITICAL "expected baseline is MISSING required source '$src' — re-capture baseline with STRICT=1"
      echo "1|baseline_incomplete"
      return
    fi
    if [ "$exp_present" != "true" ]; then
      log CRITICAL "expected baseline marks required source '$src' as present=$exp_present — re-capture baseline with STRICT=1"
      echo "1|baseline_incomplete"
      return
    fi
    if [ "$liv_present" != "true" ]; then
      log CRITICAL "required source '$src' is unavailable on this host (live present=$liv_present, detail=$(read_src "$LIVE_PATH" "$src" detail))"
      echo "1|unavailable"
      return
    fi
  fi
  if [ "$exp_h" != "$liv_h" ]; then
    log CRITICAL "config drift in source '$src': expected=$exp_h live=$liv_h"
    echo "1|drift"
    return
  fi
  echo "0|ok"
}

ENV_R="$(cmp_source env)"
DBF_R="$(cmp_source db_flags)"
RED_R="$(cmp_source redis_flags)"
NGX_R="$(cmp_source nginx)"

ENV_M="${ENV_R%%|*}";   ENV_S="${ENV_R##*|}"
DBF_M="${DBF_R%%|*}";   DBF_S="${DBF_R##*|}"
RED_M="${RED_R%%|*}";   RED_S="${RED_R##*|}"
NGX_M="${NGX_R%%|*}";   NGX_S="${NGX_R##*|}"

COMP_M=0
if [ "$EXPECTED_COMP" != "$LIVE_COMP" ]; then COMP_M=1; fi
if [ "$ENV_M" = "1" ] || [ "$DBF_M" = "1" ] || [ "$RED_M" = "1" ] || [ "$NGX_M" = "1" ]; then
  COMP_M=1
fi

write_metrics "$COMP_M" "$ENV_M" "$DBF_M" "$RED_M" "$NGX_M"

if [ "$COMP_M" = "0" ]; then
  log INFO "ok composite=$LIVE_COMP"
  exit 0
fi

# Determine the most specific exit code.
# Order of severity: baseline_incomplete (25) > unavailable (21) > drift (20).
# Baseline-incomplete is its own code so the operator immediately knows
# to fix the baseline rather than chasing a phantom drift.
RC=20
if [ "$ENV_S" = "unavailable" ] || [ "$DBF_S" = "unavailable" ] \
   || [ "$RED_S" = "unavailable" ] || [ "$NGX_S" = "unavailable" ]; then
  RC=21
fi
if [ "$ENV_S" = "baseline_incomplete" ] || [ "$DBF_S" = "baseline_incomplete" ] \
   || [ "$RED_S" = "baseline_incomplete" ] || [ "$NGX_S" = "baseline_incomplete" ]; then
  RC=25
fi

log CRITICAL "CONFIG DRIFT composite_expected=$EXPECTED_COMP composite_live=$LIVE_COMP env=$ENV_S db_flags=$DBF_S redis_flags=$RED_S nginx=$NGX_S"

if [ "$KILL" = "1" ]; then
  log CRITICAL "killing process due to config drift (FINGERPRINT_KILL_ON_MISMATCH=1)"
fi
exit "$RC"
