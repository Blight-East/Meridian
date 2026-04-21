#!/usr/bin/env bash
# deploy/fingerprint/verify_runtime_fingerprint.sh
#
# Compares the LIVE runtime fingerprint (recomputed against the host's
# installed Python interpreter + site-packages) against the EXPECTED
# runtime fingerprint captured at deploy time. Per-component granularity
# — verifier tells you which layer drifted (interpreter vs site-packages).
#
# This is the third tier of the integrity model:
#   * runtime_verify.sh             — source-tree (signed, Sigstore)
#   * verify_config_fingerprint.sh  — runtime config (env/db/redis/nginx)
#   * verify_runtime_fingerprint.sh — installed Python (THIS FILE)
#
# It closes the "edit installed package in site-packages" attack the
# source verifier deliberately ignores.
#
# Inputs:
#   FINGERPRINT_RUNTIME_EXPECTED_PATH  default: deploy/artifacts/expected_runtime_fingerprint.json
#   ROLE                               required label for metric/log
#   plus everything build_runtime_fingerprint.sh accepts (RUNTIME_PYTHON_BIN,
#   INCLUDE_USER_SITE, etc.)
#
# Optional:
#   FINGERPRINT_KILL_ON_MISMATCH       0|1 (default 0) — exit non-zero so
#                                       the supervising process tears down
#   FINGERPRINT_METRICS_DIR            default: /var/lib/payflux/metrics
#   FINGERPRINT_LOG_PATH               optional explicit log file
#   FINGERPRINT_RUNTIME_REQUIRED_COMPONENTS
#                                      default: "interpreter,site_packages"
#                                      components that MUST be present:true
#                                      in both baseline and live; anything
#                                      missing is fail-closed.
#
# Exit codes:
#   0   ok (composite matches AND every required component present:true with matching hash)
#   30  composite mismatch (one or more components drifted)
#   31  interpreter drift (interpreter binary changed)
#   32  site_packages drift (installed packages changed)
#   33  required component unavailable on host
#   34  recomputation failed
#   35  config error (missing ROLE, jq, builder, etc.)
#   36  expected baseline missing
#   37  baseline incomplete (expected file does not declare a required component)

set -euo pipefail

ROLE="${ROLE:-unknown}"
EXPECTED_PATH="${FINGERPRINT_RUNTIME_EXPECTED_PATH:-deploy/artifacts/expected_runtime_fingerprint.json}"
METRICS_DIR="${FINGERPRINT_METRICS_DIR:-/var/lib/payflux/metrics}"
LOG_PATH="${FINGERPRINT_LOG_PATH:-}"
KILL="${FINGERPRINT_KILL_ON_MISMATCH:-0}"
REQUIRED_COMPONENTS="${FINGERPRINT_RUNTIME_REQUIRED_COMPONENTS:-interpreter,site_packages}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() {
  local level="$1"; shift
  local line="$(ts) [$level] [runtime-fingerprint] role=$ROLE $*"
  echo "$line" >&2
  if [ -n "$LOG_PATH" ]; then
    mkdir -p "$(dirname "$LOG_PATH")" 2>/dev/null || true
    printf '%s\n' "$line" >> "$LOG_PATH" 2>/dev/null || true
  fi
}

write_metrics() {
  # Args: composite_mismatch interpreter_mismatch site_packages_mismatch
  local composite="$1" interp="$2" sitep="$3"
  if ! mkdir -p "$METRICS_DIR" 2>/dev/null; then
    log WARN "metrics dir not writable: $METRICS_DIR"
    return 0
  fi
  local out="$METRICS_DIR/runtime_fingerprint.prom"
  local tmp="${out}.tmp.$$"
  {
    echo "# HELP runtime_fingerprint_mismatch 1 if the composite runtime fingerprint differs from expected"
    echo "# TYPE runtime_fingerprint_mismatch gauge"
    echo "runtime_fingerprint_mismatch{component=\"$ROLE\",source=\"composite\"} $composite"
    echo "runtime_fingerprint_mismatch{component=\"$ROLE\",source=\"interpreter\"} $interp"
    echo "runtime_fingerprint_mismatch{component=\"$ROLE\",source=\"site_packages\"} $sitep"
    echo "# HELP runtime_fingerprint_last_check_unixtime epoch seconds of last verifier run"
    echo "# TYPE runtime_fingerprint_last_check_unixtime gauge"
    echo "runtime_fingerprint_last_check_unixtime{component=\"$ROLE\"} $(date -u +%s)"
  } > "$tmp" && mv -f "$tmp" "$out"
}

if [ "$ROLE" = "unknown" ]; then
  log CRITICAL "ROLE env var is required"
  exit 35
fi

if [ ! -s "$EXPECTED_PATH" ]; then
  log CRITICAL "expected runtime fingerprint missing: $EXPECTED_PATH"
  write_metrics 1 1 1
  exit 36
fi

if ! command -v jq >/dev/null 2>&1; then
  log CRITICAL "jq unavailable"
  exit 35
fi

# --- Recompute live runtime fingerprint ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILDER="$SCRIPT_DIR/build_runtime_fingerprint.sh"
if [ ! -r "$BUILDER" ]; then
  log CRITICAL "missing builder script: $BUILDER"
  exit 35
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
LIVE_PATH="$WORK_DIR/live_runtime_fingerprint.json"
if ! ART_DIR="$WORK_DIR" bash "$BUILDER" "$LIVE_PATH" >/dev/null 2>"$WORK_DIR/build.err"; then
  log CRITICAL "runtime recomputation failed: $(tail -n5 "$WORK_DIR/build.err" | tr '\n' ' ')"
  write_metrics 1 1 1
  exit 34
fi

# --- Extract values ---
EXPECTED_COMP="$(jq -r '.fingerprint' "$EXPECTED_PATH")"
LIVE_COMP="$(jq -r '.fingerprint' "$LIVE_PATH")"

read_comp() {
  local file="$1" comp="$2" field="$3"
  jq -r ".components.\"$comp\".\"$field\"" "$file"
}

cmp_component() {
  local comp="$1"
  local exp_h liv_h liv_present exp_present
  exp_h="$(read_comp "$EXPECTED_PATH" "$comp" sha256)"
  liv_h="$(read_comp "$LIVE_PATH"     "$comp" sha256)"
  liv_present="$(read_comp "$LIVE_PATH"     "$comp" present)"
  exp_present="$(read_comp "$EXPECTED_PATH" "$comp" present)"

  # Baseline integrity: the EXPECTED file must declare every required
  # component with present:true. A baseline captured without STRICT=1
  # (e.g. on a degraded host) is fail-closed — we will not silently
  # accept a baseline that itself does not assert the required surface.
  if printf ',%s,' "$REQUIRED_COMPONENTS" | grep -q ",$comp,"; then
    if [ "$exp_h" = "null" ] || [ -z "$exp_h" ]; then
      log CRITICAL "expected baseline is MISSING required component '$comp' — re-capture baseline with STRICT=1"
      echo "1|baseline_incomplete"
      return
    fi
    if [ "$exp_present" != "true" ]; then
      log CRITICAL "expected baseline marks required component '$comp' as present=$exp_present — re-capture baseline with STRICT=1"
      echo "1|baseline_incomplete"
      return
    fi
    if [ "$liv_present" != "true" ]; then
      log CRITICAL "required component '$comp' is unavailable on this host (live present=$liv_present)"
      echo "1|unavailable"
      return
    fi
  fi
  if [ "$exp_h" != "$liv_h" ]; then
    log CRITICAL "runtime drift in component '$comp': expected=$exp_h live=$liv_h"
    echo "1|drift"
    return
  fi
  echo "0|ok"
}

INTERP_R="$(cmp_component interpreter)"
SITEP_R="$(cmp_component site_packages)"

INTERP_M="${INTERP_R%%|*}"; INTERP_S="${INTERP_R##*|}"
SITEP_M="${SITEP_R%%|*}";   SITEP_S="${SITEP_R##*|}"

COMP_M=0
if [ "$EXPECTED_COMP" != "$LIVE_COMP" ]; then COMP_M=1; fi
if [ "$INTERP_M" = "1" ] || [ "$SITEP_M" = "1" ]; then
  COMP_M=1
fi

write_metrics "$COMP_M" "$INTERP_M" "$SITEP_M"

if [ "$COMP_M" = "0" ]; then
  log INFO "ok composite=$LIVE_COMP"
  exit 0
fi

# Determine the most specific exit code.
# Order of severity: baseline_incomplete (37) > unavailable (33)
#                    > component-specific drift (31/32) > composite mismatch (30).
# Baseline-incomplete is its own code so the operator immediately knows
# to fix the baseline rather than chasing a phantom drift.
RC=30
if [ "$INTERP_S" = "drift" ]; then
  RC=31
fi
if [ "$SITEP_S" = "drift" ]; then
  # Site-packages drift takes precedence over interpreter drift in the
  # exit code only when ONLY site_packages drifted — if both drifted,
  # surface the more impactful one (interpreter, since that's the host
  # binary). The metrics file still flags both individually.
  if [ "$INTERP_S" != "drift" ]; then
    RC=32
  fi
fi
if [ "$INTERP_S" = "unavailable" ] || [ "$SITEP_S" = "unavailable" ]; then
  RC=33
fi
if [ "$INTERP_S" = "baseline_incomplete" ] || [ "$SITEP_S" = "baseline_incomplete" ]; then
  RC=37
fi

log CRITICAL "RUNTIME DRIFT composite_expected=$EXPECTED_COMP composite_live=$LIVE_COMP interpreter=$INTERP_S site_packages=$SITEP_S"

if [ "$KILL" = "1" ]; then
  log CRITICAL "killing process due to runtime drift (FINGERPRINT_KILL_ON_MISMATCH=1)"
fi
exit "$RC"
