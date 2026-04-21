#!/usr/bin/env bash
# deploy/fingerprint/capture_baselines.sh
#
# One script per host that captures BOTH the config-fingerprint and
# runtime-fingerprint baselines that the boot-time verifiers compare
# against. Designed for the Phase 2 step in deploy/fingerprint/
# TRUST_MODEL.md ("provisioning").
#
# What it does — in this exact order, because the order matters:
#
#   1. SOURCE VERIFY GATE.
#      Runs runtime_verify.sh against the deployed release fingerprint.
#      If the deployed source tree fails Sigstore verification, we
#      REFUSE TO CAPTURE BASELINES — capturing on a tampered source
#      tree would launder the tampering into the new baseline and
#      silently bless it forever. The only escape is --skip-source-verify
#      AND --rotate together (a deliberately awkward combination that
#      forces an operator to type two flags they have to think about).
#
#   2. EXISTING-BASELINE GUARD.
#      If either expected_*_fingerprint.json already exists, refuse to
#      overwrite it unless --rotate is passed. This is the SECOND
#      defense against drift-laundering — re-running capture on a host
#      that has already drifted from its baseline would silently
#      ratify the drift if we just clobbered the file.
#
#   3. STRICT CAPTURE.
#      Both builder scripts are invoked with STRICT=1 so a partial
#      baseline (e.g. Postgres unreachable, nginx not on PATH) never
#      lands on disk. The verifiers treat such a baseline as
#      fail-closed (exit 25 / 37) anyway, but failing at capture time
#      with a clear message beats failing at boot time with a generic
#      "baseline_incomplete" log.
#
#   4. ROUND-TRIP VERIFY.
#      Each captured baseline is immediately verified against itself.
#      A capture that doesn't round-trip is a capture that's broken —
#      we'd rather find out now than at the next boot.
#
#   5. RECEIPT.
#      Writes a timestamped JSON receipt with: hostname, who ran it,
#      both composite hashes, the deployed source fingerprint hash,
#      and (if --rotate) the prior baseline composites. Receipts are
#      never overwritten — they're an append-only audit trail in
#      $ART_DIR/baseline_capture_receipts/.
#
# Usage:
#   capture_baselines.sh [--rotate] [--art-dir <path>] [--skip-source-verify]
#
# Flags:
#   --rotate              Allow overwriting an existing baseline. The
#                         prior composite is recorded in the receipt.
#   --art-dir <path>      Output directory (default: deploy/artifacts).
#   --skip-source-verify  Skip the Sigstore gate. Requires --rotate
#                         (deliberately awkward combination — only ever
#                         valid for first-ever capture on a host that
#                         doesn't have the deployed artifact yet, e.g.
#                         a fresh dev VM bootstrapping itself).
#
# Required env:
#   RELEASE_OIDC_REPO      e.g. "payflux/payflux"
#                          (skipped only when --skip-source-verify is on)
#
# Exit codes:
#    0   captured + round-trip verified + receipt written
#    1   would-overwrite-existing baseline without --rotate
#    2   source verification failed (refusing to capture on tampered tree)
#    3   STRICT capture failed (required source unavailable on host)
#    4   round-trip verify failed (capture is broken)
#    5   missing required env (RELEASE_OIDC_REPO)
#    6   tooling missing (jq, cosign, etc.)
#    7   invalid flag combination

set -euo pipefail

# ----- defaults -----
ROTATE=0
SKIP_SOURCE_VERIFY=0
ART_DIR_ARG=""

# ----- arg parsing -----
while [ $# -gt 0 ]; do
  case "$1" in
    --rotate)              ROTATE=1; shift ;;
    --skip-source-verify)  SKIP_SOURCE_VERIFY=1; shift ;;
    --art-dir)
      [ -n "${2:-}" ] || { echo "FAIL: --art-dir requires a value" >&2; exit 7; }
      ART_DIR_ARG="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,75p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "FAIL: unknown flag: $1" >&2
      exit 7 ;;
  esac
done

# Awkward combo enforcement — see header comment.
if [ "$SKIP_SOURCE_VERIFY" = "1" ] && [ "$ROTATE" = "0" ]; then
  echo "FAIL: --skip-source-verify requires --rotate (see header comment)" >&2
  exit 7
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ART_DIR="${ART_DIR_ARG:-${ART_DIR:-$REPO_ROOT/deploy/artifacts}}"
mkdir -p "$ART_DIR"
RECEIPT_DIR="$ART_DIR/baseline_capture_receipts"
mkdir -p "$RECEIPT_DIR"

CONFIG_BUILDER="$SCRIPT_DIR/build_config_fingerprint.sh"
RUNTIME_BUILDER="$SCRIPT_DIR/build_runtime_fingerprint.sh"
CONFIG_VERIFIER="$SCRIPT_DIR/verify_config_fingerprint.sh"
RUNTIME_VERIFIER="$SCRIPT_DIR/verify_runtime_fingerprint.sh"
SOURCE_VERIFIER="$SCRIPT_DIR/runtime_verify.sh"

CONFIG_BASELINE="$ART_DIR/expected_config_fingerprint.json"
RUNTIME_BASELINE="$ART_DIR/expected_runtime_fingerprint.json"
DEPLOYED_SOURCE_FP="$ART_DIR/release_fingerprint.json"

# ----- preflight -----
if ! command -v jq >/dev/null 2>&1; then
  echo "FAIL: jq is required" >&2
  exit 6
fi
for s in "$CONFIG_BUILDER" "$RUNTIME_BUILDER" "$CONFIG_VERIFIER" "$RUNTIME_VERIFIER"; do
  if [ ! -r "$s" ]; then
    echo "FAIL: missing required script: $s" >&2
    exit 6
  fi
done

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
ts_compact() { date -u +%Y-%m-%dT%H-%M-%SZ; }

log() {
  local level="$1"; shift
  echo "$(ts) [$level] [capture-baselines] $*" >&2
}

# ----- step 1: source verify gate -----
DEPLOYED_SOURCE_HASH="unverified"
if [ "$SKIP_SOURCE_VERIFY" = "1" ]; then
  log WARN "skipping source verification (--skip-source-verify); receipt will mark deployed_source_hash=skipped"
  DEPLOYED_SOURCE_HASH="skipped"
else
  if [ -z "${RELEASE_OIDC_REPO:-}" ]; then
    log CRITICAL "RELEASE_OIDC_REPO unset — required for source verification (or pass --skip-source-verify --rotate)"
    exit 5
  fi
  if [ ! -r "$SOURCE_VERIFIER" ]; then
    log CRITICAL "source verifier not found: $SOURCE_VERIFIER"
    exit 6
  fi
  if [ ! -s "$DEPLOYED_SOURCE_FP" ]; then
    log CRITICAL "deployed source fingerprint not found at $DEPLOYED_SOURCE_FP — has the release artifact been deployed yet?"
    exit 2
  fi
  log INFO "verifying deployed source fingerprint via Sigstore..."
  # The source verifier exits 0 on full success (signature + identity +
  # fingerprint match). Anything else is fail-closed for this script.
  if ! ROLE="capture-baselines" \
       FINGERPRINT_DEPLOYED_PATH="$DEPLOYED_SOURCE_FP" \
       FINGERPRINT_KILL_ON_MISMATCH=0 \
       bash "$SOURCE_VERIFIER" >/dev/null 2>"$ART_DIR/.capture_source_verify.err"; then
    log CRITICAL "source verification FAILED — refusing to capture baselines on a tampered tree"
    log CRITICAL "verifier stderr (last 5 lines):"
    tail -n5 "$ART_DIR/.capture_source_verify.err" | sed 's/^/    /' >&2 || true
    exit 2
  fi
  DEPLOYED_SOURCE_HASH="$(jq -r '.fingerprint' "$DEPLOYED_SOURCE_FP")"
  log INFO "source verified ok (deployed=$DEPLOYED_SOURCE_HASH)"
fi

# ----- step 2: existing-baseline guard -----
PRIOR_CONFIG_HASH=""
PRIOR_RUNTIME_HASH=""
existing=()
[ -s "$CONFIG_BASELINE"  ] && existing+=("$CONFIG_BASELINE")
[ -s "$RUNTIME_BASELINE" ] && existing+=("$RUNTIME_BASELINE")
if [ "${#existing[@]}" -gt 0 ]; then
  if [ "$ROTATE" = "0" ]; then
    log CRITICAL "baseline(s) already exist; refusing to overwrite without --rotate:"
    for f in "${existing[@]}"; do
      log CRITICAL "  $f  (composite=$(jq -r '.fingerprint' "$f" 2>/dev/null || echo '?'))"
    done
    log CRITICAL "if you intend to re-baseline (e.g. after a planned config change), pass --rotate."
    log CRITICAL "the prior composite will be recorded in the receipt."
    exit 1
  fi
  [ -s "$CONFIG_BASELINE"  ] && PRIOR_CONFIG_HASH="$(jq -r '.fingerprint' "$CONFIG_BASELINE"  2>/dev/null || true)"
  [ -s "$RUNTIME_BASELINE" ] && PRIOR_RUNTIME_HASH="$(jq -r '.fingerprint' "$RUNTIME_BASELINE" 2>/dev/null || true)"
  log WARN "ROTATING existing baseline(s); prior composites recorded in receipt"
  log WARN "  prior_config=$PRIOR_CONFIG_HASH"
  log WARN "  prior_runtime=$PRIOR_RUNTIME_HASH"
fi

# ----- step 3: STRICT capture -----
# Capture into a tempdir first; only move into place after BOTH builders
# succeed AND BOTH round-trip checks pass. Otherwise a half-captured
# state could leave one new baseline next to one stale baseline, which
# is the kind of subtle inconsistency that would burn an operator at
# 3 AM during a real drift investigation.
STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT
STAGED_CONFIG="$STAGING/expected_config_fingerprint.json"
STAGED_RUNTIME="$STAGING/expected_runtime_fingerprint.json"

log INFO "capturing config baseline (STRICT=1)..."
if ! STRICT=1 ART_DIR="$STAGING" \
       bash "$CONFIG_BUILDER" "$STAGED_CONFIG" \
       >"$STAGING/config_build.out" 2>"$STAGING/config_build.err"; then
  log CRITICAL "config baseline capture FAILED — required source unavailable on host:"
  tail -n10 "$STAGING/config_build.err" | sed 's/^/    /' >&2 || true
  exit 3
fi
NEW_CONFIG_HASH="$(jq -r '.fingerprint' "$STAGED_CONFIG")"
log INFO "  config composite: $NEW_CONFIG_HASH"

log INFO "capturing runtime baseline (STRICT=1) — this walks site-packages and may take ~30-60s..."
if ! STRICT=1 ART_DIR="$STAGING" \
       bash "$RUNTIME_BUILDER" "$STAGED_RUNTIME" \
       >"$STAGING/runtime_build.out" 2>"$STAGING/runtime_build.err"; then
  log CRITICAL "runtime baseline capture FAILED — required component unavailable on host:"
  tail -n10 "$STAGING/runtime_build.err" | sed 's/^/    /' >&2 || true
  exit 3
fi
NEW_RUNTIME_HASH="$(jq -r '.fingerprint' "$STAGED_RUNTIME")"
NEW_RUNTIME_FILE_COUNT="$(jq -r '.file_count' "$STAGED_RUNTIME")"
log INFO "  runtime composite: $NEW_RUNTIME_HASH ($NEW_RUNTIME_FILE_COUNT files)"

# ----- step 4: round-trip verify -----
log INFO "round-trip verifying captured baselines against the same host..."
if ! ROLE="capture-baselines" \
     FINGERPRINT_CONFIG_EXPECTED_PATH="$STAGED_CONFIG" \
     FINGERPRINT_METRICS_DIR="$STAGING/metrics" \
     FINGERPRINT_KILL_ON_MISMATCH=0 \
     bash "$CONFIG_VERIFIER" >"$STAGING/config_verify.out" 2>"$STAGING/config_verify.err"; then
  log CRITICAL "config baseline did NOT round-trip — capture is broken:"
  tail -n10 "$STAGING/config_verify.err" | sed 's/^/    /' >&2 || true
  exit 4
fi
if ! ROLE="capture-baselines" \
     FINGERPRINT_RUNTIME_EXPECTED_PATH="$STAGED_RUNTIME" \
     FINGERPRINT_METRICS_DIR="$STAGING/metrics" \
     FINGERPRINT_KILL_ON_MISMATCH=0 \
     bash "$RUNTIME_VERIFIER" >"$STAGING/runtime_verify.out" 2>"$STAGING/runtime_verify.err"; then
  log CRITICAL "runtime baseline did NOT round-trip — capture is broken:"
  tail -n10 "$STAGING/runtime_verify.err" | sed 's/^/    /' >&2 || true
  exit 4
fi
log INFO "  round-trip ok (both baselines verify against themselves)"

# ----- step 5: atomic move into place -----
mv -f "$STAGED_CONFIG"  "$CONFIG_BASELINE"
mv -f "$STAGED_RUNTIME" "$RUNTIME_BASELINE"

# Companion runtime manifest moves alongside (handy for audit reproduction).
if [ -s "${STAGED_RUNTIME%.json}.manifest" ]; then
  mv -f "${STAGED_RUNTIME%.json}.manifest" "${RUNTIME_BASELINE%.json}.manifest"
fi

# ----- step 6: receipt -----
RECEIPT="$RECEIPT_DIR/$(ts_compact).json"
WHO="$(whoami 2>/dev/null || echo unknown)"
HOST="$(hostname 2>/dev/null || echo unknown)"
ROTATED_CONFIG_JSON="null"
ROTATED_RUNTIME_JSON="null"
[ -n "$PRIOR_CONFIG_HASH"  ] && ROTATED_CONFIG_JSON="\"$PRIOR_CONFIG_HASH\""
[ -n "$PRIOR_RUNTIME_HASH" ] && ROTATED_RUNTIME_JSON="\"$PRIOR_RUNTIME_HASH\""

cat > "$RECEIPT" <<EOF
{
  "captured_at_utc":         "$(ts)",
  "captured_by":             "$WHO",
  "host":                    "$HOST",
  "art_dir":                 "$ART_DIR",
  "rotate":                  $( [ "$ROTATE" = "1" ] && echo true || echo false ),
  "skip_source_verify":      $( [ "$SKIP_SOURCE_VERIFY" = "1" ] && echo true || echo false ),
  "deployed_source_hash":    "$DEPLOYED_SOURCE_HASH",
  "captured": {
    "config_composite":      "$NEW_CONFIG_HASH",
    "runtime_composite":     "$NEW_RUNTIME_HASH",
    "runtime_file_count":    $NEW_RUNTIME_FILE_COUNT
  },
  "rotated_from": {
    "config_composite":      $ROTATED_CONFIG_JSON,
    "runtime_composite":     $ROTATED_RUNTIME_JSON
  }
}
EOF

# ----- summary -----
echo
echo "================ baseline capture complete ================"
echo "  host:            $HOST"
echo "  config:          $NEW_CONFIG_HASH"
[ -n "$PRIOR_CONFIG_HASH"  ] && echo "    rotated from:  $PRIOR_CONFIG_HASH"
echo "  runtime:         $NEW_RUNTIME_HASH ($NEW_RUNTIME_FILE_COUNT files)"
[ -n "$PRIOR_RUNTIME_HASH" ] && echo "    rotated from:  $PRIOR_RUNTIME_HASH"
echo "  source-verified: $DEPLOYED_SOURCE_HASH"
echo "  receipt:         $RECEIPT"
echo "==========================================================="
echo
echo "Next step: leave FINGERPRINT_ENFORCEMENT_MODE unset (or =report)"
echo "for at least 7 days, then flip per-process to =enforce."
echo "See deploy/fingerprint/TRUST_MODEL.md → 'Rollout phases'."

exit 0
