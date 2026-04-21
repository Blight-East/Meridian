#!/usr/bin/env bash
# deploy/fingerprint/runtime_verify.sh
#
# Sigstore-keyless runtime integrity verifier. Two phases:
#   PHASE A — verify the deployed release fingerprint's cosign signature
#             against the Sigstore Fulcio root + GitHub OIDC issuer.
#             Fails closed if the signing identity does not match the
#             expected GitHub repository / workflow.
#   PHASE B — recompute the fingerprint from the live filesystem via
#             build_release_fingerprint.sh and byte-compare against the
#             (signature-validated) deployed value.
#
# Trust root: the Sigstore root + GitHub OIDC issuer. Both are external
# and globally consistent. There is NO repo-pinned public key — an
# attacker with repo-write cannot forge a valid signature without also
# compromising GitHub's OIDC issuer AND the Sigstore Fulcio CA AND the
# Rekor transparency log.
#
# Required env:
#   ROLE                              required label for metric/log (api|worker|scheduler|fingerprint-watcher)
#   RELEASE_OIDC_REPO                 e.g. "payflux/payflux"; binds the cert SAN to this repo
#   RELEASE_OIDC_WORKFLOW_REF_REGEX   e.g. "^refs/heads/release/.*$"; binds the cert to release branches
#
# Optional env:
#   FINGERPRINT_DEPLOYED_PATH         default: deploy/artifacts/release_fingerprint.json
#   FINGERPRINT_KILL_ON_MISMATCH      0|1 (default 0)
#   FINGERPRINT_METRICS_DIR           default: /var/lib/payflux/metrics
#   FINGERPRINT_LOG_PATH              optional explicit log file
#   COSIGN_OFFLINE                    0|1 (default 0); use bundle, skip Rekor lookup
#
# Exit codes:
#   0   ok (signature valid AND identity matches AND fingerprint matches)
#   10  signature invalid OR identity mismatch (deployed fingerprint untrusted)
#   11  fingerprint mismatch (running code differs from deployed)
#   12  deployed fingerprint missing
#   13  recomputation failed
#   14  config error (missing ROLE, missing identity binding)
#   15  cosign unavailable

set -euo pipefail

ROLE="${ROLE:-unknown}"
FINGERPRINT_DEPLOYED_PATH="${FINGERPRINT_DEPLOYED_PATH:-deploy/artifacts/release_fingerprint.json}"
KILL_ON_MISMATCH="${FINGERPRINT_KILL_ON_MISMATCH:-0}"
METRICS_DIR="${FINGERPRINT_METRICS_DIR:-/var/lib/payflux/metrics}"
LOG_PATH="${FINGERPRINT_LOG_PATH:-}"
COSIGN_OFFLINE="${COSIGN_OFFLINE:-0}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() {
  local level="$1"; shift
  local line="$(ts) [$level] [fingerprint] role=$ROLE $*"
  echo "$line" >&2
  if [ -n "$LOG_PATH" ]; then
    mkdir -p "$(dirname "$LOG_PATH")" 2>/dev/null || true
    printf '%s\n' "$line" >> "$LOG_PATH" 2>/dev/null || true
  fi
}

write_metrics() {
  local mismatch="$1" sig_invalid="$2"
  if ! mkdir -p "$METRICS_DIR" 2>/dev/null; then
    log WARN "metrics dir not writable: $METRICS_DIR"
    return 0
  fi
  local out="$METRICS_DIR/fingerprint.prom"
  local tmp="${out}.tmp.$$"
  {
    echo "# HELP fingerprint_mismatch 1 if recomputed fingerprint differs from deployed"
    echo "# TYPE fingerprint_mismatch gauge"
    echo "fingerprint_mismatch{component=\"$ROLE\"} $mismatch"
    echo "# HELP fingerprint_signature_invalid 1 if cosign signature/identity does not validate"
    echo "# TYPE fingerprint_signature_invalid gauge"
    echo "fingerprint_signature_invalid{component=\"$ROLE\"} $sig_invalid"
    echo "# HELP fingerprint_last_check_unixtime epoch seconds of last verifier run"
    echo "# TYPE fingerprint_last_check_unixtime gauge"
    echo "fingerprint_last_check_unixtime{component=\"$ROLE\"} $(date -u +%s)"
  } > "$tmp" && mv -f "$tmp" "$out"
}

# --- Config validation ---
if [ "$ROLE" = "unknown" ]; then
  log CRITICAL "ROLE env var is required (api|worker|scheduler|fingerprint-watcher)"
  exit 14
fi
if [ -z "${RELEASE_OIDC_REPO:-}" ]; then
  log CRITICAL "RELEASE_OIDC_REPO is required (e.g. payflux/payflux); identity binding cannot be wildcard"
  exit 14
fi
# Tight identity bindings — every one of these has a default but NONE may
# silently widen. A bypass attempt that reuses a Sigstore signature from
# a different workflow, branch type, trigger, or commit SHA fails here.
WORKFLOW_REF_REGEX="${RELEASE_OIDC_WORKFLOW_REF_REGEX:-^refs/heads/release/.*$}"
WORKFLOW_NAME="${RELEASE_OIDC_WORKFLOW_NAME:-release-gate}"
WORKFLOW_FILE="${RELEASE_OIDC_WORKFLOW_FILE:-release-gate.yml}"
WORKFLOW_TRIGGER="${RELEASE_OIDC_WORKFLOW_TRIGGER:-push}"

if ! command -v cosign >/dev/null 2>&1; then
  log CRITICAL "cosign not found on PATH"
  write_metrics 1 1
  exit 15
fi

# --- Locate sibling generator script ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GEN_SCRIPT="$SCRIPT_DIR/build_release_fingerprint.sh"
if [ ! -r "$GEN_SCRIPT" ]; then
  log CRITICAL "missing generator script: $GEN_SCRIPT"
  write_metrics 1 0
  exit 13
fi

# --- Deployed fingerprint must exist ---
if [ ! -s "$FINGERPRINT_DEPLOYED_PATH" ]; then
  log CRITICAL "deployed fingerprint missing: $FINGERPRINT_DEPLOYED_PATH"
  write_metrics 1 0
  exit 12
fi

SIG_PATH="${FINGERPRINT_DEPLOYED_PATH}.sig"
CRT_PATH="${FINGERPRINT_DEPLOYED_PATH}.crt"
BUNDLE_PATH="${FINGERPRINT_DEPLOYED_PATH}.bundle"

# Bind cosign verification to the EXACT commit SHA the deployed
# fingerprint claims to represent. The fingerprint JSON's git_sha field
# was filled by build_release_fingerprint.sh from `git rev-parse HEAD`
# at sign time. If a Sigstore signature for a different commit SHA is
# substituted in place of this one, cosign rejects it.
if ! command -v jq >/dev/null 2>&1; then
  log CRITICAL "jq unavailable; cannot read git_sha from deployed fingerprint"
  write_metrics 1 1
  exit 15
fi
EXPECTED_GIT_SHA="$(jq -r '.git_sha' "$FINGERPRINT_DEPLOYED_PATH")"
if [ -z "$EXPECTED_GIT_SHA" ] || [ "$EXPECTED_GIT_SHA" = "null" ]; then
  log CRITICAL "deployed fingerprint missing git_sha — cannot bind cosign cert to commit"
  write_metrics 1 1
  exit 14
fi
if ! printf '%s' "$EXPECTED_GIT_SHA" | grep -Eq '^[0-9a-f]{40}$'; then
  log CRITICAL "deployed fingerprint git_sha is not a 40-char hex SHA: $EXPECTED_GIT_SHA"
  write_metrics 1 1
  exit 14
fi

# --- PHASE A: cosign signature + identity verification ---
# Identity SAN is anchored: scheme + host + repo + EXACT workflow file +
# branch ref class. Wildcard on workflow filename was the prior loose
# spot — closed here.
IDENTITY_REGEX="^https://github\\.com/${RELEASE_OIDC_REPO}/\\.github/workflows/${WORKFLOW_FILE}@${WORKFLOW_REF_REGEX#^}"

VERIFY_CMD=(cosign verify-blob
  --certificate-identity-regexp            "$IDENTITY_REGEX"
  --certificate-oidc-issuer                "https://token.actions.githubusercontent.com"
  --certificate-github-workflow-repository "$RELEASE_OIDC_REPO"
  --certificate-github-workflow-ref-regexp "$WORKFLOW_REF_REGEX"
  --certificate-github-workflow-name       "$WORKFLOW_NAME"
  --certificate-github-workflow-trigger    "$WORKFLOW_TRIGGER"
  --certificate-github-workflow-sha        "$EXPECTED_GIT_SHA"
)

if [ -s "$BUNDLE_PATH" ] && [ "$COSIGN_OFFLINE" = "1" ]; then
  VERIFY_CMD+=( --bundle "$BUNDLE_PATH" --offline )
elif [ -s "$BUNDLE_PATH" ]; then
  VERIFY_CMD+=( --bundle "$BUNDLE_PATH" )
elif [ -s "$SIG_PATH" ] && [ -s "$CRT_PATH" ]; then
  VERIFY_CMD+=( --signature "$SIG_PATH" --certificate "$CRT_PATH" )
else
  log CRITICAL "neither bundle ($BUNDLE_PATH) nor sig+crt ($SIG_PATH, $CRT_PATH) present; cannot verify"
  write_metrics 1 1
  exit 10
fi
VERIFY_CMD+=( "$FINGERPRINT_DEPLOYED_PATH" )

if ! "${VERIFY_CMD[@]}" >/tmp/cosign_verify.out 2>&1; then
  log CRITICAL "cosign verify-blob FAILED:"
  while IFS= read -r line; do log CRITICAL "  cosign: $line"; done < /tmp/cosign_verify.out
  write_metrics 1 1
  if [ "$KILL_ON_MISMATCH" = "1" ]; then
    log CRITICAL "killing process due to invalid signature/identity (FINGERPRINT_KILL_ON_MISMATCH=1)"
  fi
  exit 10
fi

# --- PHASE B: recompute and compare ---
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
RECHECK_OUT="$WORK_DIR/release_fingerprint.json"

if ! ART_DIR="$WORK_DIR" bash "$GEN_SCRIPT" "$RECHECK_OUT" >/dev/null 2>"$WORK_DIR/gen.err"; then
  log CRITICAL "recomputation failed: $(tail -n5 "$WORK_DIR/gen.err" | tr '\n' ' ')"
  write_metrics 1 0
  exit 13
fi

if ! command -v jq >/dev/null 2>&1; then
  log CRITICAL "jq unavailable; cannot parse fingerprint JSON"
  write_metrics 1 0
  exit 13
fi

DEPLOYED_FP="$(jq -r '.fingerprint' "$FINGERPRINT_DEPLOYED_PATH")"
LIVE_FP="$(jq -r '.fingerprint' "$RECHECK_OUT")"
DEPLOYED_GIT="$(jq -r '.git_sha' "$FINGERPRINT_DEPLOYED_PATH")"
LIVE_GIT="$(jq -r '.git_sha' "$RECHECK_OUT")"

if [ "$DEPLOYED_FP" != "$LIVE_FP" ]; then
  log CRITICAL "FINGERPRINT MISMATCH deployed=$DEPLOYED_FP live=$LIVE_FP deployed_git=$DEPLOYED_GIT live_git=$LIVE_GIT"
  write_metrics 1 0
  if [ "$KILL_ON_MISMATCH" = "1" ]; then
    log CRITICAL "killing process due to fingerprint mismatch (FINGERPRINT_KILL_ON_MISMATCH=1)"
  fi
  exit 11
fi

write_metrics 0 0
log INFO "ok fingerprint=$LIVE_FP git_sha=$LIVE_GIT cosign_identity=ok"
exit 0
