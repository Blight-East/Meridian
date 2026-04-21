#!/usr/bin/env bash
# deploy/ci/gate.sh
# Hard CI gate for release/* branches. Refuses to publish a deployable
# artifact if any precondition fails. Fail-fast: first error aborts.
#
# Exit codes:
#   0  all checks passed
#   2  change-control gate failed
#   3  H2 writer integrity check failed
#   4  inflight alert verification failed
#   5  Stripe webhook 503 enforcement smoke test failed
#   6  release fingerprint mismatch (or generator failure)
#   9  unexpected internal error
#
# Required env:
#   DEPLOY_SHA                  git SHA being released (40 chars)
#   STRIPE_WEBHOOK_PROBE_URL    URL of webhook endpoint under preflight (must be in recovery mode)
#   ALERTMANAGER_RULES_PATH     path to alert rules file (default: deploy/observability/inflight_alert.yml)
#
# Optional env:
#   ART_DIR                     artifact directory (default: deploy/artifacts)
#   GATE_LOG                    full log path (default: $ART_DIR/gate_${DEPLOY_SHA}.log)

set -euo pipefail

ART_DIR="${ART_DIR:-deploy/artifacts}"
mkdir -p "$ART_DIR"

: "${DEPLOY_SHA:?DEPLOY_SHA must be set to the git SHA being released}"
GATE_LOG="${GATE_LOG:-$ART_DIR/gate_${DEPLOY_SHA}.log}"

# Tee everything to the gate log for audit.
exec > >(tee -a "$GATE_LOG") 2>&1

TOTAL_STEPS=5
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

step() {
  local n="$1" name="$2"
  echo
  echo "[$n/$TOTAL_STEPS] $(ts) — $name"
  echo "----------------------------------------------------------------"
}

fail() {
  local code="$1" msg="$2"
  echo
  echo "GATE FAIL ($(ts)) — exit=$code"
  echo "Reason: $msg"
  echo "Log:    $GATE_LOG"
  exit "$code"
}

require_script() {
  local path="$1"
  if [ ! -x "$path" ] && [ ! -r "$path" ]; then
    fail 9 "required script not found: $path"
  fi
}

echo "================================================================"
echo "PAYFLUX RELEASE GATE"
echo "deploy_sha:       $DEPLOY_SHA"
echo "started_at_utc:   $(ts)"
echo "host:             $(hostname)"
echo "user:             $(id -un 2>/dev/null || echo unknown)"
echo "art_dir:          $ART_DIR"
echo "================================================================"

# ----------------------------------------------------------------------
step 1 "Change-control gate (deploy/preflight/00_check_change_control.sh)"
# ----------------------------------------------------------------------
CC_SCRIPT="deploy/preflight/00_check_change_control.sh"
require_script "$CC_SCRIPT"
if ! bash "$CC_SCRIPT"; then
  fail 2 "change-control gate failed; CC record incomplete or attestations missing"
fi
echo "PASS [1/5] — change-control approved"

# ----------------------------------------------------------------------
step 2 "H2 writer integrity re-grep (deploy/preflight/03_h2_regrep.sh)"
# ----------------------------------------------------------------------
H2_SCRIPT="deploy/preflight/03_h2_regrep.sh"
require_script "$H2_SCRIPT"
H2_LOG="$ART_DIR/h2_regrep_${DEPLOY_SHA}.log"

if ! DEPLOY_SHA="$DEPLOY_SHA" ART_DIR="$ART_DIR" bash "$H2_SCRIPT"; then
  fail 3 "H2 re-grep failed (extra writers, missing writers, or unsuffixed DDL detected)"
fi

# Defense-in-depth: scan the H2 artifact for prohibited DDL referencing
# the unsuffixed base tables. View rename collides if any DDL still creates
# the base table.
if [ -f "$H2_LOG" ]; then
  if grep -E 'CREATE[[:space:]]+TABLE[[:space:]]+(IF[[:space:]]+NOT[[:space:]]+EXISTS[[:space:]]+)?(deal_stage_transitions|learning_feedback_ledger)[[:space:]]*\(' "$H2_LOG" >/dev/null; then
    fail 3 "H2 artifact contains DDL for non-_raw base table; view rename will collide"
  fi
fi
echo "PASS [2/5] — H2 writer map matches authoritative set; no base-table DDL"

# ----------------------------------------------------------------------
step 3 "Inflight alert verification (rule present + tested)"
# ----------------------------------------------------------------------
ALERT_RULES="${ALERTMANAGER_RULES_PATH:-deploy/observability/inflight_alert.yml}"
if [ ! -r "$ALERT_RULES" ]; then
  fail 4 "inflight alert rules file not found: $ALERT_RULES"
fi

# Required alert structure — all four fields must be present.
required_patterns=(
  'alert:[[:space:]]*AgentTasksInflightStuck'
  'redis_list_length\{key="agent_tasks_inflight"\}[[:space:]]*>[[:space:]]*0'
  'for:[[:space:]]*600s'
  'severity:[[:space:]]*P1'
)
for pat in "${required_patterns[@]}"; do
  if ! grep -E "$pat" "$ALERT_RULES" >/dev/null; then
    fail 4 "inflight alert rule missing required field matching: $pat"
  fi
done

# Synthetic page receipt evidence must be present in the artifacts dir.
# CI is responsible for placing this file after the operator confirms the
# test page was received by PagerDuty/Slack.
ALERT_TEST_EVIDENCE="$ART_DIR/inflight_alert_test_${DEPLOY_SHA}.evidence"
if [ ! -s "$ALERT_TEST_EVIDENCE" ]; then
  fail 4 "missing alert test evidence: $ALERT_TEST_EVIDENCE (run synthetic page test and record receipt)"
fi
echo "PASS [3/5] — inflight alert rule present + synthetic test evidence on file"

# ----------------------------------------------------------------------
step 4 "Stripe webhook recovery-mode 503 smoke test (EXTERNAL endpoint only)"
# ----------------------------------------------------------------------
: "${STRIPE_WEBHOOK_PROBE_URL:?STRIPE_WEBHOOK_PROBE_URL must point at the EXTERNAL preflight webhook URL (in recovery mode)}"

# --- External-only enforcement ---
# A localhost probe bypasses the reverse proxy / load balancer / TLS
# termination layer — the actual production boundary. Refuse anything
# that does not look like an externally reachable HTTPS URL.
url="$STRIPE_WEBHOOK_PROBE_URL"

# Must be HTTPS — Stripe webhooks are HTTPS only and we want TLS in path.
case "$url" in
  https://*) ;;
  *) fail 5 "STRIPE_WEBHOOK_PROBE_URL must be https:// (got: ${url%%:*}://...)";;
esac

# Extract host (strip scheme, then path/query/port).
host="${url#https://}"; host="${host%%/*}"; host="${host%%\?*}"; host="${host%%:*}"
if [ -z "$host" ]; then
  fail 5 "could not extract host from STRIPE_WEBHOOK_PROBE_URL"
fi

# Reject loopback / private / link-local / metadata-service hosts.
case "$host" in
  localhost|*.localhost|*.local|ip6-localhost) \
    fail 5 "STRIPE_WEBHOOK_PROBE_URL host '$host' is local; must be externally reachable";;
  127.*|0.0.0.0|::1|fe80:*) \
    fail 5 "STRIPE_WEBHOOK_PROBE_URL host '$host' is loopback/link-local; must be externally reachable";;
  10.*|192.168.*|169.254.*) \
    fail 5 "STRIPE_WEBHOOK_PROBE_URL host '$host' is in a private/link-local range";;
  169.254.169.254) \
    fail 5 "STRIPE_WEBHOOK_PROBE_URL host '$host' is the cloud metadata service";;
esac
# RFC1918 172.16.0.0/12 (172.16-172.31).
case "$host" in
  172.1[6-9].*|172.2[0-9].*|172.3[01].*) \
    fail 5 "STRIPE_WEBHOOK_PROBE_URL host '$host' is in private range 172.16/12";;
esac

# Resolve and verify the resolved IP is publicly routable.
if command -v getent >/dev/null 2>&1; then
  resolved="$(getent hosts "$host" | awk '{print $1}' | head -n1 || true)"
elif command -v dig >/dev/null 2>&1; then
  resolved="$(dig +short "$host" | grep -E '^[0-9a-f.:]+$' | head -n1 || true)"
else
  resolved=""
fi
if [ -n "$resolved" ]; then
  case "$resolved" in
    127.*|10.*|192.168.*|169.254.*|::1|fe80:*) \
      fail 5 "STRIPE_WEBHOOK_PROBE_URL '$host' resolves to non-routable address $resolved";;
    172.1[6-9].*|172.2[0-9].*|172.3[01].*) \
      fail 5 "STRIPE_WEBHOOK_PROBE_URL '$host' resolves to RFC1918 address $resolved";;
  esac
fi

# Probe with deliberately invalid signature; recovery-mode middleware must
# short-circuit BEFORE signature verification and return 503. Verify the
# TLS chain — a webhook reachable over plaintext defeats Stripe security
# guarantees and the gate must refuse it.
http_code=$(
  curl --silent --show-error --output /dev/null \
       --max-time 15 \
       --connect-timeout 5 \
       --proto '=https' \
       --tlsv1.2 \
       --write-out '%{http_code}' \
       -X POST "$STRIPE_WEBHOOK_PROBE_URL" \
       -H 'Content-Type: application/json' \
       -H 'Stripe-Signature: t=0,v1=invalid' \
       --data '{"id":"evt_gate_probe","type":"gate.probe"}' \
  || true
)

if [ "$http_code" != "503" ]; then
  fail 5 "Stripe webhook (external) returned HTTP $http_code; expected 503 under recovery mode (any other response = hard fail). host=$host"
fi
echo "PASS [4/5] — external webhook returned 503 (TLS+recovery mode enforced; host=$host)"

# ----------------------------------------------------------------------
step 5 "Release fingerprint generation + match"
# ----------------------------------------------------------------------
FP_SCRIPT="deploy/fingerprint/build_release_fingerprint.sh"
require_script "$FP_SCRIPT"

# (a) Compute fingerprint of the current checked-out tree.
LIVE_FP_FILE="$ART_DIR/release_fingerprint.json"
ART_DIR="$ART_DIR" bash "$FP_SCRIPT" "$LIVE_FP_FILE"

if [ ! -s "$LIVE_FP_FILE" ]; then
  fail 6 "fingerprint generator did not produce $LIVE_FP_FILE"
fi

if ! command -v jq >/dev/null 2>&1; then
  fail 9 "jq is required to validate fingerprint JSON"
fi

LIVE_FP=$(jq -r '.fingerprint' "$LIVE_FP_FILE")
LIVE_GIT_SHA=$(jq -r '.git_sha' "$LIVE_FP_FILE")

if [ -z "$LIVE_FP" ] || [ "$LIVE_FP" = "null" ]; then
  fail 6 "live fingerprint is empty"
fi

if [ "$LIVE_GIT_SHA" != "$DEPLOY_SHA" ]; then
  fail 6 "fingerprint git_sha=$LIVE_GIT_SHA != DEPLOY_SHA=$DEPLOY_SHA (working tree drifted from release SHA)"
fi

# (b) Recompute against a clean checkout to verify determinism. If a
# committed reference fingerprint exists at deploy/fingerprint/expected.json
# (e.g. produced by an upstream signer), match against it as well.
RECHECK_FP_FILE="$ART_DIR/release_fingerprint_recheck.json"
ART_DIR="$ART_DIR" bash "$FP_SCRIPT" "$RECHECK_FP_FILE"
RECHECK_FP=$(jq -r '.fingerprint' "$RECHECK_FP_FILE")
if [ "$LIVE_FP" != "$RECHECK_FP" ]; then
  fail 6 "fingerprint not deterministic across two runs ($LIVE_FP != $RECHECK_FP)"
fi

EXPECTED_FILE="deploy/fingerprint/expected.json"
if [ -f "$EXPECTED_FILE" ]; then
  EXPECTED_FP=$(jq -r '.fingerprint' "$EXPECTED_FILE")
  if [ "$EXPECTED_FP" != "$LIVE_FP" ]; then
    fail 6 "release fingerprint mismatch: expected=$EXPECTED_FP live=$LIVE_FP"
  fi
  echo "fingerprint matches signed expected.json"
fi

# (c) Refuse to proceed if the legacy trust root has crept back in.
# Sigstore keyless replaced openssl/pkeyutl + in-repo public key. Any
# regression to the prior model is an automatic NO-GO.
NO_LEGACY="deploy/fingerprint/no_legacy_trust.sh"
require_script "$NO_LEGACY"
if ! bash "$NO_LEGACY"; then
  fail 6 "legacy trust references detected; gate refuses to sign"
fi

# (d) Sign the fingerprint via Sigstore keyless (cosign + GitHub OIDC).
# No private key is involved; the signing identity is the GitHub Actions
# workload that ran this script. Outputs travel with the artifact and
# are independently verifiable using only the Sigstore + GitHub OIDC
# public roots — there is no shared secret on any host.
SIGN_SCRIPT="deploy/fingerprint/sign_release_fingerprint.sh"
require_script "$SIGN_SCRIPT"
if ! command -v cosign >/dev/null 2>&1; then
  fail 6 "cosign not found on PATH (install via sigstore/cosign-installer@v3)"
fi
if ! FINGERPRINT_PATH="$LIVE_FP_FILE" bash "$SIGN_SCRIPT"; then
  fail 6 "cosign signing failed for $LIVE_FP_FILE"
fi

# (e) Independent post-sign verification — call the same runtime_verify.sh
# the production hosts use. This guarantees the artifact CI is about to
# publish is one the deploy fleet will accept; a divergence between
# signer and verifier semantics is caught here, not at boot on prod.
VERIFY_SCRIPT="deploy/fingerprint/runtime_verify.sh"
require_script "$VERIFY_SCRIPT"
: "${RELEASE_OIDC_REPO:?RELEASE_OIDC_REPO must be set in CI for identity binding (e.g. payflux/payflux)}"
if ! ROLE="ci-postsign" \
     RELEASE_OIDC_REPO="$RELEASE_OIDC_REPO" \
     RELEASE_OIDC_WORKFLOW_REF_REGEX="${RELEASE_OIDC_WORKFLOW_REF_REGEX:-^refs/heads/release/.*$}" \
     RELEASE_OIDC_WORKFLOW_NAME="${RELEASE_OIDC_WORKFLOW_NAME:-release-gate}" \
     RELEASE_OIDC_WORKFLOW_FILE="${RELEASE_OIDC_WORKFLOW_FILE:-release-gate.yml}" \
     RELEASE_OIDC_WORKFLOW_TRIGGER="${RELEASE_OIDC_WORKFLOW_TRIGGER:-push}" \
     FINGERPRINT_DEPLOYED_PATH="$LIVE_FP_FILE" \
     FINGERPRINT_KILL_ON_MISMATCH=1 \
     bash "$VERIFY_SCRIPT"; then
  fail 6 "post-sign runtime_verify.sh failed; deploy fleet would reject this artifact"
fi

echo "PASS [5/5] — release fingerprint $LIVE_FP (deterministic; matches DEPLOY_SHA; Sigstore-signed; identity verified)"

# ----------------------------------------------------------------------
echo
echo "================================================================"
echo "GATE OK ($(ts))"
echo "deploy_sha:       $DEPLOY_SHA"
echo "fingerprint:      $LIVE_FP"
echo "artifact_log:     $GATE_LOG"
echo "fingerprint_file: $LIVE_FP_FILE"
echo "signature_file:   ${LIVE_FP_FILE}.sig"
echo "certificate_file: ${LIVE_FP_FILE}.crt"
echo "bundle_file:      ${LIVE_FP_FILE}.bundle"
echo "rekor_ref_file:   ${LIVE_FP_FILE}.rekor"
echo "trust_root:       Sigstore Fulcio + GitHub OIDC (no in-repo key)"
echo "================================================================"
exit 0
