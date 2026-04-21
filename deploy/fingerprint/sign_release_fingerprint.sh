#!/usr/bin/env bash
# deploy/fingerprint/sign_release_fingerprint.sh
#
# Sigstore keyless signer for the release fingerprint.
#
# Trust model:
#   - No private key on disk, in env, or anywhere persistent.
#   - Cosign requests a short-lived (~10 min) X.509 cert from the
#     Sigstore Fulcio CA, bound to the GitHub Actions OIDC token of the
#     workload identity that ran this script.
#   - The cert + signature + Rekor transparency-log entry travel with
#     the release artifact. Anyone can verify them later using only the
#     Sigstore + GitHub OIDC public roots — no shared secret required.
#
# Required runtime context:
#   - Must run inside a GitHub Actions job with `permissions: id-token: write`.
#   - cosign v2.x must be on PATH.
#
# Inputs:
#   FINGERPRINT_PATH   release fingerprint JSON (default: deploy/artifacts/release_fingerprint.json)
#
# Outputs (alongside the fingerprint):
#   <FINGERPRINT_PATH>.sig       detached signature (base64)
#   <FINGERPRINT_PATH>.crt       Fulcio-issued X.509 cert (PEM)
#   <FINGERPRINT_PATH>.rekor     Rekor transparency-log entry reference (JSON)
#   <FINGERPRINT_PATH>.bundle    cosign offline bundle (sig + cert + Rekor proof)
#
# Exit codes:
#   0  signed and Rekor entry recorded
#   2  fingerprint missing
#   3  cosign unavailable
#   4  cosign sign failed
#   5  not running with OIDC identity (id-token: write missing)

set -euo pipefail

FP="${FINGERPRINT_PATH:-deploy/artifacts/release_fingerprint.json}"

if [ ! -s "$FP" ]; then
  echo "FAIL: fingerprint not found or empty: $FP" >&2
  exit 2
fi

if ! command -v cosign >/dev/null 2>&1; then
  echo "FAIL: cosign not found on PATH (install via sigstore/cosign-installer@v3)" >&2
  exit 3
fi

# Require an OIDC token issuer endpoint. GitHub Actions exposes it as
# ACTIONS_ID_TOKEN_REQUEST_URL when permissions: id-token: write is set.
# Outside that context, cosign will block on browser auth — refuse here.
if [ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || [ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
  echo "FAIL: GitHub Actions OIDC token endpoint not exposed." >&2
  echo "      The workflow must declare 'permissions: id-token: write'." >&2
  exit 5
fi

cosign --version | head -n1

# `--yes` skips the cosign confirmation prompt.
# Cosign auto-detects the Actions OIDC identity in this environment.
if ! cosign sign-blob \
      --yes \
      --output-signature   "$FP.sig" \
      --output-certificate "$FP.crt" \
      --bundle             "$FP.bundle" \
      "$FP" 2> /tmp/cosign.err; then
  echo "FAIL: cosign sign-blob failed:" >&2
  cat /tmp/cosign.err >&2 || true
  exit 4
fi

# Extract Rekor log index + UUID from cosign stderr (printed as
# "tlog entry created with index: <N>") and write a small Rekor reference
# file. This is what the user spec calls .rekor — `cosign triangulate` is
# for OCI refs and does not apply to blobs, so we synthesise the
# equivalent reference from the run-time log.
REKOR_INDEX="$(grep -oE 'index:[[:space:]]*[0-9]+' /tmp/cosign.err | awk -F: '{print $2}' | tr -d ' ' | head -n1 || true)"
REKOR_URL_BASE="${REKOR_URL:-https://rekor.sigstore.dev}"
{
  printf '{\n'
  printf '  "rekor_url":  "%s",\n' "$REKOR_URL_BASE"
  printf '  "log_index":  %s,\n'   "${REKOR_INDEX:-null}"
  printf '  "entry_url":  "%s/api/v1/log/entries?logIndex=%s",\n' "$REKOR_URL_BASE" "${REKOR_INDEX:-}"
  printf '  "bundle_path":"%s",\n' "$FP.bundle"
  printf '  "signed_at_utc":"%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '}\n'
} > "$FP.rekor"

echo "OK: cosign signing complete"
echo "  signature:   $FP.sig"
echo "  certificate: $FP.crt"
echo "  bundle:      $FP.bundle"
echo "  rekor:       $FP.rekor (log_index=${REKOR_INDEX:-unknown})"
exit 0
