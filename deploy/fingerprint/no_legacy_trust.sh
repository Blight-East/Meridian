#!/usr/bin/env bash
# deploy/fingerprint/no_legacy_trust.sh
#
# Hard regression guard: refuses any deployment that still references the
# pre-Sigstore trust model (in-repo public key, openssl pkeyutl signing,
# or the RELEASE_SIGNING_PRIVATE_KEY_PEM secret).
#
# Run from the repo root. Exits 0 if clean, 7 if any legacy reference is
# found. Called from deploy/ci/gate.sh as a preflight check.

set -euo pipefail

# (1) The legacy public key file MUST NOT exist on disk.
if [ -e "deploy/fingerprint/release_signing_pubkey.pem" ]; then
  echo "FAIL: legacy trust root present: deploy/fingerprint/release_signing_pubkey.pem" >&2
  echo "      Trust root has moved to Sigstore + GitHub OIDC. Delete this file." >&2
  exit 7
fi

# (2) No file in the repo may reference the legacy artifacts. We scope to
#     deploy/, runtime/, .github/, ecosystem.config.js — the surfaces that
#     actually drive signing or verification. Documentation references in
#     deploy/fingerprint/TRUST_MODEL.md are explicitly allowed (the doc
#     explains the migration).
LEGACY_PATTERNS=(
  'release_signing_pubkey'
  'RELEASE_SIGNING_PRIVATE_KEY_PEM'
  'RELEASE_SIGNING_PRIVATE_KEY_PATH'
  'RELEASE_SIGNING_PUBLIC_KEY_PATH'
  'openssl[[:space:]]+pkeyutl'
)

SCAN_PATHS=(deploy runtime .github ecosystem.config.js)
ALLOW_FILES=(
  'deploy/fingerprint/TRUST_MODEL.md'
  'deploy/fingerprint/no_legacy_trust.sh'
)

allow_re=""
for f in "${ALLOW_FILES[@]}"; do
  if [ -z "$allow_re" ]; then allow_re="^$f$"; else allow_re="$allow_re|^$f$"; fi
done

found=0
for pat in "${LEGACY_PATTERNS[@]}"; do
  while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    file="${hit%%:*}"
    if echo "$file" | grep -Eq "$allow_re"; then
      continue
    fi
    echo "FAIL: legacy trust reference '$pat' at: $hit" >&2
    found=1
  done < <(grep -RInE "$pat" "${SCAN_PATHS[@]}" 2>/dev/null || true)
done

if [ "$found" != "0" ]; then
  echo "FAIL: legacy trust references found; remove them and re-run the gate." >&2
  exit 7
fi

echo "OK: no legacy trust references"
exit 0
