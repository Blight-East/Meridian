PAYFLUX RELEASE TRUST MODEL — SIGSTORE KEYLESS
==============================================

This document is the source of truth for how a deployed PayFlux release
is cryptographically bound to the GitHub Actions run that produced it.
It supersedes the prior `release_signing_pubkey.pem` model entirely.

------------------------------------------------------------------------
Why we moved off in-repo public keys
------------------------------------------------------------------------

Pinning the public key inside the repo it is supposed to protect was a
trust-root-on-the-protected-system pattern: an attacker with repo-write
could replace the key AND re-sign with their key, and the verifier
would accept it. The repo cannot be both subject and authority.

The Sigstore keyless model moves the trust root OUT of the repo:

  * Identity     — GitHub OIDC issuer (token.actions.githubusercontent.com)
  * Authority    — Sigstore Fulcio CA
  * Audit log    — Sigstore Rekor transparency log

Forging a PayFlux release signature now requires compromising all three
external systems simultaneously — an attack surface roughly equivalent
to compromising Let's Encrypt.

------------------------------------------------------------------------
What is signed
------------------------------------------------------------------------

The signed object is `release_fingerprint.json` — the deterministic
content-addressed digest of every tracked source file, produced by
`build_release_fingerprint.sh`. Signing happens once per release, in
the GitHub Actions release-gate workflow, after every other gate check
has passed.

------------------------------------------------------------------------
Signing flow (CI only)
------------------------------------------------------------------------

  1. Workflow declares: permissions: id-token: write
  2. cosign-installer@v3 puts cosign on PATH.
  3. sign_release_fingerprint.sh calls:
       cosign sign-blob \
         --yes \
         --output-signature   release_fingerprint.json.sig \
         --output-certificate release_fingerprint.json.crt \
         --bundle             release_fingerprint.json.bundle \
         release_fingerprint.json
  4. Cosign requests a short-lived (~10 min) cert from Fulcio, bound to
     the Actions OIDC token's identity:
       https://github.com/<owner>/<repo>/.github/workflows/<wf>@<ref>
  5. The signature + cert are submitted to Rekor; the inclusion proof
     is embedded in the bundle.
  6. The `.sig`, `.crt`, `.bundle`, and `.rekor` (log-index reference)
     files are uploaded as deployable artifacts.

There is no private key. There is nothing to rotate, nothing to leak,
nothing to store in a secret manager.

------------------------------------------------------------------------
Verification flow (deploy host, every boot, every 5 min)
------------------------------------------------------------------------

  cosign verify-blob \
    --bundle                                 release_fingerprint.json.bundle \
    --certificate-identity-regexp            "https://github\.com/<owner>/<repo>/\.github/workflows/.+@^refs/heads/release/.*$" \
    --certificate-oidc-issuer                "https://token.actions.githubusercontent.com" \
    --certificate-github-workflow-repository "<owner>/<repo>" \
    --certificate-github-workflow-ref-regexp "^refs/heads/release/.*$" \
    release_fingerprint.json

This passes ONLY if:
  - The signature validates against the Fulcio-issued cert.
  - The cert chains to the Sigstore root.
  - The cert SAN matches the expected GitHub Actions workflow identity
    in the configured repo, AND was issued for a release/* branch ref.
  - The Rekor inclusion proof in the bundle is valid (or, when online,
    the entry exists in the live Rekor log).

All four bindings are checked. Identity mismatch alone is a hard fail
(exit 10), even if the signature itself is cryptographically valid.

------------------------------------------------------------------------
Configuration on the deploy host
------------------------------------------------------------------------

Required env on every PayFlux process that calls `verify_or_die()`:

  RELEASE_OIDC_REPO                e.g. "payflux/payflux"
  RELEASE_OIDC_WORKFLOW_REF_REGEX  default "^refs/heads/release/.*$"

Both are deploy-time settings, not secrets. They live in the system env
(or pm2 ecosystem.config.js) and are read by:
  - runtime/safety/fingerprint_check.py  (boot check)
  - deploy/fingerprint/runtime_verify.sh (one-shot + watcher loop)

Required tooling on every host:
  - cosign v2.x
  - jq
  - bash + coreutils
  - network egress to https://rekor.sigstore.dev (for online verify)
    OR set COSIGN_OFFLINE=1 and rely on the bundled inclusion proof.

------------------------------------------------------------------------
What this trust model deliberately does NOT cover
------------------------------------------------------------------------

The release fingerprint covers tracked SOURCE files only. Runtime
behaviour also depends on:

  - .env on the host
  - Postgres rows in system_flags / system_mode / recovery_window
  - Redis keys under agent_flux:*
  - Reverse-proxy config (nginx/Caddy)

Drift in any of those is invisible to release_fingerprint.json. The
config fingerprint system (see build_config_fingerprint.sh and
verify_config_fingerprint.sh in this directory) is the second layer
that closes that gap. The runtime fingerprint system
(build_runtime_fingerprint.sh and verify_runtime_fingerprint.sh) is
the third layer — it covers the installed Python interpreter and
every file under site-packages, closing the "edit installed package"
attack vector that the source-tree fingerprint deliberately ignores.

------------------------------------------------------------------------
Enforcement modes and rollout phases
------------------------------------------------------------------------

The Python boot helper `runtime/safety/fingerprint_check.py` exposes
ONE canonical knob, `FINGERPRINT_ENFORCEMENT_MODE`, with three values:

  enforce  — verifiers run; fatal codes raise SystemExit and crash boot.
             pm2 records the crash; the inflight-queue alert fires if
             the worker stops draining.
  report   — verifiers run; fatal codes are logged at CRITICAL and
             metrics still update, but the process keeps running. This
             is the SHIPPING DEFAULT (locked in by
             runtime/safety/test_fingerprint_check.py).
  off      — verifiers skipped entirely. Use for unit tests / dev
             boxes only; never in any deployed environment.

Legacy aliases still work for backwards compatibility:
  FINGERPRINT_KILL_ON_MISMATCH=1     → enforce
  FINGERPRINT_BOOT_CHECK_DISABLED=1  → off
The canonical knob always wins if both are set.

Rollout phases (DO NOT skip):

  Phase 1 — code lands.
    Default mode is `report`. Every PayFlux process logs CRITICAL on
    integrity issues but does not crash. Operators have time to capture
    baselines and provision env without an outage.

  Phase 2 — provisioning.
    On every host, run:
      STRICT=1 ART_DIR=/opt/payflux/deploy/artifacts \
        bash deploy/fingerprint/build_config_fingerprint.sh \
          /opt/payflux/deploy/artifacts/expected_config_fingerprint.json
      STRICT=1 ART_DIR=/opt/payflux/deploy/artifacts \
        bash deploy/fingerprint/build_runtime_fingerprint.sh \
          /opt/payflux/deploy/artifacts/expected_runtime_fingerprint.json
    Set on every PayFlux pm2 entry:
      RELEASE_OIDC_REPO=payflux/payflux

  Phase 3 — bake.
    Leave mode=`report` for at least 7 days. Watch:
      fingerprint_mismatch{source="composite"} 0
      config_fingerprint_mismatch{source="composite"} 0
      runtime_fingerprint_mismatch{source="composite"} 0
    Any non-zero reading is a real signal that needs investigation
    BEFORE the cutover, not after.

  Phase 4 — cutover.
    Set FINGERPRINT_ENFORCEMENT_MODE=enforce per process. Roll one
    process at a time (api → worker → scheduler). Confirm pm2 shows
    a clean restart after each before moving to the next.

A typoed mode value (e.g. `enfoce`) falls back to `report`, NOT `off`.
That's a deliberate fail-loud choice — silently disabling the verifier
because of a typo would be worse than running in report mode.

------------------------------------------------------------------------
Hard rules
------------------------------------------------------------------------

  - No public key file in the repo. The regression guard
    deploy/fingerprint/no_legacy_trust.sh fails the gate if one appears.
  - No openssl pkeyutl in any signing or verification path.
  - No RELEASE_SIGNING_PRIVATE_KEY_* secret. Neither CI nor any host
    holds a private key.
  - No verifier may be invoked without an explicit RELEASE_OIDC_REPO
    binding. A wildcard identity is treated as a configuration error
    (exit 14), not a permissive fallback.
