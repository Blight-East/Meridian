#!/usr/bin/env bash
# deploy/fingerprint/runtime_verify_loop.sh
#
# Long-running supervisor that re-runs ALL THREE integrity verifiers
# every FINGERPRINT_VERIFY_INTERVAL_SECONDS (default: 300). Designed to
# be managed by pm2 or systemd as a sidecar process.
#
# Each cycle runs:
#   1. runtime_verify.sh                (Sigstore source-tree verification)
#   2. verify_config_fingerprint.sh     (env/db/redis/nginx drift)
#   3. verify_runtime_fingerprint.sh    (installed Python interpreter + site-packages drift)
#
# Any verifier failing flips its own metric to 1; the loop itself
# continues so the next cycle still updates the metric. Set
# FINGERPRINT_KILL_LOOP_ON_MISMATCH=1 to make pm2/systemd record a crash
# instead and page on it.
#
# Env:
#   ROLE                                  required (defaults to "fingerprint-watcher")
#   RELEASE_OIDC_REPO                     required (passed through to source verifier)
#   RELEASE_OIDC_WORKFLOW_REF_REGEX       optional (default ^refs/heads/release/.*$)
#   FINGERPRINT_VERIFY_INTERVAL_SECONDS   default 300
#   FINGERPRINT_KILL_LOOP_ON_MISMATCH     0|1 (default 0)
#   FINGERPRINT_CONFIG_REQUIRED_SOURCES   forwarded to config verifier
#   FINGERPRINT_RUNTIME_REQUIRED_COMPONENTS  forwarded to runtime verifier
#   plus everything any verifier accepts (DATABASE_URL, REDIS_URL,
#   ENV_FILE_PATH, RUNTIME_PYTHON_BIN, etc.)

set -u  # do NOT set -e: a non-zero verify must not stop the loop unless killed

INTERVAL="${FINGERPRINT_VERIFY_INTERVAL_SECONDS:-300}"
KILL_LOOP="${FINGERPRINT_KILL_LOOP_ON_MISMATCH:-0}"
ROLE="${ROLE:-fingerprint-watcher}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_VERIFIER="$SCRIPT_DIR/runtime_verify.sh"
CONFIG_VERIFIER="$SCRIPT_DIR/verify_config_fingerprint.sh"
RUNTIME_VERIFIER="$SCRIPT_DIR/verify_runtime_fingerprint.sh"

if [ ! -r "$SOURCE_VERIFIER" ]; then
  echo "FAIL: source verifier not found: $SOURCE_VERIFIER" >&2
  exit 1
fi
if [ ! -r "$CONFIG_VERIFIER" ]; then
  echo "FAIL: config verifier not found: $CONFIG_VERIFIER" >&2
  exit 1
fi
if [ ! -r "$RUNTIME_VERIFIER" ]; then
  echo "FAIL: runtime verifier not found: $RUNTIME_VERIFIER" >&2
  exit 1
fi

export ROLE

shutdown=0
trap 'shutdown=1' TERM INT

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

echo "$(ts) [INFO] [fingerprint-loop] starting role=$ROLE interval=${INTERVAL}s kill_on_mismatch=$KILL_LOOP"

while [ "$shutdown" = "0" ]; do
  # The loop is non-fatal by default — every verifier must run every
  # cycle so metrics stay fresh. Aggregate worst-case rc for KILL_LOOP.
  src_rc=0
  cfg_rc=0
  rt_rc=0

  bash "$SOURCE_VERIFIER"  || src_rc=$?
  bash "$CONFIG_VERIFIER"  || cfg_rc=$?
  bash "$RUNTIME_VERIFIER" || rt_rc=$?

  if [ "$KILL_LOOP" = "1" ] && \
     { [ "$src_rc" != "0" ] || [ "$cfg_rc" != "0" ] || [ "$rt_rc" != "0" ]; }; then
    echo "$(ts) [CRITICAL] [fingerprint-loop] verifier failure src_rc=$src_rc cfg_rc=$cfg_rc rt_rc=$rt_rc; KILL_LOOP=1 — exiting" >&2
    # Surface the most actionable code: source > runtime > config.
    # Source = signed-artifact tampering (worst); runtime = installed
    # binary edit; config = drifted env/flag (lower severity).
    if   [ "$src_rc" != "0" ]; then exit "$src_rc"
    elif [ "$rt_rc"  != "0" ]; then exit "$rt_rc"
    else exit "$cfg_rc"
    fi
  fi

  # Sleep in 5s slices so SIGTERM is responsive.
  remaining="$INTERVAL"
  while [ "$remaining" -gt 0 ] && [ "$shutdown" = "0" ]; do
    sleep 5
    remaining=$((remaining - 5))
  done
done

echo "$(ts) [INFO] [fingerprint-loop] stopped"
exit 0
