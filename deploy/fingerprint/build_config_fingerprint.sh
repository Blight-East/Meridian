#!/usr/bin/env bash
# deploy/fingerprint/build_config_fingerprint.sh
#
# Second-tier fingerprint covering RUNTIME CONFIGURATION — the layer the
# source fingerprint deliberately ignores. Hashes:
#
#   env       sha256 of declared .env file contents (key=value lines, sorted)
#   db_flags  sha256 of `SELECT key,value FROM system_flags ORDER BY key`
#             plus `SELECT mode,entered_at FROM system_mode`
#             plus `SELECT * FROM recovery_window`
#   redis_flags sha256 of every key matching agent_flux:* (sorted) and its value
#   nginx     sha256 of `nginx -T` rendered output (full effective config)
#
# A composite sha256 over the four per-source hashes (in fixed order) is
# the headline `fingerprint`. Any drift in any source flips the composite
# AND the per-source flag, so the verifier can pinpoint which layer moved.
#
# Output schema (deterministic, sorted keys):
#   {
#     "fingerprint":      "<sha256>",
#     "generated_at_utc": "<ISO 8601 Z>",
#     "host":             "<hostname>",
#     "sources": {
#       "env":         {"sha256":"...", "present":true,  "detail":"<path>"},
#       "db_flags":    {"sha256":"...", "present":true,  "detail":"system_flags+system_mode+recovery_window"},
#       "redis_flags": {"sha256":"...", "present":true,  "detail":"agent_flux:*"},
#       "nginx":       {"sha256":"...", "present":true,  "detail":"nginx -T"}
#     }
#   }
#
# A source that is unreachable (no DATABASE_URL, no nginx on PATH, etc.)
# is recorded with present:false and sha256:"unavailable". The verifier
# treats present:false as a configuration warning, NOT a pass — drift
# detection requires every declared source to be reachable.
#
# Env (all optional; sensible defaults):
#   ENV_FILE_PATH                 default: .env
#   DATABASE_URL                  if unset, db_flags is marked unavailable
#   REDIS_URL                     default: redis://127.0.0.1:6379
#   REDIS_FLAG_PREFIX             default: agent_flux:
#   NGINX_CONF_DUMP_CMD           default: "nginx -T"
#
# Usage:
#   build_config_fingerprint.sh [output_path]
# Default output: $ART_DIR/config_fingerprint.json (ART_DIR default deploy/artifacts)

set -euo pipefail

ART_DIR="${ART_DIR:-deploy/artifacts}"
OUT_PATH="${1:-$ART_DIR/config_fingerprint.json}"
mkdir -p "$(dirname "$OUT_PATH")"

ENV_FILE_PATH="${ENV_FILE_PATH:-.env}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
REDIS_FLAG_PREFIX="${REDIS_FLAG_PREFIX:-agent_flux:}"

# STRICT mode: refuse to emit a fingerprint with any required source
# unavailable. This is what the deploy-time baseline-capture step uses
# so a degraded host (Postgres down, redis-cli missing, nginx absent)
# cannot produce a "partial" baseline that would later be compared
# against a healthy host and silently mask config drift behind a
# composite-mismatch error.
STRICT="${STRICT:-0}"
BUILD_REQUIRED_SOURCES="${BUILD_REQUIRED_SOURCES:-env,db_flags,redis_flags,nginx}"

# --- sha256 helper (portable across linux + macos) ---
if command -v sha256sum >/dev/null 2>&1; then
  SHA() { sha256sum | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  SHA() { shasum -a 256 | awk '{print $1}'; }
else
  echo "FAIL: no sha256sum or shasum on PATH" >&2
  exit 3
fi

# --- per-source hashers ---

hash_env() {
  if [ ! -r "$ENV_FILE_PATH" ]; then
    printf 'unavailable\nfalse\nenv_file_missing:%s' "$ENV_FILE_PATH"
    return
  fi
  # Strip comments + blank lines, sort by key, hash. Whitespace inside a
  # value is preserved; only key-line ordering and comment noise is
  # normalized.
  local hash
  hash="$(
    awk '
      /^[[:space:]]*$/ {next}
      /^[[:space:]]*#/ {next}
      {print}
    ' "$ENV_FILE_PATH" \
    | LC_ALL=C sort \
    | SHA
  )"
  printf '%s\ntrue\n%s' "$hash" "$ENV_FILE_PATH"
}

hash_db_flags() {
  if [ -z "${DATABASE_URL:-}" ] || ! command -v psql >/dev/null 2>&1; then
    printf 'unavailable\nfalse\ndatabase_url_or_psql_missing'
    return
  fi
  # Three relations, in fixed order. Tab-separated, ORDER BY for
  # determinism. Errors (table missing) collapse to a fixed sentinel
  # rather than a randomly-formatted error so a missing table doesn't
  # masquerade as drift.
  local body
  body="$(
    {
      echo "## system_flags"
      psql "$DATABASE_URL" -At -F $'\t' -v ON_ERROR_STOP=0 -c \
        "SELECT key, value FROM system_flags ORDER BY key" 2>/dev/null \
        || echo "__UNAVAILABLE__"
      echo "## system_mode"
      psql "$DATABASE_URL" -At -F $'\t' -v ON_ERROR_STOP=0 -c \
        "SELECT mode, entered_at FROM system_mode ORDER BY mode" 2>/dev/null \
        || echo "__UNAVAILABLE__"
      echo "## recovery_window"
      psql "$DATABASE_URL" -At -F $'\t' -v ON_ERROR_STOP=0 -c \
        "SELECT corruption_start, corruption_end FROM recovery_window ORDER BY corruption_start" 2>/dev/null \
        || echo "__UNAVAILABLE__"
    }
  )"
  local hash
  hash="$(printf '%s' "$body" | SHA)"
  printf '%s\ntrue\nsystem_flags+system_mode+recovery_window' "$hash"
}

hash_redis_flags() {
  if ! command -v redis-cli >/dev/null 2>&1; then
    printf 'unavailable\nfalse\nredis_cli_missing'
    return
  fi
  # Probe connectivity; redis-cli returns non-zero on auth/conn failure.
  if ! redis-cli -u "$REDIS_URL" PING >/dev/null 2>&1; then
    printf 'unavailable\nfalse\nredis_unreachable:%s' "$REDIS_URL"
    return
  fi
  # SCAN over the prefix, sort, then MGET each key in order.
  local keys body hash
  keys="$(redis-cli -u "$REDIS_URL" --scan --pattern "${REDIS_FLAG_PREFIX}*" \
            2>/dev/null | LC_ALL=C sort)"
  if [ -z "$keys" ]; then
    # Empty set has a stable hash — distinct from "unavailable".
    body=""
  else
    body="$(
      while IFS= read -r k; do
        v="$(redis-cli -u "$REDIS_URL" GET "$k" 2>/dev/null)"
        # `redis-cli GET` on a non-string type returns a `WRONGTYPE`
        # error to stderr and empty stdout; that's fine — we'd want
        # such a flag to flip the hash anyway when its type changes.
        printf '%s\t%s\n' "$k" "$v"
      done <<< "$keys"
    )"
  fi
  hash="$(printf '%s' "$body" | SHA)"
  printf '%s\ntrue\n%s*' "$hash" "$REDIS_FLAG_PREFIX"
}

hash_nginx() {
  local cmd="${NGINX_CONF_DUMP_CMD:-nginx -T}"
  local first; first="${cmd%% *}"
  if ! command -v "$first" >/dev/null 2>&1; then
    printf 'unavailable\nfalse\n%s_not_on_path' "$first"
    return
  fi
  # `nginx -T` dumps the FULL effective config (every include resolved).
  # Suppress stderr — boot-time syntax warnings don't affect the rendered
  # config and shouldn't flip the hash.
  local body hash
  body="$($cmd 2>/dev/null || true)"
  if [ -z "$body" ]; then
    printf 'unavailable\nfalse\nnginx_dump_empty'
    return
  fi
  hash="$(printf '%s' "$body" | SHA)"
  printf '%s\ntrue\nnginx -T' "$hash"
}

# --- run all four ---
read_triple() {
  local var_prefix="$1" cmd="$2"
  local out; out="$(eval "$cmd")"
  # out is three lines: hash, present, detail
  local h p d
  h="$(printf '%s' "$out" | sed -n '1p')"
  p="$(printf '%s' "$out" | sed -n '2p')"
  d="$(printf '%s' "$out" | sed -n '3p')"
  eval "${var_prefix}_HASH=\"\$h\""
  eval "${var_prefix}_PRESENT=\"\$p\""
  eval "${var_prefix}_DETAIL=\"\$d\""
}

read_triple ENV         hash_env
read_triple DBFLAGS     hash_db_flags
read_triple REDISFLAGS  hash_redis_flags
read_triple NGINX       hash_nginx

# --- STRICT mode enforcement ---
# Run BEFORE composite hashing / file emission so a partial baseline
# never lands on disk to be picked up by a later verifier run.
if [ "$STRICT" = "1" ]; then
  missing=""
  for pair in "ENV:env" "DBFLAGS:db_flags" "REDISFLAGS:redis_flags" "NGINX:nginx"; do
    var_prefix="${pair%%:*}"
    src_name="${pair##*:}"
    case ",$BUILD_REQUIRED_SOURCES," in
      *",$src_name,"*)
        eval "present=\"\$${var_prefix}_PRESENT\""
        if [ "$present" != "true" ]; then
          eval "detail=\"\$${var_prefix}_DETAIL\""
          missing="${missing}  - $src_name (detail: $detail)\n"
        fi
        ;;
    esac
  done
  if [ -n "$missing" ]; then
    echo "FAIL: STRICT=1 — required config sources unavailable on this host:" >&2
    printf '%b' "$missing" >&2
    echo "      Refusing to emit a partial baseline at $OUT_PATH." >&2
    rm -f "$OUT_PATH"
    exit 4
  fi
fi

# --- composite hash ---
# Fixed source order: env, db_flags, redis_flags, nginx. The composite
# is the sha256 of "<source>=<hash>\n" lines in that order.
COMPOSITE="$(
  {
    printf 'env=%s\n'         "$ENV_HASH"
    printf 'db_flags=%s\n'    "$DBFLAGS_HASH"
    printf 'redis_flags=%s\n' "$REDISFLAGS_HASH"
    printf 'nginx=%s\n'       "$NGINX_HASH"
  } | SHA
)"

# --- emit JSON (hand-written for stable key order) ---
{
  printf '{\n'
  printf '  "fingerprint":      "%s",\n' "$COMPOSITE"
  printf '  "generated_at_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '  "host":             "%s",\n' "$(hostname)"
  printf '  "sources": {\n'
  printf '    "db_flags":    {"sha256": "%s", "present": %s, "detail": "%s"},\n' "$DBFLAGS_HASH"     "$DBFLAGS_PRESENT"     "$DBFLAGS_DETAIL"
  printf '    "env":         {"sha256": "%s", "present": %s, "detail": "%s"},\n' "$ENV_HASH"         "$ENV_PRESENT"         "$ENV_DETAIL"
  printf '    "nginx":       {"sha256": "%s", "present": %s, "detail": "%s"},\n' "$NGINX_HASH"       "$NGINX_PRESENT"       "$NGINX_DETAIL"
  printf '    "redis_flags": {"sha256": "%s", "present": %s, "detail": "%s"}\n'  "$REDISFLAGS_HASH"  "$REDISFLAGS_PRESENT"  "$REDISFLAGS_DETAIL"
  printf '  }\n'
  printf '}\n'
} > "$OUT_PATH"

echo "config_fingerprint: $COMPOSITE"
echo "  env:         $ENV_HASH (present=$ENV_PRESENT)"
echo "  db_flags:    $DBFLAGS_HASH (present=$DBFLAGS_PRESENT)"
echo "  redis_flags: $REDISFLAGS_HASH (present=$REDISFLAGS_PRESENT)"
echo "  nginx:       $NGINX_HASH (present=$NGINX_PRESENT)"
echo "output:       $OUT_PATH"
exit 0
