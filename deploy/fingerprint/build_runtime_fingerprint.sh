#!/usr/bin/env bash
# deploy/fingerprint/build_runtime_fingerprint.sh
#
# Third-tier fingerprint covering the INSTALLED PYTHON RUNTIME — what
# actually executes on the host, as opposed to the *.py source under
# version control. Closes the "edit installed package in site-packages"
# attack vector that the source fingerprint deliberately ignores.
#
# Hashed components:
#   interpreter    sha256 of the resolved python binary (defeats PATH
#                  manipulation; readlink -f resolves symlinks first)
#   site_packages  sha256 over a sorted manifest of every regular file
#                  under each site-packages directory, with per-file
#                  hashes:
#                    *.pyc — bytes 17.. (PEP 552 header stripped so
#                            same-version Pythons hash identically across
#                            hosts; the marshalled code object IS hashed)
#                    other — full raw bytes (no normalization; pip writes
#                            canonical bytes and any deviation is suspect)
#
# Composite = sha256 over "interpreter=<h>\nsite_packages=<h>\n".
#
# Determinism boundary:
#   - Same OS + same arch + same Python minor version + same locked
#     dependency set => bit-for-bit identical fingerprint.
#   - Different Python minor (e.g. 3.13 -> 3.13.1) changes the .pyc
#     marshal format and may flip the hash even if .py is identical;
#     this is correct behaviour — a Python upgrade is drift.
#
# Env:
#   RUNTIME_PYTHON_BIN          path or PATH-name of the python interpreter
#                               (default: python3)
#   INCLUDE_USER_SITE           0|1 — include site.getusersitepackages()
#                               (default: 0; user site is per-user noise)
#   STRICT                      0|1 — refuse to emit if any required
#                               component is unavailable (default: 0)
#   ART_DIR                     output dir (default: deploy/artifacts)
#
# Usage:
#   build_runtime_fingerprint.sh [output_path]
# Default output: $ART_DIR/runtime_fingerprint.json
#
# Exit codes:
#   0  fingerprint written
#   2  python interpreter or site-packages unreachable / empty
#   3  hashing tool unavailable / per-file hash failed
#   4  STRICT mode and a required component is unavailable

set -euo pipefail

ART_DIR="${ART_DIR:-deploy/artifacts}"
OUT_PATH="${1:-$ART_DIR/runtime_fingerprint.json}"
mkdir -p "$(dirname "$OUT_PATH")"

PYTHON_BIN_ENV="${RUNTIME_PYTHON_BIN:-python3}"
INCLUDE_USER_SITE="${INCLUDE_USER_SITE:-0}"
STRICT="${STRICT:-0}"

# --- sha256 helper (portable across linux + macos) ---
if command -v sha256sum >/dev/null 2>&1; then
  SHA() { sha256sum | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  SHA() { shasum -a 256 | awk '{print $1}'; }
else
  echo "FAIL: no sha256sum or shasum on PATH" >&2
  exit 3
fi

# --- Resolve interpreter to an absolute, symlink-followed path. PATH
# resolution happens first; if the user supplied a relative `python3`,
# command -v finds the active one. readlink -f then defeats any symlink
# (e.g. /usr/local/bin/python3 -> /usr/local/Cellar/python@3.13/...).
if ! command -v "$PYTHON_BIN_ENV" >/dev/null 2>&1; then
  echo "FAIL: python interpreter not found: $PYTHON_BIN_ENV" >&2
  [ "$STRICT" = "1" ] && exit 4
  exit 2
fi
PYTHON_BIN="$(command -v "$PYTHON_BIN_ENV")"
PYTHON_BIN_ABS="$(readlink -f "$PYTHON_BIN" 2>/dev/null || echo "$PYTHON_BIN")"

# --- Interrogate the resolved interpreter for its site-packages dirs ---
PY_VERSION="$("$PYTHON_BIN_ABS" -c 'import sys;print(sys.version.split()[0])' 2>/dev/null || echo "unknown")"
PY_PLATFORM="$("$PYTHON_BIN_ABS" -c 'import platform;print(platform.platform())' 2>/dev/null || echo "unknown")"

SITE_DIRS_RAW="$("$PYTHON_BIN_ABS" -c \
  'import site;[print(p) for p in site.getsitepackages()]' 2>/dev/null || true)"
if [ "$INCLUDE_USER_SITE" = "1" ]; then
  USER_SITE="$("$PYTHON_BIN_ABS" -c 'import site;print(site.getusersitepackages())' 2>/dev/null || true)"
  if [ -n "$USER_SITE" ]; then
    SITE_DIRS_RAW="$(printf '%s\n%s\n' "$SITE_DIRS_RAW" "$USER_SITE")"
  fi
fi
SITE_DIRS="$(printf '%s\n' "$SITE_DIRS_RAW" | grep -v '^$' | LC_ALL=C sort -u || true)"

if [ -z "$SITE_DIRS" ]; then
  echo "FAIL: no site-packages directories discovered for $PYTHON_BIN_ABS" >&2
  [ "$STRICT" = "1" ] && exit 4
  exit 2
fi

# --- Component 1: interpreter binary ---
INTERPRETER_PRESENT="true"
if [ ! -r "$PYTHON_BIN_ABS" ]; then
  INTERPRETER_HASH="unavailable"
  INTERPRETER_PRESENT="false"
  if [ "$STRICT" = "1" ]; then
    echo "FAIL: STRICT — interpreter not readable: $PYTHON_BIN_ABS" >&2
    exit 4
  fi
else
  INTERPRETER_HASH="$(SHA < "$PYTHON_BIN_ABS")"
fi

# --- Component 2: site-packages walk + per-file hash + manifest ---
TMP_MANIFEST="$(mktemp)"
trap 'rm -f "$TMP_MANIFEST"' EXIT

hash_one() {
  local path="$1"
  case "$path" in
    *.pyc)
      # Strip 16-byte PEP 552 header (magic + flags + ts/hash). Bytes
      # 17.. are the marshalled code object — that's what executes.
      tail -c +17 -- "$path" 2>/dev/null | SHA
      ;;
    *)
      SHA < "$path"
      ;;
  esac
}

FILE_COUNT=0
DIR_COUNT=0
while IFS= read -r site_dir; do
  [ -z "$site_dir" ] && continue
  [ ! -d "$site_dir" ] && continue
  DIR_COUNT=$((DIR_COUNT + 1))
  while IFS= read -r -d '' f; do
    if ! fhash="$(hash_one "$f")"; then
      echo "FAIL: hashing $f" >&2
      exit 3
    fi
    # NUL-delimited tuples — defeats path-separator ambiguity (git allows
    # \n in paths; site-packages should not but we don't trust it).
    printf '%s\0%s\0' "$f" "$fhash" >> "$TMP_MANIFEST"
    FILE_COUNT=$((FILE_COUNT + 1))
  done < <(
    find "$site_dir" -type f \
      ! -name '*.pyc.tmp' \
      ! -name '*.lock' \
      ! -name '.DS_Store' \
      ! -path '*/.cache/*' \
      -print0 \
    | LC_ALL=C sort -z
  )
done <<< "$SITE_DIRS"

SITE_PRESENT="true"
if [ "$FILE_COUNT" -eq 0 ]; then
  SITE_HASH="unavailable"
  SITE_PRESENT="false"
  if [ "$STRICT" = "1" ]; then
    echo "FAIL: STRICT — zero files matched across site-packages dirs ($SITE_DIRS)" >&2
    exit 4
  fi
  echo "FAIL: zero files matched in site-packages — refusing to emit empty fingerprint" >&2
  exit 2
else
  SITE_HASH="$(SHA < "$TMP_MANIFEST")"
fi

# --- Composite ---
COMPOSITE="$(
  {
    printf 'interpreter=%s\n'   "$INTERPRETER_HASH"
    printf 'site_packages=%s\n' "$SITE_HASH"
  } | SHA
)"

# --- JSON site_dirs array (handle 1+ entries; comma-joined) ---
SITE_DIRS_JSON="$(
  printf '%s\n' "$SITE_DIRS" \
    | awk 'BEGIN{first=1}
           { if (!first) printf ", "; first=0; printf "\"%s\"", $0 }'
)"

# --- Emit JSON (sorted keys, hand-written for stable order) ---
{
  printf '{\n'
  printf '  "components": {\n'
  printf '    "interpreter":   {"sha256": "%s", "present": %s, "path": "%s"},\n' \
         "$INTERPRETER_HASH" "$INTERPRETER_PRESENT" "$PYTHON_BIN_ABS"
  printf '    "site_packages": {"sha256": "%s", "present": %s, "file_count": %d}\n' \
         "$SITE_HASH" "$SITE_PRESENT" "$FILE_COUNT"
  printf '  },\n'
  printf '  "file_count":         %d,\n'    "$FILE_COUNT"
  printf '  "fingerprint":        "%s",\n'  "$COMPOSITE"
  printf '  "generated_at_utc":   "%s",\n'  "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '  "host":               "%s",\n'  "$(hostname)"
  printf '  "python_bin":         "%s",\n'  "$PYTHON_BIN_ABS"
  printf '  "python_platform":    "%s",\n'  "$PY_PLATFORM"
  printf '  "python_version":     "%s",\n'  "$PY_VERSION"
  printf '  "site_packages_dirs": [%s]\n'   "$SITE_DIRS_JSON"
  printf '}\n'
} > "$OUT_PATH"

# Companion manifest for audit reproduction.
MANIFEST_OUT="${OUT_PATH%.json}.manifest"
cp -f "$TMP_MANIFEST" "$MANIFEST_OUT"

echo "runtime_fingerprint: $COMPOSITE"
echo "  interpreter:    $INTERPRETER_HASH ($PYTHON_BIN_ABS)"
echo "  site_packages:  $SITE_HASH ($FILE_COUNT files across $DIR_COUNT dir(s))"
echo "  python:         $PY_VERSION on $PY_PLATFORM"
echo "output:           $OUT_PATH"
echo "manifest:         $MANIFEST_OUT"
exit 0
