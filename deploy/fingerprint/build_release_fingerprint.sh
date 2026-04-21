#!/usr/bin/env bash
# deploy/fingerprint/build_release_fingerprint.sh
#
# Deterministic content-addressed release fingerprint generator.
#
# Hashes every git-tracked source file in the repository in a way that is
# bit-for-bit reproducible across machines, OSes, and clones. The resulting
# fingerprint is the cryptographic identity of the release: if the deployed
# bundle does not match this fingerprint, it is not what was validated.
#
# Determinism rules (all enforced):
#   - File set is sourced from `git ls-files` (tracked only — no build
#     artifacts, no untracked files, no .git internals).
#   - Excludes are explicit and applied AFTER ls-files (see EXCLUDE_RE).
#   - Files are sorted lexicographically (LC_ALL=C) before hashing.
#   - Per-file content is hashed as raw bytes (no transcoding).
#   - The composite hash is sha256( for each path: path + "\n" + sha256(content) + "\n" ).
#     Embedding the per-file sha256 (not raw content) keeps the streaming
#     hash O(1) memory and makes audit reproduction trivial.
#   - Whitespace-only line-ending differences are normalized: CRLF -> LF
#     and trailing whitespace stripped per line, BEFORE per-file hashing.
#     Binary files (detected via NUL byte) bypass normalization.
#
# Usage:
#   build_release_fingerprint.sh [output_path]
#
# Output (default: $ART_DIR/release_fingerprint.json):
#   {
#     "fingerprint":      "<sha256>",
#     "git_sha":          "<commit>",
#     "file_count":       <int>,
#     "generated_at_utc": "<ISO 8601 Z>"
#   }
#
# Exit codes:
#   0  fingerprint written
#   1  not in a git repo / git not available
#   2  no tracked files matched after exclusion (suspicious)
#   3  internal hashing error

set -euo pipefail

ART_DIR="${ART_DIR:-deploy/artifacts}"
OUT_PATH="${1:-$ART_DIR/release_fingerprint.json}"

mkdir -p "$(dirname "$OUT_PATH")"

# --- Sanity: in a git repo ---
if ! command -v git >/dev/null 2>&1; then
  echo "FAIL: git not found in PATH" >&2
  exit 1
fi
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "FAIL: not inside a git repository" >&2
  exit 1
fi

GIT_SHA="$(git rev-parse HEAD)"

# --- Tooling: pick a sha256 implementation ---
if command -v sha256sum >/dev/null 2>&1; then
  SHA_CMD() { sha256sum | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  SHA_CMD() { shasum -a 256 | awk '{print $1}'; }
else
  echo "FAIL: neither sha256sum nor shasum available" >&2
  exit 3
fi

# --- Exclusion regex ---
# Applied with `grep -Ev` against each tracked path. Anchored to start of
# string. Keep this list explicit and minimal; everything else is in.
EXCLUDE_RE='^(\.git/|deploy/artifacts/|.*/__pycache__/|.*\.pyc$|.*\.pyo$|.*\.log$|.*\.tmp$|build/|dist/|node_modules/|\.venv/|venv/|\.mypy_cache/|\.pytest_cache/|\.ruff_cache/|\.DS_Store$|.*\.egg-info/)'

# --- File list: tracked, sorted, excluded ---
# LC_ALL=C ensures byte-wise lexicographic ordering, identical on every host.
mapfile -t FILES < <(
  git ls-files -z \
  | tr '\0' '\n' \
  | LC_ALL=C sort \
  | grep -Ev "$EXCLUDE_RE" \
  || true
)

FILE_COUNT="${#FILES[@]}"
if [ "$FILE_COUNT" -eq 0 ]; then
  echo "FAIL: zero files after exclusion — refusing to emit empty fingerprint" >&2
  exit 2
fi

# --- Per-file normalization ---
# - Detect binary via NUL in first 8KB. Binary files: hash raw bytes.
# - Text files: CRLF -> LF, strip trailing whitespace per line.
#   Strips whitespace-only diffs while preserving meaningful changes.
hash_one_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    # Tracked but missing on disk (e.g., submodule pointer). Hash empty content.
    printf '' | SHA_CMD
    return
  fi
  # Binary check: any NUL in first 8192 bytes => treat as binary.
  if head -c 8192 -- "$path" 2>/dev/null | LC_ALL=C tr -d -c '\0' | head -c 1 | read -r _; then
    # NUL found
    SHA_CMD < "$path"
  else
    # Text: normalize line endings + strip trailing whitespace.
    # awk handles CRLF and trailing whitespace in one pass.
    awk '{ sub(/\r$/, ""); sub(/[ \t]+$/, ""); print }' "$path" | SHA_CMD
  fi
}

# --- Composite streaming hash ---
# We feed "path\n<file_sha256>\n" for every file, in sorted order, into a
# single sha256. This is order-sensitive and content-sensitive but
# constant-memory regardless of repo size.
TMP_MANIFEST="$(mktemp)"
trap 'rm -f "$TMP_MANIFEST"' EXIT

: > "$TMP_MANIFEST"
for path in "${FILES[@]}"; do
  fhash="$(hash_one_file "$path")" || { echo "FAIL: hashing $path" >&2; exit 3; }
  printf '%s\n%s\n' "$path" "$fhash" >> "$TMP_MANIFEST"
done

FINGERPRINT="$(SHA_CMD < "$TMP_MANIFEST")"

# --- Emit JSON (sorted keys for determinism) ---
GENERATED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Hand-write JSON to avoid jq-version variance in key ordering.
{
  printf '{\n'
  printf '  "file_count": %s,\n'      "$FILE_COUNT"
  printf '  "fingerprint": "%s",\n'    "$FINGERPRINT"
  printf '  "generated_at_utc": "%s",\n' "$GENERATED_AT_UTC"
  printf '  "git_sha": "%s"\n'         "$GIT_SHA"
  printf '}\n'
} > "$OUT_PATH"

# Also emit the manifest alongside the fingerprint for audit reproduction.
MANIFEST_OUT="${OUT_PATH%.json}.manifest"
cp -f "$TMP_MANIFEST" "$MANIFEST_OUT"

echo "fingerprint:    $FINGERPRINT"
echo "git_sha:        $GIT_SHA"
echo "file_count:     $FILE_COUNT"
echo "output:         $OUT_PATH"
echo "manifest:       $MANIFEST_OUT"
echo "generated_at:   $GENERATED_AT_UTC"
exit 0
