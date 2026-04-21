#!/usr/bin/env bash
#
# meridian-stealth-fix.sh
# Applies the stealth + Haiku fixes to the Meridian / Agent Flux stack.
#
# Idempotent: safe to re-run. Creates timestamped backups before editing.
#
# Usage:
#   bash meridian-stealth-fix.sh
#   REPO="$HOME/Agent Flux" bash meridian-stealth-fix.sh     # override path
#   PYTHON_BIN=/path/to/venv/bin/python bash meridian-stealth-fix.sh   # venv
#   NO_RESTART=1 bash meridian-stealth-fix.sh                # skip pm2 restart
#
set -euo pipefail

# ---------- config ----------
REPO="${REPO:-$HOME/Agent Flux}"
PYTHON_BIN="${PYTHON_BIN:-}"
NO_RESTART="${NO_RESTART:-0}"
TS="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="$REPO/.backups/stealth-fix-$TS"

BROWSER_TOOL="$REPO/tools/browser_tool.py"
SALES_ASSISTANT="$REPO/deploy/sales_assistant.py"

# ---------- helpers ----------
log()  { printf "\033[1;36m[meridian-fix]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[meridian-fix][warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[meridian-fix][error]\033[0m %s\n" "$*" >&2; exit 1; }

# ---------- preflight ----------
[ -d "$REPO" ]            || die "Repo not found at: $REPO  (override with REPO=...)"
[ -f "$BROWSER_TOOL" ]    || die "Missing file: $BROWSER_TOOL"
[ -f "$SALES_ASSISTANT" ] || die "Missing file: $SALES_ASSISTANT"

if [ -z "$PYTHON_BIN" ]; then
  if   command -v python3 >/dev/null 2>&1; then PYTHON_BIN="$(command -v python3)"
  elif command -v python  >/dev/null 2>&1; then PYTHON_BIN="$(command -v python)"
  else die "python3/python not found. Set PYTHON_BIN=/path/to/python."
  fi
fi
log "Using python: $PYTHON_BIN"

mkdir -p "$BACKUP_DIR"
cp "$BROWSER_TOOL"    "$BACKUP_DIR/browser_tool.py.bak"
cp "$SALES_ASSISTANT" "$BACKUP_DIR/sales_assistant.py.bak"
log "Backups: $BACKUP_DIR"

# ---------- 1. stop pm2 (best-effort) ----------
if [ "$NO_RESTART" != "1" ] && command -v pm2 >/dev/null 2>&1; then
  log "Stopping PM2 services (best-effort)..."
  pm2 stop agent-flux-api agent-flux-worker agent-flux-scheduler \
           agentflux-telegram meridian-mcp 2>/dev/null || true
fi

# ---------- 2. install playwright-stealth ----------
# Pin <2 so the v1 `stealth_sync(page)` API keeps working.
if "$PYTHON_BIN" -c "import playwright_stealth" >/dev/null 2>&1; then
  log "playwright-stealth already importable — skipping install"
else
  log "Installing playwright-stealth<2 ..."
  "$PYTHON_BIN" -m pip install --upgrade 'playwright-stealth<2' \
    || die "pip install playwright-stealth failed. Activate the Meridian venv and re-run, or set PYTHON_BIN."
fi

# Make sure the chromium binary is actually on disk (harmless if already present).
if "$PYTHON_BIN" -c "import playwright" >/dev/null 2>&1; then
  log "Ensuring playwright chromium is installed..."
  "$PYTHON_BIN" -m playwright install chromium >/dev/null 2>&1 \
    || warn "playwright install chromium failed (non-fatal — may need deps: playwright install-deps)"
fi

# ---------- 3. patch tools/browser_tool.py ----------
log "Patching $BROWSER_TOOL (stealth + networkidle)..."
export BROWSER_TOOL
"$PYTHON_BIN" <<'PYEOF'
import os, sys

path = os.environ["BROWSER_TOOL"]
with open(path, "r", encoding="utf-8") as f:
    src = f.read()
orig = src
changed = []

# 3a. Import playwright_stealth alongside playwright itself.
if "_stealth_sync" not in src:
    src = src.replace(
        "        from playwright.sync_api import sync_playwright",
        "        from playwright.sync_api import sync_playwright\n"
        "        try:\n"
        "            from playwright_stealth import stealth_sync as _stealth_sync\n"
        "        except Exception:\n"
        "            _stealth_sync = None",
        1,
    )
    changed.append("added playwright-stealth import")

# 3b. Default wait_for -> networkidle.
old_sig = 'def browser_fetch(url: str, wait_for: str = "domcontentloaded", timeout: int = 30000):'
new_sig = 'def browser_fetch(url: str, wait_for: str = "networkidle", timeout: int = 30000):'
if old_sig in src:
    src = src.replace(old_sig, new_sig, 1)
    changed.append('default wait_for -> "networkidle"')

# 3c. Apply stealth to the page right after creation.
if "_stealth_sync(page)" not in src:
    src = src.replace(
        "            page = context.new_page()",
        "            page = context.new_page()\n"
        "            if _stealth_sync is not None:\n"
        "                try:\n"
        "                    _stealth_sync(page)\n"
        "                except Exception as _stealth_err:\n"
        '                    logger.warning(f"playwright-stealth failed to apply: {_stealth_err}")',
        1,
    )
    changed.append("stealth_sync applied to page")

# 3d. Add an explicit networkidle wait after goto (tolerant if it times out).
if 'wait_for_load_state("networkidle"' not in src:
    src = src.replace(
        "            page.goto(url, wait_until=wait_for, timeout=timeout)\n"
        "            page.wait_for_timeout(1500)",
        "            page.goto(url, wait_until=wait_for, timeout=timeout)\n"
        "            try:\n"
        '                page.wait_for_load_state("networkidle", timeout=timeout)\n'
        "            except Exception:\n"
        "                pass\n"
        "            page.wait_for_timeout(1500)",
        1,
    )
    changed.append("explicit networkidle wait after goto")

if src == orig:
    print("[meridian-fix] browser_tool.py already patched (no changes).")
else:
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    print("[meridian-fix] browser_tool.py patched:")
    for c in changed:
        print("   -", c)
PYEOF

# ---------- 4. patch deploy/sales_assistant.py (Haiku) ----------
log "Patching $SALES_ASSISTANT (Haiku for drafts)..."
if grep -q '"model": "claude-3-haiku-20240307"' "$SALES_ASSISTANT"; then
  log "sales_assistant.py already pinned to Haiku — skipping"
else
  # In-place with a local backup next to the file as well.
  sed -i.presed.bak 's|"model": "claude-sonnet-4-6"|"model": "claude-3-haiku-20240307"|' "$SALES_ASSISTANT"
  if grep -q '"model": "claude-3-haiku-20240307"' "$SALES_ASSISTANT"; then
    log "sales_assistant.py patched -> claude-3-haiku-20240307"
  else
    die "Failed to patch sales_assistant.py — expected model string \"claude-sonnet-4-6\" not found. Restore from $BACKUP_DIR."
  fi
fi

# ---------- 5. verify ----------
log "--- Verification ---"
echo
echo "# browser_tool.py"
grep -nE "playwright_stealth|_stealth_sync|networkidle|wait_for_load_state" "$BROWSER_TOOL" || true
echo
echo "# deploy/sales_assistant.py"
grep -nE "claude-3-haiku|claude-sonnet" "$SALES_ASSISTANT" || true
echo

# Quick sanity: file still parses as valid Python.
"$PYTHON_BIN" -c "import ast,sys; ast.parse(open('$BROWSER_TOOL').read()); ast.parse(open('$SALES_ASSISTANT').read()); print('[meridian-fix] syntax ok')" \
  || die "Post-patch syntax check failed. Restore from $BACKUP_DIR."

# ---------- 6. restart pm2 ----------
if [ "$NO_RESTART" = "1" ]; then
  log "NO_RESTART=1 — skipping pm2 restart"
elif command -v pm2 >/dev/null 2>&1; then
  log "Restarting PM2 services..."
  if [ -f "$REPO/ecosystem.config.js" ]; then
    pm2 start "$REPO/ecosystem.config.js" --update-env 2>/dev/null \
      || pm2 restart all --update-env
  else
    pm2 restart all --update-env
  fi
  echo
  pm2 status || true
else
  warn "pm2 not on PATH — skipping restart. Install with: npm i -g pm2"
fi

log "Done."
log "Backups:     $BACKUP_DIR"
log "Rollback:    cp \"$BACKUP_DIR/browser_tool.py.bak\" \"$BROWSER_TOOL\" && cp \"$BACKUP_DIR/sales_assistant.py.bak\" \"$SALES_ASSISTANT\""
