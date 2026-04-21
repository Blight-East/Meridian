#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

if command -v pm2 >/dev/null 2>&1; then
  pm2 stop ecosystem.config.js >/dev/null 2>&1 || true
  pm2 delete ecosystem.config.js >/dev/null 2>&1 || true
fi

if [[ -f "$PROJECT_DIR/.pids" ]]; then
  for pid in $(cat "$PROJECT_DIR/.pids"); do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  rm -f "$PROJECT_DIR/.pids"
fi

echo "[Agent Flux] Stop sequence complete."
