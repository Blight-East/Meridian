#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

if command -v pm2 >/dev/null 2>&1; then
  echo "[Agent Flux] PM2 status"
  pm2 list
  echo ""
fi

echo "[Agent Flux] Local health"
curl -s http://127.0.0.1:8000/health || echo "API health endpoint unavailable"
