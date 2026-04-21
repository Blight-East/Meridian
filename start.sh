#!/bin/bash
# Agent Flux - Service Startup Script
set -e
set -u

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="$PROJECT_DIR:$PROJECT_DIR/runtime:$PROJECT_DIR/deploy:${PYTHONPATH:-}"
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
export AGENT_FLUX_LOG_DIR="/tmp/agent-flux/logs"
mkdir -p "$AGENT_FLUX_LOG_DIR"

# Load .env
set -a
source "$PROJECT_DIR/.env"
set +a

START_MODE="${AGENT_FLUX_START_MODE:-pm2}"

echo "[Agent Flux] Starting services..."

TELEGRAM_PID=""

if [[ "$START_MODE" != "foreground" ]] && command -v pm2 >/dev/null 2>&1; then
  echo "[Agent Flux] Using PM2 for durable local startup..."
  cd "$PROJECT_DIR"
  if [[ "${AGENT_FLUX_ENABLE_LOCAL_MCP:-0}" != "1" ]] && [[ "$PROJECT_DIR" != "/opt/agent-flux" ]]; then
    pm2 delete meridian-mcp >/dev/null 2>&1 || true
  fi
  pm2 startOrReload ecosystem.config.js --update-env
  pm2 save >/dev/null
  echo "[Agent Flux] PM2 services are up. Use ./status.sh to inspect them."
  pm2 list
  exit 0
fi

# 1. API Server
echo "[Agent Flux] Starting API server on port 8000..."
cd "$PROJECT_DIR"
python3 -m uvicorn runtime.main:app --host 0.0.0.0 --port 8000 --log-level info &
API_PID=$!
echo "[Agent Flux] API PID: $API_PID"

# 2. Worker
echo "[Agent Flux] Starting worker..."
cd "$PROJECT_DIR"
python3 runtime/worker.py &
WORKER_PID=$!
echo "[Agent Flux] Worker PID: $WORKER_PID"

# 3. Scheduler (the heartbeat)
echo "[Agent Flux] Starting scheduler..."
cd "$PROJECT_DIR"
python3 runtime/scheduler/cron_tasks.py &
SCHEDULER_PID=$!
echo "[Agent Flux] Scheduler PID: $SCHEDULER_PID"

# 4. Telegram bot (operator delivery + proactive queue drain)
if [[ -n "${TELEGRAM_TOKEN:-}" ]]; then
  echo "[Agent Flux] Starting Telegram bot..."
  cd "$PROJECT_DIR"
  python3 runtime/telegram_bot.py &
  TELEGRAM_PID=$!
  echo "[Agent Flux] Telegram PID: $TELEGRAM_PID"
else
  echo "[Agent Flux] WARNING: TELEGRAM_TOKEN is not set. Telegram delivery will stay offline and proactive updates will queue in Redis."
fi

echo "[Agent Flux] All services started."
echo "  API:       http://localhost:8000 (PID $API_PID)"
echo "  Worker:    PID $WORKER_PID"
echo "  Scheduler: PID $SCHEDULER_PID"
if [[ -n "$TELEGRAM_PID" ]]; then
  echo "  Telegram:  PID $TELEGRAM_PID"
else
  echo "  Telegram:  OFFLINE (missing TELEGRAM_TOKEN)"
fi
echo ""
echo "Logs: $AGENT_FLUX_LOG_DIR"

# Save PIDs for stop script
echo "$API_PID $WORKER_PID $SCHEDULER_PID $TELEGRAM_PID" > "$PROJECT_DIR/.pids"

# Wait for all
wait
