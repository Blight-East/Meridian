#!/bin/bash
# Agent Flux - Deploy to VPS
set -e

VPS_IP="159.65.45.64"
VPS_USER="root"
APP_DIR="/opt/agent-flux"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[Deploy] Syncing files to $VPS_USER@$VPS_IP:$APP_DIR ..."

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude '.claude' \
  --exclude 'node_modules' \
  --exclude '.git' \
  --exclude 'logs' \
  "$LOCAL_DIR/runtime/" "$VPS_USER@$VPS_IP:$APP_DIR/runtime/"

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$LOCAL_DIR/deploy/" "$VPS_USER@$VPS_IP:$APP_DIR/deploy/"

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$LOCAL_DIR/config/" "$VPS_USER@$VPS_IP:$APP_DIR/config/"

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$LOCAL_DIR/memory/" "$VPS_USER@$VPS_IP:$APP_DIR/memory/"

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$LOCAL_DIR/safety/" "$VPS_USER@$VPS_IP:$APP_DIR/safety/"

rsync -avz --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$LOCAL_DIR/tools/" "$VPS_USER@$VPS_IP:$APP_DIR/tools/"

# Sync config files
rsync -avz "$LOCAL_DIR/.env" "$VPS_USER@$VPS_IP:$APP_DIR/.env"
rsync -avz "$LOCAL_DIR/ecosystem.config.js" "$VPS_USER@$VPS_IP:$APP_DIR/ecosystem.config.js"

echo "[Deploy] Creating directories on VPS..."
ssh "$VPS_USER@$VPS_IP" "mkdir -p $APP_DIR/logs"

echo "[Deploy] Ensuring FastMCP is installed on VPS..."
ssh "$VPS_USER@$VPS_IP" "python3 -m pip install --disable-pip-version-check fastmcp"

echo "[Deploy] Ensuring postgresql-client and redis-tools are installed on VPS..."
ssh "$VPS_USER@$VPS_IP" "command -v psql >/dev/null && command -v redis-cli >/dev/null || (apt-get update -qq && apt-get install -y postgresql-client redis-tools)"

echo "[Deploy] Restarting PM2 services..."
ssh "$VPS_USER@$VPS_IP" "cd $APP_DIR && set -a && source .env && set +a && pm2 stop ecosystem.config.js 2>/dev/null; pm2 delete ecosystem.config.js 2>/dev/null; pm2 delete agentflux-telegram 2>/dev/null; pm2 start ecosystem.config.js && pm2 save"

echo "[Deploy] Checking status..."
ssh "$VPS_USER@$VPS_IP" "pm2 status"

echo "[Deploy] Done! Services deployed to $VPS_IP"
