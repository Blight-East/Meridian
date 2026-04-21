from __future__ import annotations

import json
import os
from typing import Any

import redis

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.health.telemetry import get_component_state, is_stale, record_component_state, utc_now_iso
from runtime.ops.operator_alerts import send_operator_alert


logger = get_logger("telegram_delivery_watchdog")
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

PROACTIVE_QUEUE = "agent_flux:proactive_messages"
WATCHDOG_STATUS_KEY = "agent_flux:telegram_delivery_watchdog:last_status"
WATCHDOG_STATE_KEY = "agent_flux:telegram_delivery_watchdog:last_snapshot"
QUEUE_STALL_THRESHOLD = int(os.getenv("AGENT_FLUX_TELEGRAM_QUEUE_STALL_THRESHOLD", "5"))
STALE_HEARTBEAT_SECONDS = int(os.getenv("AGENT_FLUX_TELEGRAM_STALE_HEARTBEAT_SECONDS", "900"))
STALE_DELIVERY_SECONDS = int(os.getenv("AGENT_FLUX_TELEGRAM_STALE_DELIVERY_SECONDS", "1800"))


def _truthy_env(name: str) -> bool:
    return bool(str(os.getenv(name) or "").strip())


def _status_from_snapshot(snapshot: dict[str, Any]) -> str:
    if snapshot.get("missing_required_config"):
        return "offline"
    if snapshot.get("queue_stalled"):
        return "degraded"
    if snapshot.get("delivery_stale"):
        return "attention"
    return "healthy"


def _headline_from_snapshot(snapshot: dict[str, Any]) -> str:
    queue_depth = int(snapshot.get("queue_depth") or 0)
    if snapshot.get("missing_required_config"):
        missing = ", ".join(snapshot.get("missing_required_config") or [])
        return f"Telegram delivery is offline because required config is missing: {missing}."
    if snapshot.get("queue_stalled"):
        return f"Telegram delivery looks stalled and {queue_depth} queued update(s) are waiting."
    if snapshot.get("delivery_stale"):
        return f"Telegram delivery is behind and {queue_depth} queued update(s) are waiting."
    if queue_depth > 0:
        return f"Telegram delivery is healthy and {queue_depth} queued update(s) are waiting to drain."
    return "Telegram delivery is healthy and the proactive queue is clear."


def get_telegram_delivery_health() -> dict[str, Any]:
    state = get_component_state("telegram") or {}
    queue_depth = int(_redis.llen(PROACTIVE_QUEUE) or 0)
    last_heartbeat_at = str(state.get("last_heartbeat_at") or "").strip()
    last_successful_at = str(state.get("last_successful_bot_reply") or "").strip()
    missing_required = [name for name in ("TELEGRAM_TOKEN",) if not _truthy_env(name)]
    missing_recommended = [
        name
        for name in ("OPERATOR_CHAT_ID", "ALLOWED_OPERATOR_IDS", "ALLOWED_OPERATOR_CHAT_IDS")
        if not _truthy_env(name)
    ]
    queue_stalled = queue_depth >= QUEUE_STALL_THRESHOLD and is_stale(last_heartbeat_at, STALE_HEARTBEAT_SECONDS)
    delivery_stale = queue_depth > 0 and is_stale(last_successful_at, STALE_DELIVERY_SECONDS)
    snapshot: dict[str, Any] = {
        "status": "",
        "queue_depth": queue_depth,
        "last_heartbeat_at": last_heartbeat_at,
        "last_successful_bot_reply": last_successful_at,
        "adapter_health": str(state.get("adapter_health") or "").strip() or "unknown",
        "component_status": str(state.get("status") or "").strip() or "unknown",
        "config_status": str(state.get("config_status") or "").strip() or "ok",
        "missing_required_config": missing_required,
        "missing_recommended_config": missing_recommended,
        "queue_stalled": bool(queue_stalled),
        "delivery_stale": bool(delivery_stale),
        "watchdog_checked_at": utc_now_iso(),
    }
    snapshot["status"] = _status_from_snapshot(snapshot)
    snapshot["headline"] = _headline_from_snapshot(snapshot)
    return snapshot


def run_telegram_delivery_watchdog() -> dict[str, Any]:
    snapshot = get_telegram_delivery_health()
    previous_status = str(_redis.get(WATCHDOG_STATUS_KEY) or "").strip().lower()
    current_status = str(snapshot.get("status") or "unknown").strip().lower()

    record_component_state(
        "telegram",
        ttl=900,
        proactive_watchdog_status=current_status,
        proactive_watchdog_checked_at=snapshot.get("watchdog_checked_at"),
        proactive_queue_depth=snapshot.get("queue_depth", 0),
        proactive_delivery_headline=snapshot.get("headline", ""),
    )
    _redis.set(WATCHDOG_STATUS_KEY, current_status)
    _redis.set(WATCHDOG_STATE_KEY, json.dumps(snapshot, sort_keys=True))
    save_event("telegram_delivery_watchdog_checked", snapshot)

    if current_status in {"offline", "degraded", "attention"} and previous_status != current_status:
        logger.warning(snapshot.get("headline"))
        save_event(
            "telegram_delivery_watchdog_alert",
            {
                "status": current_status,
                "headline": snapshot.get("headline"),
                "queue_depth": snapshot.get("queue_depth", 0),
            },
        )

    if previous_status in {"offline", "degraded", "attention"} and current_status == "healthy":
        send_operator_alert(
            "Telegram delivery recovered. Meridian is sending again and the proactive queue is draining normally.",
            dedupe_key="telegram_delivery_notice",
            cooldown_seconds=900,
        )
        save_event(
            "telegram_delivery_watchdog_recovered",
            {
                "headline": snapshot.get("headline"),
                "queue_depth": snapshot.get("queue_depth", 0),
            },
        )

    return snapshot
