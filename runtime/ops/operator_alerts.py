"""
Operator Alerts — Routes system alerts via Telegram.
"""
import os
import json
import time
import re
import redis
from config.logging_config import get_logger
from runtime.health.telemetry import get_component_state, is_stale, record_component_state, utc_now_iso

logger = get_logger("operator_alerts")

# Connect to Redis
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
PROACTIVE_QUEUE = "agent_flux:proactive_messages"
OPERATOR_CHAT_ID = os.environ.get("OPERATOR_CHAT_ID", "5940537326")
_TELEGRAM_DELIVERY_NOTICE_COOLDOWN_SECONDS = int(
    os.getenv("AGENT_FLUX_TELEGRAM_DELIVERY_NOTICE_COOLDOWN_SECONDS", "900")
)

_TELEGRAM_DELIVERY_NOTICE_PATTERNS = (
    re.compile(r"\btelegram delivery recovered\b", re.IGNORECASE),
    re.compile(r"\bmeridian delivery check\b", re.IGNORECASE),
    re.compile(r"\btelegram is back online\b", re.IGNORECASE),
    re.compile(r"\bproactive-only telegram delivery is healthy\b", re.IGNORECASE),
)


def _record_proactive_delivery_state() -> None:
    queue_depth = _redis.llen(PROACTIVE_QUEUE)
    telegram_state = get_component_state("telegram") or {}
    last_heartbeat = str(telegram_state.get("last_heartbeat_at") or "").strip()
    delivery_health = "healthy"
    if queue_depth >= 10 and is_stale(last_heartbeat, 300):
        delivery_health = "degraded"
    record_component_state(
        "telegram",
        ttl=900,
        last_enqueued_at=utc_now_iso(),
        proactive_queue_depth=queue_depth,
        proactive_delivery_health=delivery_health,
    )


def _infer_alert_dedupe_key(message_text: str) -> tuple[str | None, int | None]:
    text = str(message_text or "").strip()
    if not text:
        return None, None
    if any(pattern.search(text) for pattern in _TELEGRAM_DELIVERY_NOTICE_PATTERNS):
        return "telegram_delivery_notice", _TELEGRAM_DELIVERY_NOTICE_COOLDOWN_SECONDS
    return None, None


def _dedupe_cache_key(dedupe_key: str) -> str:
    return f"agent_flux:operator_alerts:dedupe:{dedupe_key}"


def send_operator_alert(
    message_text: str,
    *,
    dedupe_key: str | None = None,
    cooldown_seconds: int | None = None,
) -> bool:
    """
    Queue an alert for delivery via the Telegram bot process.
    Uses the agent workloop queue to guarantee delivery.
    """
    try:
        if len(message_text) > 4000:
            message_text = message_text[:3997] + "..."

        inferred_key, inferred_cooldown = _infer_alert_dedupe_key(message_text)
        dedupe_key = dedupe_key or inferred_key
        cooldown_seconds = cooldown_seconds if cooldown_seconds is not None else inferred_cooldown
        if dedupe_key and cooldown_seconds and cooldown_seconds > 0:
            cache_key = _dedupe_cache_key(dedupe_key)
            if _redis.get(cache_key):
                logger.info(f"Operator alert suppressed by dedupe: {dedupe_key}")
                return False

        _redis.rpush(PROACTIVE_QUEUE, json.dumps({
            "chat_id": OPERATOR_CHAT_ID,
            "text": message_text,
            "timestamp": time.time(),
        }))
        if dedupe_key and cooldown_seconds and cooldown_seconds > 0:
            _redis.setex(_dedupe_cache_key(dedupe_key), int(cooldown_seconds), utc_now_iso())
        _record_proactive_delivery_state()
        logger.info(f"Operator alert queued: {message_text[:50]}...")
        return True
    except Exception as e:
        logger.error(f"Failed to queue operator alert: {e}")
        return False
