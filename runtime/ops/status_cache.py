from __future__ import annotations

import json
import os
from typing import Any

import redis

from runtime.health.telemetry import utc_now_iso

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

OPERATOR_STATE_CACHE_KEY = "agent_flux:operator_state:last_good"
SYSTEM_STATUS_CACHE_KEY = "agent_flux:system_status:last_good"
OPPORTUNITY_COUNTS_CACHE_KEY = "agent_flux:opportunity_counts:last_good"
STATUS_CACHE_TTL_SECONDS = int(os.getenv("AGENT_FLUX_STATUS_CACHE_TTL_SECONDS", "3600"))


def read_status_snapshot(cache_key: str) -> dict[str, Any] | None:
    raw = _redis.get(cache_key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def write_status_snapshot(cache_key: str, payload: dict[str, Any]) -> None:
    snapshot = dict(payload)
    meta = dict(snapshot.get("_meta") or {})
    meta.update({"fresh": True, "stale": False, "cached_at": utc_now_iso()})
    snapshot["_meta"] = meta
    _redis.setex(cache_key, STATUS_CACHE_TTL_SECONDS, json.dumps(snapshot, default=str, sort_keys=True))


def stale_status_snapshot(cached: dict[str, Any], reason: str) -> dict[str, Any]:
    snapshot = dict(cached or {})
    meta = dict(snapshot.get("_meta") or {})
    meta.update(
        {
            "fresh": False,
            "stale": True,
            "fallback_reason": str(reason),
            "served_at": utc_now_iso(),
        }
    )
    snapshot["_meta"] = meta
    return snapshot


def unavailable_status_snapshot(reason: str, *, last_scan_time: str = "unknown") -> dict[str, Any]:
    return {
        "last_scan_time": last_scan_time or "unknown",
        "_meta": {
            "fresh": False,
            "stale": False,
            "unavailable": True,
            "fallback_reason": str(reason),
            "served_at": utc_now_iso(),
        },
    }


def cached_status_brief(
    *,
    health_snapshot: dict[str, Any] | None = None,
    system_status: dict[str, Any] | None = None,
    operator_state: dict[str, Any] | None = None,
) -> str | None:
    system = system_status or read_status_snapshot(SYSTEM_STATUS_CACHE_KEY) or {}
    operator = operator_state or read_status_snapshot(OPERATOR_STATE_CACHE_KEY) or {}
    health = health_snapshot or {}

    queue_depth = (
        ((health.get("queue") or {}).get("depth"))
        if isinstance(health.get("queue"), dict)
        else None
    )
    gmail_status = (
        (((health.get("components") or {}).get("gmail") or {}).get("gmail_status"))
        if isinstance((health.get("components") or {}).get("gmail"), dict)
        else None
    )
    signals_1h = system.get("signals_1h")
    pending_review = system.get("qualified_leads_24h")
    last_scan = operator.get("minutes_since_last_scan")

    parts = []
    if queue_depth is not None:
        parts.append("the queue is clear" if int(queue_depth) == 0 else f"queue depth is {queue_depth}")
    if isinstance(signals_1h, (int, float)):
        parts.append(f"{int(signals_1h)} signal(s) landed in the last hour")
    if isinstance(pending_review, (int, float)):
        parts.append(f"{int(pending_review)} qualified lead(s) showed up in the last day")
    if gmail_status:
        parts.append(f"Gmail is {gmail_status}")
    if isinstance(last_scan, (int, float)):
        parts.append(f"the last scan was {round(float(last_scan), 1)} minutes ago")

    if not parts:
        return None
    return "Last good read: " + ", ".join(parts[:4]) + "."
