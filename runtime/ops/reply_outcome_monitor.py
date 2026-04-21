from __future__ import annotations

import json
from datetime import datetime, timezone

import redis

from memory.structured.db import save_event
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.operator_commands import (
    show_outcome_review_queue_command,
    show_reply_review_queue_command,
)
from runtime.ops.outreach_execution import sync_outreach_execution_state

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
_SEEN_REPLY_KEY = "agent_flux:reply_monitor:seen_reply_cases"
_SEEN_OUTCOME_KEY = "agent_flux:reply_monitor:seen_outcome_cases"
_SEEN_TTL_SECONDS = 7 * 24 * 60 * 60


def _iso_to_ts(value: str) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _reply_case_key(case: dict) -> str:
    return "|".join(
        [
            str(case.get("opportunity_id") or 0),
            str(case.get("gmail_thread_id") or ""),
            str(case.get("reply_received_at") or ""),
        ]
    )


def _outcome_case_key(case: dict) -> str:
    return "|".join(
        [
            str(case.get("opportunity_id") or 0),
            str(case.get("gmail_thread_id") or ""),
            str(case.get("suggested_outcome") or ""),
            str(case.get("reply_received_at") or ""),
        ]
    )


def _track_new_cases(redis_key: str, cases: list[dict], key_fn) -> list[dict]:
    fresh: list[dict] = []
    for case in cases:
        token = key_fn(case)
        if not token:
            continue
        added = _redis.sadd(redis_key, token)
        if added:
            fresh.append(case)
    if cases:
        _redis.expire(redis_key, _SEEN_TTL_SECONDS)
    return fresh


def _render_monitor_message(reply_cases: list[dict], outcome_cases: list[dict]) -> str:
    lines = ["Meridian reply monitor"]
    if reply_cases:
        lines.append("")
        lines.append("New reply-review cases:")
        for case in reply_cases[:3]:
            merchant = case.get("merchant_domain") or case.get("contact_email") or f"opportunity {case.get('opportunity_id')}"
            lines.append(
                f"- Opportunity {case.get('opportunity_id')} — {merchant}: {case.get('suggested_next_move') or 'Review manually.'}"
            )
            lines.append(f"  Say: review reply {case.get('opportunity_id')}")
    if outcome_cases:
        lines.append("")
        lines.append("New high-confidence outcome candidates:")
        for case in outcome_cases[:3]:
            merchant = case.get("merchant_domain") or case.get("contact_email") or f"opportunity {case.get('opportunity_id')}"
            lines.append(
                f"- Opportunity {case.get('opportunity_id')} — {merchant}: {case.get('suggested_outcome')} ({case.get('confidence') or 0.0:.2f})"
            )
            lines.append(f"  Say: apply suggested outcome {case.get('opportunity_id')}")
    return "\n".join(lines).strip()


def run_reply_outcome_monitor(send_update: bool = True, reply_limit: int = 5, outcome_limit: int = 5) -> dict:
    sync_state = sync_outreach_execution_state(limit=max(50, int(reply_limit) * 10))
    reply_queue = show_reply_review_queue_command(limit=max(1, int(reply_limit)))
    outcome_queue = show_outcome_review_queue_command(limit=max(1, int(outcome_limit)))

    reply_cases = sorted(
        list(reply_queue.get("cases") or []),
        key=lambda case: (
            float(case.get("confidence") or 0.0),
            _iso_to_ts(case.get("reply_received_at") or ""),
        ),
        reverse=True,
    )
    outcome_cases = sorted(
        list(outcome_queue.get("cases") or []),
        key=lambda case: (
            float(case.get("confidence") or 0.0),
            _iso_to_ts(case.get("reply_received_at") or ""),
        ),
        reverse=True,
    )

    new_reply_cases = _track_new_cases(_SEEN_REPLY_KEY, reply_cases, _reply_case_key)
    new_outcome_cases = _track_new_cases(_SEEN_OUTCOME_KEY, outcome_cases, _outcome_case_key)

    message = ""
    if send_update and (new_reply_cases or new_outcome_cases):
        message = _render_monitor_message(new_reply_cases, new_outcome_cases)
        send_telegram(message)

    result = {
        "status": "ok",
        "checked_at": utc_now_iso(),
        "sync_state": sync_state,
        "reply_review_count": int(reply_queue.get("count") or 0),
        "outcome_review_count": int(outcome_queue.get("count") or 0),
        "new_reply_review_cases": len(new_reply_cases),
        "new_outcome_review_cases": len(new_outcome_cases),
        "reply_cases": reply_cases,
        "outcome_cases": outcome_cases,
        "message_sent": bool(message),
        "message_preview": message,
    }
    record_component_state(
        "reply_outcome_monitor",
        ttl=1800,
        last_run_at=result["checked_at"],
        last_reply_review_count=result["reply_review_count"],
        last_outcome_review_count=result["outcome_review_count"],
        last_new_reply_review_cases=result["new_reply_review_cases"],
        last_new_outcome_review_cases=result["new_outcome_review_cases"],
        last_message_sent=result["message_sent"],
    )
    save_event(
        "reply_outcome_monitor_run",
        {
            "reply_review_count": result["reply_review_count"],
            "outcome_review_count": result["outcome_review_count"],
            "new_reply_review_cases": result["new_reply_review_cases"],
            "new_outcome_review_cases": result["new_outcome_review_cases"],
            "message_sent": result["message_sent"],
        },
    )
    return result
