from __future__ import annotations

from datetime import datetime

import redis

from memory.structured.db import save_event
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.operator_commands import (
    draft_follow_up_for_opportunity_command,
    rewrite_outreach_draft_command,
    show_reply_review_command,
    show_reply_review_queue_command,
)
from runtime.ops.outreach_execution import sync_outreach_execution_state

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
_SEEN_DRAFTED_REPLY_KEY = "agent_flux:reply_draft_monitor:drafted"
_TTL_SECONDS = 7 * 24 * 60 * 60


def _iso_to_ts(value: str) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _case_key(case: dict) -> str:
    return "|".join(
        [
            str(case.get("opportunity_id") or 0),
            str(case.get("gmail_thread_id") or ""),
            str(case.get("reply_received_at") or ""),
        ]
    )


def _draft_is_ready(result: dict) -> bool:
    return result.get("status") == "ok" and bool(result.get("subject") or result.get("body"))


def _render_message(cases: list[dict]) -> str:
    lines = ["Meridian drafted reply follow-ups"]
    for case in cases[:3]:
        merchant = case.get("merchant_domain") or case.get("contact_email") or f"opportunity {case.get('opportunity_id')}"
        lines.append("")
        lines.append(f"- Opportunity {case.get('opportunity_id')} — {merchant}")
        lines.append(f"  Reason: {case.get('suggestion_reason') or 'Live merchant reply needs a fast next touch.'}")
        if case.get("rewrite_style") and case.get("rewrite_style") != "standard":
            lines.append(f"  Rewrite style: {case.get('rewrite_style')}")
        if case.get("auto_approved"):
            lines.append(f"  Status: Auto-Approved (confidence {float(case.get('confidence') or 0.0):.2f})")
        else:
            lines.append(f"  Draft ready: show draft {case.get('opportunity_id')}")
            lines.append(f"  Review thread: review reply {case.get('opportunity_id')}")
    return "\n".join(lines).strip()


def _choose_reply_rewrite_style(learning_signal: dict) -> tuple[str, str]:
    preferred = str(learning_signal.get("preferred_rewrite_style") or "").strip().lower()
    if preferred and preferred != "standard":
        return preferred, "Use the strongest learned reply-follow-up style from prior outcomes."
    wins = int(learning_signal.get("wins") or 0)
    losses = int(learning_signal.get("losses") or 0)
    ignored = int(learning_signal.get("ignored") or 0)
    records = int(learning_signal.get("records") or 0)
    if records < 2:
        return "standard", ""
    if ignored >= max(2, wins + losses):
        return "shorter", "Recent reply follow-ups are being ignored, so keep the next response tighter."
    if losses > wins:
        return "softer", "Recent reply follow-ups are closing cold, so soften the tone."
    if wins >= max(2, losses) and float(learning_signal.get("win_rate") or 0.0) >= 0.5:
        return "direct", "Recent reply follow-ups convert better when the next step is stated plainly."
    return "standard", ""


def run_reply_draft_monitor(send_update: bool = True, limit: int = 5) -> dict:
    sync_state = sync_outreach_execution_state(limit=max(50, int(limit) * 10))
    queue = show_reply_review_queue_command(limit=max(1, int(limit) * 2))
    cases = sorted(
        list(queue.get("cases") or []),
        key=lambda case: (
            float(case.get("confidence") or 0.0),
            _iso_to_ts(case.get("reply_received_at") or ""),
        ),
        reverse=True,
    )

    drafted_cases: list[dict] = []
    for case in cases:
        opportunity_id = int(case.get("opportunity_id") or 0)
        if not opportunity_id:
            continue
        if str(case.get("suggested_outcome") or "").strip().lower() != "pending":
            continue
        if float(case.get("confidence") or 0.0) < 0.65:
            continue
        seen_key = _case_key(case)
        if not seen_key:
            continue
        if _redis.sismember(_SEEN_DRAFTED_REPLY_KEY, seen_key):
            continue
        review = show_reply_review_command(opportunity_id=opportunity_id)
        learning_signal = review.get("reply_learning_signal") or {}
        draft = draft_follow_up_for_opportunity_command(opportunity_id=opportunity_id, follow_up_type="reply_follow_up")
        if not _draft_is_ready(draft):
            continue
        rewrite_style, rewrite_reason = _choose_reply_rewrite_style(learning_signal)
        if rewrite_style != "standard":
            rewritten = rewrite_outreach_draft_command(
                opportunity_id=opportunity_id,
                style=rewrite_style,
                instructions=rewrite_reason,
            )
            if rewritten.get("status") == "ok":
                draft = rewritten
        _redis.sadd(_SEEN_DRAFTED_REPLY_KEY, seen_key)

        from runtime.ops.operator_commands import approve_outreach_for_opportunity_command
        confidence = float(case.get("confidence") or 0.0)
        auto_approved = False
        if confidence >= 0.90:
            appr = approve_outreach_for_opportunity_command(opportunity_id=opportunity_id, approval_source="reply_draft_monitor")
            if appr.get("status") == "ok":
                auto_approved = True

        drafted_cases.append(
            {
                **case,
                "draft_subject": draft.get("subject") or "",
                "draft_status": draft.get("status") or "",
                "rewrite_style": draft.get("rewrite_style") or rewrite_style or "standard",
                "reply_learning_signal": learning_signal,
                "auto_approved": auto_approved,
            }
        )

    if drafted_cases:
        _redis.expire(_SEEN_DRAFTED_REPLY_KEY, _TTL_SECONDS)

    message = ""
    if send_update and drafted_cases:
        message = _render_message(drafted_cases)
        send_telegram(message)

    result = {
        "status": "ok",
        "checked_at": utc_now_iso(),
        "sync_state": sync_state,
        "reply_review_count": int(queue.get("count") or 0),
        "drafted_reply_follow_ups": len(drafted_cases),
        "drafted_cases": drafted_cases,
        "message_sent": bool(message),
        "message_preview": message,
    }
    record_component_state(
        "reply_draft_monitor",
        ttl=1800,
        last_run_at=result["checked_at"],
        last_reply_review_count=result["reply_review_count"],
        last_drafted_reply_follow_ups=result["drafted_reply_follow_ups"],
        last_message_sent=result["message_sent"],
    )
    save_event(
        "reply_draft_monitor_run",
        {
            "reply_review_count": result["reply_review_count"],
            "drafted_reply_follow_ups": result["drafted_reply_follow_ups"],
            "message_sent": result["message_sent"],
        },
    )
    return result
