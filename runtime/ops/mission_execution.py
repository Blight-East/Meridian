from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import redis

from memory.structured.db import save_event
from runtime.conversation.mission_memory import get_mission_memory
from runtime.conversation.operator_profile import get_operator_profile
from runtime.health.telemetry import get_component_state, record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.brain_state import get_brain_state
from runtime.ops.value_heartbeat import get_value_heartbeat

import logging as _logging
_mission_logger = _logging.getLogger("meridian.mission_execution")

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
_PLAN_SEND_HASH_KEY = "agent_flux:mission_execution:last_sent_plan_hash"
_PLAN_SEND_AT_KEY = "agent_flux:mission_execution:last_sent_plan_at"
_DAILY_BRIEFING_SENT_AT_KEY = "agent_flux:operator_updates:last_daily_briefing_sent_at"
_DEFAULT_PLAN_SEND_MIN_INTERVAL_SECONDS = int(
    os.getenv("AGENT_FLUX_MISSION_EXECUTION_MIN_SEND_INTERVAL_SECONDS", "3600")
)
_DEFAULT_PLAN_UNCHANGED_RESEND_INTERVAL_SECONDS = int(
    os.getenv("AGENT_FLUX_MISSION_EXECUTION_UNCHANGED_RESEND_INTERVAL_SECONDS", "43200")
)
_DAILY_BRIEFING_SUPPRESSION_SECONDS = int(
    os.getenv("AGENT_FLUX_MISSION_EXECUTION_DAILY_BRIEFING_SUPPRESSION_SECONDS", "1800")
)
_OPERATOR_TIMEZONE = os.getenv("AGENT_FLUX_OPERATOR_TIMEZONE", "America/New_York").strip() or "America/New_York"
_EXECUTION_UPDATE_START_HOUR = int(os.getenv("AGENT_FLUX_EXECUTION_UPDATE_START_HOUR", "8"))
_EXECUTION_UPDATE_END_HOUR = int(os.getenv("AGENT_FLUX_EXECUTION_UPDATE_END_HOUR", "18"))


def _normalized_context_value(value: Any) -> str:
    lowered = str(value or "").strip().lower().replace("_", " ")
    if lowered in {"", "unknown", "unclassified", "none", "n/a"}:
        return ""
    return lowered


def _context_summary(item: dict[str, Any], *, for_clarification: bool = False) -> str:
    action_type = str(item.get("action_type") or "").strip().lower()
    distress = _normalized_context_value(item.get("distress_type"))
    processor = _normalized_context_value(item.get("processor"))
    parts: list[str] = []
    if distress:
        parts.append(distress)
    elif for_clarification:
        parts.append("distress unclear")
    elif action_type == "review_reply":
        parts.append("reply needs classification")
    if processor:
        parts.append(processor)
    elif for_clarification or action_type in {"review_reply", "review_draft", "send_outreach"}:
        parts.append("processor unclear")
    if not parts:
        return ""
    return f" ({'; '.join(parts)})"


def _review_command(opportunity_id: Any) -> str:
    try:
        return f"`review opportunity {int(opportunity_id)}`"
    except Exception:
        return "`show top opportunities`"


def _render_operator_queue_line(item: dict[str, Any]) -> str:
    domain = item.get("merchant_domain") or f"opp #{item.get('opportunity_id')}"
    action = str(item.get("action_type", "")).replace("_", " ").title()
    opportunity_id = item.get("opportunity_id")
    label = f"{domain} #{opportunity_id}" if opportunity_id else str(domain)
    return f"  → {action}: {label}{_context_summary(item)} — open with {_review_command(opportunity_id)}"


def _render_clarification_line(item: dict[str, Any]) -> str:
    domain = item.get("merchant_domain") or f"opp #{item.get('opportunity_id')}"
    opportunity_id = item.get("opportunity_id")
    label = f"{domain} #{opportunity_id}" if opportunity_id else str(domain)
    return f"  → Clarify or suppress: {label}{_context_summary(item, for_clarification=True)} — open with {_review_command(opportunity_id)}"


def _render_auto_line(item: dict[str, Any]) -> str:
    domain = item.get("merchant_domain") or f"opp #{item.get('opportunity_id')}"
    action = str(item.get("action_type", "")).replace("_", " ").title()
    opportunity_id = item.get("opportunity_id")
    label = f"{domain} #{opportunity_id}" if opportunity_id else str(domain)
    return f"  → {action}: {label}{_context_summary(item)}"


def _append_unique(lines: list[str], text: str) -> None:
    value = str(text or "").strip()
    if value and value not in lines:
        lines.append(value)


def get_primary_operator_user_id() -> str:
    configured = [item.strip() for item in os.getenv("ALLOWED_OPERATOR_IDS", "").split(",") if item.strip()]
    if configured:
        return configured[0]

    operator_chat_id = (os.getenv("OPERATOR_CHAT_ID") or "").strip()
    if operator_chat_id:
        trusted_owner = _redis.get(f"agent_flux:trusted_chat_owner:{operator_chat_id}")
        if trusted_owner:
            return str(trusted_owner)
        if _redis.get(f"agent_flux:chat_id:{operator_chat_id}") is not None:
            return operator_chat_id

    for key in _redis.scan_iter(match="agent_flux:trusted_chat_id:*"):
        user_id = str(key).rsplit(":", 1)[-1].strip()
        if user_id:
            return user_id

    for key in _redis.scan_iter(match="agent_flux:chat_id:*"):
        user_id = str(key).rsplit(":", 1)[-1].strip()
        if user_id:
            return user_id

    return ""


def build_execution_plan(user_id: str, *, value: dict[str, Any] | None = None) -> dict[str, Any]:
    mission = get_mission_memory(user_id)
    profile = get_operator_profile(user_id)
    value = value or get_value_heartbeat(refresh=False)
    brain = get_brain_state(user_id)

    priorities: list[str] = []
    immediate_actions: list[str] = []
    rationale: list[str] = []
    effective_strategic_bet = str(brain.get("strategic_bet") or "").strip()
    effective_brain_bottleneck = str(brain.get("main_bottleneck") or "").strip()
    effective_decisive_action = str(brain.get("next_decisive_action") or "").strip()
    effective_success_signal = str(brain.get("success_signal") or "").strip()
    effective_abort_signal = str(brain.get("abort_signal") or "").strip()

    if mission.get("primary_mission"):
        priorities.append(f"Protect the main mission: {mission['primary_mission']}")
    if profile.get("current_focus"):
        priorities.append(f"Honor current operator focus: {profile['current_focus']}")

    pending = int(value.get("opportunities_pending_review", 0) or 0)
    approved = int(value.get("opportunities_approved", 0) or 0)
    sent = int(value.get("opportunities_outreach_sent", 0) or 0)
    converted = int(value.get("opportunities_converted", 0) or 0)
    leads = int(value.get("qualified_leads_24h", 0) or 0)
    incidents = int(value.get("open_security_incidents", 0) or 0)
    drafts_waiting = int(value.get("outreach_awaiting_approval", 0) or 0)
    ready_to_send = int(value.get("outreach_ready_to_send", 0) or 0)
    outcome_review_ready = int(value.get("outcome_review_ready", 0) or 0)
    replies_open = int(value.get("reply_review_needed", 0) or 0)
    follow_ups_due = int(value.get("follow_ups_due", 0) or 0)
    send_ready = int(value.get("send_eligible_leads", 0) or 0)
    reachable_contacts = int(value.get("reachable_contact_leads", send_ready) or 0)
    contact_blocked = int(value.get("contact_blocked_opportunities", 0) or 0)
    bottleneck = str(value.get("primary_bottleneck") or "").strip()
    next_move = str(value.get("next_revenue_move") or "").strip()
    overnight_status = str(value.get("overnight_contract_status") or "").strip()
    overnight_prospects = list(value.get("overnight_best_new_prospects") or [])
    overnight_queue_sharpened = int(value.get("overnight_queue_sharpened_24h", 0) or 0)
    overnight_action_ready = int(value.get("overnight_action_ready_count", 0) or 0)
    live_thread_present = bool(value.get("live_thread_present"))
    live_thread_status = str(value.get("live_thread_status") or "").strip()
    live_thread_status_label = str(value.get("live_thread_status_label") or live_thread_status.replace("_", " ")).strip()
    live_thread_domain = str(value.get("live_thread_merchant_domain") or "").strip()
    live_thread_next_step = str(value.get("live_thread_next_step") or "").strip()
    top_suppression_reason = str(value.get("overnight_top_suppression_reason") or "").strip()
    security_headline = str(value.get("security_guardrail_headline") or "").strip()

    if live_thread_present and live_thread_domain:
        effective_strategic_bet = f"Turn the live {live_thread_domain} thread into a real merchant conversation."
        if live_thread_next_step:
            effective_decisive_action = live_thread_next_step
        if live_thread_status in {"awaiting_reply", "reply_live", "follow_up_due"}:
            effective_success_signal = "The live thread replies, or the follow-up goes out on time and keeps the conversation alive."
            effective_abort_signal = "The live thread goes cold without a reply or timely follow-up."

    if effective_strategic_bet:
        _append_unique(priorities, f"Strategic bet: {effective_strategic_bet}")
    if effective_brain_bottleneck:
        _append_unique(priorities, f"Brain bottleneck: {effective_brain_bottleneck}")
    if effective_decisive_action:
        _append_unique(immediate_actions, f"Decisive move: {effective_decisive_action}")
    if effective_success_signal:
        _append_unique(rationale, f"Success signal: {effective_success_signal}")
    if effective_abort_signal:
        _append_unique(rationale, f"Abort signal: {effective_abort_signal}")

    if incidents > 0:
        _append_unique(priorities, f"Resolve {incidents} open security incident(s) before increasing autonomy")
        _append_unique(
            immediate_actions,
            f"Review security first: {security_headline or 'close the open prompt-injection or channel-control risk'}.",
        )

    if overnight_status:
        _append_unique(priorities, f"Overnight contract: {overnight_status}")
    if live_thread_present and live_thread_domain:
        _append_unique(priorities, f"Protect the live thread: {live_thread_domain} ({live_thread_status_label})")
        if live_thread_next_step:
            _append_unique(immediate_actions, f"Live thread move: {live_thread_next_step}")
        _append_unique(rationale, "The live thread is the closest thing to revenue, so it should outrank broad queue work.")
    if overnight_prospects:
        _append_unique(priorities, f"{len(overnight_prospects)} new real prospect(s) surfaced overnight")
        best = overnight_prospects[0] if isinstance(overnight_prospects[0], dict) else {}
        _append_unique(
            immediate_actions,
            "Best new prospect: {} ({}, trust {}).".format(
                best.get("merchant_domain") or "unknown",
                best.get("contact_email") or "no contact",
                int(best.get("contact_trust_score") or 0),
            ),
        )
    if overnight_queue_sharpened > 0:
        _append_unique(priorities, f"Queue sharpened overnight on {overnight_queue_sharpened} suppression event(s)")
        if top_suppression_reason:
            _append_unique(rationale, f"Top suppression reason: {top_suppression_reason}")
    if overnight_action_ready > 0:
        _append_unique(priorities, f"{overnight_action_ready} action-ready revenue step(s) are already teed up")
        _append_unique(rationale, "Overnight work only matters if it leaves a concrete move ready by morning.")

    if reachable_contacts > 0:
        _append_unique(priorities, f"Work {reachable_contacts} sendable lead(s) with verified contact paths before expanding the queue")
        _append_unique(immediate_actions, "Open the sendable queue and move the strongest verified-contact case into a real draft or send.")
        _append_unique(rationale, "A verified contact path is what turns pipeline objects into real revenue motion.")

    if contact_blocked > 0 and reachable_contacts <= 0:
        _append_unique(priorities, f"Unblock {contact_blocked} contact-blocked lead(s) before doing more broad queue review")
        _append_unique(immediate_actions, "Run contact acquisition on the strongest blocked merchant and do not count the case as pipeline until a trusted contact exists.")
        _append_unique(rationale, "Contactless opportunities are not real pipeline, so contact acquisition should outrank generic review volume.")

    if ready_to_send > 0:
        _append_unique(priorities, f"Send {ready_to_send} approved draft(s) that are already ready")
        _append_unique(immediate_actions, "Open the strongest approved draft, do one final check, and send it.")
        _append_unique(rationale, "An approved draft is closer to revenue than writing or approving another message.")

    if drafts_waiting > 0 and reachable_contacts > 0:
        _append_unique(priorities, f"Convert {drafts_waiting} approval-ready draft(s) into operator decisions")
        _append_unique(immediate_actions, "Review the strongest pending draft, tighten it if needed, then approve or reject it explicitly.")
        _append_unique(rationale, "Approval-ready drafts are the closest objects to a real send, so this is the fastest path to revenue motion.")

    if outcome_review_ready > 0:
        _append_unique(priorities, f"Capture {outcome_review_ready} high-confidence outcome candidate(s)")
        _append_unique(immediate_actions, "Apply the suggested outcome on the clearest bounce-like or dead-end replies so the learning loop stays current.")
        _append_unique(rationale, "High-confidence outcome capture is low-risk and keeps the queue from pretending dead threads are still live.")

    if replies_open > 0:
        if live_thread_present and replies_open == 1:
            _append_unique(rationale, "A live reply is closer to revenue than a cold opportunity, so reply review should outrank broad queue expansion.")
        elif live_thread_present and replies_open > 1:
            remaining_replies = replies_open - 1
            _append_unique(priorities, f"Review {remaining_replies} additional live merchant reply thread(s) behind the main live case")
            _append_unique(immediate_actions, "After the main live thread, review the next strongest merchant reply and decide whether it is active, dead-end, or ready for a draft.")
            _append_unique(rationale, "The main live reply comes first, but additional replies still need fast judgment before they go stale.")
        else:
            _append_unique(priorities, f"Review {replies_open} live merchant reply thread(s)")
            _append_unique(immediate_actions, "Open the strongest merchant reply, decide whether it is an active conversation or a dead-end, and draft the next response if the lead is still live.")
            _append_unique(rationale, "A live reply is closer to revenue than a cold opportunity, so reply review should outrank broad queue expansion.")

    if follow_ups_due > 0:
        _append_unique(priorities, f"Work the {follow_ups_due} overdue follow-up case(s)")
        _append_unique(immediate_actions, "Surface the strongest sent or replied case that now needs follow-up and decide whether to nudge, close, or mark lost.")
        _append_unique(rationale, "Follow-up is often the shortest path from existing attention to a real response.")

    if pending > 0:
        _append_unique(priorities, f"Process the {pending} pending opportunities already in the queue")
        _append_unique(immediate_actions, "Rank the top pending opportunities and convert them into approval-ready outreach or explicit rejects.")
        _append_unique(rationale, "There is already pipeline inventory waiting for action, so new discovery is less valuable than moving the queue.")

    if send_ready > 0 and drafts_waiting == 0 and ready_to_send == 0:
        _append_unique(priorities, f"Turn {send_ready} send-eligible lead(s) into approval-ready outreach")
        _append_unique(immediate_actions, "Draft outreach for the strongest send-eligible merchant instead of expanding discovery breadth.")

    if approved == 0 and pending > 0:
        _append_unique(immediate_actions, "Identify why none of the pending opportunities are being approved and surface the strongest 3 cases.")

    if leads == 0:
        _append_unique(priorities, "Restore fresh qualified lead generation")
        _append_unique(immediate_actions, "Inspect the last 24h ingestion and qualification path because no new qualified leads landed.")
        _append_unique(rationale, "A zero-lead day means the system is not replenishing the sales funnel.")

    if sent == 0 and pending > 0:
        _append_unique(immediate_actions, "Create at least one operator-reviewable outreach draft from the best current opportunity.")

    if converted == 0:
        rationale.append("There are no recorded conversions yet, so the system should optimize for a single credible win, not broad scale.")

    if bottleneck:
        priorities.append(f"Current bottleneck: {bottleneck.replace('_', ' ')}")
    if next_move:
        immediate_actions.append(f"Revenue move: {next_move}")

    for action in mission.get("next_actions", [])[:3]:
        immediate_actions.append(f"Operator-planned action: {action}")

    for blocker in mission.get("active_blockers", [])[:3]:
        rationale.append(f"Known blocker: {blocker}")

    if not priorities:
        priorities.append("Stabilize the pipeline and produce one clear value event")
    if not immediate_actions:
        immediate_actions.append("Inspect live value heartbeat and convert the biggest queue bottleneck into one concrete operator action.")

    # ── Structured Action Queue from Deal Lifecycle ──
    action_items = []
    try:
        from runtime.ops.action_queue import build_action_queue, render_execution_summary
        action_items_raw = build_action_queue(limit=10)
        action_items = render_execution_summary(action_items_raw)
    except Exception as exc:
        _mission_logger.debug("action queue unavailable: %s", exc)

    return {
        "generated_at": utc_now_iso(),
        "user_id": user_id,
        "mission": mission,
        "profile": profile,
        "brain": brain,
        "value": value,
        "priorities": priorities[:5],
        "immediate_actions": immediate_actions[:6],
        "rationale": rationale[:6],
        "action_queue": action_items,
    }


def render_execution_plan(plan: dict[str, Any]) -> str:
    lines = ["Agent Flux execution plan"]
    mission_label = (plan.get("mission") or {}).get("primary_mission") or "No primary mission set"
    lines.append(f"Mission: {mission_label}")
    lines.append("")

    # ── Structured Action Queue (if available) ──
    action_queue = plan.get("action_queue") or {}
    operator_queue = action_queue.get("operator_queue") or []
    auto_count = int(action_queue.get("auto_queue_size", 0))
    clarification_queue = action_queue.get("clarification_queue") or []

    if operator_queue:
        lines.append("Open now:")
        for item in operator_queue[:4]:
            lines.append(_render_operator_queue_line(item))
        remaining = max(0, int(action_queue.get("operator_queue_size", len(operator_queue))) - min(len(operator_queue), 4))
        if remaining > 0:
            lines.append(f"  + {remaining} more review item(s)")
        lines.append("")

    if auto_count > 0:
        lines.append(f"Auto-handling: {auto_count} step(s) in progress")
        auto_preview = action_queue.get("auto_queue_preview") or []
        for item in auto_preview[:3]:
            lines.append(_render_auto_line(item))
        if auto_count > len(auto_preview[:3]):
            lines.append(f"  + {auto_count - len(auto_preview[:3])} more background step(s)")
        lines.append("")

    if clarification_queue:
        lines.append("Needs clarification:")
        for item in clarification_queue[:3]:
            lines.append(_render_clarification_line(item))
        lines.append("")

    lines.append("Top priorities:")
    for item in plan.get("priorities", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("Immediate actions:")
    for item in plan.get("immediate_actions", []):
        lines.append(f"- {item}")
    if plan.get("rationale"):
        lines.append("")
        lines.append("Why this matters:")
        for item in plan.get("rationale", []):
            lines.append(f"- {item}")
    return "\n".join(lines)


def _plan_signature(plan: dict[str, Any]) -> str:
    action_queue = plan.get("action_queue") or {}
    operator_queue = action_queue.get("operator_queue") or []
    clarification_queue = action_queue.get("clarification_queue") or []
    auto_preview = action_queue.get("auto_queue_preview") or []
    payload = {
        "operator_queue": [
            {
                "id": item.get("opportunity_id"),
                "action": item.get("action_type"),
                "domain": item.get("merchant_domain"),
                "processor": _normalized_context_value(item.get("processor")),
                "distress": _normalized_context_value(item.get("distress_type")),
            }
            for item in operator_queue[:4]
        ],
        "clarification_queue": [
            {"id": item.get("opportunity_id"), "domain": item.get("merchant_domain")}
            for item in clarification_queue[:3]
        ],
        "auto_queue_size": int(action_queue.get("auto_queue_size", 0) or 0),
        "auto_queue_preview": [
            {
                "id": item.get("opportunity_id"),
                "action": item.get("action_type"),
                "domain": item.get("merchant_domain"),
            }
            for item in auto_preview[:3]
        ],
        "reply_review_needed": int((plan.get("value") or {}).get("reply_review_needed", 0) or 0),
        "follow_ups_due": int((plan.get("value") or {}).get("follow_ups_due", 0) or 0),
        "outreach_awaiting_approval": int((plan.get("value") or {}).get("outreach_awaiting_approval", 0) or 0),
        "outreach_ready_to_send": int((plan.get("value") or {}).get("outreach_ready_to_send", 0) or 0),
        "open_security_incidents": int((plan.get("value") or {}).get("open_security_incidents", 0) or 0),
        "top_priority": list((plan.get("priorities") or [])[:2]),
        "top_action": list((plan.get("immediate_actions") or [])[:2]),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _within_execution_update_window() -> bool:
    try:
        now_local = datetime.now(ZoneInfo(_OPERATOR_TIMEZONE))
        hour = int(now_local.hour)
    except Exception:
        hour = int(time.localtime().tm_hour)
    return _EXECUTION_UPDATE_START_HOUR <= hour <= _EXECUTION_UPDATE_END_HOUR


def _should_send_plan(
    plan: dict[str, Any],
    min_interval_seconds: int,
    unchanged_resend_interval_seconds: int,
) -> tuple[bool, str]:
    signature = _plan_signature(plan)
    last_signature = str(_redis.get(_PLAN_SEND_HASH_KEY) or "").strip()
    last_sent_at_raw = str(_redis.get(_PLAN_SEND_AT_KEY) or "").strip()
    if signature != last_signature:
        if not last_sent_at_raw:
            return True, "first_send"
        try:
            last_sent_at = float(last_sent_at_raw)
        except Exception:
            return True, "changed"
        if time.time() - last_sent_at >= max(60, int(min_interval_seconds)):
            return True, "changed"
        return False, "changed_within_min_interval"
    if not last_sent_at_raw:
        return True, "first_send"
    try:
        last_sent_at = float(last_sent_at_raw)
    except Exception:
        return True, "stale_unknown"
    if time.time() - last_sent_at >= max(300, int(unchanged_resend_interval_seconds)):
        return True, "stale"
    return False, "unchanged_within_send_interval"


def _record_sent_plan(plan: dict[str, Any]) -> None:
    _redis.set(_PLAN_SEND_HASH_KEY, _plan_signature(plan))
    _redis.set(_PLAN_SEND_AT_KEY, str(time.time()))


def _daily_briefing_sent_recently() -> bool:
    raw = str(_redis.get(_DAILY_BRIEFING_SENT_AT_KEY) or "").strip()
    if not raw:
        return False
    try:
        sent_at = float(raw)
    except Exception:
        return False
    return time.time() - sent_at < max(60, _DAILY_BRIEFING_SUPPRESSION_SECONDS)


def get_latest_execution_plan(refresh: bool = False) -> dict[str, Any]:
    if refresh:
        return run_mission_execution_loop(send_update=False)
    state = get_component_state("mission_execution")
    if state and state.get("latest_plan_json"):
        cached_plan = state.get("latest_plan_json") or {}
        return {
            "generated_at": state.get("latest_plan_at", ""),
            "rendered_plan": state.get("latest_plan_text", ""),
            "plan": cached_plan,
            "plan_json": cached_plan,
            "status": "cached",
        }
    return run_mission_execution_loop(send_update=False)


def run_mission_execution_loop(
    send_update: bool = True,
    *,
    user_id: str | None = None,
    heartbeat: dict[str, Any] | None = None,
    min_send_interval_seconds: int = _DEFAULT_PLAN_SEND_MIN_INTERVAL_SECONDS,
    unchanged_resend_interval_seconds: int = _DEFAULT_PLAN_UNCHANGED_RESEND_INTERVAL_SECONDS,
) -> dict[str, Any]:
    user_id = user_id or get_primary_operator_user_id()
    if not user_id:
        result = {
            "generated_at": utc_now_iso(),
            "status": "skipped",
            "reason": "no_primary_operator_user_configured",
        }
        record_component_state("mission_execution", ttl=900, latest_plan_at=result["generated_at"], latest_plan_text="No primary operator configured.", latest_plan_json=result)
        save_event("mission_execution_skipped", result)
        return result

    heartbeat = heartbeat or get_value_heartbeat(refresh=True)
    plan = build_execution_plan(user_id, value=heartbeat)
    rendered = render_execution_plan(plan)
    record_component_state(
        "mission_execution",
        ttl=900,
        latest_plan_at=plan["generated_at"],
        latest_plan_text=rendered,
        latest_plan_json=plan,
        mission_execution_status="healthy",
    )
    save_event("mission_execution_plan_generated", plan)
    plan_sent = False
    plan_send_reason = "send_disabled"
    if send_update:
        if _daily_briefing_sent_recently():
            plan_send_reason = "suppressed_after_daily_briefing"
        elif not _within_execution_update_window():
            plan_send_reason = "suppressed_outside_operator_hours"
        else:
            should_send, send_reason = _should_send_plan(
                plan,
                min_send_interval_seconds,
                unchanged_resend_interval_seconds,
            )
            if should_send:
                send_telegram(rendered)
                _record_sent_plan(plan)
                plan_sent = True
                plan_send_reason = send_reason
            else:
                plan_send_reason = send_reason
    record_component_state(
        "mission_execution",
        ttl=900,
        last_plan_sent_at=utc_now_iso() if plan_sent else get_component_state("mission_execution").get("last_plan_sent_at", ""),
        last_plan_send_reason=plan_send_reason,
        last_plan_sent=plan_sent,
    )
    return {
        "generated_at": plan["generated_at"],
        "status": "generated",
        "rendered_plan": rendered,
        "plan": plan,
        "sent_update": plan_sent,
        "send_reason": plan_send_reason,
    }
