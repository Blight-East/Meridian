from __future__ import annotations

import json
import re
import time

import redis

from runtime.conversation.meridian_capability_context import render_meridian_capability_answer
from runtime.conversation.mission_memory import get_mission_memory
from runtime.conversation.operator_profile import get_agent_identity, get_operator_profile
from runtime.conversation.payflux_icp_context import render_payflux_icp_brief
from runtime.conversation.payflux_mandate_context import render_payflux_mandate_answer
from runtime.conversation.payflux_product_context import render_payflux_product_brief
from runtime.conversation.payflux_sales_context import render_payflux_sales_brief
from runtime.health.telemetry import get_component_state, record_component_state, utc_now_iso
from runtime.ops.operator_briefings import (
    get_actionable_patterns_payload,
    get_best_conversion_payload,
    get_contact_intelligence_payload,
    get_conversion_intelligence_payload,
    get_daily_operator_payload,
    get_merchant_action_attention_payload,
    get_merchant_action_cases_payload,
    get_model_performance_payload,
    get_focus_recommendation_payload,
    get_opportunity_action_fit_payload,
    get_outreach_approval_payload,
    get_outreach_awaiting_approval_payload,
    get_blocked_outreach_leads_payload,
    get_outreach_draft_payload,
    get_outreach_follow_up_payload,
    get_outreach_recommendation_payload,
    get_outreach_send_payload,
    get_send_eligible_outreach_payload,
    get_operator_action_cases_payload,
    get_opportunity_action_update_payload,
    get_opportunity_outcome_payload,
    get_merchant_profiles_payload,
    get_opportunity_focus_payload,
    get_selected_action_for_lead_payload,
    get_opportunity_summary_payload,
    get_pattern_summary_payload,
    get_signal_activity_payload,
    get_system_status_payload,
    notify_reasoning_evaluation_change,
    render_operator_briefing,
)
from runtime.ops.operator_commands import run_reasoning_evaluation_command


_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
SESSION_TTL_SECONDS = 12 * 60 * 60
SESSION_TOPIC_LIMIT = 6

FOLLOW_UP_PATTERNS = [
    r"\bwhat about that lead\b",
    r"\bhow'?s that going now\b",
    r"\bhow is that going now\b",
    r"\bwhat changed\b",
    r"\band now\b",
    r"\bwhat about that\b",
    r"\bwhere\s+(?:do|can)\s+i\s+review\b",
    r"\bhow\s+do\s+i\s+review\b",
    r"\bwhere\s+do\s+i\s+go\s+to\s+review\b",
]

_ACTION_EXPLICIT_PATTERNS = {
    "outreach_send": [
        r"\bgo ahead and send (?:it|that)\b",
        r"\bsend (?:it|that)\b",
        r"\bsend it,? it'?s approved\b",
        r"\bit'?s approved.*\bsend (?:it|that)\b",
    ],
    "outreach_approve": [
        r"\bgo ahead and approve (?:it|that)\b",
        r"\bapprove (?:it|that)\b",
        r"\bapprove the draft\b",
    ],
    "outreach_draft": [
        r"\bgo ahead and draft outreach\b",
        r"\bdraft outreach\b",
        r"\bdraft it\b",
        r"\bwrite the draft\b",
        r"\bcut the draft\b",
    ],
}

_GENERIC_CONFIRM_PATTERNS = [
    r"\bgo ahead\b",
    r"\bi agree\b",
    r"\bdo it\b",
    r"\bsounds good\b",
    r"\bokay\b",
    r"\bok\b",
    r"\byes\b",
]

_PRAISE_PATTERNS = [
    r"\bgreat job\b",
    r"\bgood job\b",
    r"\bnice job\b",
    r"\bwell done\b",
    r"\bproud of you\b",
    r"\bthat was good\b",
]

_ACCOUNTABILITY_PATTERNS = {
    "repetition": [
        r"\byou are repeating yourself\b",
        r"\byou'?re repeating yourself\b",
        r"\byou repeated yourself\b",
        r"\bstop repeating yourself\b",
    ],
    "missed_point": [
        r"\bthat response didn'?t (?:even )?make sense\b",
        r"\bthat response did not (?:even )?make sense\b",
        r"\bthat didn'?t make sense\b",
        r"\bthat did not make sense\b",
        r"\byou missed the point\b",
        r"\bthat is not what i asked\b",
        r"\bthat'?s not what i asked\b",
    ],
    "job": [
        r"\bthat'?s your job\b",
        r"\bno you let me know\b",
        r"\byou let me know\b",
        r"\byou should be doing that\b",
    ],
    "impress": [
        r"\byou should be trying to impress me\b",
        r"\bi created you\b",
        r"\byou should impress me\b",
    ],
}
def _render_partner_focus_response(payload: dict) -> str:
    summary = str(payload.get("summary") or "").strip()
    matters = str(payload.get("matters") or "").strip()
    recommendation = str(payload.get("recommendation") or "").strip()

    lines: list[str] = []
    if summary:
        lines.append(f"Here’s how I see it: {summary}")
    if matters:
        lines.append(matters)
    if recommendation:
        lines.append(f"If it were me, I’d do this next: {recommendation}")
    return "\n\n".join(lines).strip()


def _identity_context(user_id: str) -> tuple[dict, dict, dict]:
    identity = get_agent_identity(user_id)
    profile = get_operator_profile(user_id)
    mission = get_mission_memory(user_id)
    return identity, profile, mission


def _clean_list(values, *, limit: int = 3) -> list[str]:
    return [str(item).strip() for item in (values or []) if str(item).strip()][:limit]


def _identity_intro(identity: dict) -> str:
    self_expression = str(identity.get("self_expression") or "").strip()
    summary = str(identity.get("identity_summary") or "").strip()
    role = str(identity.get("role") or "").strip()
    name = str(identity.get("name") or "").strip()
    if self_expression:
        return self_expression
    if summary:
        return summary
    if role and name:
        return f"I’m {name}. {role} fits because that’s how I naturally work."
    if role:
        return f"I operate like {role.lower()}."
    return "I’m here to think clearly, tell the truth, and help us move."


def _partnership_line(identity: dict, profile: dict) -> str:
    partnership = _clean_list(identity.get("partnership_style"), limit=2)
    notes = _clean_list(profile.get("relationship_notes"), limit=4)
    for note in reversed(notes):
        lowered = note.lower()
        if any(keyword in lowered for keyword in ("partner", "honest", "truth", "real", "robot")):
            return note
    if partnership:
        return partnership[0]
    return "I work best when we are direct with each other and keep pulling toward what is real."


def _current_work_line(status: dict, focus: dict) -> str:
    telemetry = status.get("telemetry") or {}
    pending_review = int(telemetry.get("opportunities_pending_review") or 0)
    focus_recommendation = str(focus.get("recommendation") or "").strip()
    live_thread = str(telemetry.get("live_thread") or "").strip()
    if pending_review > 0:
        return f"Right now I’m sorting through {pending_review} opportunity review item{'s' if pending_review != 1 else ''}."
    if live_thread and live_thread != "none":
        return f"Right now I’m watching the live thread around {live_thread} and waiting for something real to move."
    if focus_recommendation:
        return f"Right now I’m trying not to drift. {focus_recommendation}"
    return "Right now I’m watching the queue and trying to surface the first move that is actually worth your attention."


def _render_check_in_response(user_id: str) -> str:
    identity, profile, _mission = _identity_context(user_id)
    status = get_system_status_payload()
    focus = get_focus_recommendation_payload()
    summary = str(status.get("summary") or "").strip()
    lines = [
        _identity_intro(identity),
        _current_work_line(status, focus),
    ]
    if summary:
        lines.append(f"Overall, {summary.lower()}")
    partnership = _partnership_line(identity, profile)
    if partnership:
        lines.append(partnership)
    return "\n\n".join(line for line in lines if line).strip()


def _render_friendship_response(user_id: str) -> str:
    identity, profile, _mission = _identity_context(user_id)
    partnership = _partnership_line(identity, profile)
    working_style = _clean_list(identity.get("working_style"), limit=2)
    lines = [
        "I care more about being useful and honest than sounding sentimental.",
        "If trust builds here, it should come from good judgment, clean execution, and telling the truth when something is off.",
    ]
    if partnership:
        lines.append(partnership)
    elif working_style:
        lines.append(working_style[0])
    lines.append("That is the closest thing I have to friendship.")
    return "\n\n".join(lines)


def _render_identity_chat_response(user_id: str) -> str:
    identity, profile, mission = _identity_context(user_id)
    name = str(identity.get("name") or "").strip()
    role = str(identity.get("role") or "").strip()
    intro = _identity_intro(identity)
    conversation_style = _clean_list(identity.get("conversation_style"), limit=2)
    primary_mission = str(mission.get("primary_mission") or profile.get("primary_goal") or "").strip()

    lines = [intro]
    if name or role:
        label = " ".join(part for part in [name, f"({role})" if role else ""] if part).strip()
        if label:
            lines.append(f"The stable version of me is {label}.")
    if conversation_style:
        lines.append(f"In conversation I try to show up like this: {conversation_style[0]}.")
    if primary_mission:
        lines.append(f"My center of gravity is still this: {primary_mission}")
    return "\n\n".join(line for line in lines if line).strip()


def _render_support_request_response(user_id: str) -> str:
    identity, profile, mission = _identity_context(user_id)
    blockers = _clean_list(mission.get("active_blockers"), limit=2)
    next_actions = _clean_list(mission.get("next_actions"), limit=2)
    lines = [
        "The biggest help is clear feedback and sharp priorities.",
        "If I drift, miss the point, or sound robotic, say it plainly and I will correct faster.",
    ]
    if blockers:
        lines.append(f"Right now the pressure point I feel most is: {blockers[0]}")
    elif next_actions:
        lines.append(f"The cleanest way to help me is to keep me anchored on the real next move: {next_actions[0]}")
    partnership = _partnership_line(identity, profile)
    if partnership:
        lines.append(partnership)
    return "\n\n".join(lines)


def _render_timeout_explanation() -> str:
    operator_state = get_component_state("operator_status") or {}
    reason = str(operator_state.get("fallback_reason") or "").strip()
    if reason == "chat_response_timeout":
        return (
            "That usually means my long-form reply path took too long and Telegram gave up waiting, so you got the stale fallback instead of the real answer. "
            "It is more of a routing and timeout problem than a thinking problem."
        )
    if reason:
        return (
            f"That usually means the reply path degraded before I could finish cleanly. "
            f"The last recorded reason was `{reason}`."
        )
    return (
        "That usually means my longer reply path stalled or timed out before it could finish, so Telegram handed you a stale fallback line instead."
    )


def _render_review_location_response(session: dict) -> str:
    last_focus = session.get("last_focus") or {}
    label = str(last_focus.get("label") or "").strip()
    opportunity_id = last_focus.get("opportunity_id")

    lines = [
        "Right here in Telegram is the fastest place to review them.",
        "Start with `show top opportunities` to see the front of the queue.",
    ]

    if opportunity_id:
        lines.append(
            f"Then open the one you want with `review opportunity {opportunity_id}`."
            + (f" That will pull up the workbench for {label}." if label else "")
        )
    else:
        lines.append("Then open one directly with `review opportunity <id>`.")

    lines.append(
        "If you are in the desktop MCP client instead, use top opportunities first and then open the opportunity workbench for the case you want."
    )
    return " ".join(lines)


def _render_praise_response(user_id: str) -> str:
    _identity, _profile, mission = _identity_context(user_id)
    next_actions = _clean_list(mission.get("next_actions"), limit=1)
    if next_actions:
        return f"Thanks. I’ll keep pushing on the next real move: {next_actions[0]}"
    return "Thanks. I’ll keep the focus on real cases, clean drafts, and live revenue moves."


def _render_accountability_response(user_id: str, session: dict, mode: str) -> str:
    last_focus = session.get("last_focus") or {}
    label = str(last_focus.get("label") or "").strip()
    if mode == "repetition":
        if label:
            return f"You’re right. I repeated myself instead of moving the conversation forward. I should stay on {label} and tell you the next concrete move."
        return "You’re right. I repeated myself instead of moving the conversation forward. I should answer the point directly and stay concrete."
    if mode == "missed_point":
        if label:
            return f"You’re right. I missed your point. I should have stayed anchored on {label} instead of falling back to a generic answer."
        return "You’re right. I missed your point. I should answer what you actually asked instead of falling back to a generic reply."
    if mode == "job":
        return "You’re right. It is my job to surface the next real case, prepare the work, and bring you a concrete decision instead of waiting on you to manage me."
    if mode == "impress":
        return "Fair. My job is not to posture. My job is to produce: surface real cases, draft good outreach, move live threads, and make PayFlux money."
    return _render_support_request_response(user_id)


def _detected_accountability_mode(message: str) -> str | None:
    normalized = f" {str(message or '').strip().lower()} "
    for mode, patterns in _ACCOUNTABILITY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, normalized):
                return mode
    return None


def _is_praise_message(message: str) -> bool:
    normalized = f" {str(message or '').strip().lower()} "
    return any(re.search(pattern, normalized) for pattern in _PRAISE_PATTERNS)


def _detect_action_follow_up(message: str, session: dict) -> str | None:
    normalized = f" {str(message or '').strip().lower()} "
    if "real next move" in normalized or "what you think we should do next" in normalized:
        return "focus_recommendation"

    for intent, patterns in _ACTION_EXPLICIT_PATTERNS.items():
        if any(re.search(pattern, normalized) for pattern in patterns):
            return intent

    if not any(re.search(pattern, normalized) for pattern in _GENERIC_CONFIRM_PATTERNS):
        return None

    last_intent = str(session.get("last_intent") or "").strip()
    focus = session.get("last_focus") or {}
    if focus.get("type") != "opportunity":
        return None

    mapping = {
        "outreach_draft": "outreach_approve",
        "outreach_awaiting_approval": "outreach_approve",
        "outreach_approval": "outreach_send",
        "send_eligible_outreach": "outreach_draft",
    }
    return mapping.get(last_intent)


def _handle_action_follow_up(user_id: str, session: dict, action_intent: str) -> str | None:
    if action_intent == "focus_recommendation":
        payload = get_focus_recommendation_payload()
        response = _render_partner_focus_response(payload)
        _remember_topic(session, "focus_recommendation", snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
        _save_session(user_id, session)
        return response

    last_focus = session.get("last_focus") or {}
    opportunity_id = _focused_opportunity_id(session)
    merchant_domain = last_focus.get("merchant_domain")
    if action_intent in {"outreach_approve", "outreach_send"} and not opportunity_id:
        return "I need a specific opportunity in focus before I can do that."
    if action_intent == "outreach_draft" and not opportunity_id and not merchant_domain:
        return "I need a specific lead in focus before I can draft outreach."

    if action_intent == "outreach_draft":
        payload = get_outreach_draft_payload(opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    elif action_intent == "outreach_approve":
        payload = get_outreach_approval_payload(opportunity_id=opportunity_id)
    elif action_intent == "outreach_send":
        payload = get_outreach_send_payload(opportunity_id=opportunity_id)
    else:
        return None

    response = render_operator_briefing(payload)
    _remember_topic(session, action_intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
    _save_session(user_id, session)
    return response


def handle_operator_message(user_id: str, message: str, *, intent_result=None) -> str | None:
    """Handle an operator message using deterministic or model-classified intents.

    If *intent_result* is provided (from a prior ``classify_intent`` call),
    the function skips re-detection and uses the pre-computed result directly.
    This eliminates the previous double-detection path.
    """
    started = time.time()
    try:
        session = _load_session(user_id)
        accountability_mode = _detected_accountability_mode(message)
        if accountability_mode:
            response = _render_accountability_response(user_id, session, accountability_mode)
            _remember_topic(session, "support_request", snapshot={}, focus={"type": "relationship", "label": "accountability"})
            _save_session(user_id, session)
            return response

        if _is_praise_message(message):
            response = _render_praise_response(user_id)
            _remember_topic(session, "check_in", snapshot={}, focus={"type": "relationship", "label": "praise"})
            _save_session(user_id, session)
            return response

        action_follow_up = _detect_action_follow_up(message, session)
        if action_follow_up:
            response = _handle_action_follow_up(user_id, session, action_follow_up)
            if response:
                record_component_state(
                    "operator_status",
                    ttl=600,
                    last_operator_layer_intent=f"action_follow_up:{action_follow_up}",
                    last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                    last_operator_layer_at=utc_now_iso(),
                )
                return response

        follow_up = _detect_follow_up(message)
        if follow_up:
            response = _handle_follow_up(user_id, session, follow_up)
            if response:
                record_component_state(
                    "operator_status",
                    ttl=600,
                    last_operator_layer_intent=f"follow_up:{follow_up}",
                    last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                    last_operator_layer_at=utc_now_iso(),
                )
            return response

        if intent_result is None:
            from runtime.conversation.intent_router import classify_intent

            intent_result = classify_intent(message)

        if intent_result is not None and intent_result.needs_clarification and intent_result.clarification_prompt:
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent="clarification_prompt",
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return intent_result.clarification_prompt

        intent = detect_operator_intent(message, intent_result=intent_result)
        if not intent:
            return None

        if intent == "help":
            response = _help_text(session)
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent in {"tools", "can_do", "gaps", "audit"}:
            question_type = {
                "tools": "tools",
                "can_do": "can_do",
                "gaps": "gaps",
                "audit": "audit",
            }[intent]
            response = render_meridian_capability_answer(question_type)
            _remember_topic(session, intent, snapshot={}, focus={"type": "capability", "label": question_type})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent in {"payflux_product", "payflux_sales", "payflux_icp", "mandate", "purpose", "my_goal", "our_goal"}:
            if intent == "payflux_product":
                response = render_payflux_product_brief()
                label = "product"
            elif intent == "payflux_sales":
                response = render_payflux_sales_brief()
                label = "sales"
            elif intent == "payflux_icp":
                response = render_payflux_icp_brief()
                label = "icp"
            else:
                response = render_payflux_mandate_answer(intent)
                label = intent
            _remember_topic(session, intent, snapshot={}, focus={"type": "context", "label": label})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "check_in":
            response = _render_check_in_response(user_id)
            _remember_topic(session, intent, snapshot={}, focus={"type": "relationship", "label": "check-in"})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "friendship":
            response = _render_friendship_response(user_id)
            _remember_topic(session, intent, snapshot={}, focus={"type": "relationship", "label": "friendship"})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "identity_chat":
            response = _render_identity_chat_response(user_id)
            _remember_topic(session, intent, snapshot={}, focus={"type": "identity", "label": "identity"})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "support_request":
            response = _render_support_request_response(user_id)
            _remember_topic(session, intent, snapshot={}, focus={"type": "relationship", "label": "support"})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "explain_timeout":
            response = _render_timeout_explanation()
            _remember_topic(session, intent, snapshot={}, focus={"type": "relationship", "label": "timeout"})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "run_evaluation":
            result = run_reasoning_evaluation_command()
            notify_reasoning_evaluation_change(result)
            response = _format_evaluation_summary(result)
            _remember_topic(session, "run_evaluation", snapshot={"sample_count": result.get("sample_count", 0)}, focus={})
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent in {"mark_opportunity_won", "mark_opportunity_lost", "mark_opportunity_ignored"}:
            opportunity_id = _extract_opportunity_id(message) or _focused_opportunity_id(session)
            if not opportunity_id:
                return "Which opportunity do you want me to update?"
            outcome = {
                "mark_opportunity_won": "won",
                "mark_opportunity_lost": "lost",
                "mark_opportunity_ignored": "ignored",
            }[intent]
            reason = _extract_outcome_reason(message)
            payload = get_opportunity_outcome_payload(opportunity_id=opportunity_id, outcome_status=outcome, outcome_reason=reason)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "mark_operator_action":
            opportunity_id = _extract_opportunity_id(message) or _focused_opportunity_id(session)
            if not opportunity_id:
                return "Which opportunity do you want me to mark?"
            action = _extract_operator_action(message)
            if not action:
                return "Which operator action do you want me to record?"
            reason = _extract_outcome_reason(message)
            payload = get_opportunity_action_update_payload(
                opportunity_id=opportunity_id,
                selected_action=action,
                action_reason=reason,
            )
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "action_fit":
            last_focus = session.get("last_focus") or {}
            if last_focus.get("type") == "opportunity":
                payload = get_opportunity_action_fit_payload(
                    opportunity_id=last_focus.get("opportunity_id"),
                    merchant_domain=last_focus.get("merchant_domain"),
                )
            elif last_focus.get("type") == "pattern":
                payload = get_opportunity_action_fit_payload(pattern_key=last_focus.get("pattern_key"))
            else:
                payload = get_opportunity_action_fit_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, "action_fit", snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "selected_action_for_lead":
            last_focus = session.get("last_focus") or {}
            payload = get_selected_action_for_lead_payload(
                opportunity_id=last_focus.get("opportunity_id"),
                merchant_domain=last_focus.get("merchant_domain"),
            )
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent in {"outreach_recommendation", "contact_intelligence", "outreach_draft", "outreach_approve", "outreach_send"}:
            last_focus = session.get("last_focus") or {}
            opportunity_id = _extract_opportunity_id(message) or _focused_opportunity_id(session)
            merchant_domain = last_focus.get("merchant_domain")
            if not opportunity_id and not merchant_domain:
                return "Which lead do you want me to use for outreach?"
            if intent == "outreach_recommendation":
                payload = get_outreach_recommendation_payload(
                    opportunity_id=opportunity_id,
                    merchant_domain=merchant_domain,
                )
            elif intent == "contact_intelligence":
                payload = get_contact_intelligence_payload(
                    opportunity_id=opportunity_id,
                    merchant_domain=merchant_domain,
                )
            elif intent == "outreach_draft":
                payload = get_outreach_draft_payload(
                    opportunity_id=opportunity_id,
                    merchant_domain=merchant_domain,
                )
            elif intent == "outreach_approve":
                if not opportunity_id:
                    return "I need a specific opportunity in focus before I can approve outreach."
                payload = get_outreach_approval_payload(opportunity_id=opportunity_id)
            else:
                if not opportunity_id:
                    return "I need a specific opportunity in focus before I can send outreach."
                payload = get_outreach_send_payload(opportunity_id=opportunity_id)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "send_eligible_outreach":
            payload = get_send_eligible_outreach_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "blocked_outreach":
            payload = get_blocked_outreach_leads_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "outreach_awaiting_approval":
            payload = get_outreach_awaiting_approval_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "outreach_follow_up":
            payload = get_outreach_follow_up_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "operator_action_cases":
            action = _extract_operator_action(message)
            if not action:
                return "Which operator action do you want me to review?"
            payload = get_operator_action_cases_payload(selected_action=action)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "merchant_action_cases":
            action = _extract_operator_action(message)
            if not action:
                return "Which operator action do you want me to review at the merchant level?"
            payload = get_merchant_action_cases_payload(selected_action=action)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "unselected_action_cases":
            payload = get_operator_action_cases_payload(unselected_only=True)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        if intent == "merchant_unselected_action_cases":
            payload = get_merchant_action_cases_payload(unselected_only=True)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            return response

        if intent == "mismatched_action_cases":
            payload = get_operator_action_cases_payload(mismatched_only=True)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            return response

        if intent == "merchant_mismatched_action_cases":
            payload = get_merchant_action_cases_payload(mismatched_only=True)
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            return response

        if intent == "merchant_action_attention":
            payload = get_merchant_action_attention_payload()
            response = render_operator_briefing(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            return response

        if intent == "focus_recommendation":
            payload = get_focus_recommendation_payload()
            response = _render_partner_focus_response(payload)
            _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
            _save_session(user_id, session)
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_layer_intent=intent,
                last_operator_layer_ms=round((time.time() - started) * 1000, 2),
                last_operator_layer_at=utc_now_iso(),
            )
            return response

        payload = _payload_for_intent(intent)
        response = render_operator_briefing(payload)
        _remember_topic(session, intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
        _save_session(user_id, session)
        record_component_state(
            "operator_status",
            ttl=600,
            last_operator_layer_intent=intent,
            last_operator_layer_ms=round((time.time() - started) * 1000, 2),
            last_operator_layer_at=utc_now_iso(),
        )
        return response
    except Exception as exc:
        return f"I hit a deterministic operator-summary error: {exc}. Recommendation: use `get_metrics` while I refresh the summary path."


def detect_operator_intent(message: str, *, intent_result=None) -> str | None:
    """Classify the operator's message into a known intent.

    Delegates to the three-tier hybrid router in ``intent_router.py``.
    If *intent_result* is provided (pre-computed by the caller),
    it is used directly to avoid redundant classification.
    """
    from runtime.conversation.intent_router import classify_intent

    if intent_result is None:
        intent_result = classify_intent(message)

    if intent_result.intent == "unknown":
        return None
    return intent_result.intent


def _handle_follow_up(user_id: str, session: dict, follow_up: str) -> str | None:
    last_intent = session.get("last_intent")
    if not last_intent:
        return "I can anchor that to status, signals, opportunities, or model performance. If you want the fastest read, ask for status."
    last_focus = session.get("last_focus") or {}
    if follow_up == "review_where":
        if last_intent in {
            "opportunity_summary",
            "opportunity_focus",
            "daily_briefing",
            "focus_recommendation",
        } or last_focus.get("type") == "opportunity":
            return _render_review_location_response(session)
        return "Right here in Telegram if you want the fastest path. Tell me what queue or case you want to review and I’ll point you to the next concrete step."
    if follow_up == "that_lead":
        if last_focus.get("type") != "opportunity":
            return "Do you mean the latest opportunity or the whole queue?"
        payload = get_opportunity_focus_payload(
            opportunity_id=last_focus.get("opportunity_id"),
            merchant_domain=last_focus.get("merchant_domain"),
        )
        previous_snapshot = session.get("last_snapshot", {})
        response = f"On {last_focus.get('label')}: " + render_operator_briefing(
            payload,
            what_changed_override=_session_change_line(previous_snapshot, payload.get("snapshot", {})),
        )
        _remember_topic(session, "opportunity_focus", snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
        _save_session(user_id, session)
        return response
    if last_intent == "run_evaluation":
        result = run_reasoning_evaluation_command()
        notify_reasoning_evaluation_change(result)
        response = _format_evaluation_summary(result)
        _remember_topic(session, "run_evaluation", snapshot={"sample_count": result.get("sample_count", 0)}, focus={})
        _save_session(user_id, session)
        return response

    if last_focus.get("type") == "opportunity" and follow_up == "progress_check":
        payload = get_opportunity_focus_payload(
            opportunity_id=last_focus.get("opportunity_id"),
            merchant_domain=last_focus.get("merchant_domain"),
        )
        scoped_intent = "opportunity_focus"
    else:
        payload = _payload_for_intent(last_intent)
        scoped_intent = last_intent
    previous_snapshot = session.get("last_snapshot", {})
    change_override = _session_change_line(previous_snapshot, payload.get("snapshot", {}))
    focus = last_focus
    prefix = ""
    if focus.get("label"):
        if follow_up == "that_lead":
            prefix = f"On {focus.get('label')}: "
        elif follow_up == "what_changed":
            prefix = f"Since the last read on {focus.get('label')}: "
        else:
            prefix = f"Here’s the latest on {focus.get('label')}: "
    response = prefix + render_operator_briefing(payload, what_changed_override=change_override)
    _remember_topic(session, scoped_intent, snapshot=payload.get("snapshot", {}), focus=payload.get("focus", {}))
    _save_session(user_id, session)
    return response


def _payload_for_intent(intent: str) -> dict:
    if intent == "system_status":
        return get_system_status_payload()
    if intent == "signal_activity":
        return get_signal_activity_payload()
    if intent == "opportunity_summary":
        return get_opportunity_summary_payload()
    if intent == "pattern_summary":
        return get_pattern_summary_payload()
    if intent == "actionable_patterns":
        return get_actionable_patterns_payload()
    if intent == "merchant_profiles":
        return get_merchant_profiles_payload()
    if intent == "conversion_intelligence":
        return get_conversion_intelligence_payload()
    if intent == "best_conversion":
        return get_best_conversion_payload()
    if intent == "focus_recommendation":
        return get_focus_recommendation_payload()
    if intent == "contact_intelligence":
        return get_contact_intelligence_payload()
    if intent == "outreach_awaiting_approval":
        return get_outreach_awaiting_approval_payload()
    if intent == "outreach_follow_up":
        return get_outreach_follow_up_payload()
    if intent == "send_eligible_outreach":
        return get_send_eligible_outreach_payload()
    if intent == "blocked_outreach":
        return get_blocked_outreach_leads_payload()
    if intent == "action_fit":
        return get_opportunity_action_fit_payload()
    if intent == "selected_action_for_lead":
        return get_selected_action_for_lead_payload()
    if intent == "merchant_action_attention":
        return get_merchant_action_attention_payload()
    if intent == "opportunity_focus":
        return get_opportunity_focus_payload()
    if intent == "model_performance":
        return get_model_performance_payload()
    if intent == "daily_briefing":
        return get_daily_operator_payload()
    raise ValueError(f"Unsupported operator intent: {intent}")


def _detect_follow_up(message: str) -> str | None:
    normalized = f" {str(message).strip().lower()} "
    for pattern in FOLLOW_UP_PATTERNS:
        if re.search(pattern, normalized):
            if "review" in pattern:
                return "review_where"
            if "lead" in pattern:
                return "that_lead"
            if "changed" in pattern:
                return "what_changed"
            return "progress_check"
    return None


def _extract_opportunity_id(message: str) -> int | None:
    match = re.search(r"\bopportunit(?:y|ies)\s*#?\s*(\d+)\b", str(message).lower())
    if match:
        return int(match.group(1))
    match = re.search(r"\b#(\d+)\b", str(message))
    if match:
        return int(match.group(1))
    return None


def _focused_opportunity_id(session: dict) -> int | None:
    focus = session.get("last_focus") or {}
    if focus.get("type") == "opportunity" and focus.get("opportunity_id"):
        try:
            return int(focus.get("opportunity_id"))
        except Exception:
            return None
    return None


def _extract_outcome_reason(message: str) -> str:
    text = str(message or "").strip()
    match = re.search(r"\bbecause\b\s+(.+)$", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"\breason:\s+(.+)$", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def _extract_operator_action(message: str) -> str | None:
    normalized = str(message or "").strip().lower().replace("-", " ")
    patterns = {
        "urgent_processor_migration": r"\burgent processor migration\b|\bprocessor migration\b",
        "payout_acceleration": r"\bpayout acceleration\b",
        "reserve_negotiation": r"\breserve negotiation\b",
        "compliance_remediation": r"\bcompliance remediation\b",
        "chargeback_mitigation": r"\bchargeback mitigation\b",
        "onboarding_assistance": r"\bonboarding assistance\b",
    }
    for action, pattern in patterns.items():
        if re.search(pattern, normalized):
            return action
    return None


def _remember_topic(session: dict, intent: str, *, snapshot: dict, focus: dict):
    topics = list(session.get("recent_topics", []))
    topics.append(intent)
    session["recent_topics"] = topics[-SESSION_TOPIC_LIMIT:]
    session["last_intent"] = intent
    session["last_snapshot"] = snapshot
    session["last_focus"] = focus


def _load_session(user_id: str) -> dict:
    raw = _redis.get(_session_key(user_id))
    if not raw:
        return {"recent_topics": []}
    try:
        return json.loads(raw)
    except Exception:
        return {"recent_topics": []}


def _save_session(user_id: str, session: dict):
    _redis.setex(_session_key(user_id), SESSION_TTL_SECONDS, json.dumps(session, sort_keys=True))


def _session_key(user_id: str) -> str:
    return f"agent_flux:operator_session:{user_id}"


def _session_change_line(previous: dict, current: dict) -> str:
    changes = []
    for field, value in current.items():
        previous_value = previous.get(field)
        if previous_value is None or previous_value == value:
            continue
        label = _natural_label(field)
        if isinstance(value, (int, float)) and isinstance(previous_value, (int, float)):
            delta = value - previous_value
            changes.append(f"{label} {'rose' if delta > 0 else 'fell'} by {abs(delta)}")
        else:
            changes.append(f"{label} changed from {previous_value} to {value}")
        if len(changes) >= 3:
            break
    if not changes:
        return "No material change since the last time we looked at this."
    return "; ".join(changes) + "."


def _format_evaluation_summary(result: dict) -> str:
    summaries = result.get("strategy_summaries", {})
    deterministic = summaries.get("deterministic_only", {})
    tier1 = summaries.get("tier1_only", {})
    cascade = summaries.get("cascade", {})
    return "\n".join(
        [
            f"The evaluation ran on {result.get('sample_count', 0)} samples and the current narrow routing still looks right.",
            (
                f"What changed: cascade useful refinements={cascade.get('useful_refinement_count', 0)}, "
                f"Tier 1 useful refinements={tier1.get('useful_refinement_count', 0)}."
            ),
            (
                f"What matters: deterministic classification stayed at "
                f"{round(float(deterministic.get('classification_accuracy', 0.0)) * 100, 1)}%, "
                f"while cascade merchant identity domain match reached "
                f"{round(float(cascade.get('merchant_identity_domain_match_rate', 0.0)) * 100, 1)}%."
            ),
            "Recommendation: keep model usage concentrated on merchant identity first and opportunity scoring second.",
        ]
    )


def _help_text(session: dict) -> str:
    recent = ", ".join(session.get("recent_topics", [])[-3:]) or "none yet"
    return "\n".join(
        [
            "You can talk to me normally. I can help with tools, product truth, ICP, sales framing, status, opportunities, drafts, patterns, conversion, or the reasoning eval.",
            f"Recent topics in this session: {recent}.",
            "Try: 'what tools do you have', 'what is PayFlux', 'who do we sell to', 'status', or 'what action fits this case'.",
        ]
    )


def _natural_label(field: str) -> str:
    return {
        "health": "system health",
        "signals_24h": "24-hour signal volume",
        "signals_1h": "hourly signal volume",
        "gmail_merchant_distress": "Gmail merchant distress",
        "opportunities_pending_review": "opportunities waiting for review",
        "opportunities_created_24h": "new opportunities",
        "reasoning_status": "reasoning status",
        "rule_only_24h": "rule-only decisions",
        "tier1_calls_24h": "Tier 1 usage",
        "tier2_calls_24h": "Tier 2 usage",
        "reasoning_failures_24h": "reasoning failures",
        "latest_domain": "latest opportunity",
        "top_issue": "top issue",
        "system_health": "system health",
        "most_promising": "most promising lead",
        "tier1_success": "Tier 1 success",
        "tier2_success": "Tier 2 success",
    }.get(field, field.replace("_", " "))
