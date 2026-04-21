from __future__ import annotations

import ast
import json
import os
from typing import Any

import requests
from dotenv import dotenv_values
from sqlalchemy import text

from memory.structured.db import engine, save_event
from runtime.conversation.mission_memory import get_mission_memory
from runtime.conversation.operator_profile import get_operator_profile
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.brain_state import get_brain_state, render_brain_state, save_brain_state
from runtime.ops.mission_execution import get_latest_execution_plan, get_primary_operator_user_id, run_mission_execution_loop
from runtime.ops.outreach_learning import summarize_outreach_learning
from runtime.ops.value_heartbeat import get_value_heartbeat

ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
ENV_FILE_VALUES = dotenv_values(ENV_FILE_PATH)
MODEL_NAME = "claude-sonnet-4-6"


def _extract_json_object(text: str) -> dict[str, Any]:
    candidate = (text or "").strip()
    if not candidate:
        raise RuntimeError("Strategic deliberation returned no text")
    if not (candidate.startswith("{") and candidate.endswith("}")):
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = candidate[start : end + 1]
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        parsed = ast.literal_eval(candidate)
    if not isinstance(parsed, dict):
        raise RuntimeError("Strategic deliberation returned invalid JSON")
    return parsed


def _anthropic_api_key() -> str:
    return (ENV_FILE_VALUES.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _call_model_for_json(prompt: str, max_tokens: int = 600) -> dict[str, Any]:
    api_key = _anthropic_api_key()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured")
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": MODEL_NAME,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=70,
    )
    response.raise_for_status()
    result = response.json()
    for block in result.get("content") or []:
        if block.get("type") == "text":
            return _extract_json_object(block.get("text", ""))
    raise RuntimeError("Strategic deliberation returned no text block")


def _infer_strongest_case(
    *,
    heartbeat: dict[str, Any],
    execution: dict[str, Any],
    previous_state: dict[str, Any],
) -> str:
    try:
        from runtime.ops.operator_commands import show_top_queue_opportunities_command

        top_queue = show_top_queue_opportunities_command(limit=1)
        top_cases = list(top_queue.get("top_opportunities") or [])
        if top_cases:
            case = top_cases[0]
            merchant = str(case.get("merchant_name") or case.get("merchant_domain") or "").strip()
            queue = str(case.get("queue_label") or case.get("queue") or "").strip()
            if merchant:
                return f"{merchant} ({queue})"
    except Exception:
        pass
    if int(heartbeat.get("outreach_awaiting_approval") or 0) > 0:
        return "An approval-ready outreach draft is the closest live case to revenue."
    if int(heartbeat.get("outreach_ready_to_send") or 0) > 0:
        return "An approved outreach draft is ready to send and is the closest live case to revenue."
    if int(heartbeat.get("reply_review_needed") or 0) > 0:
        return "A live merchant reply thread needs review before momentum dies."
    if int(heartbeat.get("follow_ups_due") or 0) > 0:
        return "An overdue follow-up case exists in the active pipeline."
    plan = execution.get("plan") or {}
    actions = [str(item).strip() for item in plan.get("immediate_actions", []) if str(item).strip()]
    if actions:
        return actions[0]
    return str(previous_state.get("strongest_case") or "").strip()


def _deterministic_brain_state(
    *,
    mission: dict[str, Any],
    profile: dict[str, Any],
    heartbeat: dict[str, Any],
    execution: dict[str, Any],
) -> dict[str, Any]:
    bottleneck = str(heartbeat.get("primary_bottleneck") or "").replace("_", " ").strip() or "unknown bottleneck"
    next_move = str(heartbeat.get("next_revenue_move") or "").strip() or "Create one decisive move."
    top_case = _infer_strongest_case(
        heartbeat=heartbeat,
        execution=execution,
        previous_state={},
    )
    top_case_action = ""
    try:
        from runtime.ops.operator_commands import show_top_queue_opportunities_command

        top_queue = show_top_queue_opportunities_command(limit=1)
        top_cases = list(top_queue.get("top_opportunities") or [])
        if top_cases:
            case = top_cases[0]
            merchant = str(case.get("merchant_name") or case.get("merchant_domain") or "").strip()
            next_step = str(case.get("next_step") or "").strip()
            queue = str(case.get("queue_label") or case.get("queue") or "").strip()
            if merchant and next_step:
                top_case_action = f"{merchant}: {next_step}"
            elif merchant and queue:
                top_case_action = f"Move {merchant} through the {queue} lane."
    except Exception:
        pass
    strategic_bet = ""
    if top_case:
        strategic_bet = f"Close one real conversation starting with {top_case}."
    elif mission.get("primary_mission"):
        strategic_bet = f"Translate the mission into one narrow revenue move: {mission.get('primary_mission')}"
    else:
        strategic_bet = "Reduce the main bottleneck until one narrow revenue loop can close."
    priorities = []
    if heartbeat.get("outreach_ready_to_send"):
        priorities.append("Send the strongest approved draft before creating more queue activity.")
    if heartbeat.get("outreach_awaiting_approval"):
        priorities.append("Work approval-ready drafts before broadening the queue.")
    if heartbeat.get("reply_review_needed"):
        priorities.append("Treat live replies as the highest-conviction revenue lane.")
    if heartbeat.get("send_eligible_leads"):
        priorities.append("Convert one send-eligible merchant into an approval-ready message.")
    if mission.get("next_actions"):
        priorities.extend(str(item).strip() for item in mission.get("next_actions", [])[:2] if str(item).strip())
    distractions = [
        "Broad idea generation without a revenue path.",
        "New infrastructure work that does not improve the active loop.",
    ]
    if not heartbeat.get("qualified_leads_24h"):
        distractions.append("Pretending queue quality is fine while fresh lead generation is at zero.")
    return {
        "current_revenue_loop": heartbeat.get("active_revenue_loop") or "qualify -> draft -> approve -> send -> follow_up -> close",
        "strategic_bet": strategic_bet,
        "main_bottleneck": bottleneck,
        "strongest_case": top_case or "No strong case surfaced yet.",
        "next_decisive_action": top_case_action or next_move,
        "success_signal": "One live merchant conversation advances to the next real step.",
        "abort_signal": "The active queue produces only weak or noise-like cases for multiple cycles.",
        "weekly_priorities": priorities[:5],
        "distractions_to_ignore": distractions[:5],
        "rationale": [
            f"Primary mission: {mission.get('primary_mission') or 'not set'}",
            f"Current focus: {profile.get('current_focus') or 'not set'}",
            f"Primary bottleneck: {bottleneck}",
        ],
        "confidence": 0.56,
        "source": "deterministic_fallback",
    }


def _fast_top_queue_context() -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        opportunity_id,
                        merchant_domain,
                        contact_email,
                        selected_play,
                        status,
                        approval_state,
                        updated_at
                    FROM opportunity_outreach_actions
                    WHERE outcome_status NOT IN ('won', 'lost', 'ignored')
                    ORDER BY
                        CASE
                            WHEN status = 'replied' THEN 0
                            WHEN status = 'follow_up_needed' THEN 1
                            WHEN status = 'sent' THEN 2
                            WHEN status = 'draft_ready' AND approval_state = 'approved' THEN 3
                            WHEN status = 'awaiting_approval' THEN 4
                            WHEN status = 'draft_ready' AND approval_state = 'approval_required' THEN 5
                            ELSE 6
                        END,
                        updated_at DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
        if not row:
            return {"count": 0, "top_opportunities": []}
        item = dict(row)
        status = str(item.get("status") or "").strip().lower()
        approval_state = str(item.get("approval_state") or "").strip().lower()
        queue_label = "active case"
        next_step = "Advance the strongest live case."
        if status == "replied":
            queue_label = "live reply awaiting review"
            next_step = "Review the live reply and draft the next response."
        elif status == "follow_up_needed":
            queue_label = "follow-up due"
            next_step = "Send the scheduled follow-up on the strongest live case."
        elif status == "sent":
            queue_label = "sent outreach awaiting reply"
            next_step = "Monitor the sent thread and respond immediately if a reply lands."
        elif status == "draft_ready" and approval_state == "approved":
            queue_label = "approved draft ready to send"
            next_step = "Open the approved draft, do a final check, and send it."
        elif status in {"awaiting_approval", "draft_ready"}:
            queue_label = "approval-ready draft"
            next_step = "Review the draft and approve it for send."
        merchant = str(item.get("merchant_domain") or "").strip()
        contact = str(item.get("contact_email") or "").strip()
        return {
            "count": 1,
            "top_opportunities": [
                {
                    "opportunity_id": int(item.get("opportunity_id") or 0),
                    "merchant_name": merchant,
                    "merchant_domain": merchant,
                    "contact_email": contact,
                    "recommended_action": str(item.get("selected_play") or "").strip(),
                    "queue": status or "unknown",
                    "queue_label": queue_label,
                    "next_step": next_step,
                }
            ],
        }
    except Exception:
        return {"count": 0, "top_opportunities": []}


def build_live_brain_state(user_id: str, *, heartbeat: dict[str, Any] | None = None) -> dict[str, Any]:
    mission = get_mission_memory(user_id)
    profile = get_operator_profile(user_id)
    heartbeat = heartbeat or get_value_heartbeat(refresh=False)
    top_queue_context = _fast_top_queue_context()
    top_cases = list(top_queue_context.get("top_opportunities") or [])
    top_case = top_cases[0] if top_cases else {}
    merchant = str(top_case.get("merchant_name") or top_case.get("merchant_domain") or "").strip()
    queue_label = str(top_case.get("queue_label") or "").strip()
    next_step = str(top_case.get("next_step") or "").strip()
    bottleneck = str(heartbeat.get("primary_bottleneck") or "").replace("_", " ").strip() or "unknown bottleneck"
    strongest_case = f"{merchant} ({queue_label})" if merchant and queue_label else merchant or "No strong case surfaced yet."
    strategic_bet = (
        f"Close one real conversation starting with {merchant} ({queue_label})."
        if merchant and queue_label
        else f"Translate the mission into one narrow revenue move: {mission.get('primary_mission') or 'not set'}"
    )
    priorities: list[str] = []
    if heartbeat.get("outreach_ready_to_send"):
        priorities.append("Send the strongest approved draft before creating more queue activity.")
    if heartbeat.get("reply_review_needed"):
        priorities.append("Treat live replies as the highest-conviction revenue lane.")
    if heartbeat.get("follow_ups_due"):
        priorities.append("Work the overdue follow-up before broadening the queue.")
    if heartbeat.get("outreach_awaiting_approval"):
        priorities.append("Convert approval-ready drafts into operator decisions.")
    if heartbeat.get("send_eligible_leads") and not heartbeat.get("outreach_ready_to_send"):
        priorities.append("Turn the strongest send-eligible lead into an approval-ready message.")
    result = {
        "current_revenue_loop": heartbeat.get("active_revenue_loop") or "qualify -> draft -> approve -> send -> follow_up -> close",
        "strategic_bet": strategic_bet,
        "main_bottleneck": bottleneck,
        "strongest_case": strongest_case,
        "next_decisive_action": next_step or str(heartbeat.get("next_revenue_move") or "").strip() or "Advance the strongest live case.",
        "success_signal": "One live merchant conversation advances to the next real step.",
        "abort_signal": "The active queue produces only weak or noise-like cases for multiple cycles.",
        "weekly_priorities": priorities[:5],
        "distractions_to_ignore": [
            "Broad idea generation without a revenue path.",
            "New infrastructure work that does not improve the active loop.",
        ],
        "rationale": [
            f"Primary mission: {mission.get('primary_mission') or 'not set'}",
            f"Current focus: {profile.get('current_focus') or 'not set'}",
            f"Primary bottleneck: {bottleneck}",
        ],
        "confidence": 0.6,
        "source": "live_fast_refresh",
    }
    return _apply_live_brain_guardrails(
        state=result,
        heartbeat=heartbeat,
        top_queue_context=top_queue_context,
    )


def _apply_live_brain_guardrails(
    *,
    state: dict[str, Any],
    heartbeat: dict[str, Any],
    top_queue_context: dict[str, Any],
) -> dict[str, Any]:
    corrected = dict(state or {})
    top_cases = list((top_queue_context or {}).get("top_opportunities") or [])
    top_case = top_cases[0] if top_cases else {}
    top_label = str(top_case.get("queue_label") or "").strip().lower()
    top_merchant = str(top_case.get("merchant_name") or top_case.get("merchant_domain") or "").strip()
    top_next_step = str(top_case.get("next_step") or "").strip()

    if int(heartbeat.get("outreach_ready_to_send") or 0) > 0:
        corrected["main_bottleneck"] = "send execution"
        corrected["next_decisive_action"] = top_next_step or "Open the strongest approved draft, do a final check, and send it."
        corrected["success_signal"] = "One approved draft is sent and turns into a live merchant response."
        corrected["abort_signal"] = "An approved draft sits unsent for another cycle while no live conversation advances."
        if top_merchant:
            corrected["strongest_case"] = f"{top_merchant} ({top_label or 'approved draft ready to send'})"
            corrected["strategic_bet"] = f"Close one real conversation starting with {top_merchant} ({top_label or 'approved draft ready to send'})."
        priorities = [str(item).strip() for item in corrected.get("weekly_priorities", []) if str(item).strip()]
        send_priority = "Send the strongest approved draft before creating more queue activity."
        if send_priority not in priorities:
            priorities.insert(0, send_priority)
        corrected["weekly_priorities"] = priorities[:5]
        rationale = [str(item).strip() for item in corrected.get("rationale", []) if str(item).strip()]
        send_rationale = "The live queue already contains an approved draft, so send execution is now the shortest path to a real conversation."
        if send_rationale not in rationale:
            rationale.insert(0, send_rationale)
        corrected["rationale"] = rationale[:6]

    return corrected


def deliberate_brain_state(
    user_id: str,
    *,
    heartbeat: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mission = get_mission_memory(user_id)
    profile = get_operator_profile(user_id)
    heartbeat = heartbeat or get_value_heartbeat(refresh=False)
    execution = execution or get_latest_execution_plan(refresh=False)
    learning = summarize_outreach_learning(limit=8)
    previous_state = get_brain_state(user_id)
    strongest_case = _infer_strongest_case(
        heartbeat=heartbeat,
        execution=execution,
        previous_state=previous_state,
    )
    top_queue_context: dict[str, Any] = {}
    try:
        from runtime.ops.operator_commands import show_top_queue_opportunities_command

        top_queue_context = show_top_queue_opportunities_command(limit=3)
    except Exception:
        top_queue_context = {}

    prompt = f"""You are Meridian's central strategic brain.

Your job is to decide the clearest current strategic posture for Meridian as a revenue operator.

Operator profile:
{json.dumps({
    "primary_goal": profile.get("primary_goal") or "",
    "current_focus": profile.get("current_focus") or "",
    "relationship_notes": profile.get("relationship_notes") or [],
}, ensure_ascii=True)}

Mission memory:
{json.dumps({
    "primary_mission": mission.get("primary_mission") or "",
    "active_experiments": mission.get("active_experiments") or [],
    "active_blockers": mission.get("active_blockers") or [],
    "recent_wins": mission.get("recent_wins") or [],
    "next_actions": mission.get("next_actions") or [],
}, ensure_ascii=True)}

Value heartbeat:
{json.dumps(heartbeat, ensure_ascii=True)}

Current execution plan:
{json.dumps(execution, ensure_ascii=True)}

Recent outreach learning:
{json.dumps(learning, ensure_ascii=True)}

Current top queue:
{json.dumps(top_queue_context, ensure_ascii=True)}

Previous brain state:
{json.dumps(previous_state, ensure_ascii=True)}

Rules:
- Be commercially grounded.
- Pick one narrow strategic bet, not many.
- Do not optimize for activity. Optimize for the next real revenue step.
- Explicitly say what Meridian should ignore.
- Keep it crisp and operator-useful.

Return JSON only:
{{
  "current_revenue_loop": "...",
  "strategic_bet": "...",
  "main_bottleneck": "...",
  "strongest_case": "...",
  "next_decisive_action": "...",
  "success_signal": "...",
  "abort_signal": "...",
  "weekly_priorities": ["...", "...", "..."],
  "distractions_to_ignore": ["...", "...", "..."],
  "rationale": ["...", "...", "..."],
  "confidence": 0.0
}}
"""
    try:
        result = _call_model_for_json(prompt, max_tokens=650)
        if not str(result.get("strongest_case") or "").strip():
            result["strongest_case"] = strongest_case
        result = _apply_live_brain_guardrails(
            state=result,
            heartbeat=heartbeat,
            top_queue_context=top_queue_context,
        )
        result["source"] = MODEL_NAME
        return result
    except Exception:
        result = _deterministic_brain_state(
            mission=mission,
            profile=profile,
            heartbeat=heartbeat,
            execution=execution,
        )
        return _apply_live_brain_guardrails(
            state=result,
            heartbeat=heartbeat,
            top_queue_context=top_queue_context,
        )


def get_latest_brain_state(refresh: bool = False) -> dict[str, Any]:
    user_id = get_primary_operator_user_id()
    if not user_id:
        return {"status": "error", "error": "no_primary_operator_user_configured"}
    if refresh:
        heartbeat = get_value_heartbeat(refresh=True)
        result = build_live_brain_state(user_id, heartbeat=heartbeat)
        return save_brain_state(user_id, result)
    return get_brain_state(user_id)


def run_strategic_deliberation_loop(send_update: bool = True) -> dict[str, Any]:
    user_id = get_primary_operator_user_id()
    if not user_id:
        result = {
            "generated_at": utc_now_iso(),
            "status": "skipped",
            "reason": "no_primary_operator_user_configured",
        }
        record_component_state(
            "brain",
            ttl=900,
            brain_status="skipped",
            latest_brain_state_at=result["generated_at"],
            latest_brain_error=result["reason"],
        )
        save_event("brain_state_skipped", result)
        return result

    heartbeat = get_value_heartbeat(refresh=True)
    execution = run_mission_execution_loop(send_update=False, user_id=user_id, heartbeat=heartbeat)
    state = deliberate_brain_state(user_id, heartbeat=heartbeat, execution=execution)
    saved = save_brain_state(user_id, state)
    rendered = render_brain_state(saved)
    record_component_state(
        "brain",
        ttl=900,
        brain_status="healthy",
        latest_brain_state_at=saved.get("updated_at") or utc_now_iso(),
        latest_brain_state_text=rendered,
        latest_brain_state_json=saved,
    )
    save_event("brain_state_generated", {"user_id": user_id, "brain_state": saved})
    if send_update:
        send_telegram(rendered)
    return {
        "status": "generated",
        "generated_at": saved.get("updated_at") or utc_now_iso(),
        "brain_state": saved,
        "rendered_brain_state": rendered,
    }
