from __future__ import annotations

import ast
import json
import os
import re
from typing import Any

import requests
from dotenv import dotenv_values

from memory.structured.db import save_event
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.brain_state import (
    get_brain_state,
    get_latest_brain_review,
    record_brain_review,
    render_brain_review,
)
from runtime.ops.mission_execution import get_primary_operator_user_id, get_latest_execution_plan
from runtime.ops.outreach_learning import summarize_outreach_learning
from runtime.ops.value_heartbeat import get_value_heartbeat

ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
ENV_FILE_VALUES = dotenv_values(ENV_FILE_PATH)
MODEL_NAME = "claude-sonnet-4-6"


def _extract_json_object(text: str) -> dict[str, Any]:
    candidate = (text or "").strip()
    if not candidate:
        raise RuntimeError("Critic review returned no text")
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
        raise RuntimeError("Critic review returned invalid JSON")
    return parsed


def _anthropic_api_key() -> str:
    return (ENV_FILE_VALUES.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _call_model_for_json(prompt: str, max_tokens: int = 500) -> dict[str, Any]:
    from runtime.reasoning.llm_provider import call_llm
    result = call_llm(
        {
            "model": MODEL_NAME,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=70,
    )
    for block in result.get("content") or []:
        if block.get("type") == "text":
            return _extract_json_object(block.get("text", ""))
    raise RuntimeError("Critic review returned no text block")


def _deterministic_review(
    *,
    brain_state: dict[str, Any],
    heartbeat: dict[str, Any],
) -> dict[str, Any]:
    risks: list[str] = []
    strengths: list[str] = []
    distractions: list[str] = []
    if heartbeat.get("reply_review_needed"):
        strengths.append("Live replies are present, which is closer to revenue than cold queue expansion.")
    if heartbeat.get("outreach_awaiting_approval"):
        strengths.append("Approval-ready drafts already exist, so the system is close to a send.")
    if not heartbeat.get("qualified_leads_24h"):
        risks.append("Fresh qualified lead generation is at zero, so the pipeline may be starving.")
    if str(brain_state.get("next_decisive_action") or "").lower().startswith("create"):
        distractions.append("Broad creation work can become fake progress if a live conversation already exists.")
    distractions.extend(list(brain_state.get("distractions_to_ignore") or [])[:3])
    verdict = "aligned" if strengths and not risks else "fragile"
    sharpened = brain_state.get("next_decisive_action") or heartbeat.get("next_revenue_move") or "Move the best live case forward."
    return {
        "verdict": verdict,
        "strengths": strengths[:4],
        "risks": risks[:4],
        "distractions": distractions[:4],
        "sharpened_action": sharpened,
        "better_success_signal": brain_state.get("success_signal") or "A live case advances one real step.",
        "confidence": 0.52,
        "source": "deterministic_fallback",
    }


def _deterministic_draft_review(
    *,
    brain_state: dict[str, Any],
    draft: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    body = str(draft.get("body") or "")
    subject = str(draft.get("subject") or "")
    distress = str(context.get("distress_type") or "").replace("_", " ").strip()
    merchant = str(context.get("merchant_name_display") or context.get("merchant_name") or context.get("merchant_domain") or "").strip()
    strategic_bet = str(brain_state.get("strategic_bet") or "").lower()

    weak_points: list[str] = []
    generic_signals: list[str] = []
    sharpen: list[str] = []
    strategic_alignment = "aligned"
    strategic_reason = "The draft supports the current live case."

    lowered_body = body.lower()
    lowered_subject = subject.lower()
    distress_tokens = {
        token.strip()
        for token in re.split(r"[\s_/]+", distress)
        if token.strip()
    }
    distress_mentioned = not distress_tokens or all(token in lowered_body for token in distress_tokens)
    if distress == "chargeback issue" and "chargeback pressure" in lowered_body:
        distress_mentioned = True
    if distress == "payouts delayed" and "payout delay" in lowered_body:
        distress_mentioned = True
    if distress == "reserve hold" and ("reserve" in lowered_body or "hold pressure" in lowered_body):
        distress_mentioned = True
    if distress == "verification review" and ("review pressure" in lowered_body or "account review" in lowered_body):
        distress_mentioned = True

    if merchant and merchant.lower() not in lowered_body and merchant.lower() not in lowered_subject:
        generic_signals.append("The draft does not name the merchant directly.")
        sharpen.append("Anchor the opening line to the merchant name so it feels specific.")
    if distress and not distress_mentioned:
        generic_signals.append("The draft does not clearly name the distress pattern.")
        sharpen.append(f"Reference the {distress} signal directly instead of speaking in generalities.")
    if body.count("?") > 2:
        weak_points.append("The draft asks too many questions for an initial operator-style touch.")
        sharpen.append("Reduce the ask to one or two sharp questions.")
    if len(body.split()) > 150:
        weak_points.append("The draft is longer than it needs to be for first contact.")
        sharpen.append("Tighten the body so the value proposition lands faster.")
    if "if helpful" in lowered_body or ("can send back" in lowered_body and "reply with the current blocker" not in lowered_body):
        generic_signals.append("The close is polite but soft and may undersell the operator value.")
        sharpen.append("End with a firmer next-step ask tied to the live issue.")
    if strategic_bet and merchant and merchant.lower() not in strategic_bet:
        strategic_alignment = "partial"
        strategic_reason = "The draft may be useful, but it is not obviously tied to Meridian's current strongest case."

    verdict = "strong"
    if generic_signals or weak_points:
        verdict = "needs_sharpening"
    if strategic_alignment == "partial" and verdict == "strong":
        verdict = "watch_alignment"

    return {
        "verdict": verdict,
        "weak_points": weak_points[:4],
        "generic_signals": generic_signals[:4],
        "sharpen": sharpen[:4],
        "strategic_alignment": strategic_alignment,
        "strategic_alignment_reason": strategic_reason,
        "recommended_action": "tighten_before_send" if (weak_points or generic_signals) else "ready_if_operator_agrees",
        "confidence": 0.56,
        "source": "deterministic_fallback",
    }


def review_brain_state(user_id: str) -> dict[str, Any]:
    brain_state = get_brain_state(user_id)
    heartbeat = get_value_heartbeat(refresh=False)
    execution = get_latest_execution_plan(refresh=False)
    learning = summarize_outreach_learning(limit=8)

    prompt = f"""You are Meridian's internal critic.

Your job is to sharpen Meridian's current strategic brain state without becoming vague, overly cautious, or theoretical.

Current brain state:
{json.dumps(brain_state, ensure_ascii=True)}

Current value heartbeat:
{json.dumps(heartbeat, ensure_ascii=True)}

Current execution plan:
{json.dumps(execution, ensure_ascii=True)}

Recent learning:
{json.dumps(learning, ensure_ascii=True)}

Rules:
- Be direct.
- Flag drift, distraction, fake progress, or weak commercial logic.
- Keep the sharpened action specific.
- Do not invent strategy unrelated to the active loop.

Return JSON only:
{{
  "verdict": "aligned|fragile|drifting|stalled",
  "strengths": ["...", "..."],
  "risks": ["...", "..."],
  "distractions": ["...", "..."],
  "sharpened_action": "...",
  "better_success_signal": "...",
  "confidence": 0.0
}}
"""
    try:
        result = _call_model_for_json(prompt, max_tokens=550)
        result["source"] = MODEL_NAME
        return result
    except Exception:
        return _deterministic_review(
            brain_state=brain_state,
            heartbeat=heartbeat,
        )


def review_outreach_draft(user_id: str, *, draft: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    brain_state = get_brain_state(user_id)
    context = context or {}

    prompt = f"""You are Meridian's internal outreach critic.

Your job is to critique one approval-ready draft before it is shown or approved.

Current brain state:
{json.dumps(brain_state, ensure_ascii=True)}

Draft metadata:
{json.dumps({
    "merchant": draft.get("merchant_domain") or context.get("merchant_domain"),
    "selected_play": draft.get("selected_play") or context.get("selected_play"),
    "outreach_type": draft.get("outreach_type") or "",
    "why_now": draft.get("why_now") or context.get("why_now"),
    "distress_type": context.get("distress_type"),
    "processor": context.get("processor"),
}, ensure_ascii=True)}

Draft:
{json.dumps({
    "subject": draft.get("subject") or "",
    "body": draft.get("body") or "",
}, ensure_ascii=True)}

Rules:
- Be direct and commercially honest.
- Flag what is weak, generic, soft, or off-strategy.
- If the draft is strong, say so plainly.
- Keep sharpening advice concrete.
- Return JSON only.

Return JSON only:
{{
  "verdict": "strong|needs_sharpening|off_strategy|watch_alignment",
  "weak_points": ["...", "..."],
  "generic_signals": ["...", "..."],
  "sharpen": ["...", "..."],
  "strategic_alignment": "aligned|partial|misaligned",
  "strategic_alignment_reason": "...",
  "recommended_action": "ready_if_operator_agrees|tighten_before_send|rewrite_before_approval",
  "confidence": 0.0
}}
"""
    try:
        result = _call_model_for_json(prompt, max_tokens=550)
        result["source"] = MODEL_NAME
        return result
    except Exception:
        return _deterministic_draft_review(
            brain_state=brain_state,
            draft=draft,
            context=context,
        )


def run_brain_critic_loop(send_update: bool = True) -> dict[str, Any]:
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
            brain_critic_status="skipped",
            latest_brain_review_at=result["generated_at"],
            latest_brain_review_error=result["reason"],
        )
        save_event("brain_review_skipped", result)
        return result

    review = review_brain_state(user_id)
    saved = record_brain_review(user_id, review)
    rendered = render_brain_review(saved)
    record_component_state(
        "brain",
        ttl=900,
        brain_critic_status="healthy",
        latest_brain_review_at=saved.get("created_at") or utc_now_iso(),
        latest_brain_review_text=rendered,
        latest_brain_review_json=saved,
    )
    save_event("brain_review_generated", {"user_id": user_id, "brain_review": saved})
    if send_update:
        send_telegram(rendered)
    return {
        "status": "generated",
        "generated_at": saved.get("created_at") or utc_now_iso(),
        "brain_review": saved,
        "rendered_brain_review": rendered,
    }


def show_latest_brain_review(refresh: bool = False) -> dict[str, Any]:
    user_id = get_primary_operator_user_id()
    if not user_id:
        return {"status": "error", "error": "no_primary_operator_user_configured"}
    if refresh:
        result = run_brain_critic_loop(send_update=False)
        return result.get("brain_review") or {}
    return get_latest_brain_review(user_id)
