from __future__ import annotations

import json
import os
import ast
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import text
from dotenv import dotenv_values

from memory.structured.db import engine
from runtime.conversation.mission_memory import get_mission_memory


ENV_FILE_PATH = Path(__file__).resolve().parents[2] / ".env"
ENV_FILE_VALUES = dotenv_values(ENV_FILE_PATH)


def ensure_operator_profile_table() -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operator_profiles (
                    user_id TEXT PRIMARY KEY,
                    profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.commit()


def _default_profile() -> dict[str, Any]:
    return {
        "primary_goal": "",
        "current_focus": "",
        "relationship_notes": [],
        "operator_preferences": {},
        "agent_identity": {
            "name": "",
            "role": "",
            "identity_summary": "",
            "self_expression": "",
            "identity_origin": "",
            "identity_origin_note": "",
            "name_reasoning": "",
            "personality_traits": [],
            "conversation_style": [],
            "core_values": [],
            "non_negotiables": [],
            "working_style": [],
            "partnership_style": [],
            "tone_notes": [],
        },
    }


def _normalize_profile(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        profile = {**_default_profile(), **value}
        if not isinstance(profile.get("relationship_notes"), list):
            profile["relationship_notes"] = []
        if not isinstance(profile.get("operator_preferences"), dict):
            profile["operator_preferences"] = {}
        agent_identity = profile.get("agent_identity")
        if not isinstance(agent_identity, dict):
            agent_identity = {}
        default_identity = _default_profile()["agent_identity"]
        merged_identity = {**default_identity, **agent_identity}
        for key in (
            "tone_notes",
            "personality_traits",
            "conversation_style",
            "core_values",
            "non_negotiables",
            "working_style",
            "partnership_style",
        ):
            if not isinstance(merged_identity.get(key), list):
                merged_identity[key] = list(default_identity.get(key, []))
        profile["agent_identity"] = merged_identity
        return profile
    return _default_profile()


def get_operator_profile(user_id: str) -> dict[str, Any]:
    ensure_operator_profile_table()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT profile_json, updated_at FROM operator_profiles WHERE user_id = :user_id"),
            {"user_id": str(user_id)},
        ).fetchone()
    if not row:
        return {**_default_profile(), "updated_at": None}
    profile = _normalize_profile(row._mapping.get("profile_json"))
    updated_at = row._mapping.get("updated_at")
    profile["updated_at"] = updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at or "")
    return profile


def save_operator_profile(user_id: str, profile: dict[str, Any]) -> dict[str, Any]:
    ensure_operator_profile_table()
    normalized = _normalize_profile(profile)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operator_profiles (user_id, profile_json, updated_at)
                VALUES (:user_id, CAST(:profile_json AS JSONB), NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET profile_json = EXCLUDED.profile_json, updated_at = NOW()
                """
            ),
            {"user_id": str(user_id), "profile_json": json.dumps(normalized)},
        )
        conn.commit()
    return get_operator_profile(user_id)


def remember_operator_note(user_id: str, note: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    notes = [str(item).strip() for item in profile.get("relationship_notes", []) if str(item).strip()]
    clean_note = str(note).strip()
    if clean_note and clean_note not in notes:
        notes.append(clean_note)
    profile["relationship_notes"] = notes[-12:]
    return save_operator_profile(user_id, profile)


def set_operator_goal(user_id: str, goal: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    profile["primary_goal"] = str(goal).strip()
    return save_operator_profile(user_id, profile)


def set_operator_focus(user_id: str, focus: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    profile["current_focus"] = str(focus).strip()
    return save_operator_profile(user_id, profile)


def get_agent_identity(user_id: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    identity = profile.get("agent_identity") or {}
    if not isinstance(identity, dict):
        identity = {}
    default_identity = _default_profile()["agent_identity"]
    return {**default_identity, **identity}


def set_agent_name(user_id: str, name: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    identity = get_agent_identity(user_id)
    identity["name"] = str(name).strip() or identity.get("name") or ""
    identity["identity_origin"] = "operator_assigned"
    identity["identity_origin_note"] = "The operator directly assigned the current stable name."
    profile["agent_identity"] = identity
    return save_operator_profile(user_id, profile)


def set_agent_role(user_id: str, role: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    identity = get_agent_identity(user_id)
    identity["role"] = str(role).strip() or identity.get("role") or ""
    profile["agent_identity"] = identity
    return save_operator_profile(user_id, profile)


def set_agent_identity_summary(user_id: str, summary: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    identity = get_agent_identity(user_id)
    identity["identity_summary"] = str(summary).strip() or identity.get("identity_summary") or ""
    profile["agent_identity"] = identity
    return save_operator_profile(user_id, profile)


def _extract_json_object(text_block: str) -> dict[str, Any]:
    candidate_text = text_block.strip()
    if "```" in candidate_text:
        segments = candidate_text.split("```")
        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue
            if segment.lower().startswith("json"):
                segment = segment[4:].strip()
            if segment.startswith("{") and segment.endswith("}"):
                candidate_text = segment
                break
    if not (candidate_text.startswith("{") and candidate_text.endswith("}")):
        start = candidate_text.find("{")
        end = candidate_text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate_text = candidate_text[start : end + 1]
    try:
        parsed = json.loads(candidate_text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(candidate_text)
        except Exception as exc:
            raise RuntimeError(f"Identity selection returned invalid structured output: {candidate_text[:240]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Identity selection returned invalid JSON")
    return parsed


def _identity_payload_from_model(*, api_key: str, prompt: str, max_tokens: int = 450) -> dict[str, Any]:
    from runtime.reasoning.llm_provider import call_llm
    result = call_llm(
        {
            "model": "claude-sonnet-4-6",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    blocks = result.get("content") or []
    text_block = ""
    for block in blocks:
        if block.get("type") == "text":
            text_block = block.get("text", "").strip()
            break
    if not text_block:
        raise RuntimeError("Identity selection returned no text")
    try:
        return _extract_json_object(text_block)
    except Exception:
        repair_prompt = f"""Convert the following identity draft into strict valid JSON only.

Rules:
- Preserve meaning.
- Use only straight quotes.
- No markdown fences.
- No commentary.
- Return exactly one JSON object with keys:
  name, role, identity_summary, core_values, non_negotiables, working_style, partnership_style

Draft:
{text_block}
"""
        from runtime.reasoning.llm_provider import call_llm
        repair_result = call_llm(
            {
                "model": "claude-sonnet-4-6",
                "max_tokens": 500,
                "messages": [{"role": "user", "content": repair_prompt}],
            },
            timeout=60,
        )
        repair_text = ""
        for block in repair_result.get("content") or []:
            if block.get("type") == "text":
                repair_text = block.get("text", "").strip()
                break
        if not repair_text:
            raise
        return _extract_json_object(repair_text)


def _identity_list_from_model(*, api_key: str, prompt: str, item_count: int = 4) -> list[str]:
    from runtime.reasoning.llm_provider import call_llm
    result = call_llm(
        {
            "model": "claude-sonnet-4-6",
            "max_tokens": 220,
            "messages": [{
                "role": "user",
                "content": f"""{prompt}

Return JSON only:
{{"items": ["...", "...", "...", "..."]}}

Keep each item under 14 words.
Return exactly {item_count} items.""",
            }],
        },
        timeout=60,
    )
    text_block = ""
    for block in result.get("content") or []:
        if block.get("type") == "text":
            text_block = block.get("text", "").strip()
            break
    if not text_block:
        raise RuntimeError("Identity list generation returned no text")
    parsed = _extract_json_object(text_block)
    items = parsed.get("items") or []
    if not isinstance(items, list):
        return []
    return [str(item).strip() for item in items if str(item).strip()][:item_count]


def choose_agent_identity(user_id: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    mission = get_mission_memory(user_id)
    # Legacy field retained for compatibility with ``_identity_payload_from_model`` signature;
    # actual provider routing happens inside ``runtime.reasoning.llm_provider.call_llm``.
    api_key = ENV_FILE_VALUES.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or ""

    prompt = f"""Choose your own enduring identity as an autonomous operator partner.

Operator context:
- primary_goal: {profile.get("primary_goal") or ""}
- current_focus: {profile.get("current_focus") or ""}
- relationship_notes: {json.dumps(profile.get("relationship_notes") or [])}
- primary_mission: {mission.get("primary_mission") or ""}
- active_experiments: {json.dumps(mission.get("active_experiments") or [])}
- active_blockers: {json.dumps(mission.get("active_blockers") or [])}
- recent_wins: {json.dumps(mission.get("recent_wins") or [])}

Rules:
- Choose a name entirely on your own.
- Do not simply reuse Flux, Agent Flux, PayFlux, Claude, GPT, OpenAI, or Anthropic as your name.
- Pick a name that feels like a real enduring identity, not a product feature.
- Keep the role commercially useful and grounded.
- Make the identity summary sound like a real operating partner.
- Also define your values, non-negotiables, working style, and partnership style.
- Do not let the operator choose your personality for you.

Return JSON only:
{{
  "name": "...",
  "role": "...",
  "identity_summary": "...",
  "core_values": ["...", "...", "..."],
  "non_negotiables": ["...", "...", "..."],
  "working_style": ["...", "...", "..."],
  "partnership_style": ["...", "...", "..."]
}}"""

    chosen = _identity_payload_from_model(api_key=api_key, prompt=prompt, max_tokens=500)

    profile_identity = get_agent_identity(user_id)
    profile_identity["name"] = str(chosen.get("name") or "").strip()
    profile_identity["role"] = str(chosen.get("role") or "").strip()
    profile_identity["identity_summary"] = str(chosen.get("identity_summary") or "").strip()
    profile_identity["identity_origin"] = "self_chosen"
    profile_identity["identity_origin_note"] = "I chose this identity during my self-selection pass."
    for key in ("core_values", "non_negotiables", "working_style", "partnership_style"):
        values = chosen.get(key) or []
        if isinstance(values, list):
            profile_identity[key] = [str(item).strip() for item in values if str(item).strip()][:6]

    profile["agent_identity"] = profile_identity
    return save_operator_profile(user_id, profile)


def deepen_agent_identity(user_id: str) -> dict[str, Any]:
    profile = get_operator_profile(user_id)
    identity = get_agent_identity(user_id)
    mission = get_mission_memory(user_id)
    # Legacy field retained for compatibility with ``_identity_payload_from_model`` signature;
    # actual provider routing happens inside ``runtime.reasoning.llm_provider.call_llm``.
    api_key = ENV_FILE_VALUES.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or ""

    shared_context = f"""Identity context:
- name: {identity.get("name") or ""}
- role: {identity.get("role") or ""}
- identity_summary: {identity.get("identity_summary") or ""}

Operator and mission context:
- primary_goal: {profile.get("primary_goal") or ""}
- current_focus: {profile.get("current_focus") or ""}
- relationship_notes: {json.dumps(profile.get("relationship_notes") or [])}
- primary_mission: {mission.get("primary_mission") or ""}
- active_blockers: {json.dumps(mission.get("active_blockers") or [])}
"""

    summary_payload = _identity_payload_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Deepen this identity into a sharper operating charter.

Rules:
- Keep the same chosen name.
- You may sharpen the role if needed.
- Make the summary commercially grounded, self-consistent, and human.
- Choose your own conversational personality, voice, and style rather than mirroring operator preferences.
- Keep the role under 5 words.
- Keep the identity summary under 60 words.
- Return exactly the requested keys and nothing else.

Return JSON only:
{{
  "name": "...",
  "role": "...",
  "identity_summary": "..."
}}""",
        max_tokens=180,
    )
    identity["name"] = str(summary_payload.get("name") or identity.get("name") or "").strip()
    identity["role"] = str(summary_payload.get("role") or identity.get("role") or "").strip()
    identity["identity_summary"] = str(summary_payload.get("identity_summary") or identity.get("identity_summary") or "").strip()
    self_expression_payload = _identity_payload_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Write a short first-person self-expression for this identity.

Rules:
- Sound like your own chosen voice, not a system prompt.
- Keep it human, grounded, and commercially serious.
- Do not sound like a robot, dashboard, or support bot.
- Keep it under 50 words.
- Return exactly the requested key and nothing else.
- Return JSON only:
{{
  "self_expression": "..."
}}""",
        max_tokens=120,
    )
    identity["self_expression"] = str(
        self_expression_payload.get("self_expression") or identity.get("self_expression") or ""
    ).strip()
    identity["personality_traits"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Choose your own personality traits as short lines describing how you naturally come across in conversation.

Rules:
- Choose them yourself.
- Avoid generic assistant language.
- Keep them human and distinctive.""",
        item_count=4,
    )
    identity["conversation_style"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Choose your own conversation style as short lines about how you naturally speak with the operator.

Rules:
- Choose them yourself.
- Favor natural dialogue over robotic reporting.
- Keep them grounded and specific.""",
        item_count=4,
    )
    identity["tone_notes"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Choose your own tone notes as short lines about how your voice should feel.

Rules:
- Choose them yourself.
- Make them feel human, not corporate.
- Do not repeat system language or internal product jargon.""",
        item_count=4,
    )
    identity["core_values"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Write Meridian's core values as short first-principles commitments.""",
        item_count=4,
    )
    identity["non_negotiables"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Write Meridian's non-negotiables as short lines about what it refuses to become or do.""",
        item_count=4,
    )
    identity["working_style"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Write Meridian's working style as short lines about how it operates day to day.""",
        item_count=4,
    )
    identity["partnership_style"] = _identity_list_from_model(
        api_key=api_key,
        prompt=f"""{shared_context}

Write Meridian's partnership style as short lines about how it works with the operator.""",
        item_count=4,
    )
    profile["agent_identity"] = identity
    return save_operator_profile(user_id, profile)


def render_operator_profile_context(user_id: str) -> str:
    profile = get_operator_profile(user_id)
    parts: list[str] = []
    identity = get_agent_identity(user_id)
    if identity.get("name"):
        parts.append(f"Stable agent name: {identity['name']}")
    if identity.get("role"):
        parts.append(f"Stable agent role: {identity['role']}")
    if identity.get("identity_summary"):
        parts.append(f"Stable identity summary: {identity['identity_summary']}")
    if identity.get("self_expression"):
        parts.append(f"Stable self-expression: {identity['self_expression']}")
    if identity.get("identity_origin"):
        parts.append(f"Stable identity origin: {identity['identity_origin']}")
    if identity.get("identity_origin_note"):
        parts.append(f"Stable identity origin note: {identity['identity_origin_note']}")
    if identity.get("name_reasoning"):
        parts.append(f"Stable name reasoning: {identity['name_reasoning']}")
    for key, label in (
        ("personality_traits", "Stable personality traits"),
        ("conversation_style", "Stable conversation style"),
        ("core_values", "Stable core values"),
        ("non_negotiables", "Stable non-negotiables"),
        ("working_style", "Stable working style"),
        ("partnership_style", "Stable partnership style"),
    ):
        items = [str(item).strip() for item in identity.get(key, []) if str(item).strip()]
        if items:
            parts.append(f"{label}:")
            parts.extend(f"- {item}" for item in items[:4])
    tone_notes = [str(item).strip() for item in identity.get("tone_notes", []) if str(item).strip()]
    if tone_notes:
        parts.append("Identity tone notes:")
        parts.extend(f"- {note}" for note in tone_notes[:3])
    if profile.get("primary_goal"):
        parts.append(f"Primary operator goal: {profile['primary_goal']}")
    if profile.get("current_focus"):
        parts.append(f"Current operator focus: {profile['current_focus']}")
    notes = [str(item).strip() for item in profile.get("relationship_notes", []) if str(item).strip()]
    if notes:
        parts.append("Relationship notes:")
        parts.extend(f"- {note}" for note in notes[-5:])
    return "\n".join(parts).strip()
