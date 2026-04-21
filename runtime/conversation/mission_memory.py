from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from memory.structured.db import engine


def ensure_mission_memory_table() -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operator_mission_memory (
                    user_id TEXT PRIMARY KEY,
                    memory_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.commit()


def _default_memory() -> dict[str, Any]:
    return {
        "primary_mission": "",
        "active_experiments": [],
        "active_blockers": [],
        "recent_wins": [],
        "next_actions": [],
        "self_corrections": [],
    }


def _normalize(memory: Any) -> dict[str, Any]:
    base = _default_memory()
    if not isinstance(memory, dict):
        return base
    merged = {**base, **memory}
    for key in ("active_experiments", "active_blockers", "recent_wins", "next_actions", "self_corrections"):
        if not isinstance(merged.get(key), list):
            merged[key] = []
    return merged


def get_mission_memory(user_id: str) -> dict[str, Any]:
    ensure_mission_memory_table()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT memory_json, updated_at FROM operator_mission_memory WHERE user_id = :user_id"),
            {"user_id": str(user_id)},
        ).fetchone()
    if not row:
        return {**_default_memory(), "updated_at": None}
    memory = _normalize(row._mapping.get("memory_json"))
    updated_at = row._mapping.get("updated_at")
    memory["updated_at"] = updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at or "")
    return memory


def save_mission_memory(user_id: str, memory: dict[str, Any]) -> dict[str, Any]:
    ensure_mission_memory_table()
    normalized = _normalize(memory)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operator_mission_memory (user_id, memory_json, updated_at)
                VALUES (:user_id, CAST(:memory_json AS JSONB), NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET memory_json = EXCLUDED.memory_json, updated_at = NOW()
                """
            ),
            {"user_id": str(user_id), "memory_json": json.dumps(normalized)},
        )
        conn.commit()
    return get_mission_memory(user_id)


def set_primary_mission(user_id: str, mission: str) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    memory["primary_mission"] = str(mission).strip()
    return save_mission_memory(user_id, memory)


def _append_unique(memory: dict[str, Any], key: str, value: str, limit: int = 12) -> dict[str, Any]:
    current = [str(item).strip() for item in memory.get(key, []) if str(item).strip()]
    clean = str(value).strip()
    if clean and clean not in current:
        current.append(clean)
    memory[key] = current[-limit:]
    return memory


def add_experiment(user_id: str, experiment: str) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    return save_mission_memory(user_id, _append_unique(memory, "active_experiments", experiment))


def add_blocker(user_id: str, blocker: str) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    return save_mission_memory(user_id, _append_unique(memory, "active_blockers", blocker))


def add_win(user_id: str, win: str) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    return save_mission_memory(user_id, _append_unique(memory, "recent_wins", win))


def add_next_action(user_id: str, action: str) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    return save_mission_memory(user_id, _append_unique(memory, "next_actions", action))


def add_self_correction(
    user_id: str,
    *,
    issue: str,
    adjustment: str,
    why: str,
    apply_when: str = "",
    limit: int = 12,
) -> dict[str, Any]:
    memory = get_mission_memory(user_id)
    current = memory.get("self_corrections", [])
    if not isinstance(current, list):
        current = []
    item = {
        "issue": str(issue).strip(),
        "adjustment": str(adjustment).strip(),
        "why": str(why).strip(),
        "apply_when": str(apply_when).strip(),
    }
    signature = "|".join(
        [
            item["issue"].lower(),
            item["adjustment"].lower(),
            item["why"].lower(),
            item["apply_when"].lower(),
        ]
    )
    normalized = []
    seen = set()
    for row in current:
        if not isinstance(row, dict):
            continue
        row_item = {
            "issue": str(row.get("issue") or "").strip(),
            "adjustment": str(row.get("adjustment") or "").strip(),
            "why": str(row.get("why") or "").strip(),
            "apply_when": str(row.get("apply_when") or "").strip(),
        }
        row_sig = "|".join(
            [
                row_item["issue"].lower(),
                row_item["adjustment"].lower(),
                row_item["why"].lower(),
                row_item["apply_when"].lower(),
            ]
        )
        if row_sig in seen or not row_item["issue"] or not row_item["adjustment"]:
            continue
        seen.add(row_sig)
        normalized.append(row_item)
    if signature not in seen:
        normalized.append(item)
    memory["self_corrections"] = normalized[-limit:]
    return save_mission_memory(user_id, memory)


def render_mission_memory_context(user_id: str) -> str:
    memory = get_mission_memory(user_id)
    parts: list[str] = []
    if memory.get("primary_mission"):
        parts.append(f"Primary mission: {memory['primary_mission']}")
    for key, label in (
        ("active_experiments", "Active experiments"),
        ("active_blockers", "Active blockers"),
        ("recent_wins", "Recent wins"),
        ("next_actions", "Next actions"),
    ):
        items = [str(item).strip() for item in memory.get(key, []) if str(item).strip()]
        if not items:
            continue
        parts.append(f"{label}:")
        parts.extend(f"- {item}" for item in items[-5:])
    corrections = [row for row in memory.get("self_corrections", []) if isinstance(row, dict)]
    if corrections:
        parts.append("Recent self-corrections:")
        for row in corrections[-5:]:
            issue = str(row.get("issue") or "").strip()
            adjustment = str(row.get("adjustment") or "").strip()
            why = str(row.get("why") or "").strip()
            apply_when = str(row.get("apply_when") or "").strip()
            line = f"- Issue: {issue}. Adjustment: {adjustment}."
            if why:
                line = f"{line} Why: {why}."
            if apply_when:
                line = f"{line} Apply when: {apply_when}."
            parts.append(line)
    return "\n".join(parts).strip()
