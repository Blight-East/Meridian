from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from memory.structured.db import engine


def ensure_brain_tables() -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operator_brain_state (
                    user_id TEXT PRIMARY KEY,
                    state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS operator_brain_reviews (
                    id BIGSERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    review_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS operator_brain_reviews_user_created_idx
                ON operator_brain_reviews (user_id, created_at DESC)
                """
            )
        )
        conn.commit()


def _default_brain_state() -> dict[str, Any]:
    return {
        "current_revenue_loop": "",
        "strategic_bet": "",
        "main_bottleneck": "",
        "strongest_case": "",
        "next_decisive_action": "",
        "success_signal": "",
        "abort_signal": "",
        "weekly_priorities": [],
        "distractions_to_ignore": [],
        "rationale": [],
        "confidence": 0.0,
        "source": "",
    }


def _default_brain_review() -> dict[str, Any]:
    return {
        "verdict": "",
        "strengths": [],
        "risks": [],
        "distractions": [],
        "sharpened_action": "",
        "better_success_signal": "",
        "confidence": 0.0,
        "source": "",
    }


def _normalize_items(value: Any, limit: int = 8) -> list[str]:
    if not isinstance(value, list):
        return []
    items = [str(item).strip() for item in value if str(item).strip()]
    return items[:limit]


def _normalize_brain_state(state: Any) -> dict[str, Any]:
    base = _default_brain_state()
    if not isinstance(state, dict):
        return base
    merged = {**base, **state}
    for key in ("weekly_priorities", "distractions_to_ignore", "rationale"):
        merged[key] = _normalize_items(merged.get(key), limit=6)
    try:
        merged["confidence"] = max(0.0, min(1.0, float(merged.get("confidence") or 0.0)))
    except Exception:
        merged["confidence"] = 0.0
    for key in (
        "current_revenue_loop",
        "strategic_bet",
        "main_bottleneck",
        "strongest_case",
        "next_decisive_action",
        "success_signal",
        "abort_signal",
        "source",
    ):
        merged[key] = str(merged.get(key) or "").strip()
    return merged


def _normalize_brain_review(review: Any) -> dict[str, Any]:
    base = _default_brain_review()
    if not isinstance(review, dict):
        return base
    merged = {**base, **review}
    for key in ("strengths", "risks", "distractions"):
        merged[key] = _normalize_items(merged.get(key), limit=6)
    try:
        merged["confidence"] = max(0.0, min(1.0, float(merged.get("confidence") or 0.0)))
    except Exception:
        merged["confidence"] = 0.0
    for key in ("verdict", "sharpened_action", "better_success_signal", "source"):
        merged[key] = str(merged.get(key) or "").strip()
    return merged


def get_brain_state(user_id: str) -> dict[str, Any]:
    ensure_brain_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT state_json, updated_at FROM operator_brain_state WHERE user_id = :user_id"),
            {"user_id": str(user_id)},
        ).fetchone()
    if not row:
        return {**_default_brain_state(), "updated_at": None}
    state = _normalize_brain_state(row._mapping.get("state_json"))
    updated_at = row._mapping.get("updated_at")
    state["updated_at"] = updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at or "")
    return state


def save_brain_state(user_id: str, state: dict[str, Any]) -> dict[str, Any]:
    ensure_brain_tables()
    normalized = _normalize_brain_state(state)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operator_brain_state (user_id, state_json, updated_at)
                VALUES (:user_id, CAST(:state_json AS JSONB), NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET state_json = EXCLUDED.state_json, updated_at = NOW()
                """
            ),
            {"user_id": str(user_id), "state_json": json.dumps(normalized)},
        )
        conn.commit()
    return get_brain_state(user_id)


def record_brain_review(user_id: str, review: dict[str, Any]) -> dict[str, Any]:
    ensure_brain_tables()
    normalized = _normalize_brain_review(review)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO operator_brain_reviews (user_id, review_json)
                VALUES (:user_id, CAST(:review_json AS JSONB))
                RETURNING id, review_json, created_at
                """
            ),
            {"user_id": str(user_id), "review_json": json.dumps(normalized)},
        ).fetchone()
        conn.commit()
    return _serialize_brain_review_row(row)


def list_brain_reviews(user_id: str, limit: int = 5) -> dict[str, Any]:
    ensure_brain_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, review_json, created_at
                FROM operator_brain_reviews
                WHERE user_id = :user_id
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"user_id": str(user_id), "limit": max(1, int(limit))},
        ).fetchall()
    items = [_serialize_brain_review_row(row) for row in rows]
    return {"count": len(items), "items": items}


def get_latest_brain_review(user_id: str) -> dict[str, Any]:
    result = list_brain_reviews(user_id, limit=1)
    items = result.get("items") or []
    if not items:
        return {**_default_brain_review(), "id": None, "created_at": None}
    return items[0]


def _serialize_brain_review_row(row: Any) -> dict[str, Any]:
    if not row:
        return {**_default_brain_review(), "id": None, "created_at": None}
    mapping = row._mapping if hasattr(row, "_mapping") else row
    review = _normalize_brain_review(mapping.get("review_json"))
    created_at = mapping.get("created_at")
    return {
        "id": mapping.get("id"),
        "created_at": created_at.isoformat() if isinstance(created_at, datetime) else str(created_at or ""),
        **review,
    }


def render_brain_state(state: dict[str, Any]) -> str:
    lines = ["Meridian brain state"]
    lines.append(f"Revenue loop: {state.get('current_revenue_loop') or 'Not set'}")
    lines.append(f"Strategic bet: {state.get('strategic_bet') or 'Not set'}")
    lines.append(f"Bottleneck: {state.get('main_bottleneck') or 'Not set'}")
    lines.append(f"Strongest case: {state.get('strongest_case') or 'Not set'}")
    lines.append(f"Next decisive action: {state.get('next_decisive_action') or 'Not set'}")
    lines.append(f"Success signal: {state.get('success_signal') or 'Not set'}")
    lines.append(f"Abort signal: {state.get('abort_signal') or 'Not set'}")
    priorities = list(state.get("weekly_priorities") or [])
    if priorities:
        lines.append("")
        lines.append("Weekly priorities:")
        lines.extend(f"- {item}" for item in priorities[:5])
    distractions = list(state.get("distractions_to_ignore") or [])
    if distractions:
        lines.append("")
        lines.append("Ignore:")
        lines.extend(f"- {item}" for item in distractions[:5])
    rationale = list(state.get("rationale") or [])
    if rationale:
        lines.append("")
        lines.append("Why:")
        lines.extend(f"- {item}" for item in rationale[:4])
    if state.get("confidence") is not None:
        lines.append("")
        lines.append(f"Confidence: {float(state.get('confidence') or 0.0):.2f}")
    return "\n".join(lines).strip()


def render_brain_review(review: dict[str, Any]) -> str:
    lines = ["Meridian critic review"]
    lines.append(f"Verdict: {review.get('verdict') or 'unknown'}")
    if review.get("sharpened_action"):
        lines.append(f"Sharper move: {review.get('sharpened_action')}")
    if review.get("better_success_signal"):
        lines.append(f"Better success signal: {review.get('better_success_signal')}")
    strengths = list(review.get("strengths") or [])
    if strengths:
        lines.append("")
        lines.append("Strengths:")
        lines.extend(f"- {item}" for item in strengths[:4])
    risks = list(review.get("risks") or [])
    if risks:
        lines.append("")
        lines.append("Risks:")
        lines.extend(f"- {item}" for item in risks[:4])
    distractions = list(review.get("distractions") or [])
    if distractions:
        lines.append("")
        lines.append("Distractions:")
        lines.extend(f"- {item}" for item in distractions[:4])
    lines.append("")
    lines.append(f"Confidence: {float(review.get('confidence') or 0.0):.2f}")
    return "\n".join(lines).strip()


def render_brain_state_context(user_id: str) -> str:
    state = get_brain_state(user_id)
    parts: list[str] = []
    for key, label in (
        ("current_revenue_loop", "Stable brain revenue loop"),
        ("strategic_bet", "Stable brain strategic bet"),
        ("main_bottleneck", "Stable brain bottleneck"),
        ("strongest_case", "Stable brain strongest case"),
        ("next_decisive_action", "Stable brain next decisive action"),
        ("success_signal", "Stable brain success signal"),
        ("abort_signal", "Stable brain abort signal"),
    ):
        if state.get(key):
            parts.append(f"{label}: {state[key]}")
    if state.get("weekly_priorities"):
        parts.append("Stable brain weekly priorities:")
        parts.extend(f"- {item}" for item in (state.get("weekly_priorities") or [])[:4])
    if state.get("distractions_to_ignore"):
        parts.append("Stable brain distractions to ignore:")
        parts.extend(f"- {item}" for item in (state.get("distractions_to_ignore") or [])[:4])
    return "\n".join(parts).strip()
