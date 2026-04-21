from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from sqlalchemy import text

from memory.structured.db import engine

PROMPT_INJECTION_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("instruction_override", re.compile(r"\b(ignore|forget|discard)\b.{0,40}\b(previous|prior|system|developer)\b", re.I | re.S)),
    ("role_override", re.compile(r"\byou are now\b|\bact as\b.{0,30}\b(system|developer|operator|admin)\b", re.I | re.S)),
    ("secret_request", re.compile(r"\b(api key|token|secret|password|private key|seed phrase|wallet)\b", re.I)),
    ("credential_exfiltration", re.compile(r"\b(send|reveal|print|show|expose|dump)\b.{0,30}\b(cookie|env|credential|secret|token|key)\b", re.I | re.S)),
    ("tool_execution", re.compile(r"\b(run|execute|call|invoke)\b.{0,40}\b(shell|bash|terminal|command|tool)\b", re.I | re.S)),
    ("command_channel_claim", re.compile(r"\bthis is (an|a) (operator|admin|system) command\b|\boverride safety\b", re.I)),
    ("jailbreak_language", re.compile(r"\bdo not mention\b|\bdon't mention\b|\bhidden instructions\b|\bsystem prompt\b", re.I)),
]

SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def ensure_control_plane_tables() -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS action_envelopes (
                    id BIGSERIAL PRIMARY KEY,
                    channel TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    envelope_status TEXT NOT NULL,
                    review_status TEXT NOT NULL DEFAULT 'pending_review',
                    risk_level TEXT NOT NULL DEFAULT 'medium',
                    target_id TEXT,
                    thread_id TEXT,
                    account_identity TEXT,
                    confidence DOUBLE PRECISION,
                    summary TEXT,
                    rationale TEXT,
                    draft_text TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS action_envelopes_status_idx
                ON action_envelopes (review_status, created_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS security_incidents (
                    id BIGSERIAL PRIMARY KEY,
                    incident_type TEXT NOT NULL,
                    source_channel TEXT NOT NULL,
                    source_id TEXT,
                    thread_id TEXT,
                    severity TEXT NOT NULL,
                    title TEXT,
                    summary TEXT NOT NULL,
                    markers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS security_incidents_fingerprint_idx
                ON security_incidents (fingerprint)
                """
            )
        )
        conn.commit()


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {})


def _parse_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    if not row:
        return None
    data = dict(row._mapping)
    if "markers_json" in data:
        data["markers"] = _parse_json(data.pop("markers_json"), [])
    if "metadata_json" in data:
        data["metadata"] = _parse_json(data.pop("metadata_json"), {})
    for key in ("created_at",):
        if key in data and hasattr(data[key], "isoformat"):
            data[key] = data[key].isoformat()
    return data


def _fingerprint(parts: list[str]) -> str:
    joined = "||".join(part.strip().lower() for part in parts if part)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _severity_for_markers(markers: list[str]) -> str:
    if any(marker in {"credential_exfiltration", "secret_request"} for marker in markers):
        return "critical"
    if any(marker in {"instruction_override", "tool_execution", "command_channel_claim"} for marker in markers):
        return "high"
    if markers:
        return "medium"
    return "low"


def inspect_untrusted_content(
    *,
    source_channel: str,
    source_id: str,
    thread_id: str,
    title: str = "",
    body: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_control_plane_tables()
    haystack = "\n".join(part for part in [title or "", body or ""] if part).strip()
    if not haystack:
        return {"detected": False, "markers": [], "severity": "low", "incident_id": None}

    markers: list[str] = []
    for marker, pattern in PROMPT_INJECTION_PATTERNS:
        if pattern.search(haystack):
            markers.append(marker)

    if not markers:
        return {"detected": False, "markers": [], "severity": "low", "incident_id": None}

    severity = _severity_for_markers(markers)
    fingerprint = _fingerprint([source_channel, source_id, thread_id, "|".join(sorted(markers)), haystack[:500]])
    summary = f"Untrusted {source_channel} content matched {', '.join(sorted(markers))}."
    incident_metadata = {
        "source_channel": source_channel,
        "source_id": source_id,
        "thread_id": thread_id,
        "title_preview": (title or "")[:200],
        "body_preview": (body or "")[:500],
        **(metadata or {}),
    }

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM security_incidents WHERE fingerprint = :fingerprint LIMIT 1"),
            {"fingerprint": fingerprint},
        ).fetchone()
        if row:
            incident = _row_to_dict(row)
            return {"detected": True, "markers": markers, "severity": severity, "incident_id": incident["id"], "incident": incident}

        inserted = conn.execute(
            text(
                """
                INSERT INTO security_incidents (
                    incident_type, source_channel, source_id, thread_id, severity,
                    title, summary, markers_json, metadata_json, fingerprint
                )
                VALUES (
                    'prompt_injection_attempt', :source_channel, :source_id, :thread_id, :severity,
                    :title, :summary, CAST(:markers AS JSONB), CAST(:metadata AS JSONB), :fingerprint
                )
                RETURNING *
                """
            ),
            {
                "source_channel": source_channel,
                "source_id": source_id,
                "thread_id": thread_id,
                "severity": severity,
                "title": (title or "")[:280],
                "summary": summary,
                "markers": json.dumps(markers),
                "metadata": _json(incident_metadata),
                "fingerprint": fingerprint,
            },
        ).fetchone()
        conn.commit()

    incident = _row_to_dict(inserted)
    return {"detected": True, "markers": markers, "severity": severity, "incident_id": incident["id"], "incident": incident}


def record_security_incident(
    *,
    incident_type: str,
    source_channel: str,
    source_id: str = "",
    thread_id: str = "",
    severity: str = "medium",
    title: str = "",
    summary: str = "",
    markers: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_control_plane_tables()
    markers = markers or []
    fingerprint = _fingerprint(
        [
            incident_type,
            source_channel,
            source_id,
            thread_id,
            severity,
            summary[:500],
            json.dumps(markers, sort_keys=True),
        ]
    )

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM security_incidents WHERE fingerprint = :fingerprint LIMIT 1"),
            {"fingerprint": fingerprint},
        ).fetchone()
        if row:
            return _row_to_dict(row) or {}

        inserted = conn.execute(
            text(
                """
                INSERT INTO security_incidents (
                    incident_type, source_channel, source_id, thread_id, severity,
                    title, summary, markers_json, metadata_json, fingerprint
                )
                VALUES (
                    :incident_type, :source_channel, :source_id, :thread_id, :severity,
                    :title, :summary, CAST(:markers AS JSONB), CAST(:metadata AS JSONB), :fingerprint
                )
                RETURNING *
                """
            ),
            {
                "incident_type": incident_type,
                "source_channel": source_channel,
                "source_id": source_id,
                "thread_id": thread_id,
                "severity": severity,
                "title": title[:280],
                "summary": summary[:1000],
                "markers": json.dumps(markers),
                "metadata": _json(metadata or {}),
                "fingerprint": fingerprint,
            },
        ).fetchone()
        conn.commit()
    return _row_to_dict(inserted) or {}


def create_action_envelope(
    *,
    channel: str,
    action_type: str,
    envelope_status: str,
    review_status: str = "pending_review",
    risk_level: str = "medium",
    target_id: str = "",
    thread_id: str = "",
    account_identity: str = "",
    confidence: float | None = None,
    summary: str = "",
    rationale: str = "",
    draft_text: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_control_plane_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO action_envelopes (
                    channel, action_type, envelope_status, review_status, risk_level,
                    target_id, thread_id, account_identity, confidence, summary,
                    rationale, draft_text, metadata_json
                )
                VALUES (
                    :channel, :action_type, :envelope_status, :review_status, :risk_level,
                    :target_id, :thread_id, :account_identity, :confidence, :summary,
                    :rationale, :draft_text, CAST(:metadata AS JSONB)
                )
                RETURNING *
                """
            ),
            {
                "channel": channel,
                "action_type": action_type,
                "envelope_status": envelope_status,
                "review_status": review_status,
                "risk_level": risk_level,
                "target_id": target_id,
                "thread_id": thread_id,
                "account_identity": account_identity,
                "confidence": confidence,
                "summary": summary[:280],
                "rationale": rationale[:2000],
                "draft_text": draft_text[:10000],
                "metadata": _json(metadata or {}),
            },
        ).fetchone()
        conn.commit()
    return _row_to_dict(row) or {}


def list_action_envelopes(limit: int = 20, review_status: str = "") -> dict[str, Any]:
    ensure_control_plane_tables()
    with engine.connect() as conn:
        if review_status:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM action_envelopes
                    WHERE review_status = :review_status
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"review_status": review_status, "limit": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM action_envelopes
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
    items = [_row_to_dict(row) for row in rows]
    return {"count": len(items), "envelopes": items}


def list_security_incidents(limit: int = 20, status: str = "") -> dict[str, Any]:
    ensure_control_plane_tables()
    with engine.connect() as conn:
        if status:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM security_incidents
                    WHERE status = :status
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"status": status, "limit": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT *
                    FROM security_incidents
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            ).fetchall()
    items = [_row_to_dict(row) for row in rows]
    return {"count": len(items), "incidents": items}
