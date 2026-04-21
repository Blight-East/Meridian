from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime

from sqlalchemy import text

from memory.structured.db import engine


VALID_CHANNELS = {"reddit", "gmail"}
VALID_MODES = {"dry_run", "approval_required", "auto_send_high_confidence"}


def _json(value):
    return json.dumps({} if value is None else value)


def _parse_json(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _row_to_dict(row):
    if not row:
        return None
    data = dict(row._mapping)
    for key in ("allowlist_json", "denylist_json", "config_json", "metadata_json", "distress_signals_json"):
        if key in data:
            fallback = [] if "list" in key or key == "distress_signals_json" else {}
            data[key] = _parse_json(data[key], fallback)
    if "created_at" in data and isinstance(data["created_at"], datetime):
        data["created_at"] = data["created_at"].isoformat()
    if "updated_at" in data and isinstance(data["updated_at"], datetime):
        data["updated_at"] = data["updated_at"].isoformat()
    if "sent_at" in data and isinstance(data["sent_at"], datetime):
        data["sent_at"] = data["sent_at"].isoformat()
    if "last_action_at" in data and isinstance(data["last_action_at"], datetime):
        data["last_action_at"] = data["last_action_at"].isoformat()
    if "triaged_at" in data and isinstance(data["triaged_at"], datetime):
        data["triaged_at"] = data["triaged_at"].isoformat()
    return data


def default_mode(channel: str) -> str:
    key = f"{channel.upper()}_CHANNEL_MODE"
    return os.getenv(key, "approval_required")


def ensure_channel_tables():
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS channel_action_settings (
                    channel TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    kill_switch BOOLEAN NOT NULL DEFAULT FALSE,
                    allowlist_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    denylist_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_by TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS channel_action_audit_log (
                    id BIGSERIAL PRIMARY KEY,
                    channel TEXT NOT NULL,
                    account_identity TEXT,
                    merchant_id BIGINT,
                    merchant_name TEXT,
                    thread_id TEXT,
                    target_id TEXT,
                    target_parent_id TEXT,
                    action_type TEXT NOT NULL,
                    approval_state TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    confidence DOUBLE PRECISION,
                    rationale TEXT,
                    draft_text TEXT,
                    final_text TEXT,
                    idempotency_key TEXT,
                    result TEXT NOT NULL,
                    error TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    sent_at TIMESTAMPTZ
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS channel_action_audit_log_idempotency_idx
                ON channel_action_audit_log (idempotency_key)
                WHERE idempotency_key IS NOT NULL
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS channel_action_journal (
                    id BIGSERIAL PRIMARY KEY,
                    channel TEXT NOT NULL,
                    account_identity TEXT,
                    external_id TEXT NOT NULL,
                    thread_id TEXT,
                    merchant_id BIGINT,
                    last_action_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'recorded',
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    last_action_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS channel_action_journal_unique_idx
                ON channel_action_journal (channel, external_id, last_action_type)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS gmail_thread_intelligence (
                    thread_id TEXT PRIMARY KEY,
                    message_id TEXT,
                    source TEXT NOT NULL DEFAULT 'gmail',
                    sender_email TEXT NOT NULL,
                    sender_name TEXT,
                    sender_domain TEXT,
                    subject TEXT NOT NULL DEFAULT '',
                    snippet TEXT NOT NULL DEFAULT '',
                    thread_category TEXT NOT NULL,
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                    processor TEXT NOT NULL DEFAULT 'unknown',
                    distress_type TEXT,
                    distress_signals_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    industry TEXT NOT NULL DEFAULT 'unknown',
                    merchant_name_candidate TEXT,
                    merchant_domain_candidate TEXT,
                    merchant_identity_confidence DOUBLE PRECISION,
                    reply_priority TEXT NOT NULL DEFAULT 'none',
                    reply_allowed BOOLEAN NOT NULL DEFAULT FALSE,
                    draft_recommended BOOLEAN NOT NULL DEFAULT FALSE,
                    signal_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                    opportunity_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                    test_thread BOOLEAN NOT NULL DEFAULT FALSE,
                    triaged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    content_fingerprint TEXT,
                    signal_id BIGINT,
                    opportunity_id BIGINT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS gmail_thread_intelligence_category_idx
                ON gmail_thread_intelligence (thread_category, triaged_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS gmail_thread_intelligence_priority_idx
                ON gmail_thread_intelligence (reply_priority, confidence DESC, triaged_at DESC)
                """
            )
        )
        conn.commit()


def get_channel_settings(channel: str) -> dict:
    if channel not in VALID_CHANNELS:
        raise ValueError(f"Unsupported channel: {channel}")
    ensure_channel_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM channel_action_settings WHERE channel = :channel"),
            {"channel": channel},
        ).fetchone()
        if not row:
            conn.execute(
                text(
                    """
                    INSERT INTO channel_action_settings (channel, mode, allowlist_json, denylist_json, config_json)
                    VALUES (:channel, :mode, CAST(:allowlist AS JSONB), CAST(:denylist AS JSONB), CAST(:config AS JSONB))
                    """
                ),
                {
                    "channel": channel,
                    "mode": default_mode(channel),
                    "allowlist": "[]",
                    "denylist": "[]",
                    "config": "{}",
                },
            )
            conn.commit()
            row = conn.execute(
                text("SELECT * FROM channel_action_settings WHERE channel = :channel"),
                {"channel": channel},
            ).fetchone()
    settings = _row_to_dict(row) or {}
    settings["allowlist"] = settings.pop("allowlist_json", [])
    settings["denylist"] = settings.pop("denylist_json", [])
    settings["config"] = settings.pop("config_json", {})
    return settings


def set_channel_mode(channel: str, mode: str, updated_by: str = "operator") -> dict:
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported mode: {mode}")
    settings = get_channel_settings(channel)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                UPDATE channel_action_settings
                SET mode = :mode, updated_by = :updated_by, updated_at = NOW()
                WHERE channel = :channel
                """
            ),
            {"channel": channel, "mode": mode, "updated_by": updated_by},
        )
        conn.commit()
    settings["mode"] = mode
    settings["updated_by"] = updated_by
    return settings


def set_channel_kill_switch(channel: str, enabled: bool, updated_by: str = "operator") -> dict:
    settings = get_channel_settings(channel)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                UPDATE channel_action_settings
                SET kill_switch = :enabled, updated_by = :updated_by, updated_at = NOW()
                WHERE channel = :channel
                """
            ),
            {"channel": channel, "enabled": enabled, "updated_by": updated_by},
        )
        conn.commit()
    settings["kill_switch"] = enabled
    settings["updated_by"] = updated_by
    return settings


def compute_idempotency_key(channel: str, action_type: str, target_id: str, text_body: str = "") -> str:
    raw = f"{channel}:{action_type}:{target_id}:{text_body.strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def log_action(
    *,
    channel: str,
    action_type: str,
    approval_state: str,
    mode: str,
    result: str,
    account_identity: str = "",
    merchant_id=None,
    merchant_name: str = "",
    thread_id: str = "",
    target_id: str = "",
    target_parent_id: str = "",
    confidence: float | None = None,
    rationale: str = "",
    draft_text: str = "",
    final_text: str = "",
    idempotency_key: str = "",
    error: str = "",
    metadata: dict | None = None,
    sent: bool = False,
) -> int:
    ensure_channel_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO channel_action_audit_log (
                    channel, account_identity, merchant_id, merchant_name, thread_id, target_id,
                    target_parent_id, action_type, approval_state, mode, confidence, rationale,
                    draft_text, final_text, idempotency_key, result, error, metadata_json, sent_at
                )
                VALUES (
                    :channel, :account_identity, :merchant_id, :merchant_name, :thread_id, :target_id,
                    :target_parent_id, :action_type, :approval_state, :mode, :confidence, :rationale,
                    :draft_text, :final_text, :idempotency_key, :result, :error, CAST(:metadata AS JSONB),
                    CASE WHEN :sent THEN NOW() ELSE NULL END
                )
                RETURNING id
                """
            ),
            {
                "channel": channel,
                "account_identity": account_identity,
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "thread_id": thread_id,
                "target_id": target_id,
                "target_parent_id": target_parent_id,
                "action_type": action_type,
                "approval_state": approval_state,
                "mode": mode,
                "confidence": confidence,
                "rationale": rationale,
                "draft_text": draft_text,
                "final_text": final_text,
                "idempotency_key": idempotency_key or None,
                "result": result,
                "error": error,
                "metadata": _json(metadata),
                "sent": sent,
            },
        ).scalar_one()
        conn.commit()
        return int(row)


def record_journal_entry(
    *,
    channel: str,
    external_id: str,
    last_action_type: str,
    account_identity: str = "",
    thread_id: str = "",
    merchant_id=None,
    status: str = "recorded",
    metadata: dict | None = None,
):
    ensure_channel_tables()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO channel_action_journal (
                    channel, account_identity, external_id, thread_id, merchant_id,
                    last_action_type, status, metadata_json, last_action_at
                )
                VALUES (
                    :channel, :account_identity, :external_id, :thread_id, :merchant_id,
                    :last_action_type, :status, CAST(:metadata AS JSONB), NOW()
                )
                ON CONFLICT (channel, external_id, last_action_type)
                DO UPDATE SET
                    account_identity = EXCLUDED.account_identity,
                    thread_id = EXCLUDED.thread_id,
                    merchant_id = EXCLUDED.merchant_id,
                    status = EXCLUDED.status,
                    metadata_json = EXCLUDED.metadata_json,
                    last_action_at = NOW()
                """
            ),
            {
                "channel": channel,
                "account_identity": account_identity,
                "external_id": external_id,
                "thread_id": thread_id,
                "merchant_id": merchant_id,
                "last_action_type": last_action_type,
                "status": status,
                "metadata": _json(metadata),
            },
        )
        conn.commit()


def has_journal_entry(channel: str, external_id: str, last_action_type: str) -> bool:
    ensure_channel_tables()
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_journal
                WHERE channel = :channel AND external_id = :external_id AND last_action_type = :last_action_type
                """
            ),
            {
                "channel": channel,
                "external_id": external_id,
                "last_action_type": last_action_type,
            },
        ).scalar() or 0
    return count > 0


def has_recent_success(channel: str, idempotency_key: str, cooldown_hours: int = 72) -> bool:
    if not idempotency_key:
        return False
    ensure_channel_tables()
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE channel = :channel
                  AND idempotency_key = :idempotency_key
                  AND result IN ('sent', 'modified', 'skipped_duplicate')
                  AND created_at >= NOW() - (:cooldown || ' hours')::interval
                """
            ),
            {
                "channel": channel,
                "idempotency_key": idempotency_key,
                "cooldown": int(cooldown_hours),
            },
        ).scalar() or 0
    return count > 0


def count_recent_results(channel: str, result: str, hours: int = 24) -> int:
    ensure_channel_tables()
    with engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE channel = :channel
                  AND result = :result
                  AND created_at >= NOW() - (:hours || ' hours')::interval
                """
            ),
            {"channel": channel, "result": result, "hours": int(hours)},
        ).scalar() or 0


def count_recent_channel_target_results(
    channel: str,
    result: str,
    field_name: str,
    field_value: str,
    hours: int = 24,
) -> int:
    if field_name not in {"thread_id", "target_id", "target_parent_id"}:
        raise ValueError("Unsupported field name")
    ensure_channel_tables()
    with engine.connect() as conn:
        return conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE channel = :channel
                  AND result = :result
                  AND {field_name} = :field_value
                  AND created_at >= NOW() - (:hours || ' hours')::interval
                """
            ),
            {
                "channel": channel,
                "result": result,
                "field_value": field_value,
                "hours": int(hours),
            },
        ).scalar() or 0


def count_recent_subreddit_sends(subreddit: str, hours: int = 24) -> int:
    ensure_channel_tables()
    with engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE channel = 'reddit'
                  AND result = 'sent'
                  AND metadata_json->>'subreddit' = :subreddit
                  AND created_at >= NOW() - (:hours || ' hours')::interval
                """
            ),
            {"subreddit": subreddit, "hours": int(hours)},
        ).scalar() or 0


def get_recent_audit_log(channel: str | None = None, limit: int = 25) -> list[dict]:
    ensure_channel_tables()
    query = """
        SELECT *
        FROM channel_action_audit_log
    """
    params = {"limit": int(limit)}
    if channel:
        query += " WHERE channel = :channel"
        params["channel"] = channel
    query += " ORDER BY created_at DESC LIMIT :limit"
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [_row_to_dict(row) for row in rows]


def _normalize_gmail_thread_intelligence(row):
    if not row:
        return None
    data = _row_to_dict(row)
    data["distress_signals"] = data.pop("distress_signals_json", [])
    data["metadata"] = data.pop("metadata_json", {})
    return data


def get_gmail_thread_intelligence(thread_id: str) -> dict | None:
    ensure_channel_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM gmail_thread_intelligence WHERE thread_id = :thread_id"),
            {"thread_id": thread_id},
        ).fetchone()
    return _normalize_gmail_thread_intelligence(row)


def upsert_gmail_thread_intelligence(
    intelligence: dict,
    *,
    content_fingerprint: str,
    signal_id=None,
    opportunity_id=None,
    metadata: dict | None = None,
) -> dict:
    ensure_channel_tables()
    payload = {
        "thread_id": intelligence["thread_id"],
        "message_id": intelligence.get("message_id"),
        "sender_email": intelligence.get("sender_email", ""),
        "sender_name": intelligence.get("sender_name"),
        "sender_domain": intelligence.get("sender_domain"),
        "subject": intelligence.get("subject", ""),
        "snippet": intelligence.get("snippet", ""),
        "thread_category": intelligence.get("thread_category", "customer_support"),
        "confidence": float(intelligence.get("confidence", 0.0) or 0.0),
        "processor": intelligence.get("processor", "unknown"),
        "distress_type": intelligence.get("distress_type"),
        "distress_signals": _json(intelligence.get("distress_signals", [])),
        "industry": intelligence.get("industry", "unknown"),
        "merchant_name_candidate": intelligence.get("merchant_name_candidate"),
        "merchant_domain_candidate": intelligence.get("merchant_domain_candidate"),
        "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
        "reply_priority": intelligence.get("reply_priority", "none"),
        "reply_allowed": bool(intelligence.get("reply_allowed", False)),
        "draft_recommended": bool(intelligence.get("draft_recommended", False)),
        "signal_eligible": bool(intelligence.get("signal_eligible", False)),
        "opportunity_eligible": bool(intelligence.get("opportunity_eligible", False)),
        "test_thread": bool(intelligence.get("test_thread", False)),
        "triaged_at": intelligence.get("triaged_at"),
        "content_fingerprint": content_fingerprint,
        "signal_id": signal_id,
        "opportunity_id": opportunity_id,
        "metadata": _json(metadata),
    }
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO gmail_thread_intelligence (
                    thread_id, message_id, source, sender_email, sender_name, sender_domain,
                    subject, snippet, thread_category, confidence, processor, distress_type,
                    distress_signals_json, industry, merchant_name_candidate, merchant_domain_candidate,
                    merchant_identity_confidence, reply_priority, reply_allowed, draft_recommended,
                    signal_eligible, opportunity_eligible, test_thread, triaged_at,
                    content_fingerprint, signal_id, opportunity_id, metadata_json
                )
                VALUES (
                    :thread_id, :message_id, 'gmail', :sender_email, :sender_name, :sender_domain,
                    :subject, :snippet, :thread_category, :confidence, :processor, :distress_type,
                    CAST(:distress_signals AS JSONB), :industry, :merchant_name_candidate, :merchant_domain_candidate,
                    :merchant_identity_confidence, :reply_priority, :reply_allowed, :draft_recommended,
                    :signal_eligible, :opportunity_eligible, :test_thread, :triaged_at,
                    :content_fingerprint, :signal_id, :opportunity_id, CAST(:metadata AS JSONB)
                )
                ON CONFLICT (thread_id) DO UPDATE SET
                    message_id = EXCLUDED.message_id,
                    sender_email = EXCLUDED.sender_email,
                    sender_name = EXCLUDED.sender_name,
                    sender_domain = EXCLUDED.sender_domain,
                    subject = EXCLUDED.subject,
                    snippet = EXCLUDED.snippet,
                    thread_category = EXCLUDED.thread_category,
                    confidence = EXCLUDED.confidence,
                    processor = EXCLUDED.processor,
                    distress_type = EXCLUDED.distress_type,
                    distress_signals_json = EXCLUDED.distress_signals_json,
                    industry = EXCLUDED.industry,
                    merchant_name_candidate = EXCLUDED.merchant_name_candidate,
                    merchant_domain_candidate = EXCLUDED.merchant_domain_candidate,
                    merchant_identity_confidence = EXCLUDED.merchant_identity_confidence,
                    reply_priority = EXCLUDED.reply_priority,
                    reply_allowed = EXCLUDED.reply_allowed,
                    draft_recommended = EXCLUDED.draft_recommended,
                    signal_eligible = EXCLUDED.signal_eligible,
                    opportunity_eligible = EXCLUDED.opportunity_eligible,
                    test_thread = EXCLUDED.test_thread,
                    triaged_at = EXCLUDED.triaged_at,
                    content_fingerprint = EXCLUDED.content_fingerprint,
                    signal_id = COALESCE(EXCLUDED.signal_id, gmail_thread_intelligence.signal_id),
                    opportunity_id = COALESCE(EXCLUDED.opportunity_id, gmail_thread_intelligence.opportunity_id),
                    metadata_json = EXCLUDED.metadata_json
                RETURNING *
                """
            ),
            payload,
        ).fetchone()
        conn.commit()
    return _normalize_gmail_thread_intelligence(row)


def list_gmail_thread_intelligence(
    *,
    category: str | None = None,
    only_reply_worthy: bool = False,
    limit: int = 25,
) -> list[dict]:
    ensure_channel_tables()
    where = []
    params = {"limit": int(limit)}
    if category:
        where.append("thread_category = :category")
        params["category"] = category
    if only_reply_worthy:
        where.append("draft_recommended = TRUE")
    query = "SELECT * FROM gmail_thread_intelligence"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += """
        ORDER BY
            CASE reply_priority
                WHEN 'high' THEN 0
                WHEN 'medium' THEN 1
                WHEN 'low' THEN 2
                ELSE 3
            END,
            confidence DESC,
            triaged_at DESC
        LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [_normalize_gmail_thread_intelligence(row) for row in rows]


def get_channel_metrics_snapshot() -> dict:
    ensure_channel_tables()
    with engine.connect() as conn:
        approval_queue_depth = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE result IN ('drafted', 'approval_required')
                  AND approval_state = 'approval_required'
                  AND created_at >= NOW() - INTERVAL '7 days'
                """
            )
        ).scalar() or 0
        action_queue_depth = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE result IN ('drafted', 'approval_required', 'blocked')
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        last_action_sent_at = conn.execute(
            text(
                """
                SELECT sent_at
                FROM channel_action_audit_log
                WHERE sent_at IS NOT NULL
                ORDER BY sent_at DESC
                LIMIT 1
                """
            )
        ).scalar()
        gmail_triaged_threads_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM gmail_thread_intelligence
                WHERE triaged_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        gmail_merchant_distress_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM gmail_thread_intelligence
                WHERE thread_category = 'merchant_distress'
                  AND triaged_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        gmail_signal_extractions_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM gmail_thread_intelligence
                WHERE signal_id IS NOT NULL
                  AND triaged_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        gmail_opportunities_created_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM gmail_thread_intelligence
                WHERE opportunity_id IS NOT NULL
                  AND triaged_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        gmail_reply_worthy_threads = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM gmail_thread_intelligence
                WHERE draft_recommended = TRUE
                """
            )
        ).scalar() or 0
        last_gmail_triage_at = conn.execute(
            text(
                """
                SELECT triaged_at
                FROM gmail_thread_intelligence
                ORDER BY triaged_at DESC
                LIMIT 1
                """
            )
        ).scalar()
        return {
            "approval_queue_depth": approval_queue_depth,
            "action_queue_depth": action_queue_depth,
            "last_action_sent_at": last_action_sent_at.isoformat() if last_action_sent_at else None,
            "gmail_triaged_threads_24h": gmail_triaged_threads_24h,
            "gmail_merchant_distress_24h": gmail_merchant_distress_24h,
            "gmail_signal_extractions_24h": gmail_signal_extractions_24h,
            "gmail_opportunities_created_24h": gmail_opportunities_created_24h,
            "gmail_reply_worthy_threads": gmail_reply_worthy_threads,
            "last_gmail_triage_at": last_gmail_triage_at.isoformat() if last_gmail_triage_at else None,
        }
