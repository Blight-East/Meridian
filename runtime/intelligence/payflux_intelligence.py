from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

from sqlalchemy import text

from memory.structured.db import engine, save_event
from runtime.intelligence.distress_normalization import (
    normalize_distress_topic,
    normalize_operator_action,
    operator_action_for_distress,
    operator_action_label,
)
from runtime.intelligence.merchant_identity import normalize_domain
from runtime.intelligence.merchant_quality import invalid_merchant_name_reason, is_valid_domain
from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OUTREACH,
    evaluate_opportunity_queue_quality,
)
from runtime.intelligence.shopify_source_attribution import get_shopify_source_attribution_metrics


PROCESSOR_PATTERNS = {
    "stripe": [r"\bstripe\b"],
    "paypal": [r"\bpaypal\b"],
    "square": [r"\bsquare\b", r"\bsquareup\b"],
    "adyen": [r"\badyen\b"],
    "shopify_payments": [r"\bshopify payments\b"],
    "braintree": [r"\bbraintree\b"],
    "authorize_net": [r"\bauthorize\.net\b", r"\bauthorize net\b"],
    "worldpay": [r"\bworldpay\b"],
    "checkout_com": [r"\bcheckout\.com\b", r"\bcheckout com\b"],
}

DISTRESS_PATTERNS = {
    "account_frozen": [
        r"\baccount frozen\b",
        r"\bfroze our account\b",
        r"\bfrozen account\b",
        r"\bpayments disabled\b",
        r"\bprocessing disabled\b",
    ],
    "payouts_delayed": [
        r"\bpayout delayed\b",
        r"\bpayouts delayed\b",
        r"\bpayout paused\b",
        r"\bpayouts paused\b",
        r"\bdelayed payout\b",
    ],
    "reserve_hold": [
        r"\bfunds on hold\b",
        r"\bfunds held\b",
        r"\breserve hold\b",
        r"\breserve increase\b",
        r"\brolling reserve\b",
    ],
    "verification_review": [
        r"\bunder review\b",
        r"\bverification review\b",
        r"\bcompliance review\b",
        r"\bkyc\b",
        r"\bkyb\b",
        r"\bwebsite verification\b",
    ],
    "chargeback_issue": [
        r"\bchargeback spike\b",
        r"\bchargeback issue\b",
        r"\bdispute spike\b",
        r"\bnegative balance\b",
    ],
    "processor_switch_intent": [
        r"\bneed a new payment processor\b",
        r"\bnew payment processor\b",
        r"\blooking for an alternative\b",
        r"\blooking for a new processor\b",
        r"\bprocessor alternative\b",
    ],
    "account_terminated": [
        r"\baccount terminated\b",
        r"\baccount closed\b",
        r"\bterminated account\b",
    ],
    "onboarding_rejected": [
        r"\bonboarding rejected\b",
        r"\bapplication denied\b",
        r"\bmerchant account denied\b",
    ],
}

OUTCOME_STATUS_MAP = {
    "converted": "won",
    "rejected": "lost",
    "pending_review": "pending",
    "approved": "pending",
    "outreach_sent": "pending",
}

HISTORICAL_GMAIL_PROMO_PATTERNS = (
    "% off",
    "discount code",
    "newsletter",
    "new arrivals",
    "arrivals",
    "sale",
    "spring newsletter",
    "personalized card recommendations",
    "perfect new card",
    "cordially invited",
    "all dressed up",
    "new kicks",
    "peek at",
)

HISTORICAL_GMAIL_NOISY_DOMAIN_LABELS = {
    "alert",
    "alerts",
    "click",
    "email",
    "link",
    "links",
    "mail",
    "mailer",
    "mkt",
    "news",
    "newsletter",
    "notify",
    "public",
    "tracking",
    "trk",
}

AUTOMATED_GMAIL_LOCALPART_TOKENS = (
    "newsletter",
    "no-reply",
    "noreply",
    "mailer",
    "notify",
    "support",
)

LOW_QUALITY_FORUM_MERCHANT_PATTERNS = (
    re.compile(r"^unknown distressed merchant$", re.IGNORECASE),
    re.compile(r"^problem\b", re.IGNORECASE),
    re.compile(r"^since\b", re.IGNORECASE),
    re.compile(r"^closed$", re.IGNORECASE),
    re.compile(r"^customers?$", re.IGNORECASE),
    re.compile(r"^trust team$", re.IGNORECASE),
)

LOW_QUALITY_FORUM_DOMAIN_LABELS = {
    "customer",
    "customers",
    "merchant",
    "merchants",
    "shop",
    "store",
    "support",
}

NON_PROCESSOR_RESERVE_FALSE_POSITIVE_PATTERNS = (
    re.compile(r"\breserve\s+(?:item|items|product|products)\b", re.IGNORECASE),
    re.compile(r"\breserved?\s+in\s+(?:their|the)\s+cart\b", re.IGNORECASE),
    re.compile(r"\badd more items?\s+to\s+(?:their|the)\s+cart\b", re.IGNORECASE),
    re.compile(r"\bapp store\b", re.IGNORECASE),
    re.compile(r"\bstock\b", re.IGNORECASE),
)

PROCESSOR_LIQUIDITY_CONTEXT_PATTERNS = (
    re.compile(r"\bpayouts?\b", re.IGNORECASE),
    re.compile(r"\bfunds?\s+(?:held|on hold|frozen)\b", re.IGNORECASE),
    re.compile(r"\bpayment(?:s| provider| gateway)?\b", re.IGNORECASE),
    re.compile(r"\bprocessor\b", re.IGNORECASE),
    re.compile(r"\bmerchant trust team\b", re.IGNORECASE),
)


def ensure_payflux_intelligence_tables():
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS distress_pattern_clusters (
                    id BIGSERIAL PRIMARY KEY,
                    pattern_key TEXT NOT NULL UNIQUE,
                    processor TEXT NOT NULL DEFAULT 'unknown',
                    distress_type TEXT NOT NULL DEFAULT 'unknown',
                    industry TEXT NOT NULL DEFAULT 'unknown',
                    signal_count INTEGER NOT NULL DEFAULT 0,
                    affected_merchant_count INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TIMESTAMPTZ,
                    last_seen_at TIMESTAMPTZ,
                    velocity_24h INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'watch',
                    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS distress_pattern_clusters_status_idx
                ON distress_pattern_clusters (status, last_seen_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS distress_pattern_clusters_processor_idx
                ON distress_pattern_clusters (processor, distress_type, industry)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS opportunity_conversion_intelligence (
                    opportunity_id BIGINT PRIMARY KEY,
                    merchant_id BIGINT,
                    merchant_domain TEXT,
                    processor TEXT NOT NULL DEFAULT 'unknown',
                    distress_type TEXT NOT NULL DEFAULT 'unknown',
                    industry TEXT NOT NULL DEFAULT 'unknown',
                    outcome_status TEXT NOT NULL DEFAULT 'pending',
                    outcome_timestamp TIMESTAMPTZ,
                    outcome_reason TEXT,
                    pattern_key TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_conversion_intelligence_pattern_idx
                ON opportunity_conversion_intelligence (processor, distress_type, industry, outcome_status)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_conversion_intelligence_status_idx
                ON opportunity_conversion_intelligence (outcome_status, updated_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS opportunity_operator_actions (
                    opportunity_id BIGINT PRIMARY KEY,
                    merchant_id BIGINT,
                    merchant_domain TEXT,
                    processor TEXT NOT NULL DEFAULT 'unknown',
                    distress_type TEXT NOT NULL DEFAULT 'unknown',
                    recommended_action TEXT NOT NULL DEFAULT 'clarify_distress',
                    selected_action TEXT NOT NULL DEFAULT 'clarify_distress',
                    action_reason TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_operator_actions_action_idx
                ON opportunity_operator_actions (selected_action, distress_type, updated_at DESC)
                """
            )
        )
        conn.commit()


def sync_payflux_intelligence() -> dict:
    ensure_payflux_intelligence_tables()
    conversion_result = sync_opportunity_conversion_intelligence()
    pattern_result = sync_distress_pattern_clusters()
    outreach_result = {"status": "skipped", "rows_checked": 0, "replied": 0, "follow_up_needed": 0}
    try:
        from runtime.ops.outreach_execution import ensure_outreach_execution_tables, sync_outreach_execution_state

        ensure_outreach_execution_tables()
        outreach_result = sync_outreach_execution_state(limit=50)
    except Exception as exc:
        outreach_result = {"status": "error", "error": str(exc)}
    save_event(
        "payflux_intelligence_sync",
        {
            "pattern_rows": pattern_result.get("active_patterns", 0),
            "conversion_rows": conversion_result.get("tracked_opportunities", 0),
            "outreach_rows_checked": outreach_result.get("rows_checked", 0),
        },
    )
    return {
        "status": "ok",
        "patterns": pattern_result,
        "conversion": conversion_result,
        "outreach": outreach_result,
    }


def sync_distress_pattern_clusters(window_days: int = 30) -> dict:
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    s.id,
                    s.source,
                    s.content,
                    s.detected_at,
                    s.industry,
                    s.merchant_id,
                    s.classification,
                    m.canonical_name AS merchant_name,
                    m.domain AS merchant_domain,
                    m.industry AS merchant_industry,
                    gti.thread_category AS gmail_thread_category,
                    gti.confidence AS gmail_confidence,
                    gti.subject AS gmail_subject,
                    gti.sender_email AS gmail_sender_email,
                    gti.sender_domain AS gmail_sender_domain,
                    gti.merchant_name_candidate AS gmail_merchant_name_candidate,
                    gti.merchant_domain_candidate AS gmail_merchant_domain_candidate,
                    gti.processor AS gmail_processor,
                    gti.distress_type AS gmail_distress_type,
                    gti.industry AS gmail_industry
                FROM signals s
                LEFT JOIN merchants m ON m.id = s.merchant_id
                LEFT JOIN gmail_thread_intelligence gti ON gti.signal_id = s.id
                WHERE s.detected_at >= NOW() - (:window_days || ' days')::interval
                  AND COALESCE(s.classification, 'merchant_distress') = 'merchant_distress'
                ORDER BY s.detected_at DESC
                """
            ),
            {"window_days": int(window_days)},
        ).mappings().fetchall()

        grouped: dict[str, dict] = {}
        now = datetime.now(timezone.utc)
        conversion_lookup = _conversion_lookup(conn)
        conn.execute(text("UPDATE distress_pattern_clusters SET status = 'inactive', updated_at = NOW()"))
        for row in rows:
            if _is_historical_gmail_pattern_noise(row):
                continue
            if _is_low_quality_public_forum_signal(row):
                continue
            if _is_non_processor_reserve_false_positive(row):
                continue
            context = _signal_context(row)
            processor = context["processor"]
            distress_type = context["distress_type"]
            industry = context["industry"]
            if processor == "unknown" and distress_type == "unknown":
                continue
            pattern_key = _pattern_key(processor, distress_type, industry)
            detected_at = _ensure_utc_datetime(row["detected_at"])
            bucket = grouped.setdefault(
                pattern_key,
                {
                    "processor": processor,
                    "distress_type": distress_type,
                    "industry": industry,
                    "signal_count": 0,
                    "affected_merchants": set(),
                    "first_seen_at": detected_at,
                    "last_seen_at": detected_at,
                    "velocity_24h": 0,
                },
            )
            bucket["signal_count"] += 1
            if row.get("merchant_id"):
                bucket["affected_merchants"].add(int(row["merchant_id"]))
            bucket["first_seen_at"] = min(bucket["first_seen_at"], detected_at)
            bucket["last_seen_at"] = max(bucket["last_seen_at"], detected_at)
            if detected_at and (now - detected_at).total_seconds() <= 24 * 3600:
                bucket["velocity_24h"] += 1

        upserted = 0
        for pattern_key, bucket in grouped.items():
            status = _pattern_status(bucket["velocity_24h"], bucket["signal_count"])
            summary = _pattern_summary(
                bucket,
                conversion_lookup.get(pattern_key, {}),
                now=now,
            )
            conn.execute(
                text(
                    """
                    INSERT INTO distress_pattern_clusters (
                        pattern_key, processor, distress_type, industry,
                        signal_count, affected_merchant_count,
                        first_seen_at, last_seen_at, velocity_24h,
                        status, summary_json, updated_at
                    )
                    VALUES (
                        :pattern_key, :processor, :distress_type, :industry,
                        :signal_count, :affected_merchant_count,
                        :first_seen_at, :last_seen_at, :velocity_24h,
                        :status, CAST(:summary_json AS JSONB), NOW()
                    )
                    ON CONFLICT (pattern_key) DO UPDATE SET
                        signal_count = EXCLUDED.signal_count,
                        affected_merchant_count = EXCLUDED.affected_merchant_count,
                        first_seen_at = LEAST(distress_pattern_clusters.first_seen_at, EXCLUDED.first_seen_at),
                        last_seen_at = EXCLUDED.last_seen_at,
                        velocity_24h = EXCLUDED.velocity_24h,
                        status = EXCLUDED.status,
                        summary_json = EXCLUDED.summary_json,
                        updated_at = NOW()
                    """
                ),
                {
                    "pattern_key": pattern_key,
                    "processor": bucket["processor"],
                    "distress_type": bucket["distress_type"],
                    "industry": bucket["industry"],
                    "signal_count": int(bucket["signal_count"]),
                    "affected_merchant_count": len(bucket["affected_merchants"]),
                    "first_seen_at": bucket["first_seen_at"],
                    "last_seen_at": bucket["last_seen_at"],
                    "velocity_24h": int(bucket["velocity_24h"]),
                    "status": status,
                    "summary_json": json.dumps(summary),
                },
            )
            upserted += 1

        conn.commit()

    save_event(
        "distress_patterns_synced",
        {"active_patterns": len(grouped), "upserted": upserted},
    )
    return {"active_patterns": len(grouped), "upserted": upserted}


def list_distress_patterns(limit: int = 5) -> dict:
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM distress_pattern_clusters
                WHERE status != 'inactive'
                ORDER BY
                    CASE status
                        WHEN 'urgent' THEN 0
                        WHEN 'rising' THEN 1
                        WHEN 'watch' THEN 2
                        ELSE 3
                    END,
                    velocity_24h DESC,
                    signal_count DESC,
                    last_seen_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().fetchall()
    patterns = [_serialize_pattern_row(dict(row)) for row in rows]
    return {"count": len(patterns), "patterns": patterns}


def get_top_merchant_profiles(limit: int = 5) -> dict:
    fetch_limit = max(int(limit) * 5, int(limit))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    m.id,
                    m.canonical_name,
                    m.domain,
                    COALESCE(NULLIF(m.industry, ''), 'unknown') AS industry,
                    COALESCE(m.distress_score, 0) AS distress_score,
                    COALESCE(m.signal_count, 0) AS signal_count,
                    COALESCE(m.last_seen, m.created_at, m.first_seen) AS merchant_last_seen,
                    mo.id AS opportunity_id,
                    mo.status AS opportunity_status
                FROM merchants m
                LEFT JOIN LATERAL (
                    SELECT id, status
                    FROM merchant_opportunities mo
                    WHERE mo.merchant_id = m.id
                       OR (COALESCE(NULLIF(m.domain, ''), '') != '' AND mo.merchant_domain = m.domain)
                    ORDER BY mo.created_at DESC
                    LIMIT 1
                ) mo ON TRUE
                WHERE COALESCE(m.signal_count, 0) > 0
                   OR COALESCE(m.distress_score, 0) > 0
                   OR mo.id IS NOT NULL
                ORDER BY
                    CASE WHEN mo.status IN ('pending_review', 'approved') THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(NULLIF(m.domain, ''), '') != '' THEN 0 ELSE 1 END,
                    COALESCE(m.distress_score, 0) DESC,
                    COALESCE(m.last_seen, m.created_at, m.first_seen) DESC
                LIMIT :limit
                """
            ),
            {"limit": fetch_limit},
        ).mappings().fetchall()
    profiles = [get_merchant_intelligence_profile(merchant_id=row["id"]) for row in rows]
    profiles = [
        profile
        for profile in profiles
        if profile
        and profile.get("queue_eligibility_class") == ELIGIBILITY_OUTREACH
        and int(profile.get("verified_contact_count") or 0) > 0
        and float(profile.get("best_contact_confidence") or 0.0) >= 0.85
    ]
    profiles.sort(
        key=lambda profile: (
            0 if profile.get("queue_eligibility_class") == ELIGIBILITY_OUTREACH else 1,
            -(int(profile.get("urgency_score") or 0)),
            profile.get("merchant_domain") or "",
        )
    )
    return {"count": min(len(profiles), int(limit)), "profiles": profiles[: int(limit)]}


def get_merchant_intelligence_profile(*, merchant_id: int | None = None, domain: str | None = None) -> dict | None:
    with engine.connect() as conn:
        merchant = _fetch_merchant(conn, merchant_id=merchant_id, domain=domain)
        if not merchant:
            return None

        opportunity = conn.execute(
            text(
                """
                SELECT id, merchant_domain, processor, distress_topic, sales_strategy, status, created_at
                FROM merchant_opportunities
                WHERE merchant_id = :merchant_id
                   OR (COALESCE(:domain, '') != '' AND merchant_domain = :domain)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": merchant["id"], "domain": merchant.get("domain") or ""},
        ).mappings().first()

        signal_rows = conn.execute(
            text(
                """
                SELECT s.id, s.content, s.source, s.detected_at, s.industry, s.classification
                FROM signals s
                LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
                WHERE s.merchant_id = :merchant_id OR ms.merchant_id = :merchant_id
                ORDER BY s.detected_at DESC
                LIMIT 5
                """
            ),
            {"merchant_id": merchant["id"]},
        ).mappings().fetchall()

        gmail_row = conn.execute(
            text(
                """
                SELECT thread_id, subject, triaged_at, processor, distress_type
                FROM gmail_thread_intelligence
                WHERE signal_id IN (
                    SELECT s.id
                    FROM signals s
                    LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
                    WHERE s.merchant_id = :merchant_id OR ms.merchant_id = :merchant_id
                )
                ORDER BY triaged_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": merchant["id"]},
        ).mappings().first()

        action_row = conn.execute(
            text(
                """
                SELECT opportunity_id, recommended_action, selected_action, action_reason, metadata_json, updated_at
                FROM opportunity_operator_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": (opportunity or {}).get("id") or -1},
        ).mappings().first() if opportunity else None

        contact_row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '') AS verified_contact_count,
                    MAX(COALESCE(confidence, 0)) FILTER (WHERE email IS NOT NULL AND email != '') AS best_contact_confidence,
                    MAX(email) FILTER (WHERE email IS NOT NULL AND email != '') AS best_contact_email
                FROM merchant_contacts
                WHERE merchant_id = :merchant_id
                """
            ),
            {"merchant_id": merchant["id"]},
        ).mappings().first() or {}

    signal_contexts = [_signal_context(row) for row in signal_rows]
    processor_counts = Counter(
        ctx["processor"] for ctx in signal_contexts if ctx["processor"] != "unknown"
    )
    distress_counts = Counter(
        ctx["distress_type"] for ctx in signal_contexts if ctx["distress_type"] != "unknown"
    )
    processor = (
        (opportunity or {}).get("processor")
        if (opportunity or {}).get("processor") not in {None, "", "unknown"}
        else (processor_counts.most_common(1)[0][0] if processor_counts else "unknown")
    )
    normalized_opportunity_distress = normalize_distress_topic((opportunity or {}).get("distress_topic"))
    distress_type = (
        normalized_opportunity_distress
        if normalized_opportunity_distress != "unknown"
        else (distress_counts.most_common(1)[0][0] if distress_counts else "unknown")
    )
    last_signal = dict(signal_rows[0]) if signal_rows else None
    last_activity = _latest_timestamp(
        merchant.get("last_seen"),
        (opportunity or {}).get("created_at"),
        (last_signal or {}).get("detected_at"),
        (gmail_row or {}).get("triaged_at"),
    )
    latest_pattern = _pattern_lookup(processor, distress_type, merchant.get("industry") or "unknown")
    sales_strategy = _strategy_text((opportunity or {}).get("sales_strategy"))
    recommended_operator_action = operator_action_for_distress(distress_type)
    merchant_name = merchant.get("canonical_name") or merchant.get("domain")
    merchant_name_display = _display_merchant_name(merchant_name, merchant.get("domain"))
    queue_quality = evaluate_opportunity_queue_quality(
        opportunity={
            **dict(opportunity or {}),
            "processor": processor,
            "distress_topic": distress_type,
            "merchant_name": merchant_name,
        },
        merchant=dict(merchant or {}),
        signal=dict(signal_rows[0] or {}) if signal_rows else {},
    )

    return {
        "merchant_id": merchant["id"],
        "merchant_name": merchant_name,
        "merchant_name_display": merchant_name_display,
        "merchant_domain": merchant.get("domain") or "",
        "industry": merchant.get("industry") or "unknown",
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "distress_history": _counter_to_list(distress_counts),
        "processor_history": _counter_to_list(processor_counts),
        "recent_signal_summary": _recent_signal_summary(last_signal, signal_contexts[0] if signal_contexts else None),
        "last_meaningful_activity_at": _serialize_dt(last_activity),
        "opportunity_id": (opportunity or {}).get("id"),
        "opportunity_status": (opportunity or {}).get("status") or "none",
        "strategy_basis": sales_strategy,
        "recommended_operator_action": recommended_operator_action,
        "recommended_operator_action_label": operator_action_label(recommended_operator_action),
        "selected_operator_action": normalize_operator_action((action_row or {}).get("selected_action")),
        "selected_operator_action_label": operator_action_label((action_row or {}).get("selected_action")),
        "selected_operator_action_reason": (action_row or {}).get("action_reason") or "",
        "selected_operator_action_updated_at": _serialize_dt((action_row or {}).get("updated_at")),
        "pattern_context": latest_pattern,
        "urgency_score": _merchant_urgency_score(merchant, opportunity, latest_pattern),
        "why_it_matters": _merchant_why_it_matters(
            merchant,
            processor,
            distress_type,
            latest_pattern,
            merchant_name_display=merchant_name_display,
        ),
        "verified_contact_count": int(contact_row.get("verified_contact_count") or 0),
        "best_contact_confidence": float(contact_row.get("best_contact_confidence") or 0.0),
        "best_contact_email": contact_row.get("best_contact_email") or "",
        "queue_eligibility_class": queue_quality.get("eligibility_class") or ELIGIBILITY_OUTREACH,
        "queue_eligibility_reason": queue_quality.get("eligibility_reason") or "",
        "queue_quality_score": int(queue_quality.get("quality_score") or 0),
        "queue_domain_quality_label": queue_quality.get("domain_quality_label") or "",
        "queue_domain_quality_reason": queue_quality.get("domain_quality_reason") or "",
    }


def sync_opportunity_conversion_intelligence() -> dict:
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        existing_rows = conn.execute(
            text(
                """
                SELECT opportunity_id, outcome_status, outcome_timestamp, outcome_reason, metadata_json
                FROM opportunity_conversion_intelligence
                """
            )
        ).mappings().fetchall()
        existing_by_id = {int(row["opportunity_id"]): dict(row) for row in existing_rows}
        opportunity_rows = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, status, created_at
                FROM merchant_opportunities
                """
            )
        ).mappings().fetchall()

        tracked = 0
        for row in opportunity_rows:
            merchant = _fetch_merchant(conn, merchant_id=row.get("merchant_id"), domain=row.get("merchant_domain"))
            signal = _latest_signal_for_merchant(conn, merchant_id=(merchant or {}).get("id"), domain=row.get("merchant_domain"))
            signal_context = _signal_context(signal) if signal else {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}

            processor = row.get("processor")
            if processor in {None, "", "unknown"}:
                processor = signal_context["processor"]
            processor = processor or "unknown"

            distress_type = normalize_distress_topic(row.get("distress_topic"))
            if distress_type == "unknown":
                distress_type = signal_context["distress_type"]

            industry = (
                (merchant or {}).get("industry")
                or signal_context.get("industry")
                or "unknown"
            )
            source_outcome_status = OUTCOME_STATUS_MAP.get(row.get("status"), "pending")
            existing = existing_by_id.get(int(row["id"])) or {}
            existing_metadata = _json_dict(existing.get("metadata_json"))
            if (
                existing_metadata.get("manual_override")
                and source_outcome_status == "pending"
                and existing.get("outcome_status") in {"won", "lost", "ignored"}
            ):
                outcome_status = existing.get("outcome_status") or source_outcome_status
                outcome_timestamp = existing.get("outcome_timestamp") or row.get("created_at")
                outcome_reason = existing.get("outcome_reason") or row.get("status")
            else:
                outcome_status = source_outcome_status
                outcome_timestamp = row.get("created_at")
                outcome_reason = row.get("status")
            pattern_key = _pattern_key(processor or "unknown", distress_type or "unknown", industry or "unknown")
            metadata = {
                "source_status": row.get("status"),
                "signal_id": (signal or {}).get("id"),
            }
            if existing_metadata.get("manual_override"):
                metadata["manual_override"] = bool(existing_metadata.get("manual_override"))
                if existing_metadata.get("manual_override_at"):
                    metadata["manual_override_at"] = existing_metadata.get("manual_override_at")
                if existing_metadata.get("manual_override_reason"):
                    metadata["manual_override_reason"] = existing_metadata.get("manual_override_reason")
                if existing_metadata.get("manual_override_source"):
                    metadata["manual_override_source"] = existing_metadata.get("manual_override_source")
            conn.execute(
                text(
                    """
                    INSERT INTO opportunity_conversion_intelligence (
                        opportunity_id, merchant_id, merchant_domain, processor,
                        distress_type, industry, outcome_status, outcome_timestamp,
                        outcome_reason, pattern_key, metadata_json, updated_at
                    )
                    VALUES (
                        :opportunity_id, :merchant_id, :merchant_domain, :processor,
                        :distress_type, :industry, :outcome_status, :outcome_timestamp,
                        :outcome_reason, :pattern_key, CAST(:metadata_json AS JSONB), NOW()
                    )
                    ON CONFLICT (opportunity_id) DO UPDATE SET
                        merchant_id = EXCLUDED.merchant_id,
                        merchant_domain = EXCLUDED.merchant_domain,
                        processor = EXCLUDED.processor,
                        distress_type = EXCLUDED.distress_type,
                        industry = EXCLUDED.industry,
                        outcome_status = CASE
                            WHEN COALESCE((opportunity_conversion_intelligence.metadata_json ->> 'manual_override')::boolean, FALSE)
                                 AND EXCLUDED.outcome_status = 'pending'
                                 AND opportunity_conversion_intelligence.outcome_status IN ('won', 'lost', 'ignored')
                            THEN opportunity_conversion_intelligence.outcome_status
                            ELSE EXCLUDED.outcome_status
                        END,
                        outcome_timestamp = CASE
                            WHEN COALESCE((opportunity_conversion_intelligence.metadata_json ->> 'manual_override')::boolean, FALSE)
                                 AND EXCLUDED.outcome_status = 'pending'
                                 AND opportunity_conversion_intelligence.outcome_status IN ('won', 'lost', 'ignored')
                            THEN opportunity_conversion_intelligence.outcome_timestamp
                            ELSE EXCLUDED.outcome_timestamp
                        END,
                        outcome_reason = CASE
                            WHEN COALESCE((opportunity_conversion_intelligence.metadata_json ->> 'manual_override')::boolean, FALSE)
                                 AND EXCLUDED.outcome_status = 'pending'
                                 AND opportunity_conversion_intelligence.outcome_status IN ('won', 'lost', 'ignored')
                            THEN opportunity_conversion_intelligence.outcome_reason
                            ELSE EXCLUDED.outcome_reason
                        END,
                        pattern_key = EXCLUDED.pattern_key,
                        metadata_json = COALESCE(opportunity_conversion_intelligence.metadata_json, '{}'::jsonb) || EXCLUDED.metadata_json,
                        updated_at = NOW()
                    """
                ),
                {
                    "opportunity_id": int(row["id"]),
                    "merchant_id": row.get("merchant_id"),
                    "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
                    "processor": processor,
                    "distress_type": distress_type,
                    "industry": industry or "unknown",
                    "outcome_status": outcome_status,
                    "outcome_timestamp": outcome_timestamp,
                    "outcome_reason": outcome_reason,
                    "pattern_key": pattern_key,
                    "metadata_json": json.dumps(metadata),
                },
            )
            tracked += 1
        conn.commit()

    save_event("opportunity_conversion_intelligence_synced", {"tracked_opportunities": tracked})
    return {"tracked_opportunities": tracked}


def get_conversion_intelligence(limit: int = 5) -> dict:
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    processor,
                    distress_type,
                    industry,
                    COUNT(*) AS opportunity_count,
                    SUM(CASE WHEN outcome_status = 'won' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN outcome_status = 'lost' THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN outcome_status = 'ignored' THEN 1 ELSE 0 END) AS ignored,
                    SUM(CASE WHEN outcome_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    MAX(updated_at) AS last_updated_at
                FROM opportunity_conversion_intelligence
                WHERE distress_type IS NOT NULL
                  AND distress_type != 'unknown'
                  AND EXISTS (
                        SELECT 1
                        FROM merchant_contacts mc
                        WHERE mc.merchant_id = opportunity_conversion_intelligence.merchant_id
                          AND mc.email IS NOT NULL
                          AND mc.email != ''
                          AND COALESCE(mc.confidence, 0) >= 0.85
                          AND COALESCE(mc.email_verified, FALSE) = TRUE
                  )
                GROUP BY processor, distress_type, industry
                ORDER BY
                    SUM(CASE WHEN outcome_status = 'won' THEN 1 ELSE 0 END) DESC,
                    COUNT(*) DESC,
                    MAX(updated_at) DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().fetchall()

    patterns = []
    for row in rows:
        total = int(row["opportunity_count"] or 0)
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        ignored = int(row["ignored"] or 0)
        pending = int(row["pending"] or 0)
        win_rate = round((wins / total), 4) if total else 0.0
        converted_before = wins > 0
        best_chance_score = round((wins * 3 + pending - losses - ignored * 0.5), 2)
        patterns.append(
            {
                "processor": row["processor"] or "unknown",
                "distress_type": row["distress_type"] or "unknown",
                "industry": row["industry"] or "unknown",
                "opportunity_count": total,
                "wins": wins,
                "losses": losses,
                "ignored": ignored,
                "pending": pending,
                "win_rate": win_rate,
                "converted_before": converted_before,
                "best_chance_score": best_chance_score,
                "recommended_operator_action": operator_action_for_distress(row["distress_type"] or "unknown"),
                "recommended_operator_action_label": operator_action_label(operator_action_for_distress(row["distress_type"] or "unknown")),
                "last_updated_at": _serialize_dt(row["last_updated_at"]),
            }
        )
    return {"count": len(patterns), "patterns": patterns}


def mark_opportunity_outcome(
    opportunity_id: int,
    outcome_status: str,
    outcome_reason: str | None = None,
) -> dict:
    ensure_payflux_intelligence_tables()
    normalized_outcome = str(outcome_status or "").strip().lower()
    if normalized_outcome not in {"won", "lost", "ignored"}:
        return {"error": "Unsupported outcome status"}

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, status, created_at
                FROM merchant_opportunities
                WHERE id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Opportunity not found"}

        merchant = _fetch_merchant(conn, merchant_id=row.get("merchant_id"), domain=row.get("merchant_domain"))
        signal = _latest_signal_for_merchant(
            conn,
            merchant_id=(merchant or {}).get("id"),
            domain=row.get("merchant_domain"),
        )
        signal_context = _signal_context(signal) if signal else {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}

        processor = row.get("processor")
        if processor in {None, "", "unknown"}:
            processor = signal_context["processor"]
        distress_type = normalize_distress_topic(row.get("distress_topic"))
        if distress_type == "unknown":
            distress_type = signal_context["distress_type"]
        industry = (merchant or {}).get("industry") or signal_context.get("industry") or "unknown"
        pattern_key = _pattern_key(processor or "unknown", distress_type or "unknown", industry or "unknown")

        existing = conn.execute(
            text(
                """
                SELECT outcome_status, metadata_json
                FROM opportunity_conversion_intelligence
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        metadata = _json_dict((existing or {}).get("metadata_json"))
        now = datetime.now(timezone.utc)
        metadata.update(
            {
                "source_status": row.get("status"),
                "signal_id": (signal or {}).get("id"),
                "manual_override": True,
                "manual_override_source": "operator",
                "manual_override_at": now.isoformat(),
            }
        )
        if outcome_reason:
            metadata["manual_override_reason"] = outcome_reason

        conn.execute(
            text(
                """
                INSERT INTO opportunity_conversion_intelligence (
                    opportunity_id, merchant_id, merchant_domain, processor,
                    distress_type, industry, outcome_status, outcome_timestamp,
                    outcome_reason, pattern_key, metadata_json, updated_at
                )
                VALUES (
                    :opportunity_id, :merchant_id, :merchant_domain, :processor,
                    :distress_type, :industry, :outcome_status, :outcome_timestamp,
                    :outcome_reason, :pattern_key, CAST(:metadata_json AS JSONB), NOW()
                )
                ON CONFLICT (opportunity_id) DO UPDATE SET
                    merchant_id = EXCLUDED.merchant_id,
                    merchant_domain = EXCLUDED.merchant_domain,
                    processor = EXCLUDED.processor,
                    distress_type = EXCLUDED.distress_type,
                    industry = EXCLUDED.industry,
                    outcome_status = EXCLUDED.outcome_status,
                    outcome_timestamp = EXCLUDED.outcome_timestamp,
                    outcome_reason = EXCLUDED.outcome_reason,
                    pattern_key = EXCLUDED.pattern_key,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = NOW()
                """
            ),
            {
                "opportunity_id": int(row["id"]),
                "merchant_id": row.get("merchant_id"),
                "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
                "processor": processor or "unknown",
                "distress_type": distress_type or "unknown",
                "industry": industry or "unknown",
                "outcome_status": normalized_outcome,
                "outcome_timestamp": now,
                "outcome_reason": outcome_reason or normalized_outcome,
                "pattern_key": pattern_key,
                "metadata_json": json.dumps(metadata),
            },
        )
        conn.commit()

    save_event(
        "opportunity_outcome_marked",
        {
            "opportunity_id": int(opportunity_id),
            "outcome_status": normalized_outcome,
            "outcome_reason": outcome_reason or "",
            "pattern_key": pattern_key,
        },
    )
    try:
        from runtime.ops.outreach_execution import update_outreach_outcome

        update_outreach_outcome(
            opportunity_id=int(opportunity_id),
            outcome_status=normalized_outcome,
            notes=outcome_reason or "",
        )
    except Exception:
        pass
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "outcome_status": normalized_outcome,
        "outcome_reason": outcome_reason or "",
        "merchant_id": row.get("merchant_id"),
        "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "industry": industry or "unknown",
        "pattern_key": pattern_key,
        "previous_outcome_status": (existing or {}).get("outcome_status"),
    }


def get_opportunity_operator_action(opportunity_id: int) -> dict:
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, status, created_at
                FROM merchant_opportunities
                WHERE id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Opportunity not found"}

        merchant = _fetch_merchant(conn, merchant_id=row.get("merchant_id"), domain=row.get("merchant_domain"))
        signal = _latest_signal_for_merchant(
            conn,
            merchant_id=(merchant or {}).get("id"),
            domain=row.get("merchant_domain"),
        )
        signal_context = _signal_context(signal) if signal else {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}
        processor = row.get("processor")
        if processor in {None, "", "unknown"}:
            processor = signal_context["processor"]
        distress_type = normalize_distress_topic(row.get("distress_topic"))
        if distress_type == "unknown":
            distress_type = signal_context["distress_type"]
        existing = conn.execute(
            text(
                """
                SELECT selected_action, recommended_action, action_reason, updated_at, metadata_json
                FROM opportunity_operator_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()

    recommended_action = operator_action_for_distress(distress_type)
    selected_action = normalize_operator_action((existing or {}).get("selected_action") or recommended_action)
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "merchant_id": row.get("merchant_id"),
        "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "recommended_action": recommended_action,
        "recommended_action_label": operator_action_label(recommended_action),
        "selected_action": selected_action,
        "selected_action_label": operator_action_label(selected_action),
        "action_reason": (existing or {}).get("action_reason") or "",
        "selected_at": _serialize_dt((existing or {}).get("updated_at")),
        "has_manual_selection": bool(existing),
    }


def list_opportunities_by_operator_action(
    *,
    selected_action: str | None = None,
    unselected_only: bool = False,
    mismatched_only: bool = False,
    limit: int = 10,
) -> dict:
    ensure_payflux_intelligence_tables()
    normalized_action = normalize_operator_action(selected_action) if selected_action else None
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    mo.id AS opportunity_id,
                    mo.merchant_id,
                    mo.merchant_domain,
                    mo.status AS opportunity_status,
                    COALESCE(NULLIF(m.canonical_name, ''), NULLIF(m.domain, ''), mo.merchant_domain, '') AS merchant_name,
                    COALESCE(NULLIF(m.domain, ''), mo.merchant_domain, '') AS merchant_domain_fallback,
                    COALESCE(NULLIF(m.industry, ''), 'unknown') AS industry,
                    oa.selected_action,
                    oa.recommended_action,
                    oa.action_reason,
                    oa.updated_at AS selected_action_updated_at,
                    mo.processor,
                    mo.distress_topic
                FROM merchant_opportunities mo
                LEFT JOIN LATERAL (
                    SELECT canonical_name, domain, industry
                    FROM merchants m
                    WHERE m.id = mo.merchant_id
                       OR (COALESCE(NULLIF(m.domain, ''), '') != '' AND m.domain = mo.merchant_domain)
                    ORDER BY CASE WHEN m.id = mo.merchant_id THEN 0 ELSE 1 END, m.last_seen DESC NULLS LAST
                    LIMIT 1
                ) m ON TRUE
                LEFT JOIN opportunity_operator_actions oa
                  ON oa.opportunity_id = mo.id
                ORDER BY mo.created_at DESC
                """
            )
        ).mappings().fetchall()

    cases = []
    for row in rows:
        merchant = _fetch_merchant_with_fallback(merchant_id=row.get("merchant_id"), domain=row.get("merchant_domain_fallback"))
        signal = _latest_signal_for_merchant_by_lookup(
            merchant_id=(merchant or {}).get("id"),
            domain=row.get("merchant_domain_fallback"),
        )
        signal_context = _signal_context(signal) if signal else {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}
        processor = row.get("processor")
        if processor in {None, "", "unknown"}:
            processor = signal_context["processor"]
        distress_type = normalize_distress_topic(row.get("distress_topic"))
        if distress_type == "unknown":
            distress_type = signal_context["distress_type"]
        recommended_action = normalize_operator_action((row.get("recommended_action") or operator_action_for_distress(distress_type)))
        selected = row.get("selected_action")
        selected_action_value = normalize_operator_action(selected) if selected else ""
        has_selected_action = bool(selected)
        case = {
            "opportunity_id": int(row["opportunity_id"]),
            "merchant_id": row.get("merchant_id"),
            "merchant_name": row.get("merchant_name") or (merchant or {}).get("canonical_name") or "",
            "merchant_domain": row.get("merchant_domain_fallback") or "",
            "opportunity_status": row.get("opportunity_status") or "unknown",
            "industry": row.get("industry") or (merchant or {}).get("industry") or signal_context.get("industry") or "unknown",
            "processor": processor or "unknown",
            "distress_type": distress_type or "unknown",
            "selected_action": selected_action_value,
            "selected_action_label": operator_action_label(selected_action_value) if selected_action_value else "",
            "recommended_action": recommended_action,
            "recommended_action_label": operator_action_label(recommended_action),
            "action_reason": row.get("action_reason") or "",
            "selected_action_updated_at": _serialize_dt(row.get("selected_action_updated_at")),
            "has_selected_action": has_selected_action,
            "selected_matches_recommended": has_selected_action and selected_action_value == recommended_action,
        }
        if unselected_only and case["has_selected_action"]:
            continue
        if mismatched_only and (not case["has_selected_action"] or case["selected_matches_recommended"]):
            continue
        if normalized_action and case["selected_action"] != normalized_action:
            continue
        cases.append(case)
        if len(cases) >= int(limit):
            break
    return {"count": len(cases), "cases": cases}


def list_merchants_by_operator_action(
    *,
    selected_action: str | None = None,
    unselected_only: bool = False,
    mismatched_only: bool = False,
    limit: int = 10,
) -> dict:
    raw = list_opportunities_by_operator_action(
        selected_action=selected_action,
        unselected_only=False,
        mismatched_only=False,
        limit=max(int(limit) * 25, 250),
    ).get("cases", [])

    grouped: dict[str, dict] = {}
    for case in raw:
        merchant_key = (
            str(case.get("merchant_id"))
            if case.get("merchant_id") not in {None, "", 0}
            else case.get("merchant_domain")
            or f"opportunity:{case.get('opportunity_id')}"
        )
        current = grouped.get(merchant_key)
        if not current:
            grouped[merchant_key] = {
                "merchant_key": merchant_key,
                "merchant_id": case.get("merchant_id"),
                "merchant_name": case.get("merchant_name") or "",
                "merchant_domain": case.get("merchant_domain") or "",
                "industry": case.get("industry") or "unknown",
                "processor": case.get("processor") or "unknown",
                "distress_type": case.get("distress_type") or "unknown",
                "recommended_action": case.get("recommended_action") or "clarify_distress",
                "recommended_action_label": case.get("recommended_action_label") or operator_action_label(case.get("recommended_action")),
                "selected_action": case.get("selected_action") or "",
                "selected_action_label": case.get("selected_action_label") or "",
                "action_reason": case.get("action_reason") or "",
                "selected_action_updated_at": case.get("selected_action_updated_at"),
                "has_selected_action": bool(case.get("has_selected_action")),
                "selected_matches_recommended": bool(case.get("selected_matches_recommended")),
                "opportunity_count": 0,
                "pending_review_count": 0,
                "open_opportunity_ids": [],
                "selected_action_values": set(),
                "recommended_action_values": set(),
            }
            current = grouped[merchant_key]

        current["opportunity_count"] += 1
        if case.get("opportunity_status") in {"pending_review", "approved", "outreach_sent"}:
            current["pending_review_count"] += 1
        current["open_opportunity_ids"].append(case.get("opportunity_id"))
        if case.get("selected_action"):
            current["selected_action_values"].add(case.get("selected_action"))
        if case.get("recommended_action"):
            current["recommended_action_values"].add(case.get("recommended_action"))

        current_priority = (
            0 if case.get("opportunity_status") in {"pending_review", "approved"} else 1,
            0 if case.get("selected_action") else 1,
            case.get("opportunity_id") or 0,
        )
        existing_priority = (
            0 if current.get("pending_review_count", 0) > 0 else 1,
            0 if current.get("has_selected_action") else 1,
            min(current.get("open_opportunity_ids") or [10**9]),
        )
        if current_priority < existing_priority:
            for key in [
                "merchant_name",
                "merchant_domain",
                "industry",
                "processor",
                "distress_type",
                "recommended_action",
                "recommended_action_label",
                "selected_action",
                "selected_action_label",
                "action_reason",
                "selected_action_updated_at",
                "has_selected_action",
                "selected_matches_recommended",
            ]:
                current[key] = case.get(key, current.get(key))

    merchants = []
    normalized_action = normalize_operator_action(selected_action) if selected_action else None
    for merchant in grouped.values():
        selected_values = sorted(merchant.pop("selected_action_values"))
        recommended_values = sorted(merchant.pop("recommended_action_values"))
        merchant["selected_action_values"] = selected_values
        merchant["recommended_action_values"] = recommended_values
        merchant["selected_action_count"] = len(selected_values)
        merchant["recommended_action_count"] = len(recommended_values)
        merchant["has_selected_action"] = bool(selected_values)
        merchant["selected_matches_recommended"] = bool(
            selected_values and set(selected_values).issubset(set(recommended_values))
        )

        if unselected_only and merchant["has_selected_action"]:
            continue
        if mismatched_only and (not merchant["has_selected_action"] or merchant["selected_matches_recommended"]):
            continue
        if normalized_action and normalized_action not in selected_values:
            continue
        merchants.append(merchant)

    merchants.sort(
        key=lambda row: (
            0 if row.get("pending_review_count", 0) > 0 else 1,
            0 if row.get("merchant_domain") else 1,
            -(row.get("opportunity_count", 0) or 0),
            row.get("merchant_domain") or row.get("merchant_name") or "",
        )
    )
    return {"count": min(len(merchants), int(limit)), "merchants": merchants[: int(limit)]}


def mark_opportunity_operator_action(
    opportunity_id: int,
    selected_action: str,
    action_reason: str | None = None,
) -> dict:
    ensure_payflux_intelligence_tables()
    normalized_action = normalize_operator_action(selected_action)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, status, created_at
                FROM merchant_opportunities
                WHERE id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Opportunity not found"}

        merchant = _fetch_merchant(conn, merchant_id=row.get("merchant_id"), domain=row.get("merchant_domain"))
        signal = _latest_signal_for_merchant(
            conn,
            merchant_id=(merchant or {}).get("id"),
            domain=row.get("merchant_domain"),
        )
        signal_context = _signal_context(signal) if signal else {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}
        processor = row.get("processor")
        if processor in {None, "", "unknown"}:
            processor = signal_context["processor"]
        distress_type = normalize_distress_topic(row.get("distress_topic"))
        if distress_type == "unknown":
            distress_type = signal_context["distress_type"]
        recommended_action = operator_action_for_distress(distress_type)
        existing = conn.execute(
            text(
                """
                SELECT selected_action, action_reason
                FROM opportunity_operator_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        metadata = {
            "manual_override": True,
            "manual_override_source": "operator",
            "manual_override_at": datetime.now(timezone.utc).isoformat(),
        }
        if action_reason:
            metadata["manual_override_reason"] = action_reason
        conn.execute(
            text(
                """
                INSERT INTO opportunity_operator_actions (
                    opportunity_id, merchant_id, merchant_domain, processor, distress_type,
                    recommended_action, selected_action, action_reason, metadata_json, updated_at
                )
                VALUES (
                    :opportunity_id, :merchant_id, :merchant_domain, :processor, :distress_type,
                    :recommended_action, :selected_action, :action_reason, CAST(:metadata_json AS JSONB), NOW()
                )
                ON CONFLICT (opportunity_id) DO UPDATE SET
                    merchant_id = EXCLUDED.merchant_id,
                    merchant_domain = EXCLUDED.merchant_domain,
                    processor = EXCLUDED.processor,
                    distress_type = EXCLUDED.distress_type,
                    recommended_action = EXCLUDED.recommended_action,
                    selected_action = EXCLUDED.selected_action,
                    action_reason = EXCLUDED.action_reason,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = NOW()
                """
            ),
            {
                "opportunity_id": int(opportunity_id),
                "merchant_id": row.get("merchant_id"),
                "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
                "processor": processor or "unknown",
                "distress_type": distress_type or "unknown",
                "recommended_action": recommended_action,
                "selected_action": normalized_action,
                "action_reason": action_reason or "",
                "metadata_json": json.dumps(metadata),
            },
        )
        conn.commit()

    save_event(
        "opportunity_operator_action_marked",
        {
            "opportunity_id": int(opportunity_id),
            "selected_action": normalized_action,
            "recommended_action": recommended_action,
            "action_reason": action_reason or "",
        },
    )
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "merchant_id": row.get("merchant_id"),
        "merchant_domain": row.get("merchant_domain") or (merchant or {}).get("domain") or "",
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "selected_action": normalized_action,
        "selected_action_label": operator_action_label(normalized_action),
        "recommended_action": recommended_action,
        "recommended_action_label": operator_action_label(recommended_action),
        "action_reason": action_reason or "",
        "previous_selected_action": normalize_operator_action((existing or {}).get("selected_action") or recommended_action),
    }


def get_payflux_intelligence_metrics() -> dict:
    patterns = list_distress_patterns(limit=10).get("patterns", [])
    profiles = get_top_merchant_profiles(limit=5).get("profiles", [])
    conversion = get_conversion_intelligence(limit=10).get("patterns", [])
    outreach_metrics = {}
    try:
        from runtime.ops.outreach_execution import get_outreach_execution_metrics

        outreach_metrics = get_outreach_execution_metrics(refresh_state=False)
    except Exception:
        outreach_metrics = {}
    active_patterns = len(patterns)
    rising_patterns = len([p for p in patterns if p.get("status") in {"rising", "urgent"}])
    top_pattern = patterns[0] if patterns else {}
    top_conversion = next((row for row in conversion if row.get("wins", 0) > 0), conversion[0] if conversion else {})
    payload = {
        "distress_patterns_active": active_patterns,
        "distress_patterns_rising": rising_patterns,
        "strongest_distress_pattern": top_pattern.get("headline") or "",
        "merchant_profiles_ready": len(profiles),
        "top_merchant_profile": _top_profile_label(profiles[0]) if profiles else "",
        "conversion_patterns_tracked": len(conversion),
        "top_conversion_pattern": _conversion_pattern_label(top_conversion) if top_conversion else "",
    }
    payload.update(_public_source_metrics())
    payload.update(get_shopify_source_attribution_metrics())
    payload.update(outreach_metrics)
    return payload


def _public_source_metrics() -> dict:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT event_type, data
                FROM events
                WHERE event_type IN (
                    'shopify_community_ingestion_cycle',
                    'stack_overflow_ingestion_cycle',
                    'public_source_ingestion_error'
                )
                  AND created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
                """
            )
        ).mappings().fetchall()
        signal_counts = conn.execute(
            text(
                """
                SELECT source, COUNT(*) AS cnt
                FROM signals
                WHERE source IN ('shopify_community', 'stack_overflow')
                  AND detected_at >= NOW() - INTERVAL '24 hours'
                GROUP BY source
                """
            )
        ).mappings().fetchall()

    metrics = {
        "shopify_community_threads_seen_24h": 0,
        "shopify_community_signals_created_24h": 0,
        "stack_overflow_questions_seen_24h": 0,
        "stack_overflow_signals_created_24h": 0,
        "source_ingestion_errors_24h": 0,
        "source_domains_extracted_24h": 0,
        "shopify_community_top_distress_24h": "",
        "stack_overflow_top_distress_24h": "",
        "source_health_summary": "",
    }
    signal_lookup = {str(row["source"]): int(row["cnt"] or 0) for row in signal_counts}
    top_distress_counts = {
        "shopify_community": Counter(),
        "stack_overflow": Counter(),
    }
    for row in rows:
        event_type = str(row.get("event_type") or "")
        payload = _json_dict(row.get("data"))
        if event_type == "public_source_ingestion_error":
            metrics["source_ingestion_errors_24h"] += 1
            continue
        if event_type == "shopify_community_ingestion_cycle":
            metrics["shopify_community_threads_seen_24h"] += int(payload.get("threads_seen") or 0)
            metrics["source_domains_extracted_24h"] += int(payload.get("domains_extracted") or 0)
            if payload.get("top_distress_pattern"):
                top_distress_counts["shopify_community"][str(payload["top_distress_pattern"])] += 1
        elif event_type == "stack_overflow_ingestion_cycle":
            metrics["stack_overflow_questions_seen_24h"] += int(payload.get("questions_seen") or 0)
            metrics["source_domains_extracted_24h"] += int(payload.get("domains_extracted") or 0)
            if payload.get("top_distress_pattern"):
                top_distress_counts["stack_overflow"][str(payload["top_distress_pattern"])] += 1
    metrics["shopify_community_signals_created_24h"] = int(signal_lookup.get("shopify_community", 0))
    metrics["stack_overflow_signals_created_24h"] = int(signal_lookup.get("stack_overflow", 0))
    metrics["shopify_community_top_distress_24h"] = (
        top_distress_counts["shopify_community"].most_common(1)[0][0]
        if top_distress_counts["shopify_community"]
        else ""
    )
    metrics["stack_overflow_top_distress_24h"] = (
        top_distress_counts["stack_overflow"].most_common(1)[0][0]
        if top_distress_counts["stack_overflow"]
        else ""
    )
    summary_parts = []
    if metrics["shopify_community_threads_seen_24h"] or metrics["shopify_community_signals_created_24h"]:
        summary_parts.append(
            f"Shopify Community saw {metrics['shopify_community_threads_seen_24h']} thread(s) and created {metrics['shopify_community_signals_created_24h']} signal(s)"
        )
    if metrics["stack_overflow_questions_seen_24h"] or metrics["stack_overflow_signals_created_24h"]:
        summary_parts.append(
            f"Stack Overflow saw {metrics['stack_overflow_questions_seen_24h']} question(s) and created {metrics['stack_overflow_signals_created_24h']} signal(s)"
        )
    if metrics["source_ingestion_errors_24h"]:
        summary_parts.append(f"{metrics['source_ingestion_errors_24h']} source ingestion error(s) were recorded")
    metrics["source_health_summary"] = ". ".join(summary_parts)
    return metrics


def _signal_context(row) -> dict:
    if not row:
        return {"processor": "unknown", "distress_type": "unknown", "industry": "unknown"}
    content = row.get("content") or ""
    processor = (
        row.get("gmail_processor")
        or _match_group(r"\[processor:([^\]]+)\]", content)
        or _infer_processor(content)
        or "unknown"
    )
    distress_type = (
        row.get("gmail_distress_type")
        or _match_group(r"\[distress:([^\]]+)\]", content)
        or _infer_distress_type(content)
        or "unknown"
    )
    industry = (
        row.get("gmail_industry")
        or row.get("merchant_industry")
        or row.get("industry")
        or "unknown"
    )
    return {
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "industry": industry or "unknown",
    }


def _is_historical_gmail_pattern_noise(row) -> bool:
    if not row or str(row.get("source") or "").strip().lower() != "gmail":
        return False
    if str(row.get("gmail_thread_category") or "").strip().lower() != "merchant_distress":
        return False

    subject = str(row.get("gmail_subject") or "").strip().lower()
    sender_email = str(row.get("gmail_sender_email") or "").strip().lower()
    sender_domain = _normalize_host(row.get("gmail_sender_domain"))
    merchant_domain = _gmail_candidate_domain(row)
    confidence = float(row.get("gmail_confidence") or 0.0)
    context = _signal_context(row)

    promotional_subject = any(pattern in subject for pattern in HISTORICAL_GMAIL_PROMO_PATTERNS)
    noisy_sender = _gmail_noisy_sender(sender_email, sender_domain)
    noisy_merchant_domain = _gmail_noisy_candidate_domain(merchant_domain)
    low_identity = not merchant_domain or noisy_merchant_domain
    ambiguous_context = (
        context["processor"] == "unknown"
        and context["distress_type"] in {"reserve_hold", "unknown"}
    )
    weak_signal = confidence < 0.85

    if not promotional_subject or not ambiguous_context:
        return False
    if noisy_merchant_domain:
        return True
    if noisy_sender and low_identity:
        return True
    return weak_signal and (noisy_sender or low_identity)


def _gmail_candidate_domain(row) -> str:
    candidate = _normalize_host(row.get("gmail_merchant_domain_candidate"))
    if candidate:
        return candidate
    content = str(row.get("content") or "")
    match = re.search(r"Merchant domain candidate:\s*([^\s]+)", content, re.IGNORECASE)
    if not match:
        return ""
    return _normalize_host(match.group(1))


def _normalize_host(value: str | None) -> str:
    normalized = normalize_domain(str(value or "").strip())
    if not normalized:
        return ""
    return normalized.split("/")[0].strip().lower()


def _gmail_noisy_sender(sender_email: str, sender_domain: str) -> bool:
    localpart = sender_email.split("@", 1)[0] if "@" in sender_email else sender_email
    if any(token in localpart for token in AUTOMATED_GMAIL_LOCALPART_TOKENS):
        return True
    return _gmail_has_noisy_mail_subdomain(sender_domain)


def _gmail_noisy_candidate_domain(domain: str) -> bool:
    if not domain:
        return True
    host = _normalize_host(domain)
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return True
    if labels[0] in {"customer", "customers", "newsletter", "merchant"}:
        return True
    return _gmail_has_noisy_mail_subdomain(host)


def _gmail_has_noisy_mail_subdomain(domain: str) -> bool:
    host = _normalize_host(domain)
    labels = [label for label in host.split(".") if label]
    if len(labels) < 3:
        return False
    return any(label in HISTORICAL_GMAIL_NOISY_DOMAIN_LABELS for label in labels[:-2])


def _is_low_quality_public_forum_signal(row) -> bool:
    if not row:
        return False
    source = str(row.get("source") or "").strip().lower()
    if source not in {"shopify_forum", "shopify_community"}:
        return False

    merchant_domain = _normalize_host(row.get("merchant_domain"))
    merchant_name = str(row.get("merchant_name") or "").strip()
    content = str(row.get("content") or "")
    content_domain = _normalize_host(_match_group(r"Merchant domain candidate:\s*([^\s]+)", content) or "")

    if merchant_domain and is_valid_domain(merchant_domain) and not _is_low_quality_forum_domain(merchant_domain):
        return False
    if content_domain and is_valid_domain(content_domain) and not _is_low_quality_forum_domain(content_domain):
        return False
    if not merchant_name:
        return True
    if invalid_merchant_name_reason(merchant_name):
        return True
    return any(pattern.search(merchant_name) for pattern in LOW_QUALITY_FORUM_MERCHANT_PATTERNS)


def _is_low_quality_forum_domain(domain: str) -> bool:
    host = _normalize_host(domain)
    if not host:
        return True
    base = host.split(".")[0]
    return base in LOW_QUALITY_FORUM_DOMAIN_LABELS


def _is_non_processor_reserve_false_positive(row) -> bool:
    if not row:
        return False
    source = str(row.get("source") or "").strip().lower()
    if source not in {"shopify_forum", "shopify_community"}:
        return False
    context = _signal_context(row)
    if context["distress_type"] != "reserve_hold":
        return False
    content = str(row.get("content") or "")
    semantic_content = re.sub(r"\[[^\]]+\]", " ", content)
    lowered = semantic_content.lower()
    inventory_reserve_language = (
        any(pattern.search(semantic_content) for pattern in NON_PROCESSOR_RESERVE_FALSE_POSITIVE_PATTERNS)
        or "reserve item" in lowered
        or "reserved in their cart" in lowered
        or "reserve item to cart" in lowered
        or "app store" in lowered
    )
    if not inventory_reserve_language:
        return False
    processor_liquidity_language = (
        any(pattern.search(semantic_content) for pattern in PROCESSOR_LIQUIDITY_CONTEXT_PATTERNS)
        or "payout" in lowered
        or "funds held" in lowered
        or "funds on hold" in lowered
        or "payment provider" in lowered
        or "merchant trust team" in lowered
        or "processor" in lowered
    )
    return not processor_liquidity_language


def _pattern_key(processor: str, distress_type: str, industry: str) -> str:
    return f"{processor or 'unknown'}::{distress_type or 'unknown'}::{industry or 'unknown'}"


def _pattern_status(velocity_24h: int, signal_count: int) -> str:
    if velocity_24h >= 3:
        return "urgent"
    if velocity_24h >= 1:
        return "rising"
    return "watch"


def _pattern_summary(bucket: dict, conversion: dict | None = None, *, now: datetime | None = None) -> dict:
    conversion = conversion or {}
    trend_label = _pattern_trend_label(bucket, now=now)
    trend_phrase = (
        "cases are rising"
        if trend_label == "rising"
        else "cases are fading"
        if trend_label == "fading"
        else "cases are holding steady"
    )
    if bucket["distress_type"] == "unknown":
        qualifiers = []
        if bucket["processor"] not in {"", "unknown", None}:
            qualifiers.append(_display_processor(bucket["processor"]))
        if bucket["industry"] not in {"", "unknown", None}:
            qualifiers.append(str(bucket["industry"]))
        headline = f"Unclassified merchant distress{' - ' + ' / '.join(qualifiers) if qualifiers else ''} {trend_phrase}"
    else:
        headline = f"{_display_processor(bucket['processor'])} {_display_distress(bucket['distress_type'])} {trend_phrase}"
    if bucket["distress_type"] != "unknown" and bucket["industry"] not in {"", "unknown", None}:
        headline += f" in {bucket['industry']}"
    headline += "."
    wins = int(conversion.get("wins") or 0)
    pending = int(conversion.get("pending") or 0)
    total = int(conversion.get("opportunity_count") or 0)
    actionable = trend_label == "rising" or wins > 0 or pending >= 3
    return {
        "headline": headline,
        "signal_count": int(bucket["signal_count"]),
        "affected_merchant_count": len(bucket["affected_merchants"]),
        "velocity_24h": int(bucket["velocity_24h"]),
        "trend_label": trend_label,
        "actionable": actionable,
        "converted_before": wins > 0,
        "wins": wins,
        "pending": pending,
        "opportunity_count": total,
        "recommended_attention": (
            "operator_review" if actionable else "monitor"
        ),
        "recommended_operator_action": operator_action_for_distress(bucket["distress_type"]),
        "recommended_operator_action_label": operator_action_label(operator_action_for_distress(bucket["distress_type"])),
    }


def _serialize_pattern_row(row: dict) -> dict:
    summary = row.get("summary_json")
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}
    row["summary_json"] = summary or {}
    row["headline"] = row["summary_json"].get("headline", "")
    row["trend_label"] = row["summary_json"].get("trend_label", "stable")
    row["actionable"] = bool(row["summary_json"].get("actionable", False))
    row["converted_before"] = bool(row["summary_json"].get("converted_before", False))
    row["wins"] = int(row["summary_json"].get("wins", 0) or 0)
    row["pending"] = int(row["summary_json"].get("pending", 0) or 0)
    row["opportunity_count"] = int(row["summary_json"].get("opportunity_count", 0) or 0)
    row["first_seen_at"] = _serialize_dt(row.get("first_seen_at"))
    row["last_seen_at"] = _serialize_dt(row.get("last_seen_at"))
    row["updated_at"] = _serialize_dt(row.get("updated_at"))
    return row


def _fetch_merchant(conn, *, merchant_id: int | None = None, domain: str | None = None):
    if merchant_id is not None:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM merchants
                WHERE id = :merchant_id
                LIMIT 1
                """
            ),
            {"merchant_id": int(merchant_id)},
        ).mappings().first()
        if row:
            return dict(row)
    if domain:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM merchants
                WHERE domain = :domain OR normalized_domain = :domain
                ORDER BY last_seen DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"domain": domain},
        ).mappings().first()
        if row:
            return dict(row)
    return None


def _latest_signal_for_merchant(conn, *, merchant_id: int | None = None, domain: str | None = None):
    if merchant_id is None and not domain:
        return None
    params = {"merchant_id": merchant_id or -1, "domain": domain or ""}
    row = conn.execute(
        text(
            """
            SELECT s.id, s.content, s.detected_at, s.industry, s.classification, gti.processor AS gmail_processor,
                   gti.distress_type AS gmail_distress_type, gti.industry AS gmail_industry
            FROM signals s
            LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
            LEFT JOIN gmail_thread_intelligence gti ON gti.signal_id = s.id
            LEFT JOIN merchants m ON m.id = s.merchant_id
            WHERE s.merchant_id = :merchant_id
               OR ms.merchant_id = :merchant_id
               OR (COALESCE(:domain, '') != '' AND (m.domain = :domain OR m.normalized_domain = :domain))
            ORDER BY s.detected_at DESC
            LIMIT 1
            """
        ),
        params,
    ).mappings().first()
    return dict(row) if row else None


def _fetch_merchant_with_fallback(*, merchant_id: int | None = None, domain: str | None = None):
    with engine.connect() as conn:
        return _fetch_merchant(conn, merchant_id=merchant_id, domain=domain)


def _latest_signal_for_merchant_by_lookup(*, merchant_id: int | None = None, domain: str | None = None):
    with engine.connect() as conn:
        return _latest_signal_for_merchant(conn, merchant_id=merchant_id, domain=domain)


def _counter_to_list(counter: Counter) -> list[dict]:
    return [{"value": key, "count": count} for key, count in counter.most_common(3)]


def _recent_signal_summary(last_signal: dict | None, context: dict | None) -> str:
    if not last_signal:
        return "I do not have a recent linked signal summary yet."
    if context and context.get("processor") != "unknown" and context.get("distress_type") != "unknown":
        return (
            f"The newest linked signal points to {_display_processor(context['processor'])} "
            f"{_display_distress(context['distress_type'])}."
        )
    return "There is a recent linked signal, but the processor-specific distress pattern is still unclear."


def _merchant_urgency_score(merchant: dict, opportunity: dict | None, pattern: dict | None) -> int:
    score = int(float(merchant.get("distress_score") or 0))
    if (opportunity or {}).get("status") in {"pending_review", "approved"}:
        score += 30
    if pattern and pattern.get("status") == "urgent":
        score += 20
    elif pattern and pattern.get("status") == "rising":
        score += 10
    return score


def _merchant_why_it_matters(
    merchant: dict,
    processor: str,
    distress_type: str,
    pattern: dict | None,
    *,
    merchant_name_display: str | None = None,
) -> str:
    merchant_name = merchant_name_display or merchant.get("canonical_name") or merchant.get("domain") or "This merchant"
    if pattern and pattern.get("status") in {"urgent", "rising"}:
        return (
            f"{merchant_name} is tied to the {_display_processor(processor)} {_display_distress(distress_type)} "
            f"pattern, which is {pattern.get('status')} right now."
        )
    if processor != "unknown" and distress_type != "unknown":
        return f"{merchant_name} appears to rely on {_display_processor(processor)} and the current distress signal is {_display_distress(distress_type)}."
    return f"{merchant_name} is still worth watching, but the processor-specific pattern is not fully resolved yet."


def _pattern_lookup(processor: str, distress_type: str, industry: str):
    if processor == "unknown" and distress_type == "unknown":
        return None
    ensure_payflux_intelligence_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM distress_pattern_clusters
                WHERE processor = :processor
                  AND distress_type = :distress_type
                  AND industry IN (:industry, 'unknown')
                ORDER BY CASE WHEN industry = :industry THEN 0 ELSE 1 END, updated_at DESC
                LIMIT 1
                """
            ),
            {
                "processor": processor or "unknown",
                "distress_type": distress_type or "unknown",
                "industry": industry or "unknown",
            },
        ).mappings().first()
    return _serialize_pattern_row(dict(row)) if row else None


def _conversion_pattern_label(row: dict) -> str:
    if not row:
        return ""
    return f"{_display_processor(row.get('processor'))} {_display_distress(row.get('distress_type'))}"


def _display_processor(value: str | None) -> str:
    mapping = {
        "stripe": "Stripe",
        "paypal": "PayPal",
        "square": "Square",
        "adyen": "Adyen",
        "shopify_payments": "Shopify Payments",
        "braintree": "Braintree",
        "authorize_net": "Authorize.net",
        "worldpay": "Worldpay",
        "checkout_com": "Checkout.com",
        "unknown": "unresolved processor",
        "unclassified": "an unresolved processor",
    }
    return mapping.get((value or "unknown").lower(), str(value).replace("_", " "))


def _display_distress(value: str | None) -> str:
    if not value:
        return "distress"
    if value == "unknown":
        return "distress"
    return str(value).replace("_", " ")


def _top_profile_label(profile: dict) -> str:
    if not profile:
        return ""
    name = profile.get("merchant_name_display") or profile.get("merchant_name")
    domain = profile.get("merchant_domain")
    if name and domain:
        return f"{name} ({domain})"
    return domain or name or ""


def _display_merchant_name(name: str | None, domain: str | None = None) -> str | None:
    if not name and not domain:
        return None

    candidate = (name or "").strip()
    if candidate:
        candidate = re.sub(r"[_\-]+", " ", candidate)
        candidate = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        candidate = _split_known_suffixes(candidate)
        candidate = " ".join(_title_preserving_acronyms(part) for part in candidate.split())
        if len(candidate) > 2:
            return candidate

    if domain:
        root = domain.split(".", 1)[0]
        root = re.sub(r"[_\-]+", " ", root)
        root = _split_known_suffixes(root)
        root = re.sub(r"\s+", " ", root).strip()
        if root:
            return " ".join(_title_preserving_acronyms(part) for part in root.split())
    return name


def _split_known_suffixes(value: str) -> str:
    suffixes = [
        "cart",
        "goods",
        "store",
        "shop",
        "market",
        "commerce",
        "pay",
        "payments",
        "labs",
        "studio",
        "digital",
        "health",
        "wellness",
        "systems",
        "solutions",
        "agency",
        "group",
    ]
    compact = value.replace(" ", "")
    lowered = compact.lower()
    for suffix in suffixes:
        if lowered.endswith(suffix) and len(compact) > len(suffix) + 2:
            prefix = compact[: -len(suffix)]
            return f"{prefix} {suffix}"
    return value


def _title_preserving_acronyms(token: str) -> str:
    if token.isupper() and len(token) <= 5:
        return token
    if token.lower() in {"saas", "kyc", "kyb"}:
        return token.upper()
    return token.capitalize()


def _pattern_trend_label(bucket: dict, *, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    last_seen = _ensure_utc_datetime(bucket.get("last_seen_at"))
    if int(bucket.get("velocity_24h") or 0) > 0:
        return "rising"
    if last_seen and (now - last_seen).total_seconds() <= 7 * 24 * 3600:
        return "stable"
    return "fading"


def _conversion_lookup(conn) -> dict[str, dict]:
    rows = conn.execute(
        text(
            """
            SELECT
                pattern_key,
                COUNT(*) AS opportunity_count,
                SUM(CASE WHEN outcome_status = 'won' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN outcome_status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM opportunity_conversion_intelligence
            GROUP BY pattern_key
            """
        )
    ).mappings().fetchall()
    return {str(row["pattern_key"]): dict(row) for row in rows if row.get("pattern_key")}


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _strategy_text(value) -> str:
    if isinstance(value, dict):
        return value.get("angle") or value.get("recommended_pitch") or value.get("pain_point") or ""
    if value is None:
        return ""
    return str(value)


def _normalize_distress_type(value: str | None) -> str:
    return normalize_distress_topic(value)


def _infer_processor(content: str) -> str | None:
    normalized = (content or "").lower()
    for processor, patterns in PROCESSOR_PATTERNS.items():
        if any(re.search(pattern, normalized) for pattern in patterns):
            return processor
    return None


def _infer_distress_type(content: str) -> str | None:
    normalized = (content or "").lower()
    for distress_type, patterns in DISTRESS_PATTERNS.items():
        if any(re.search(pattern, normalized) for pattern in patterns):
            return distress_type
    return None


def _match_group(pattern: str, value: str) -> str | None:
    match = re.search(pattern, value or "")
    if not match:
        return None
    found = (match.group(1) or "").strip()
    return found or None


def _latest_timestamp(*values):
    normalized = []
    for value in values:
        if not value:
            continue
        dt = _ensure_utc_datetime(value)
        if dt:
            normalized.append(dt)
    if not normalized:
        return None
    return max(normalized)


def _serialize_dt(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _ensure_utc_datetime(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None
