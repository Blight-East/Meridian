from __future__ import annotations

import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from runtime.intelligence.contact_discovery import get_best_contact

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)

SOURCE_NAME = "shopify_community"


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _serialize_dt(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _signal_meta(conn, signal_id: int) -> dict:
    row = conn.execute(
        text(
            """
            SELECT data, created_at
            FROM events
            WHERE event_type = 'shopify_community_signal_created'
              AND (data->>'signal_id')::BIGINT = :signal_id
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"signal_id": int(signal_id)},
    ).mappings().first()
    if not row:
        return {}
    payload = _json_dict(row.get("data"))
    payload["created_at"] = _serialize_dt(row.get("created_at"))
    return payload


def _linked_merchant(conn, signal_id: int):
    row = conn.execute(
        text(
            """
            SELECT m.*
            FROM merchant_signals ms
            JOIN merchants m ON m.id = ms.merchant_id
            WHERE ms.signal_id = :signal_id
            ORDER BY m.last_seen DESC NULLS LAST
            LIMIT 1
            """
        ),
        {"signal_id": int(signal_id)},
    ).mappings().first()
    return dict(row) if row else None


def _linked_opportunity(conn, merchant_id: int, signal_detected_at):
    if not merchant_id:
        return None
    row = conn.execute(
        text(
            """
            SELECT *
            FROM merchant_opportunities
            WHERE merchant_id = :merchant_id
              AND created_at >= :detected_at
            ORDER BY created_at ASC
            LIMIT 1
            """
        ),
        {"merchant_id": int(merchant_id), "detected_at": signal_detected_at},
    ).mappings().first()
    return dict(row) if row else None


def _outreach_context(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict:
    if not opportunity_id and not merchant_domain:
        return {}
    try:
        from runtime.ops.outreach_execution import get_outreach_context_snapshot

        return get_outreach_context_snapshot(opportunity_id=opportunity_id, merchant_domain=merchant_domain) or {}
    except Exception:
        return {}


def _contact_state_without_opportunity(merchant_id: int | None) -> str:
    if not merchant_id:
        return ""
    best_contact = get_best_contact(int(merchant_id))
    if not best_contact:
        return ""
    if best_contact.get("email_verified") or float(best_contact.get("confidence") or 0.0) >= 0.8:
        return "verified_contact_found"
    return "weak_contact_found"


def trace_shopify_signal_funnel(signal_id: int) -> dict | None:
    with engine.connect() as conn:
        signal = conn.execute(
            text(
                """
                SELECT id, source, content, detected_at, classification, merchant_id
                FROM signals
                WHERE id = :signal_id
                  AND source = :source
                LIMIT 1
                """
            ),
            {"signal_id": int(signal_id), "source": SOURCE_NAME},
        ).mappings().first()
        if not signal:
            return None
        signal = dict(signal)
        meta = _signal_meta(conn, int(signal["id"]))
        merchant = None
        merchant_id = signal.get("merchant_id")
        if merchant_id:
            merchant = conn.execute(
                text("SELECT * FROM merchants WHERE id = :merchant_id LIMIT 1"),
                {"merchant_id": int(merchant_id)},
            ).mappings().first()
        if not merchant:
            merchant = _linked_merchant(conn, int(signal["id"]))
        merchant = dict(merchant) if merchant else {}
        opportunity = _linked_opportunity(conn, int(merchant.get("id") or 0), signal.get("detected_at"))

    opportunity = dict(opportunity) if opportunity else {}
    context = _outreach_context(opportunity_id=opportunity.get("id"), merchant_domain=opportunity.get("merchant_domain"))
    contact_state = context.get("contact_state") or _contact_state_without_opportunity(merchant.get("id"))
    queue_quality_class = context.get("queue_eligibility_class") or ""
    outreach_state = ""
    if opportunity:
        outreach_state = "outreach_eligible" if context.get("contact_send_eligible") and queue_quality_class == "outreach_eligible" else "blocked"

    return {
        "source": SOURCE_NAME,
        "board": meta.get("board") or "",
        "thread_url": meta.get("url") or "",
        "thread_title": _extract_title(signal.get("content") or ""),
        "signal_id": int(signal["id"]),
        "signal_detected_at": _serialize_dt(signal.get("detected_at")),
        "signal_created": True,
        "candidate_parsed": True,
        "thread_seen": bool(meta),
        "merchant_attribution_state": meta.get("merchant_attribution_state") or ("domain_resolved" if meta.get("merchant_domain") else "domainless"),
        "signal_domain_candidate": meta.get("merchant_domain") or "",
        "merchant_matched": bool(merchant),
        "merchant_id": merchant.get("id"),
        "merchant_domain": merchant.get("domain") or merchant.get("normalized_domain") or "",
        "merchant_name": merchant.get("canonical_name") or "",
        "opportunity_created": bool(opportunity),
        "opportunity_id": opportunity.get("id"),
        "opportunity_status": opportunity.get("status") or "",
        "queue_quality_class": queue_quality_class,
        "queue_quality_reason": context.get("queue_eligibility_reason") or "",
        "contact_state": contact_state or "",
        "outreach_state": outreach_state or "",
        "contact_send_eligible": bool(context.get("contact_send_eligible")),
        "contact_reason": context.get("contact_reason") or "",
    }


def recent_shopify_signal_funnels(limit: int = 10, window_hours: int = 24) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id
                FROM signals
                WHERE source = :source
                  AND detected_at >= NOW() - (:window_hours || ' hours')::interval
                ORDER BY detected_at DESC
                LIMIT :limit
                """
            ),
            {"source": SOURCE_NAME, "window_hours": int(window_hours), "limit": int(limit)},
        ).fetchall()
    items = []
    for row in rows:
        funnel = trace_shopify_signal_funnel(int(row[0]))
        if funnel:
            items.append(funnel)
    return items


def get_shopify_source_attribution_metrics(window_hours: int = 24) -> dict:
    funnels = recent_shopify_signal_funnels(limit=200, window_hours=window_hours)
    with engine.connect() as conn:
        cycle_rows = conn.execute(
            text(
                """
                SELECT event_type, data
                FROM events
                WHERE event_type IN (
                    'shopify_community_ingestion_cycle',
                    'shopify_community_rate_limited',
                    'shopify_community_duplicate_skipped',
                    'shopify_community_signal_created',
                    'shopify_community_board_cycle'
                )
                  AND created_at >= NOW() - (:window_hours || ' hours')::interval
                ORDER BY created_at DESC
                """
            ),
            {"window_hours": int(window_hours)},
        ).mappings().fetchall()

    metrics = {
        "shopify_community_threads_seen_24h": 0,
        "shopify_community_signals_created_24h": 0,
        "shopify_community_signals_filtered_24h": 0,
        "shopify_community_rate_limited_24h": 0,
        "shopify_community_duplicate_skips_24h": 0,
        "shopify_community_domainless_signals_24h": 0,
        "shopify_community_merchant_matched_24h": 0,
        "shopify_community_opportunities_created_24h": 0,
        "shopify_community_outreach_eligible_24h": 0,
        "shopify_community_blocked_24h": 0,
        "shopify_community_board_summary": "",
        "shopify_community_funnel_summary": "",
    }
    board_status_counts: dict[str, Counter] = {}
    for row in cycle_rows:
        event_type = str(row.get("event_type") or "")
        payload = _json_dict(row.get("data"))
        if event_type == "shopify_community_ingestion_cycle":
            metrics["shopify_community_threads_seen_24h"] += int(payload.get("threads_seen") or 0)
            metrics["shopify_community_signals_filtered_24h"] += int(payload.get("filtered") or 0)
        elif event_type == "shopify_community_rate_limited":
            metrics["shopify_community_rate_limited_24h"] += 1
        elif event_type == "shopify_community_duplicate_skipped":
            metrics["shopify_community_duplicate_skips_24h"] += 1
        elif event_type == "shopify_community_signal_created":
            metrics["shopify_community_signals_created_24h"] += 1
            if not str(payload.get("merchant_domain") or "").strip():
                metrics["shopify_community_domainless_signals_24h"] += 1
        elif event_type == "shopify_community_board_cycle":
            board = str(payload.get("board") or "").strip()
            if board:
                board_status_counts.setdefault(board, Counter())[str(payload.get("status") or "unknown")] += 1

    matched = 0
    opportunities = 0
    outreach_eligible = 0
    blocked = 0
    for funnel in funnels:
        if funnel.get("merchant_matched"):
            matched += 1
        if funnel.get("opportunity_created"):
            opportunities += 1
            if funnel.get("outreach_state") == "outreach_eligible":
                outreach_eligible += 1
            else:
                blocked += 1
    metrics["shopify_community_merchant_matched_24h"] = matched
    metrics["shopify_community_opportunities_created_24h"] = opportunities
    metrics["shopify_community_outreach_eligible_24h"] = outreach_eligible
    metrics["shopify_community_blocked_24h"] = blocked
    metrics["shopify_community_board_summary"] = _board_summary(board_status_counts)
    metrics["shopify_community_funnel_summary"] = (
        f"Shopify Community produced {metrics['shopify_community_signals_created_24h']} signals, "
        f"{matched} merchant matches, {opportunities} opportunities, and {outreach_eligible} outreach-eligible cases in the last 24h."
    )
    return metrics


def _board_summary(board_status_counts: dict[str, Counter]) -> str:
    if not board_status_counts:
        return ""
    parts = []
    for board, statuses in board_status_counts.items():
        if statuses.get("rate_limited"):
            parts.append(f"{board}: rate-limited")
        elif statuses.get("signals_ready"):
            parts.append(f"{board}: signals")
        elif statuses.get("filtered_only"):
            parts.append(f"{board}: filtered only")
        elif statuses.get("cooldown_skipped"):
            parts.append(f"{board}: cooldown")
        else:
            parts.append(f"{board}: observed")
    return "; ".join(parts[:3])


def _extract_title(content: str) -> str:
    marker = "Title:"
    if marker not in content:
        return ""
    remainder = content.split(marker, 1)[1].strip()
    if ". Body:" in remainder:
        remainder = remainder.split(". Body:", 1)[0].strip()
    return remainder
