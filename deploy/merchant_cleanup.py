"""
Merchant cleanup pass for invalid merchant names already stored in the DB.

This is a conservative remediation step:
1. Find merchants whose names do not validate against their linked signals.
2. Re-run merchant identity resolution for those signals.
3. Reassign signal and downstream merchant references when there is a clear
   replacement merchant.
4. Remove the junk merchant record and orphaned downstream rows.
"""
import os
import sys
from collections import Counter

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event

from runtime.intelligence.brand_extraction import extract_brand_candidates, is_valid_brand_candidate
from runtime.intelligence.merchant_attribution import detect_processor
from runtime.intelligence.merchant_signal_classifier import (
    CLASS_MERCHANT_OPERATOR,
    classify_merchant_signal,
)
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard

logger = get_logger("merchant_cleanup")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_SIGNALS_PER_MERCHANT = 25
NOISY_SOURCES = {"signal_pipeline", "neighbor_discovery", "merchant_graph_co_mention"}
PROCESSOR_DISPLAY_NAMES = {
    "2checkout": "2Checkout",
    "authorize.net": "Authorize.net",
    "paypal": "PayPal",
    "square": "Square",
    "stripe": "Stripe",
}


def _load_candidate_merchants(conn, limit):
    return conn.execute(text("""
        SELECT id, canonical_name, domain, detected_from, status,
               COALESCE(opportunity_score, 0) AS opportunity_score,
               COALESCE(distress_score, 0) AS distress_score
        FROM merchants
        WHERE canonical_name NOT LIKE 'Unknown %'
        ORDER BY COALESCE(opportunity_score, 0) DESC,
                 COALESCE(distress_score, 0) DESC,
                 id ASC
        LIMIT :limit
    """), {"limit": limit}).mappings().all()


def _load_signal_rows(conn, merchant_id):
    rows = conn.execute(text("""
        SELECT DISTINCT s.id, s.content, s.priority_score, s.merchant_name
        FROM signals s
        LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
        WHERE s.merchant_id = :mid OR ms.merchant_id = :mid
        ORDER BY s.id DESC
        LIMIT :limit
    """), {"mid": merchant_id, "limit": MAX_SIGNALS_PER_MERCHANT}).mappings().all()
    return rows


def _is_invalid_merchant(merchant, signal_rows):
    name = merchant["canonical_name"] or ""
    if not name:
        return False

    if merchant["detected_from"] not in NOISY_SOURCES and merchant["status"] != "provisional":
        return False

    validated_in_context = False
    has_merchant_signal = False
    for row in signal_rows:
        content = row["content"] or ""
        if classify_merchant_signal(content)["classification"] == CLASS_MERCHANT_OPERATOR:
            has_merchant_signal = True
        if is_valid_brand_candidate(name, text=content, source="cleanup"):
            validated_in_context = True
            break

    if validated_in_context:
        return False
    if signal_rows and not has_merchant_signal:
        return True
    if signal_rows:
        return True

    synthetic_context = f"Stripe froze my store {name}"
    return not is_valid_brand_candidate(name, text=synthetic_context, source="cleanup")


def _has_last_resurfaced_at(conn):
    return bool(conn.execute(text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'signals'
          AND column_name = 'last_resurfaced_at'
        LIMIT 1
    """)).fetchone())


def _unknown_processor_merchant_name(processor):
    if not processor:
        return None
    display = PROCESSOR_DISPLAY_NAMES.get(processor.lower(), processor.replace("_", " ").title())
    return f"Unknown {display} Merchant"


def _get_or_create_merchant(conn, canonical_name):
    merchant_slug = build_merchant_slug(canonical_name)
    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE slug = :slug OR LOWER(canonical_name) = LOWER(:name)
        LIMIT 1
    """), {"slug": merchant_slug, "name": canonical_name}).fetchone()
    if row:
        return row[0]

    row = conn.execute(text("""
        INSERT INTO merchants (canonical_name, slug, detected_from)
        VALUES (:name, :slug, 'merchant_cleanup')
        ON CONFLICT (slug) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
        RETURNING id
    """), {"name": canonical_name, "slug": merchant_slug}).fetchone()
    return row[0] if row else None


def _resolve_replacement_merchant_id(conn, signal_row):
    content = signal_row["content"] or ""
    signal_classification = classify_merchant_signal(content)
    if signal_classification["classification"] != CLASS_MERCHANT_OPERATOR:
        return None

    candidates = extract_brand_candidates(content, merchant_name=None, author=None)
    if candidates:
        return _get_or_create_merchant(conn, candidates[0]["brand"])

    processor = detect_processor(content)
    fallback_name = _unknown_processor_merchant_name(processor) if processor else "Unknown Distressed Merchant"
    return _get_or_create_merchant(conn, fallback_name)


def _update_signal_mapping(conn, old_merchant_id, signal_id, replacement_id=None, old_name=None, reset_resurfacing=False):
    if replacement_id:
        conn.execute(text("""
            UPDATE signals
            SET merchant_id = :replacement_id
            WHERE id = :signal_id
        """), {"replacement_id": replacement_id, "signal_id": signal_id})
        conn.execute(text("""
            INSERT INTO merchant_signals (merchant_id, signal_id)
            VALUES (:replacement_id, :signal_id)
            ON CONFLICT (signal_id) DO UPDATE SET merchant_id = EXCLUDED.merchant_id
        """), {"replacement_id": replacement_id, "signal_id": signal_id})
    else:
        params = {"signal_id": signal_id, "old_name": old_name or ""}
        if reset_resurfacing:
            conn.execute(text("""
                UPDATE signals
                SET merchant_id = NULL,
                    merchant_name = CASE
                        WHEN COALESCE(merchant_name, '') = :old_name THEN NULL
                        ELSE merchant_name
                    END,
                    last_resurfaced_at = NULL
                WHERE id = :signal_id
            """), params)
        else:
            conn.execute(text("""
                UPDATE signals
                SET merchant_id = NULL,
                    merchant_name = CASE
                        WHEN COALESCE(merchant_name, '') = :old_name THEN NULL
                        ELSE merchant_name
                    END
                WHERE id = :signal_id
            """), params)

    conn.execute(text("""
        DELETE FROM merchant_signals
        WHERE merchant_id = :old_merchant_id AND signal_id = :signal_id
    """), {"old_merchant_id": old_merchant_id, "signal_id": signal_id})


def _reassign_non_signal_refs(conn, old_merchant_id, replacement_id=None):
    table_names = (
        "merchant_contacts",
        "merchant_opportunities",
        "sales_outreach_events",
        "merchant_conversations",
    )
    action_counts = {}
    for table_name in table_names:
        if replacement_id:
            count = conn.execute(text(f"""
                UPDATE {table_name}
                SET merchant_id = :replacement_id
                WHERE merchant_id = :old_merchant_id
            """), {
                "replacement_id": replacement_id,
                "old_merchant_id": old_merchant_id,
            }).rowcount
            action_counts[table_name] = count
        else:
            count = conn.execute(text(f"""
                DELETE FROM {table_name}
                WHERE merchant_id = :old_merchant_id
            """), {"old_merchant_id": old_merchant_id}).rowcount
            action_counts[table_name] = count
    return action_counts


def run_invalid_merchant_cleanup(limit=50):
    summary = {
        "merchants_scanned": 0,
        "invalid_merchants_found": 0,
        "merchants_deleted": 0,
        "signals_reassigned": 0,
        "signals_unassigned": 0,
        "merchant_refs_reassigned": 0,
        "merchant_refs_deleted": 0,
    }

    with engine.connect() as conn:
        ensure_merchant_slug_guard()
        reset_resurfacing = _has_last_resurfaced_at(conn)
        merchants = _load_candidate_merchants(conn, limit)

        for merchant in merchants:
            summary["merchants_scanned"] += 1
            signal_rows = _load_signal_rows(conn, merchant["id"])
            if not _is_invalid_merchant(merchant, signal_rows):
                continue

            summary["invalid_merchants_found"] += 1
            replacement_counts = Counter()
            signal_actions = []

            for row in signal_rows:
                replacement_id = _resolve_replacement_merchant_id(conn, row)
                if replacement_id and replacement_id != merchant["id"]:
                    replacement_counts[replacement_id] += 1
                    signal_actions.append((row["id"], replacement_id))
                else:
                    signal_actions.append((row["id"], None))

            dominant_replacement = replacement_counts.most_common(1)[0][0] if replacement_counts else None

            for signal_id, replacement_id in signal_actions:
                chosen_replacement = replacement_id or None
                _update_signal_mapping(
                    conn,
                    merchant["id"],
                    signal_id,
                    replacement_id=chosen_replacement,
                    old_name=merchant["canonical_name"],
                    reset_resurfacing=reset_resurfacing,
                )
                if chosen_replacement:
                    summary["signals_reassigned"] += 1
                else:
                    summary["signals_unassigned"] += 1

            ref_counts = _reassign_non_signal_refs(
                conn,
                merchant["id"],
                replacement_id=dominant_replacement,
            )
            if dominant_replacement:
                summary["merchant_refs_reassigned"] += sum(ref_counts.values())
            else:
                summary["merchant_refs_deleted"] += sum(ref_counts.values())

            conn.execute(text("""
                DELETE FROM merchants WHERE id = :merchant_id
            """), {"merchant_id": merchant["id"]})
            summary["merchants_deleted"] += 1

            save_event("merchant_cleanup_action", {
                "merchant_id": merchant["id"],
                "merchant_name": merchant["canonical_name"],
                "replacement_merchant_id": dominant_replacement,
                "signals_reassigned": sum(1 for _, replacement_id in signal_actions if replacement_id),
                "signals_unassigned": sum(1 for _, replacement_id in signal_actions if not replacement_id),
                "ref_counts": ref_counts,
            })
            logger.info(
                "Cleaned invalid merchant %s (%s) replacement=%s",
                merchant["id"],
                merchant["canonical_name"],
                dominant_replacement,
            )

        conn.commit()

    save_event("merchant_cleanup_run", summary)
    logger.info(f"Merchant cleanup run complete: {summary}")
    return summary


if __name__ == "__main__":
    print(run_invalid_merchant_cleanup())
