"""
Opportunity trigger engine.

Promotes high-opportunity merchants into the deal/outreach pipeline so scoring
turns into action without manual review.
"""
import json
import os
import sys

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event

from runtime.ops.deal_sourcing import _process_merchant_opportunity

logger = get_logger("opportunity_trigger_engine")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MIN_TRIGGER_SCORE = 40
MAX_TRIGGERS_PER_RUN = 10


def _parse_signal_ids(signal_ids_raw):
    if not signal_ids_raw:
        return []
    try:
        parsed = json.loads(signal_ids_raw) if isinstance(signal_ids_raw, str) else signal_ids_raw
    except (TypeError, ValueError):
        return []
    signal_ids = []
    for item in parsed or []:
        try:
            signal_ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return signal_ids


def _load_trigger_context(conn, merchant_id):
    signal_rows = conn.execute(text("""
        SELECT id, content
        FROM signals
        WHERE merchant_id = :merchant_id
        ORDER BY detected_at DESC
        LIMIT 100
    """), {"merchant_id": merchant_id}).fetchall()
    signal_ids = {int(row[0]) for row in signal_rows}
    if not signal_ids:
        return ("High opportunity score trigger", 0)

    cluster_rows = conn.execute(text("""
        SELECT cluster_topic, cluster_size, signal_ids
        FROM clusters
        ORDER BY created_at DESC
        LIMIT 200
    """)).fetchall()
    for cluster_topic, cluster_size, cluster_signal_ids in cluster_rows:
        parsed_ids = set(_parse_signal_ids(cluster_signal_ids))
        if signal_ids.intersection(parsed_ids):
            return (cluster_topic or "High opportunity score trigger", int(cluster_size or 0))
    return ("High opportunity score trigger", 0)


def run_opportunity_trigger_engine(limit=MAX_TRIGGERS_PER_RUN):
    triggered = 0

    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT id, canonical_name, domain, industry, distress_score, opportunity_score, confidence_score
            FROM merchants
            WHERE COALESCE(opportunity_score, 0) >= :min_score
              AND COALESCE(status, 'active') != 'provisional'
              AND id NOT IN (
                    SELECT merchant_id
                    FROM merchant_opportunities
                    WHERE merchant_id IS NOT NULL
                      AND created_at >= NOW() - INTERVAL '7 days'
              )
            ORDER BY opportunity_score DESC, distress_score DESC, last_seen DESC
            LIMIT :limit
        """), {"min_score": MIN_TRIGGER_SCORE, "limit": limit}).fetchall()

        for merchant_id, merchant_name, domain, industry, distress_score, opportunity_score, confidence_score in merchants:
            processor_mentions = conn.execute(text("""
                SELECT COUNT(*)
                FROM signals
                WHERE merchant_id = :merchant_id
                  AND (
                    content ILIKE '%stripe%'
                    OR content ILIKE '%paypal%'
                    OR content ILIKE '%square%'
                  )
            """), {"merchant_id": merchant_id}).scalar() or 0

            if (confidence_score or 0) < 40:
                save_event("merchant_trigger_skipped", {"merchant": merchant_name, "reason": "low_confidence"})
                continue

            if not (
                (opportunity_score or 0) >= 40
                and (distress_score or 0) >= 10
                and ((domain and domain.strip() != "") or processor_mentions > 0)
            ):
                save_event("merchant_trigger_skipped", {"merchant": merchant_name, "reason": "gate_failed"})
                continue

            topic, cluster_size = _load_trigger_context(conn, merchant_id)
            created = _process_merchant_opportunity(
                conn,
                merchant_id,
                merchant_name,
                domain,
                industry or "unknown",
                topic,
                cluster_size,
                float(opportunity_score or distress_score or 0),
            )
            if not created:
                continue
            triggered += 1
            save_event("opportunity_triggered", {
                "merchant_id": merchant_id,
                "merchant": merchant_name,
                "opportunity_score": float(opportunity_score or 0),
                "topic": topic,
            })

        conn.commit()

    result = {"merchants_considered": len(merchants), "opportunities_triggered": triggered}
    save_event("opportunity_trigger_run", result)
    logger.info(f"Opportunity trigger engine complete: {result}")
    return result


if __name__ == "__main__":
    print(run_opportunity_trigger_engine())
