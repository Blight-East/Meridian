"""
Merchant opportunity scoring engine.

Ranks merchants by likelihood of converting into actionable sales
opportunities using deterministic merchant and signal features.
"""
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
from runtime.ops import conversion_upgrade as _upgrade

logger = get_logger("opportunity_scoring")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")


def _init_opportunity_score_column():
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS opportunity_score FLOAT DEFAULT 0,
            ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active',
            ADD COLUMN IF NOT EXISTS confidence_score INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS urgency_score FLOAT DEFAULT 0
        """))
        conn.commit()


def _clamp_score(raw_score):
    return max(0.0, min(float(raw_score), 100.0))


def score_merchants(limit=100):
    _init_opportunity_score_column()

    scored = 0

    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT m.id, m.canonical_name, m.distress_score, m.domain, m.status, m.confidence_score,
                   (SELECT c.email FROM merchant_contacts c WHERE c.merchant_id = m.id LIMIT 1) as contact_email
            FROM merchants m
            ORDER BY m.distress_score DESC, m.last_seen DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

        for merchant_id, canonical_name, distress_score, domain, status, confidence_score, contact_email in merchants:
            signal_count = conn.execute(text("""
                SELECT COUNT(*)
                FROM signals
                WHERE merchant_id = :merchant_id
            """), {"merchant_id": merchant_id}).scalar() or 0

            recent_signal_count = conn.execute(text("""
                SELECT COUNT(*)
                FROM signals
                WHERE merchant_id = :merchant_id
                  AND detected_at >= NOW() - INTERVAL '7 days'
            """), {"merchant_id": merchant_id}).scalar() or 0

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

            cluster_size = 0
            cluster_rows = conn.execute(text("""
                SELECT cluster_size, signal_ids
                FROM clusters
                ORDER BY created_at DESC
                LIMIT 200
            """)).fetchall()
            merchant_signal_ids = {
                int(row[0]) for row in conn.execute(text("""
                    SELECT id
                    FROM signals
                    WHERE merchant_id = :merchant_id
                    ORDER BY detected_at DESC
                    LIMIT 200
                """), {"merchant_id": merchant_id}).fetchall()
            }
            for row_cluster_size, signal_ids in cluster_rows:
                if not merchant_signal_ids:
                    break
                try:
                    import json
                    parsed = json.loads(signal_ids) if isinstance(signal_ids, str) else signal_ids
                    parsed_ids = {int(item) for item in (parsed or [])}
                except (TypeError, ValueError):
                    continue
                if merchant_signal_ids.intersection(parsed_ids):
                    cluster_size = max(cluster_size, int(row_cluster_size or 0))

            domain_present = bool(domain)
            
            eligible = (
                status != "junk"
                and (
                    domain is not None
                    or processor_mentions > 0
                    or contact_email is not None
                )
            )
            
            if not eligible:
                save_event("merchant_scoring_skipped", {"merchant": canonical_name, "reason": "no_domain_no_processor_or_email"})
                continue
            
            if (confidence_score or 0) < 30:
                save_event("merchant_scoring_skipped", {"merchant": canonical_name, "reason": "low_confidence"})
                continue
            
            opportunity_score = _clamp_score(
                (float(distress_score or 0) * 2)
                + (int(recent_signal_count) * 5)
                + (int(processor_mentions) * 3)
                + (int(cluster_size) * 2)
                + (10 if domain_present else 0)
            )

            # Determine the dominant processor for urgency weighting.
            # Cheap single-row scan over the last 200 signals for the merchant.
            dominant_processor = conn.execute(text("""
                SELECT CASE
                         WHEN LOWER(content) LIKE '%stripe%' THEN 'stripe'
                         WHEN LOWER(content) LIKE '%paypal%' THEN 'paypal'
                         WHEN LOWER(content) LIKE '%square%' THEN 'square'
                         ELSE 'other'
                       END AS processor, COUNT(*) AS cnt
                FROM signals
                WHERE merchant_id = :merchant_id
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 1
            """), {"merchant_id": merchant_id}).fetchone()
            urgency_processor = dominant_processor[0] if dominant_processor else "other"

            urgency_score = _upgrade.compute_urgency_score(
                distress_score=distress_score,
                recent_signal_count=recent_signal_count,
                processor=urgency_processor,
            )

            # Always write urgency_score (additive column).  Whether the
            # pipeline *reads* it at sort time is still gated by
            # MERIDIAN_ENABLE_URGENCY_SORT downstream in deal_sourcing.
            conn.execute(text("""
                UPDATE merchants
                SET opportunity_score = :score,
                    urgency_score = :urgency
                WHERE id = :merchant_id
            """), {
                "score": opportunity_score,
                "urgency": urgency_score,
                "merchant_id": merchant_id,
            })
            save_event("merchant_opportunity_scored", {
                "merchant": canonical_name,
                "score": opportunity_score,
                "urgency_score": urgency_score,
                "urgency_processor": urgency_processor,
                "signal_count": signal_count,
                "recent_signal_count": recent_signal_count,
                "processor_mentions": processor_mentions,
                "cluster_size": cluster_size,
                "domain_present": domain_present,
            })
            scored += 1

        conn.commit()

    result = {"merchants_scored": scored}
    logger.info(f"Merchant opportunity scoring complete: {result}")
    return result


if __name__ == "__main__":
    print(score_merchants())
