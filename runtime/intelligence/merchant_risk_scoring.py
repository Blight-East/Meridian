"""
Merchant-level risk scoring for the merchant intelligence graph.
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
from runtime.intelligence.merchant_graph_store import (
    init_merchant_graph_tables,
    sync_all_merchant_entities,
)

logger = get_logger("merchant_risk_scoring")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_MERCHANTS_PER_RUN = 500
NEGATIVE_SIGNAL_TERMS = (
    "frozen",
    "held",
    "holding",
    "reserve",
    "chargeback",
    "terminated",
    "suspended",
    "shutdown",
    "banned",
    "locked",
    "withheld",
    "delay",
)


def _clamp_score(value):
    return round(max(0.0, min(100.0, value)), 2)


def update_merchant_risk_scores(limit=MAX_MERCHANTS_PER_RUN):
    stats = {
        "merchants_scored": 0,
        "clusters_updated": 0,
    }

    with engine.connect() as conn:
        init_merchant_graph_tables(conn)
        sync_all_merchant_entities(conn)

        merchants = conn.execute(text("""
            SELECT
                m.id,
                COALESCE(m.distress_score, 0) AS distress_score,
                COALESCE(m.opportunity_score, 0) AS opportunity_score,
                COALESCE(ms.signal_count, 0) AS signal_count,
                COALESCE(ms.recent_signal_count, 0) AS recent_signal_count,
                COALESCE(ms.negative_signal_count, 0) AS negative_signal_count,
                COALESCE(rel.relationship_count, 0) AS relationship_count
            FROM merchants m
            LEFT JOIN (
                SELECT
                    merchant_id,
                    COUNT(*) AS signal_count,
                    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days') AS recent_signal_count,
                    COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(content, '')) ~ :negative_pattern
                    ) AS negative_signal_count
                FROM (
                    SELECT
                        ms.merchant_id,
                        ms.created_at,
                        s.content
                    FROM merchant_signals ms
                    LEFT JOIN signals s ON s.id = ms.signal_id
                ) signal_rows
                GROUP BY merchant_id
            ) ms ON ms.merchant_id = m.id
            LEFT JOIN (
                SELECT merchant_id, COUNT(*) AS relationship_count
                FROM merchant_relationships
                GROUP BY merchant_id
            ) rel ON rel.merchant_id = m.id
            WHERE COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) IS NOT NULL
               OR COALESCE(ms.signal_count, 0) > 0
               OR COALESCE(m.distress_score, 0) > 0
            ORDER BY COALESCE(m.distress_score, 0) DESC,
                     COALESCE(m.opportunity_score, 0) DESC,
                     COALESCE(m.last_seen, m.created_at, NOW()) DESC
            LIMIT :limit
        """), {
            "limit": limit,
            "negative_pattern": "(" + "|".join(NEGATIVE_SIGNAL_TERMS) + ")",
        }).fetchall()

        for merchant_id, distress_score, opportunity_score, signal_count, recent_signal_count, negative_signal_count, relationship_count in merchants:
            risk_score = _clamp_score(
                (float(distress_score or 0) * 4.0)
                + min(float(signal_count or 0) * 2.0, 18.0)
                + min(float(recent_signal_count or 0) * 2.5, 18.0)
                + min(float(negative_signal_count or 0) * 5.0, 30.0)
                + min(float(relationship_count or 0) * 1.5, 10.0)
                + min(float(opportunity_score or 0) * 0.2, 14.0)
            )

            conn.execute(text("""
                UPDATE merchants
                SET risk_score = :risk_score,
                    signal_count = :signal_count
                WHERE id = :merchant_id
            """), {
                "merchant_id": merchant_id,
                "risk_score": risk_score,
                "signal_count": int(signal_count or 0),
            })

            conn.execute(text("""
                UPDATE merchant_entities
                SET risk_score = :risk_score,
                    signal_count = :signal_count
                WHERE merchant_id = :merchant_id
            """), {
                "merchant_id": merchant_id,
                "risk_score": risk_score,
                "signal_count": int(signal_count or 0),
            })
            stats["merchants_scored"] += 1

        cluster_rows = conn.execute(text("""
            SELECT
                mc.id,
                COALESCE(AVG(COALESCE(m.risk_score, 0)), 0) AS avg_risk
            FROM merchant_clusters mc
            LEFT JOIN merchants m ON m.cluster_id = mc.id
            GROUP BY mc.id
        """)).fetchall()

        for cluster_id, avg_risk in cluster_rows:
            conn.execute(text("""
                UPDATE merchant_clusters
                SET risk_score = :risk_score
                WHERE id = :cluster_id
            """), {
                "cluster_id": cluster_id,
                "risk_score": _clamp_score(float(avg_risk or 0)),
            })

        conn.commit()
        stats["clusters_updated"] = len(cluster_rows)

    save_event("merchant_risk_scores_updated", stats)
    logger.info(f"Merchant risk scoring complete: {stats}")
    return stats


if __name__ == "__main__":
    print(update_merchant_risk_scores())
