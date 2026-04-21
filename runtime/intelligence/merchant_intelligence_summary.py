"""
Summary queries for the merchant intelligence graph.
"""
import datetime
import os
import sys

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from runtime.intelligence.merchant_graph_store import init_merchant_graph_tables

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")


def _serialize(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def get_merchant_intelligence_summary():
    with engine.connect() as conn:
        init_merchant_graph_tables(conn)

        new_merchants_detected = conn.execute(text("""
            SELECT COUNT(*)
            FROM merchants
            WHERE COALESCE(created_at, first_seen, NOW()) >= NOW() - INTERVAL '7 days'
              AND (
                  COALESCE(NULLIF(normalized_domain, ''), NULLIF(domain, '')) IS NOT NULL
                  OR COALESCE(signal_count, 0) > 0
                  OR COALESCE(distress_score, 0) > 0
              )
        """)).scalar() or 0

        high_risk_clusters = conn.execute(text("""
            SELECT COUNT(*)
            FROM merchant_clusters
            WHERE COALESCE(risk_score, 0) >= 70
        """)).scalar() or 0

        merchant_graph_size = conn.execute(text("""
            SELECT COUNT(*)
            FROM merchant_entities
            WHERE COALESCE(domain, '') != ''
               OR COALESCE(signal_count, 0) > 0
               OR COALESCE(risk_score, 0) > 0
        """)).scalar() or 0

        top_emerging_processors = [
            row[0]
            for row in conn.execute(text("""
                SELECT processor
                FROM (
                    SELECT suspected_processor AS processor, MAX(detected_at) AS observed_at
                    FROM processor_anomalies
                    WHERE suspected_processor IS NOT NULL
                    GROUP BY suspected_processor
                    UNION ALL
                    SELECT processor, MAX(created_at) AS observed_at
                    FROM qualified_leads
                    WHERE processor IS NOT NULL AND processor != 'unknown'
                    GROUP BY processor
                ) processor_events
                GROUP BY processor
                ORDER BY MAX(observed_at) DESC, COUNT(*) DESC
                LIMIT 5
            """)).fetchall()
            if row[0]
        ]

        top_clusters = [
            {
                "id": row[0],
                "cluster_label": row[1],
                "industry": row[2],
                "merchant_count": int(row[3] or 0),
                "risk_score": round(float(row[4] or 0), 2),
            }
            for row in conn.execute(text("""
                SELECT id, cluster_label, industry, merchant_count, risk_score
                FROM merchant_clusters
                ORDER BY COALESCE(risk_score, 0) DESC, merchant_count DESC
                LIMIT 5
            """)).fetchall()
        ]

        discovery_feed = [
            {
                "merchant_id": row[0],
                "canonical_name": row[1],
                "domain": row[2],
                "risk_score": round(float(row[3] or 0), 2),
                "signal_count": int(row[4] or 0),
                "last_seen_at": _serialize(row[5]),
            }
            for row in conn.execute(text("""
                SELECT merchant_id, canonical_name, domain, risk_score, signal_count, last_seen_at
                FROM merchant_entities
                WHERE COALESCE(domain, '') != ''
                   OR COALESCE(signal_count, 0) > 0
                   OR COALESCE(risk_score, 0) > 0
                ORDER BY COALESCE(risk_score, 0) DESC,
                         COALESCE(last_seen_at, first_seen_at, NOW()) DESC
                LIMIT 10
            """)).fetchall()
        ]

        relationship_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM merchant_relationships
        """)).scalar() or 0

    return {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "new_merchants_detected": int(new_merchants_detected),
        "high_risk_clusters": int(high_risk_clusters),
        "top_emerging_processors": top_emerging_processors,
        "merchant_graph_size": int(merchant_graph_size),
        "merchant_relationship_count": int(relationship_count),
        "top_clusters": top_clusters,
        "merchant_discovery_feed": discovery_feed,
    }
