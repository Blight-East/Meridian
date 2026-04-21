"""
Merchant clustering for the global merchant intelligence graph.

Clusters are lightweight rollups over the canonical merchants table and are
refreshed periodically by the scheduler.
"""
import os
import re
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

logger = get_logger("merchant_clustering")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_MERCHANTS_PER_RUN = 500
CLUSTER_RULES = [
    (("cbd", "hemp", "cannabis", "thc"), ("CBD Merchants", "cbd")),
    (("supplement", "supplements", "vitamin", "vitamins", "wellness", "nutra", "health"), ("Supplements Merchants", "supplements")),
    (("crypto", "blockchain", "token", "coin", "wallet", "web3"), ("Crypto Merchants", "crypto")),
    (("saas", "software", "cloud", "platform", "api", "app"), ("SaaS Merchants", "saas")),
    (("dropship", "dropshipping", "printful", "printify", "fulfillment"), ("Dropshipping Merchants", "dropshipping")),
    (("apparel", "fashion", "clothing", "wear", "boutique"), ("Apparel Merchants", "apparel")),
    (("beauty", "cosmetic", "cosmetics", "skincare", "skin"), ("Beauty Merchants", "beauty")),
]


def _normalized_tokens(*values):
    text_value = " ".join(value or "" for value in values).lower()
    return {token for token in re.findall(r"[a-z0-9]+", text_value) if token}


def _cluster_for_merchant(name, domain, industry, neighbors):
    tokens = _normalized_tokens(name, domain, industry, neighbors)
    for keywords, cluster in CLUSTER_RULES:
        if tokens & set(keywords):
            return cluster

    normalized_industry = (industry or "").strip().lower()
    if normalized_industry and normalized_industry != "unknown":
        return (f"{normalized_industry.title()} Merchants", normalized_industry)

    return ("General Commerce", "general")


def update_merchant_clusters(limit=MAX_MERCHANTS_PER_RUN):
    stats = {
        "merchants_processed": 0,
        "clusters_updated": 0,
    }

    with engine.connect() as conn:
        init_merchant_graph_tables(conn)
        sync_all_merchant_entities(conn)

        merchants = conn.execute(text("""
            SELECT
                m.id,
                m.canonical_name,
                COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) AS domain,
                COALESCE(NULLIF(m.industry, ''), 'unknown') AS industry,
                COALESCE(
                    (
                        SELECT string_agg(n.canonical_name, ' ')
                        FROM merchant_relationships r
                        JOIN merchants n ON n.id = CASE
                            WHEN r.merchant_id = m.id THEN r.neighbor_id
                            ELSE r.merchant_id
                        END
                        WHERE r.merchant_id = m.id OR r.neighbor_id = m.id
                    ),
                    ''
                ) AS neighbors
            FROM merchants m
            WHERE COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) IS NOT NULL
               OR COALESCE(m.signal_count, 0) > 0
               OR COALESCE(m.distress_score, 0) > 0
            ORDER BY COALESCE(m.risk_score, 0) DESC,
                     COALESCE(m.distress_score, 0) DESC,
                     COALESCE(m.last_seen, m.created_at, NOW()) DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

        cluster_ids = {}
        for merchant_id, name, domain, industry, neighbors in merchants:
            cluster_label, cluster_industry = _cluster_for_merchant(name, domain, industry, neighbors)
            cluster_id = cluster_ids.get(cluster_label)
            if not cluster_id:
                cluster_id = conn.execute(text("""
                    INSERT INTO merchant_clusters (cluster_label, industry, created_at)
                    VALUES (:label, :industry, NOW())
                    ON CONFLICT (cluster_label) DO UPDATE SET
                        industry = EXCLUDED.industry
                    RETURNING id
                """), {
                    "label": cluster_label,
                    "industry": cluster_industry,
                }).scalar_one()
                cluster_ids[cluster_label] = cluster_id

            conn.execute(text("""
                UPDATE merchants
                SET cluster_id = :cluster_id
                WHERE id = :merchant_id
            """), {
                "cluster_id": cluster_id,
                "merchant_id": merchant_id,
            })

            conn.execute(text("""
                UPDATE merchant_entities
                SET cluster_id = :cluster_id
                WHERE merchant_id = :merchant_id
            """), {
                "cluster_id": cluster_id,
                "merchant_id": merchant_id,
            })
            stats["merchants_processed"] += 1

        cluster_rows = conn.execute(text("""
            SELECT
                mc.id,
                COUNT(m.id) AS merchant_count,
                COALESCE(AVG(COALESCE(m.risk_score, 0)), 0) AS risk_score
            FROM merchant_clusters mc
            LEFT JOIN merchants m ON m.cluster_id = mc.id
            GROUP BY mc.id
        """)).fetchall()

        for cluster_id, merchant_count, risk_score in cluster_rows:
            conn.execute(text("""
                UPDATE merchant_clusters
                SET merchant_count = :merchant_count,
                    risk_score = :risk_score
                WHERE id = :cluster_id
            """), {
                "cluster_id": cluster_id,
                "merchant_count": int(merchant_count or 0),
                "risk_score": float(risk_score or 0),
            })

        conn.commit()
        stats["clusters_updated"] = len(cluster_rows)

    save_event("merchant_clusters_updated", stats)
    logger.info(f"Merchant clustering complete: {stats}")
    return stats


if __name__ == "__main__":
    print(update_merchant_clusters())
