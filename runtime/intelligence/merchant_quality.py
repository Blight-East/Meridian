"""
Merchant graph quality controls and cleanup routines.

This module quarantines low-quality legacy merchants before deletion so cleanup
is reversible and propagation cannot amplify bad graph state.
"""
import json
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

logger = get_logger("merchant_quality")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

_INVALID_NAME_PATTERNS = [
    (re.compile(r"\.html?$", re.IGNORECASE), "html_fragment"),
    (re.compile(r"\bask hn\b", re.IGNORECASE), "forum_boilerplate"),
    (re.compile(r"\bcomment\b", re.IGNORECASE), "comment_boilerplate"),
    (re.compile(r"\buser\b", re.IGNORECASE), "user_boilerplate"),
    (re.compile(r"<[^>]+>"), "html_tag"),
    (re.compile(r"^\d+\.(html?|php|aspx|jsp)$", re.IGNORECASE), "page_fragment"),
]
_DOMAIN_PATTERN = re.compile(
    r"^(?!-)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}$",
    re.IGNORECASE,
)
_IPV4_PATTERN = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_BLOCKED_TLDS = {"aspx", "csv", "doc", "docx", "htm", "html", "jpeg", "jpg", "json", "pdf", "php", "png", "rss", "txt", "xml", "xlsx", "zip"}


def merchant_quality_score(merchant):
    score = 0
    domain = (merchant.get("domain") or "").strip() if isinstance(merchant, dict) else ""
    signal_count = int(merchant.get("signal_count") or 0) if isinstance(merchant, dict) else 0
    relationship_count = int(merchant.get("relationship_count") or 0) if isinstance(merchant, dict) else 0
    risk_score = float(merchant.get("risk_score") or 0) if isinstance(merchant, dict) else 0.0

    if domain:
        score += 2
    if signal_count > 0:
        score += 3
    if relationship_count > 0:
        score += 2
    if risk_score > 0:
        score += 1
    return score


_REVERSE_DNS_PREFIXES = {"com", "net", "org", "co", "io", "dev", "app"}


def is_reverse_dns(domain):
    """Reject reverse-DNS style domains like com.scotiabank.banking."""
    if not domain:
        return False
    labels = domain.lower().split(".")
    if len(labels) < 2:
        return False
    if labels[0] in _REVERSE_DNS_PREFIXES:
        return True
    # Excessive nesting indicates hostname artifacts (but allow 3-level subdomains like payments.stripe.com)
    if len(labels) > 4:
        return True
    return False


def is_valid_domain(domain):
    host = (domain or "").strip().lower().strip(".")
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]
    if "/" in host:
        host = host.split("/", 1)[0]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host in {"localhost", "localhost.localdomain"}:
        return False
    if host.endswith((".html", ".htm")):
        return False
    if _IPV4_PATTERN.match(host):
        return False
    labels = host.split(".")
    if len(labels) < 2:
        return False
    if is_reverse_dns(host):
        return False
    if labels[-1] in _BLOCKED_TLDS:
        return False
    return bool(_DOMAIN_PATTERN.match(host))


def invalid_merchant_name_reason(name):
    candidate = (name or "").strip()
    if len(candidate) < 3:
        return "too_short"
    for pattern, reason in _INVALID_NAME_PATTERNS:
        if pattern.search(candidate):
            return reason
    return None


def _init_quarantine_table(conn):
    init_merchant_graph_tables(conn)
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchant_quarantine (
            merchant_id INTEGER PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            domain TEXT,
            reason TEXT NOT NULL,
            quarantined_at TIMESTAMP DEFAULT NOW(),
            merchant_snapshot JSONB
        )
    """))


def find_low_quality_merchants(limit=500):
    with engine.connect() as conn:
        _init_quarantine_table(conn)
        rows = conn.execute(text("""
            WITH signal_counts AS (
                SELECT merchant_id, COUNT(*) AS signal_count
                FROM merchant_signals
                GROUP BY merchant_id
            ),
            relationship_counts AS (
                SELECT merchant_id, COUNT(*) AS relationship_count
                FROM (
                    SELECT merchant_id FROM merchant_relationships
                    UNION ALL
                    SELECT neighbor_id AS merchant_id FROM merchant_relationships
                    UNION ALL
                    SELECT related_merchant_id AS merchant_id
                    FROM merchant_relationships
                    WHERE related_merchant_id IS NOT NULL
                ) rel
                GROUP BY merchant_id
            )
            SELECT
                m.id AS merchant_id,
                m.canonical_name,
                COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) AS domain,
                COALESCE(sc.signal_count, 0) AS signal_count,
                COALESCE(rc.relationship_count, 0) AS relationship_count,
                COALESCE(m.opportunity_score, 0) AS opportunity_score,
                COALESCE(m.risk_score, 0) AS risk_score,
                COALESCE(m.distress_score, 0) AS distress_score,
                to_jsonb(m) AS merchant_snapshot
            FROM merchants m
            LEFT JOIN signal_counts sc ON sc.merchant_id = m.id
            LEFT JOIN relationship_counts rc ON rc.merchant_id = m.id
            WHERE (
                    (COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) IS NULL)
                AND COALESCE(sc.signal_count, 0) = 0
                AND COALESCE(rc.relationship_count, 0) = 0
                AND COALESCE(m.opportunity_score, 0) = 0
                AND COALESCE(m.risk_score, 0) = 0
                AND COALESCE(m.distress_score, 0) = 0
            )
            ORDER BY COALESCE(m.last_seen, m.created_at, NOW()) DESC, m.id DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

    merchants = []
    for row in rows:
        merchant = dict(row._mapping)
        merchant["quality_score"] = merchant_quality_score(merchant)
        invalid_reason = invalid_merchant_name_reason(merchant["canonical_name"])
        if invalid_reason:
            merchant["reason"] = invalid_reason
        else:
            merchant["reason"] = "no_domain_no_signals_no_relationships_no_risk"
        merchants.append(merchant)
    return merchants


def quarantine_low_quality_merchants(limit=500, dry_run=False):
    candidates = find_low_quality_merchants(limit=limit)
    stats = {
        "candidates_found": len(candidates),
        "merchants_quarantined": 0,
        "merchants_deleted": 0,
        "orphan_relationships_deleted": 0,
        "orphan_entities_deleted": 0,
        "orphan_signal_links_deleted": 0,
    }

    if dry_run or not candidates:
        return stats | {"candidates": candidates}

    merchant_ids = [merchant["merchant_id"] for merchant in candidates]
    with engine.connect() as conn:
        _init_quarantine_table(conn)
        for merchant in candidates:
            conn.execute(text("""
                INSERT INTO merchant_quarantine (
                    merchant_id,
                    canonical_name,
                    domain,
                    reason,
                    quarantined_at,
                    merchant_snapshot
                ) VALUES (
                    :merchant_id,
                    :canonical_name,
                    :domain,
                    :reason,
                    NOW(),
                    CAST(:merchant_snapshot AS JSONB)
                )
                ON CONFLICT (merchant_id) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    domain = EXCLUDED.domain,
                    reason = EXCLUDED.reason,
                    quarantined_at = EXCLUDED.quarantined_at,
                    merchant_snapshot = EXCLUDED.merchant_snapshot
            """), {
                "merchant_id": merchant["merchant_id"],
                "canonical_name": merchant["canonical_name"],
                "domain": merchant["domain"],
                "reason": merchant["reason"],
                "merchant_snapshot": json.dumps(merchant["merchant_snapshot"]),
            })
            stats["merchants_quarantined"] += 1

        conn.execute(text("""
            DELETE FROM merchant_entities
            WHERE merchant_id = ANY(:merchant_ids)
        """), {"merchant_ids": merchant_ids})

        conn.execute(text("""
            DELETE FROM merchant_signals
            WHERE merchant_id = ANY(:merchant_ids)
        """), {"merchant_ids": merchant_ids})

        stats["merchants_deleted"] = conn.execute(text("""
            DELETE FROM merchants
            WHERE id = ANY(:merchant_ids)
        """), {"merchant_ids": merchant_ids}).rowcount or 0

        stats["orphan_relationships_deleted"] = conn.execute(text("""
            DELETE FROM merchant_relationships
            WHERE merchant_id NOT IN (SELECT id FROM merchants)
               OR neighbor_id NOT IN (SELECT id FROM merchants)
               OR (
                    related_merchant_id IS NOT NULL
                AND related_merchant_id NOT IN (SELECT id FROM merchants)
               )
        """)).rowcount or 0

        stats["orphan_entities_deleted"] = conn.execute(text("""
            DELETE FROM merchant_entities
            WHERE merchant_id NOT IN (SELECT id FROM merchants)
        """)).rowcount or 0

        stats["orphan_signal_links_deleted"] = conn.execute(text("""
            DELETE FROM merchant_signals
            WHERE merchant_id NOT IN (SELECT id FROM merchants)
        """)).rowcount or 0

        conn.commit()

    save_event("merchant_quality_cleanup_run", stats)
    logger.info(f"Merchant quality cleanup complete: {stats}")
    return stats


def get_graph_health_metrics():
    with engine.connect() as conn:
        _init_quarantine_table(conn)
        graph_size = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar() or 0
        relationship_count = conn.execute(text("SELECT COUNT(*) FROM merchant_relationships")).scalar() or 0
        cluster_count = conn.execute(text("SELECT COUNT(*) FROM merchant_clusters")).scalar() or 0
        average_signals_per_merchant = conn.execute(text("""
            SELECT COALESCE(AVG(signal_count), 0)
            FROM (
                SELECT m.id, COUNT(ms.signal_id) AS signal_count
                FROM merchants m
                LEFT JOIN merchant_signals ms ON ms.merchant_id = m.id
                GROUP BY m.id
            ) merchant_signal_stats
        """)).scalar() or 0
    return {
        "merchant_graph_size": int(graph_size),
        "merchant_relationship_count": int(relationship_count),
        "cluster_count": int(cluster_count),
        "average_signals_per_merchant": round(float(average_signals_per_merchant), 2),
    }


def cleanup_low_quality_merchants(limit=500):
    cleanup_stats = quarantine_low_quality_merchants(limit=limit, dry_run=False)

    from runtime.intelligence.merchant_clustering import update_merchant_clusters
    from runtime.intelligence.merchant_risk_scoring import update_merchant_risk_scores

    with engine.connect() as conn:
        sync_all_merchant_entities(conn)
        conn.commit()

    cluster_stats = update_merchant_clusters(limit=500)
    risk_stats = update_merchant_risk_scores(limit=500)
    health = get_graph_health_metrics()

    result = {
        **cleanup_stats,
        "cluster_refresh": cluster_stats,
        "risk_refresh": risk_stats,
        "graph_health": health,
    }
    save_event("merchant_quality_cleanup_completed", result)
    return result


if __name__ == "__main__":
    print(cleanup_low_quality_merchants())
