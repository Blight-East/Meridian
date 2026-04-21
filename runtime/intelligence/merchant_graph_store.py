"""
Shared persistence helpers for the merchant intelligence graph.

The runtime already uses the legacy `merchants` table as the canonical entity
store. This module layers graph-specific tables on top of that schema so new
intelligence jobs can operate without breaking downstream consumers.
"""
import os
import sys

from sqlalchemy import text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)


def init_merchant_graph_tables(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchant_entities (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER UNIQUE,
            canonical_name TEXT NOT NULL,
            domain TEXT,
            industry TEXT,
            risk_score FLOAT DEFAULT 0,
            first_seen_at TIMESTAMP DEFAULT NOW(),
            last_seen_at TIMESTAMP DEFAULT NOW(),
            signal_count INTEGER DEFAULT 0,
            cluster_id INTEGER
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_merchant_entities_name
        ON merchant_entities(canonical_name)
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_merchant_entities_cluster
        ON merchant_entities(cluster_id)
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchant_clusters (
            id SERIAL PRIMARY KEY,
            cluster_label TEXT UNIQUE NOT NULL,
            industry TEXT,
            merchant_count INTEGER DEFAULT 0,
            risk_score FLOAT DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_merchant_clusters_risk
        ON merchant_clusters(risk_score DESC)
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchant_signals (
            id SERIAL PRIMARY KEY,
            signal_id INTEGER NOT NULL,
            merchant_id INTEGER NOT NULL,
            confidence FLOAT DEFAULT 0.5,
            source TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """))
    conn.execute(text("""
        ALTER TABLE merchant_signals
        ADD COLUMN IF NOT EXISTS confidence FLOAT DEFAULT 0.5
    """))
    conn.execute(text("""
        ALTER TABLE merchant_signals
        ADD COLUMN IF NOT EXISTS source TEXT
    """))
    conn.execute(text("""
        ALTER TABLE merchant_signals
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_merchant_signals_unique
        ON merchant_signals(merchant_id, signal_id)
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_merchant_signals_signal
        ON merchant_signals(signal_id)
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS merchant_relationships (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL,
            neighbor_id INTEGER NOT NULL,
            related_merchant_id INTEGER,
            relationship_type TEXT NOT NULL,
            confidence FLOAT DEFAULT 0.5,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(merchant_id, neighbor_id, relationship_type)
        )
    """))
    conn.execute(text("""
        ALTER TABLE merchant_relationships
        ADD COLUMN IF NOT EXISTS related_merchant_id INTEGER
    """))
    conn.execute(text("""
        UPDATE merchant_relationships
        SET related_merchant_id = neighbor_id
        WHERE related_merchant_id IS NULL
    """))

    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS domain TEXT
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS normalized_domain TEXT
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS industry TEXT
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS distress_score FLOAT DEFAULT 0
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS opportunity_score FLOAT DEFAULT 0
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS risk_score FLOAT DEFAULT 0
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS cluster_id INTEGER
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS signal_count INTEGER DEFAULT 0
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS first_seen TIMESTAMP DEFAULT NOW()
    """))
    conn.execute(text("""
        ALTER TABLE merchants
        ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT NOW()
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_merchants_cluster_id
        ON merchants(cluster_id)
    """))


def link_signal_to_merchant(conn, merchant_id, signal_id, confidence=0.5, source=None):
    conn.execute(text("""
        INSERT INTO merchant_signals (merchant_id, signal_id, confidence, source, created_at)
        VALUES (:merchant_id, :signal_id, :confidence, :source, NOW())
        ON CONFLICT (merchant_id, signal_id) DO UPDATE SET
            confidence = GREATEST(COALESCE(merchant_signals.confidence, 0), EXCLUDED.confidence),
            source = COALESCE(EXCLUDED.source, merchant_signals.source)
    """), {
        "merchant_id": merchant_id,
        "signal_id": signal_id,
        "confidence": confidence,
        "source": source,
    })
    sync_merchant_entity(conn, merchant_id)


def sync_merchant_entity(conn, merchant_id):
    row = conn.execute(text("""
        SELECT
            m.id,
            m.canonical_name,
            COALESCE(NULLIF(m.normalized_domain, ''), NULLIF(m.domain, '')) AS domain,
            COALESCE(NULLIF(m.industry, ''), 'unknown') AS industry,
            COALESCE(m.risk_score, 0) AS risk_score,
            COALESCE(m.first_seen, m.created_at, NOW()) AS first_seen_at,
            COALESCE(m.last_seen, NOW()) AS last_seen_at,
            COALESCE(ms.signal_count, 0) AS signal_count,
            m.cluster_id
        FROM merchants m
        LEFT JOIN (
            SELECT merchant_id, COUNT(*) AS signal_count
            FROM merchant_signals
            GROUP BY merchant_id
        ) ms ON ms.merchant_id = m.id
        WHERE m.id = :merchant_id
    """), {"merchant_id": merchant_id}).fetchone()

    if not row:
        return

    conn.execute(text("""
        UPDATE merchants
        SET signal_count = :signal_count
        WHERE id = :merchant_id
    """), {
        "merchant_id": row[0],
        "signal_count": row[7],
    })

    conn.execute(text("""
        INSERT INTO merchant_entities (
            merchant_id,
            canonical_name,
            domain,
            industry,
            risk_score,
            first_seen_at,
            last_seen_at,
            signal_count,
            cluster_id
        ) VALUES (
            :merchant_id,
            :canonical_name,
            :domain,
            :industry,
            :risk_score,
            :first_seen_at,
            :last_seen_at,
            :signal_count,
            :cluster_id
        )
        ON CONFLICT (merchant_id) DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            domain = EXCLUDED.domain,
            industry = EXCLUDED.industry,
            risk_score = EXCLUDED.risk_score,
            first_seen_at = LEAST(merchant_entities.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at = GREATEST(merchant_entities.last_seen_at, EXCLUDED.last_seen_at),
            signal_count = EXCLUDED.signal_count,
            cluster_id = EXCLUDED.cluster_id
    """), {
        "merchant_id": row[0],
        "canonical_name": row[1],
        "domain": row[2],
        "industry": row[3],
        "risk_score": float(row[4] or 0),
        "first_seen_at": row[5],
        "last_seen_at": row[6],
        "signal_count": int(row[7] or 0),
        "cluster_id": row[8],
    })


def sync_all_merchant_entities(conn, limit=None):
    sql = """
        SELECT id
        FROM merchants
        ORDER BY COALESCE(last_seen, created_at, NOW()) DESC
    """
    params = {}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = limit

    rows = conn.execute(text(sql), params).fetchall()
    for row in rows:
        sync_merchant_entity(conn, row[0])
    return len(rows)
