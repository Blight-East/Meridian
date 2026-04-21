"""
Merchant slug guard.

Provides deterministic slug generation plus one-time schema hardening so
merchant inserts cannot create duplicate logical merchants.
"""
import os
import re
import sys
import threading

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event

logger = get_logger("merchant_slug")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
_lock = threading.Lock()
_initialized = False


def build_merchant_slug(canonical_name):
    if not canonical_name:
        return None
    slug = re.sub(r"[^a-z0-9]", "", canonical_name.lower())
    return slug or None


def _merge_duplicate_slug_group(conn, slug, merchant_rows):
    ordered_rows = sorted(
        merchant_rows,
        key=lambda row: (
            0 if row["domain"] else 1,
            -(row["opportunity_score"] or 0),
            -(row["distress_score"] or 0),
            row["id"],
        ),
    )
    survivor = ordered_rows[0]

    for duplicate in ordered_rows[1:]:
        survivor_id = survivor["id"]
        duplicate_id = duplicate["id"]

        conn.execute(text("""
            UPDATE signals SET merchant_id = :survivor_id WHERE merchant_id = :duplicate_id
        """), {"survivor_id": survivor_id, "duplicate_id": duplicate_id})
        conn.execute(text("""
            INSERT INTO merchant_signals (merchant_id, signal_id)
            SELECT :survivor_id, signal_id
            FROM merchant_signals
            WHERE merchant_id = :duplicate_id
            ON CONFLICT (signal_id) DO NOTHING
        """), {"survivor_id": survivor_id, "duplicate_id": duplicate_id})
        conn.execute(text("""
            DELETE FROM merchant_signals WHERE merchant_id = :duplicate_id
        """), {"duplicate_id": duplicate_id})
        for table_name in (
            "merchant_contacts",
            "merchant_opportunities",
            "sales_outreach_events",
            "merchant_conversations",
        ):
            conn.execute(text(f"""
                UPDATE {table_name}
                SET merchant_id = :survivor_id
                WHERE merchant_id = :duplicate_id
            """), {"survivor_id": survivor_id, "duplicate_id": duplicate_id})

        conn.execute(text("""
            UPDATE merchants
            SET domain = CASE
                    WHEN COALESCE(NULLIF(domain, ''), '') = '' THEN :duplicate_domain
                    ELSE domain
                END,
                normalized_domain = CASE
                    WHEN COALESCE(NULLIF(normalized_domain, ''), '') = '' THEN :duplicate_normalized_domain
                    ELSE normalized_domain
                END,
                detected_from = COALESCE(NULLIF(detected_from, ''), :duplicate_detected_from, detected_from),
                domain_confidence = COALESCE(NULLIF(domain_confidence, ''), :duplicate_domain_confidence, domain_confidence),
                distress_score = GREATEST(COALESCE(distress_score, 0), :duplicate_distress_score),
                opportunity_score = GREATEST(COALESCE(opportunity_score, 0), :duplicate_opportunity_score),
                status = CASE
                    WHEN COALESCE(status, 'active') = 'active'
                      OR COALESCE(:duplicate_status, 'active') = 'active' THEN 'active'
                    ELSE COALESCE(status, :duplicate_status, 'provisional')
                END,
                last_seen = GREATEST(COALESCE(last_seen, CURRENT_TIMESTAMP), COALESCE(:duplicate_last_seen, CURRENT_TIMESTAMP))
            WHERE id = :survivor_id
        """), {
            "duplicate_domain": duplicate["domain"] or "",
            "duplicate_normalized_domain": duplicate["normalized_domain"] or "",
            "duplicate_detected_from": duplicate["detected_from"],
            "duplicate_domain_confidence": duplicate["domain_confidence"],
            "duplicate_distress_score": float(duplicate["distress_score"] or 0),
            "duplicate_opportunity_score": float(duplicate["opportunity_score"] or 0),
            "duplicate_status": duplicate["status"],
            "duplicate_last_seen": duplicate["last_seen"],
            "survivor_id": survivor_id,
        })

        conn.execute(text("""
            DELETE FROM merchants WHERE id = :duplicate_id
        """), {"duplicate_id": duplicate_id})

        save_event("merchant_slug_merged", {
            "slug": slug,
            "survivor_id": survivor_id,
            "duplicate_id": duplicate_id,
            "survivor_name": survivor["canonical_name"],
            "duplicate_name": duplicate["canonical_name"],
        })
        logger.info(
            f"Merged duplicate merchant slug={slug}: survivor={survivor_id} duplicate={duplicate_id}"
        )


def ensure_merchant_slug_guard():
    global _initialized
    if _initialized:
        return

    with _lock:
        if _initialized:
            return

        with engine.connect() as conn:
            conn.execute(text("""
                ALTER TABLE merchants
                ADD COLUMN IF NOT EXISTS slug TEXT
            """))
            conn.execute(text("""
                ALTER TABLE merchants
                ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'
            """))
            conn.execute(text("""
                ALTER TABLE merchants
                ADD COLUMN IF NOT EXISTS opportunity_score FLOAT DEFAULT 0
            """))
            conn.execute(text("""
                UPDATE merchants
                SET slug = regexp_replace(lower(canonical_name), '[^a-z0-9]', '', 'g')
                WHERE slug IS NULL OR slug = ''
            """))

            duplicate_slugs = conn.execute(text("""
                SELECT slug
                FROM merchants
                WHERE slug IS NOT NULL AND slug != ''
                GROUP BY slug
                HAVING COUNT(*) > 1
            """)).fetchall()

            for (slug,) in duplicate_slugs:
                rows = conn.execute(text("""
                    SELECT id, canonical_name, domain, normalized_domain, detected_from,
                           domain_confidence, distress_score, opportunity_score,
                           status, last_seen
                    FROM merchants
                    WHERE slug = :slug
                """), {"slug": slug}).mappings().all()
                if len(rows) > 1:
                    _merge_duplicate_slug_group(conn, slug, rows)

            conn.execute(text("""
                UPDATE merchants
                SET slug = regexp_replace(lower(canonical_name), '[^a-z0-9]', '', 'g')
                WHERE slug IS NULL OR slug = ''
            """))
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS merchants_slug_idx ON merchants (slug)
            """))
            conn.commit()

        _initialized = True

