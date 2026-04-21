"""
Backfill Merchant Attribution
Re-processes existing signals where merchant_id IS NULL through
the new attribution engine.
"""
import sys, os
import re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event
try:
    from runtime.intelligence.merchant_identity import extract_domains, resolve_merchant_identity
except ImportError:
    from merchant_identity import extract_domains, resolve_merchant_identity

logger = get_logger("backfill_attribution")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)

BATCH_SIZE = 100
_GENERIC_MERCHANT_NAME_PHRASES = (
    "merchant account",
    "payment gateway",
    "payment processing",
    "processing fees",
    "processor issues",
    "credit card",
    "closed merchant",
)
_TRUSTED_BACKFILL_SOURCE_MARKERS = (
    "community.shopify.com",
    "gmail",
)


def _merchant_name_looks_specific(name: str | None) -> bool:
    value = str(name or "").strip()
    if not value or value.lower() == "unknown":
        return False
    lowered = value.lower()
    if any(phrase in lowered for phrase in _GENERIC_MERCHANT_NAME_PHRASES):
        return False
    if len(value.split()) == 1 and not re.search(r"[A-Z].*[A-Z]|[.-]", value):
        return False
    return True


def _should_attempt_backfill(*, content: str, source: str, merchant_name: str | None) -> bool:
    normalized_source = str(source or "").strip().lower()
    if extract_domains(content or ""):
        return True
    if _merchant_name_looks_specific(merchant_name):
        return True
    if any(marker in normalized_source for marker in _TRUSTED_BACKFILL_SOURCE_MARKERS):
        return True
    return False


def run_backfill(*, merchant_distress_only: bool = True, batch_size: int = BATCH_SIZE):
    """Re-process unattributed signals through the attribution engine."""
    attributed = 0
    processed = 0
    errors = 0
    skipped = 0

    where_clause = "merchant_id IS NULL"
    if merchant_distress_only:
        where_clause += " AND classification = 'merchant_distress'"

    with engine.connect() as conn:
        total = conn.execute(text(
            f"SELECT COUNT(*) FROM signals WHERE {where_clause}"
        )).scalar()
        print(f"Found {total} unattributed signals to process")

        last_id = 0
        while True:
            rows = conn.execute(text("""
                SELECT id, content, source, priority_score, merchant_name
                FROM signals
                WHERE {where_clause}
                  AND id > :last_id
                ORDER BY id
                LIMIT :limit
            """.format(where_clause=where_clause)), {"limit": batch_size, "last_id": last_id}).fetchall()

            if not rows:
                break

            for row in rows:
                signal_id, content, source, priority_score, merchant_name = row[0], row[1], row[2], row[3], row[4]
                processed += 1
                last_id = signal_id

                try:
                    if not _should_attempt_backfill(
                        content=content or "",
                        source=source or "",
                        merchant_name=merchant_name,
                    ):
                        skipped += 1
                        continue
                    merchant_id = resolve_merchant_identity(
                        signal_id,
                        content,
                        priority_score=priority_score or 0,
                        merchant_name=merchant_name,
                        allow_generic_fallback=False,
                    )
                    if merchant_id:
                        attributed += 1
                        if attributed % 10 == 0:
                            print(f"  Attributed {attributed} signals so far...")
                    else:
                        skipped += 1
                except Exception as e:
                    errors += 1
                    logger.warning(f"Backfill error for signal {signal_id}: {e}")

    print(f"\nBackfill complete: {attributed}/{processed} signals attributed")
    result = {
        "processed": processed,
        "attributed": attributed,
        "skipped": skipped,
        "errors": errors,
        "merchant_distress_only": merchant_distress_only,
    }
    save_event("attribution_backfill_run", result)
    logger.info(f"Attribution backfill complete: {result}")
    return result


if __name__ == "__main__":
    run_backfill()
