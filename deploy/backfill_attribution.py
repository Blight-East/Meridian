"""
Backfill Merchant Attribution
Re-processes existing signals where merchant_id IS NULL through
the new attribution engine.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from merchant_identity import resolve_merchant_identity

logger = get_logger("backfill_attribution")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

BATCH_SIZE = 100


def run_backfill():
    """Re-process unattributed signals through the attribution engine."""
    attributed = 0
    processed = 0

    with engine.connect() as conn:
        total = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE merchant_id IS NULL"
        )).scalar()
        print(f"Found {total} unattributed signals to process")

        last_id = 0
        while True:
            rows = conn.execute(text("""
                SELECT id, content, source
                FROM signals
                WHERE merchant_id IS NULL
                  AND id > :last_id
                ORDER BY id
                LIMIT :limit
            """), {"limit": BATCH_SIZE, "last_id": last_id}).fetchall()

            if not rows:
                break

            for row in rows:
                signal_id, content, source = row[0], row[1], row[2]
                processed += 1
                last_id = signal_id

                try:
                    merchant_id = resolve_merchant_identity(signal_id, content)
                    if merchant_id:
                        attributed += 1
                        if attributed % 10 == 0:
                            print(f"  Attributed {attributed} signals so far...")
                except Exception as e:
                    logger.warning(f"Backfill error for signal {signal_id}: {e}")

    print(f"\nBackfill complete: {attributed}/{processed} signals attributed")
    return {"processed": processed, "attributed": attributed}


if __name__ == "__main__":
    run_backfill()
