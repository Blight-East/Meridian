"""
Signal Attribution Batch Processor
Processes unlinked signals in batches to resolve merchant identities.
"""
import sys, os

# Dynamically find project root
_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, 'config')):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from sqlalchemy import text
from config.logging_config import get_logger
from memory.structured.db import engine, save_event
from runtime.intelligence.merchant_identity import resolve_merchant_identity

logger = get_logger("signal_attribution_batch")

def run_signal_attribution_batch(limit=50):
    """
    Queries signals WHERE merchant_id IS NULL AND detected_at > NOW() - INTERVAL '7 days',
    priority-first, and attempts to resolve their identities.
    """
    logger.info(f"[signal_attribution_batch] start: limit={limit}")
    
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, content, priority_score, merchant_name
                FROM signals
                WHERE merchant_id IS NULL
                  AND detected_at > NOW() - INTERVAL '7 days'
                ORDER BY priority_score DESC
                LIMIT :limit
            """), {"limit": limit}).fetchall()
    except Exception as e:
        logger.error(f"[signal_attribution_batch] database query failed: {e}")
        return

    if not rows:
        logger.info("[signal_attribution_batch] no unlinked signals found in last 7 days")
        return

    processed_count = 0
    attributed_count = 0
    errors_count = 0

    for row in rows:
        try:
            signal_id = row[0]
            content = row[1]
            priority_score = row[2]
            merchant_name = row[3]

            # Attempt to resolve identity
            # Note: signals table doesn't have an 'author' column, so we pass None
            merchant_id = resolve_merchant_identity(
                signal_id=signal_id,
                content=content,
                priority_score=priority_score,
                author=None,
                merchant_name=merchant_name,
                allow_generic_fallback=True
            )

            processed_count += 1
            if merchant_id:
                attributed_count += 1
                logger.debug(f"[signal_attribution_batch] attributed signal {signal_id} to merchant {merchant_id}")

        except MemoryError:
            logger.error("[signal_attribution_batch] MemoryError detected! Breaking loop to protect scheduler.")
            break
        except Exception as e:
            errors_count += 1
            logger.error(f"[signal_attribution_batch] Error processing signal {row[0]}: {e}")
            continue

    logger.info(
        f"[signal_attribution_batch] completed: processed={processed_count}, "
        f"attributed={attributed_count}, errors={errors_count}"
    )
    
    save_event("signal_attribution_batch", {
        "limit": limit,
        "processed": processed_count,
        "attributed": attributed_count,
        "errors": errors_count
    })

if __name__ == "__main__":
    run_signal_attribution_batch()
