"""
Historical signal resurfacing job.

Reprocesses older unattributed signals with the latest deterministic
classification, brand extraction, and merchant identity logic so historical
signals can become merchants after attribution improvements ship.
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
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.logging_config import get_logger
from memory.structured.db import save_event

try:
    from runtime.intelligence.brand_extraction import extract_brand
except ImportError:
    from brand_extraction import extract_brand

try:
    from runtime.intelligence.merchant_identity import resolve_merchant_identity
except ImportError:
    from merchant_identity import resolve_merchant_identity

try:
    from runtime.intelligence.merchant_signal_classifier import (
        CLASS_MERCHANT_OPERATOR,
        classify_merchant_signal,
    )
except ImportError:
    from merchant_signal_classifier import CLASS_MERCHANT_OPERATOR, classify_merchant_signal

try:
    from runtime.ranking.signal_clusterer import cluster_signals
except ImportError:
    from ranking.signal_clusterer import cluster_signals

logger = get_logger("signal_resurfacing")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")


def _init_resurfacing_columns():
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE signals
            ADD COLUMN IF NOT EXISTS last_resurfaced_at TIMESTAMP
        """))
        conn.execute(text("""
            UPDATE signals
            SET last_resurfaced_at = CURRENT_TIMESTAMP
            WHERE COALESCE(resurfaced, FALSE) = TRUE
              AND last_resurfaced_at IS NULL
        """))
        conn.commit()


def _compute_adaptive_batch_size():
    with engine.connect() as conn:
        backlog = conn.execute(text("""
            SELECT COUNT(*)
            FROM signals
            WHERE merchant_id IS NULL
        """)).scalar() or 0
    return min(500, max(100, backlog // 10))


def run_signal_resurfacing(batch_size=None):
    """
    Reprocess historical unattributed signals with the latest attribution stack.
    """
    _init_resurfacing_columns()
    effective_batch_size = batch_size if batch_size is not None else _compute_adaptive_batch_size()

    resurfaced_signals = 0
    merchants_created_from_resurfacing = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content, priority_score, merchant_name
            FROM signals
            WHERE merchant_id IS NULL
              AND (
                    last_resurfaced_at IS NULL
                    OR last_resurfaced_at < NOW() - INTERVAL '7 days'
              )
            ORDER BY detected_at DESC
            LIMIT :limit
        """), {"limit": effective_batch_size}).fetchall()

    signals_processed = len(rows)

    for signal_id, content, priority_score, merchant_name in rows:
        merchant_id = None
        normalized_class = None
        extracted_brand = None

        try:
            classification = classify_merchant_signal(content or "")
            normalized_class = classification["classification"]
            signal_class = "merchant_distress" if normalized_class == CLASS_MERCHANT_OPERATOR else "consumer_complaint"

            with engine.connect() as conn:
                conn.execute(text("""
                    UPDATE signals
                    SET classification = :classification
                    WHERE id = :signal_id
                """), {
                    "classification": signal_class,
                    "signal_id": signal_id,
                })
                conn.commit()

            extracted_brand = extract_brand(content or "", merchant_name=merchant_name)
            if extracted_brand:
                with engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE signals
                        SET merchant_name = :merchant_name
                        WHERE id = :signal_id
                          AND (:merchant_name IS NOT NULL)
                    """), {
                        "merchant_name": extracted_brand,
                        "signal_id": signal_id,
                    })
                    conn.commit()

            with engine.connect() as conn:
                merchants_before = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar() or 0

            merchant_id = resolve_merchant_identity(
                signal_id,
                content or "",
                priority_score or 0,
                merchant_name=extracted_brand or merchant_name,
            )

            if merchant_id:
                with engine.connect() as conn:
                    merchants_after = conn.execute(text("SELECT COUNT(*) FROM merchants")).scalar() or 0

                if merchants_after > merchants_before:
                    merchants_created_from_resurfacing += 1

                resurfaced_signals += 1
                save_event("signal_resurfaced", {
                    "signal_id": signal_id,
                    "merchant_id": merchant_id,
                    "classification": normalized_class,
                    "brand": extracted_brand,
                })
        except Exception as exc:
            logger.warning(f"Signal resurfacing failed for signal {signal_id}: {exc}")
        finally:
            with engine.connect() as conn:
                conn.execute(text("""
                    UPDATE signals
                    SET last_resurfaced_at = CURRENT_TIMESTAMP
                    WHERE id = :signal_id
                """), {"signal_id": signal_id})
                conn.commit()

    if resurfaced_signals > 0:
        try:
            cluster_signals()
        except Exception as exc:
            logger.warning(f"Cluster refresh after resurfacing failed: {exc}")

    result = {
        "signals_processed": signals_processed,
        "signals_resurfaced": resurfaced_signals,
        "merchants_created": merchants_created_from_resurfacing,
        "batch_size": effective_batch_size,
    }
    save_event("signal_resurfacing_run", result)
    logger.info(f"Signal resurfacing run complete: {result}")
    return result


if __name__ == "__main__":
    print(run_signal_resurfacing())
