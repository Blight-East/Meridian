"""
Semantic Memory Backfill Script
Generates embeddings for all existing signals in the database that don't have them yet.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from semantic_memory import store_signal_embedding
from config.logging_config import get_logger

logger = get_logger("backfill")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def run():
    logger.info("Starting Semantic Memory Backfill...")
    with engine.connect() as conn:
        # Get all signals without a corresponding embedding
        rows = conn.execute(text("""
            SELECT s.id, s.content 
            FROM signals s
            LEFT JOIN signal_embeddings se ON s.id = se.signal_id
            WHERE se.signal_id IS NULL
        """)).fetchall()

    logger.info(f"Found {len(rows)} signals requiring embeddings.")
    
    success = 0
    failed = 0
    for idx, (sig_id, content) in enumerate(rows):
        if store_signal_embedding(sig_id, content):
            success += 1
        else:
            failed += 1
            
        if (idx + 1) % 50 == 0:
            logger.info(f"Processed {idx + 1}/{len(rows)} signals...")

    logger.info("================")
    logger.info("Backfill Complete")
    logger.info(f"Successfully embedded: {success}")
    logger.info(f"Failed embeddings: {failed}")
    logger.info("================")

if __name__ == "__main__":
    run()
