"""
Module to analyze unclassified merchant distress clusters and heuristically 
determine processor attribution to repair graph links.
"""
import sys, os

# Dynamically find project root
_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, 'config')):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event

try:
    from merchant_identity import resolve_merchant_identity
except ImportError:
    from runtime.intelligence.merchant_identity import resolve_merchant_identity

logger = get_logger("cluster_investigation")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

PROCESSOR_PATTERNS = {
    "stripe": ["stripe", "payout freeze", "stripe support"],
    "paypal": ["paypal", "paypal dispute", "paypal hold"],
    "square": ["square payments", "square account"],
    "adyen": ["adyen"],
    "shopify_payments": ["shopify payments", "shopify payout"],
}

def investigate_unclassified_clusters(limit: int = 10):
    """
    Analyzes unclassified merchant distress clusters, attempts processor detection,
    and triggers merchant re-attribution on resolved clusters.
    """
    logger.info(f"Investigating up to {limit} unclassified clusters...")
    
    with engine.connect() as conn:
        clusters = conn.execute(text("""
            SELECT id, cluster_topic 
            FROM clusters 
            WHERE cluster_topic = 'Unclassified Merchant Distress'
            ORDER BY cluster_size DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
        
        if not clusters:
            logger.info("No unclassified clusters found.")
            return

        reprocessed_clusters = 0

        for cluster_id, topic in clusters:
            signals = conn.execute(text("""
                SELECT id, content
                FROM signals
                WHERE cluster_id = :cluster_id
            """), {"cluster_id": cluster_id}).fetchall()
            
            processor_mentions = {p: 0 for p in PROCESSOR_PATTERNS}
            
            for signal_id, content in signals:
                if not content:
                    continue
                content_lower = content.lower()
                
                for processor, keywords in PROCESSOR_PATTERNS.items():
                    for kw in keywords:
                        if kw in content_lower:
                            processor_mentions[processor] += 1
            
            detected_processor = None
            for processor, count in processor_mentions.items():
                if count > 3:
                    detected_processor = processor
                    break
                    
            if detected_processor:
                logger.info(f"Cluster {cluster_id} resolved to: {detected_processor}")
                
                conn.execute(text("""
                    UPDATE clusters
                    SET processor = :processor
                    WHERE id = :cluster_id
                """), {"processor": detected_processor, "cluster_id": cluster_id})
                conn.commit()
                
                save_event(
                    "cluster_processor_resolved",
                    {"cluster_id": cluster_id, "processor": detected_processor}
                )
                
                # Task 2 — Trigger Merchant Re-Attribution
                for signal_id, content in signals:
                    try:
                        resolve_merchant_identity(signal_id, content)
                    except Exception as e:
                        logger.error(f"Failed to re-attribute signal {signal_id}: {e}")
                        
                save_event("cluster_reprocessed", {"cluster_id": cluster_id})
                reprocessed_clusters += 1
                
    logger.info(f"Investigation cycle complete. Resolved {reprocessed_clusters} clusters.")

if __name__ == "__main__":
    investigate_unclassified_clusters(limit=5)
