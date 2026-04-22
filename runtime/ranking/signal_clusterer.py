import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from memory.structured.db import save_event
from config.logging_config import get_logger
from collections import defaultdict
from entity_taxonomy import classify_entity, CLASS_CONSUMER_COMPLAINT, CLASS_AFFILIATE_DISTRESS, CLASS_PROCESSOR_DISTRESS, CLASS_NON_MERCHANT
import re

logger = get_logger("clustering")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

# Expanded distress categories with comprehensive keyword matching
CLUSTER_PATTERNS = {
    # Processor-specific clusters
    "Stripe account freezes": [
        "stripe", "stripe froze", "stripe frozen", "stripe freeze",
        "stripe suspended", "stripe closed", "stripe connect"
    ],
    "PayPal holds and freezes": [
        "paypal", "paypal hold", "paypal froze", "paypal frozen",
        "paypal limited", "paypal restriction", "paypal 180"
    ],
    "Shopify payment disruptions": [
        "shopify payments", "shopify payout", "shopify hold",
        "shopify suspended", "shopify store closed"
    ],
    "Square payout issues": [
        "square payout", "square hold", "square froze",
        "square suspended", "square deactivated"
    ],
    "Adyen processing failures": [
        "adyen", "adyen payout", "adyen settlement", "adyen block"
    ],
    # Distress category clusters
    "Payout freezes and delays": [
        "payout delayed", "payout frozen", "payout held",
        "funds withheld", "payout suspended", "no payout",
        "missing payout", "late payout", "payout issue",
        "payout delay", "funds held", "holding my funds",
        "money held", "money frozen", "funds frozen",
        "holding funds", "held my funds", "withheld funds",
    ],
    "Processor reserve holds": [
        "reserve", "holding funds", "funds held", "rolling reserve",
        "reserve fund", "withheld percentage", "cash reserve",
        "reserve requirement", "collateral"
    ],
    "Account terminations": [
        "terminated", "shut down", "closed account", "account closed",
        "merchant terminated", "banned", "permanently suspended",
        "account deactivated", "service discontinued",
        "account frozen", "froze my account", "frozen account",
        "account locked", "locked out", "account disabled",
        "account shut", "shut my account",
    ],
    "Gateway shutdowns and outages": [
        "gateway down", "gateway error", "gateway offline",
        "processing down", "api outage", "gateway shutdown",
        "payment gateway", "gateway unavailable", "502", "503",
        "timeout", "connection refused"
    ],
    "Chargeback and dispute holds": [
        "chargeback", "dispute", "fraud flag", "chargeback rate",
        "chargeback threshold", "excessive chargebacks",
        "dispute rate", "fraud review", "chargeback hold"
    ],
    "KYC and compliance blocks": [
        "kyc", "verification failed", "compliance",
        "identity verification", "document request",
        "compliance review", "aml", "under review",
        "additional documentation", "risk review",
        "account review", "identity check",
    ],
    "High-risk merchant restrictions": [
        "high risk", "high-risk", "risk category",
        "restricted business", "prohibited merchant",
        "risk assessment", "underwriting",
        "flagged as high risk", "deemed high risk",
    ],
    "Revenue and financial impact": [
        "revenue lost", "sales stopped", "income frozen",
        "can't accept payments", "losing customers",
        "business impact", "going bankrupt"
    ],
    "Processing fee disputes": [
        "processing fee", "fee increase", "rate hike",
        "interchange", "hidden fees", "surcharge"
    ],
    # Entity-type aware clusters
    "Affiliate Payout Issues": [
        "affiliate payout", "commission withheld", "commission delayed",
        "affiliate earnings", "publisher payout", "referral commission",
        "affiliate account suspended", "affiliate network"
    ],
    "Consumer Complaints": [
        "cashback complaint", "consumer cashback", "refund denied",
        "cashback not showing", "cashback missing", "my cashback",
        "consumer complaint", "want my money back"
    ],
    "Marketplace Bans": [
        "marketplace ban", "seller suspended", "seller account banned",
        "marketplace account closed", "seller terminated",
        "listing removed", "store banned"
    ],
}

# Secondary keyword patterns for signals that partially match
FALLBACK_PATTERNS = {
    "Payout freezes and delays": ["payout", "deposit", "transfer", "funds", "money"],
    "Account terminations": ["suspended", "closed", "terminated", "frozen", "locked", "disabled", "banned"],
    "Chargeback and dispute holds": ["chargeback", "dispute", "fraud"],
    "Processor reserve holds": ["reserve", "held", "withheld", "collateral"],
    "Gateway shutdowns and outages": ["gateway", "outage", "down", "error"],
    "KYC and compliance blocks": ["kyc", "compliance", "verification", "review"],
}


def classify_signal(content_lower):
    """Classify a signal into a distress category. Returns topic or None."""
    # Primary pass: exact keyword matching
    for topic, keywords in CLUSTER_PATTERNS.items():
        if any(kw in content_lower for kw in keywords):
            return topic

    # Secondary pass: broader fallback matching
    for topic, keywords in FALLBACK_PATTERNS.items():
        match_count = sum(1 for kw in keywords if kw in content_lower)
        if match_count >= 2:
            return topic

    return None


def cluster_signals():
    logger.info("Signal clustering started")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content, priority_score
            FROM signals
            WHERE detected_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY priority_score DESC
        """))
        signals = [dict(row._mapping) for row in rows]

    if not signals:
        logger.info("No signals in last 24h to cluster")
        return {"clusters_created": 0}

    clusters = defaultdict(list)
    unclassified = []

    consumer_filtered = 0

    for sig in signals:
        content_lower = sig.get("content", "").lower()
        content_raw = sig.get("content", "")

        # Entity-type aware routing: classify entity first
        entity_info = classify_entity(content_raw)
        classification = entity_info.get("classification")

        # Route consumer complaints to their own cluster
        if classification in (CLASS_CONSUMER_COMPLAINT, CLASS_NON_MERCHANT):
            clusters["Consumer Complaints"].append(sig["id"])
            consumer_filtered += 1
            continue

        # Route affiliate distress to its own cluster
        if classification == CLASS_AFFILIATE_DISTRESS:
            clusters["Affiliate Payout Issues"].append(sig["id"])
            continue

        try:
            # Standard keyword-based classification
            topic = classify_signal(content_lower)
            if topic:
                clusters[topic].append(sig["id"])
            else:
                unclassified.append(sig)
        except Exception as e:
            logger.error(f"Error processing signal {sig.get('id')}: {e}")
            continue

    # For remaining unclassified signals, attempt processor-based grouping
    for sig in unclassified:
        content_lower = sig.get("content", "").lower()
        processor_found = False
        for proc_name in ["stripe", "paypal", "shopify", "square", "adyen", "braintree", "authorize.net", "worldpay"]:
            if proc_name in content_lower:
                topic = f"{proc_name.title()} - unclassified distress"
                clusters[topic].append(sig["id"])
                processor_found = True
                break
        if not processor_found:
            clusters["Unclassified merchant distress"].append(sig["id"])

    # Clear old clusters and upsert current ones
    created = 0
    with engine.connect() as conn:
        for topic, signal_ids in clusters.items():
            if len(signal_ids) > 0:
                # Update existing cluster with same topic from this cycle, or insert new
                existing = conn.execute(text("""
                    SELECT id FROM clusters
                    WHERE cluster_topic = :topic
                      AND created_at > CURRENT_TIMESTAMP - INTERVAL '2 hours'
                    ORDER BY created_at DESC LIMIT 1
                """), {"topic": topic}).fetchone()

                if existing:
                    conn.execute(text("""
                        UPDATE clusters SET cluster_size = :size, signal_ids = :ids,
                            created_at = CURRENT_TIMESTAMP
                        WHERE id = :cid
                    """), {
                        "size": len(signal_ids),
                        "ids": json.dumps(signal_ids),
                        "cid": existing[0],
                    })
                else:
                    conn.execute(text("""
                        INSERT INTO clusters (cluster_topic, cluster_size, signal_ids)
                        VALUES (:topic, :size, :ids)
                    """), {
                        "topic": topic,
                        "size": len(signal_ids),
                        "ids": json.dumps(signal_ids),
                    })
                    save_event("cluster_created", {"topic": topic, "size": len(signal_ids)})
                created += 1
        conn.commit()

        # Delete old clusters after inserting new ones
        conn.execute(text("DELETE FROM clusters WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '48 hours'"))
        conn.commit()

    classified_count = sum(len(ids) for t, ids in clusters.items() if t != "Unclassified merchant distress")
    unclassified_count = len(clusters.get("Unclassified merchant distress", []))

    logger.info(f"Created {created} clusters from {len(signals)} signals ({classified_count} classified, {unclassified_count} unclassified, {consumer_filtered} consumer complaints filtered)")
    logger.info("Signal clustering completed")
    return {"clusters_created": created, "total_signals": len(signals), "classified": classified_count, "unclassified": unclassified_count, "consumer_filtered": consumer_filtered}


if __name__ == "__main__":
    print("Running signal clustering...")
    result = cluster_signals()
    print(f"Result: {result}")
