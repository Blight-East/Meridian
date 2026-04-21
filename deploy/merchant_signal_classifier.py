"""
Deterministic merchant-operator signal classifier.

Scores whether a signal is likely authored by a merchant operator rather than
an end consumer or an unrelated poster. No LLM usage.
"""
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

logger = get_logger("merchant_signal_classifier")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

CLASS_MERCHANT_OPERATOR = "merchant_operator"
CLASS_CONSUMER_OR_IRRELEVANT = "consumer_or_irrelevant"
CLASS_NON_MERCHANT = "non_merchant"

_MERCHANT_PATTERNS = [
    (re.compile(r"\b(?:my|our)\s+(?:store|shop|brand|business|company|website)\b", re.IGNORECASE), 4, "merchant_ownership"),
    (re.compile(r"\b(?:i|we)\s+(?:run|own|operate|built|started|founded|launched)\b", re.IGNORECASE), 4, "merchant_operator"),
    (re.compile(r"\bmerchant\s+account\b", re.IGNORECASE), 3, "merchant_account"),
    (re.compile(r"\b(?:payout|settlement|reserve|chargeback|dispute|gateway|processor|processing)\b", re.IGNORECASE), 2, "merchant_ops"),
    (re.compile(r"\b(?:funds held|funds frozen|payout frozen|account closed|account frozen|payout paused|reserve hold)\b", re.IGNORECASE), 3, "distress_keywords"),
    (re.compile(r"\b(?:customers|orders|sales|revenue|cash flow|checkout)\b", re.IGNORECASE), 2, "business_impact"),
]

_CONSUMER_PATTERNS = [
    (re.compile(r"\b(?:my order|my purchase|my package|my refund|my card|my subscription)\b", re.IGNORECASE), 4, "consumer_purchase"),
    (re.compile(r"\b(?:item not received|never received|return policy|customer service|money back)\b", re.IGNORECASE), 4, "consumer_complaint"),
    (re.compile(r"\b(?:cashback|cash back|refund issue|refund issues|refund delayed|refund status)\b", re.IGNORECASE), 4, "consumer_refund"),
    (re.compile(r"\b(?:order issue|order issues|issue with my order|problem with my order|shipping issue|delivery issue)\b", re.IGNORECASE), 4, "consumer_order_issue"),
    (re.compile(r"\b(?:buyer|customer|personal account|debit card|credit card charge)\b", re.IGNORECASE), 3, "consumer_context"),
    (re.compile(r"\b(?:venmo|cash app|zelle|apple pay|google pay)\b", re.IGNORECASE), 3, "consumer_wallet"),
]

_IRRELEVANT_PATTERNS = [
    re.compile(r"\b(?:can i sue|legal advice|lawsuit|news|article|meme|press release)\b", re.IGNORECASE),
]


def classify_merchant_signal(text):
    """
    Return a deterministic merchant-operator classification.
    """
    if not text:
        return {
            "classification": CLASS_NON_MERCHANT,
            "merchant_score": 0,
            "consumer_score": 0,
            "reasons": [],
        }

    merchant_score = 0
    consumer_score = 0
    reasons = []

    for pattern, weight, reason in _MERCHANT_PATTERNS:
        if pattern.search(text):
            merchant_score += weight
            reasons.append(reason)

    for pattern, weight, reason in _CONSUMER_PATTERNS:
        if pattern.search(text):
            consumer_score += weight
            reasons.append(reason)

    if any(pattern.search(text) for pattern in _IRRELEVANT_PATTERNS):
        consumer_score += 2
        reasons.append("irrelevant_context")

    if merchant_score >= 4 and merchant_score >= consumer_score + 2:
        classification = CLASS_MERCHANT_OPERATOR
    elif consumer_score >= 3 and consumer_score >= merchant_score:
        classification = CLASS_CONSUMER_OR_IRRELEVANT
    else:
        classification = CLASS_NON_MERCHANT

    return {
        "classification": classification,
        "merchant_score": merchant_score,
        "consumer_score": consumer_score,
        "reasons": reasons,
    }


def run_merchant_signal_classification(limit=200):
    """
    Backfill classification fields for recent signals.
    """
    merchant_count = 0
    filtered_count = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content
            FROM signals
            WHERE detected_at >= NOW() - INTERVAL '48 hours'
            ORDER BY detected_at DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

        for signal_id, content in rows:
            result = classify_merchant_signal(content or "")
            normalized_class = result["classification"]
            signal_class = "merchant_distress" if normalized_class == CLASS_MERCHANT_OPERATOR else "consumer_complaint"
            conn.execute(text("""
                UPDATE signals
                SET classification = :classification
                WHERE id = :signal_id
            """), {
                "classification": signal_class,
                "signal_id": signal_id,
            })

            if normalized_class == CLASS_MERCHANT_OPERATOR:
                merchant_count += 1
            else:
                filtered_count += 1

        conn.commit()

    save_event("merchant_signal_classification_run", {
        "signals_processed": len(rows),
        "merchant_signals_detected": merchant_count,
        "consumer_signals_filtered": filtered_count,
    })
    result = {
        "signals_processed": len(rows),
        "merchant_signals_detected": merchant_count,
        "consumer_signals_filtered": filtered_count,
    }
    logger.info(f"Merchant signal classification run complete: {result}")
    return result
