"""
Merchant Identity Reranker
Advisory reranking based on historical rewards from the learning_feedback_ledger.
"""
from sqlalchemy import text
from config.logging_config import get_logger
from memory.structured.db import engine

logger = get_logger("reranker")

def score_identity_candidates(signal_id, domains, company_names):
    """
    Score identity candidates based on historical rewards.
    Returns: {
        "domain_scores": {domain: score, ...},
        "brand_scores": {brand: score, ...},
        "recommendation": "domain" | "brand" | None
    }
    """
    scores = {
        "domain_scores": {},
        "brand_scores": {},
        "recommendation": None
    }
    
    if not domains and not company_names:
        return scores

    try:
        with engine.connect() as conn:
            # Domain scores based on historical rewards for the same domain
            if domains:
                for domain in domains:
                    reward_sum = conn.execute(text("""
                        SELECT COALESCE(SUM(reward_score), 0)
                        FROM learning_feedback_ledger
                        WHERE merchant_domain_candidate = :domain
                          AND reward_score IS NOT NULL
                    """), {"domain": domain}).scalar() or 0.0
                    scores["domain_scores"][domain] = float(reward_sum)

            # Brand scores based on historical rewards for the same merchant (if known previously)
            if company_names:
                for brand in company_names:
                    reward_sum = conn.execute(text("""
                        SELECT COALESCE(SUM(reward_score), 0)
                        FROM learning_feedback_ledger lfl
                        JOIN merchants m ON lfl.merchant_id_candidate = m.id
                        WHERE m.canonical_name = :brand
                          AND reward_score IS NOT NULL
                    """), {"brand": brand}).scalar() or 0.0
                    scores["brand_scores"][brand] = float(reward_sum)

            # Simple logic: pick the strategy with the highest positive cumulative reward
            max_domain_score = max(scores["domain_scores"].values()) if scores["domain_scores"] else -1000
            max_brand_score = max(scores["brand_scores"].values()) if scores["brand_scores"] else -1000
            
            if max_domain_score > 0 or max_brand_score > 0:
                if max_domain_score >= max_brand_score:
                    scores["recommendation"] = "domain"
                else:
                    scores["recommendation"] = "brand"

    except Exception as e:
        logger.warning(f"Reranker failed to score candidates: {e}")

    return scores
