"""
Sparse Opportunity Enrichment Pass
Identifies merchant_opportunities with "unknown" processor or distress topics,
retrieves their raw signals, and uses LLM reasoning to explicitly extract them.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event, engine
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.reasoning.reasoning_router import route_reasoning
from runtime.intelligence.distress_normalization import normalize_distress_topic

logger = get_logger("sparse_opportunity_enrichment")

def run_enrich_sparse_opportunities(limit: int = 50) -> dict:
    """
    Scans for opportunities with missing inference data and asks Anthropic
    to extract the processor and distress topic from raw signals.
    """
    logger.info("Starting sparse opportunity enrichment pass...")
    
    with engine.connect() as conn:
        # Find active opportunities with sparse data
        rows = conn.execute(
            text(
                """
                SELECT mo.id as opportunity_id, mo.merchant_id, mo.merchant_domain, 
                       mo.processor, mo.distress_topic
                FROM merchant_opportunities mo
                WHERE mo.status NOT IN ('outcome_won', 'outcome_lost', 'outcome_ignored', 'rejected')
                  AND (
                      mo.processor = 'unknown' 
                      OR mo.processor IS NULL 
                      OR mo.processor = ''
                      OR mo.distress_topic = 'unknown'
                      OR mo.distress_topic IS NULL
                      OR mo.distress_topic = ''
                  )
                ORDER BY mo.created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit}
        ).mappings().all()

        if not rows:
            logger.info("No sparse opportunities found to enrich.")
            return {"enriched_count": 0, "scanned_count": 0, "errors": 0}

        enriched_count = 0
        errors = 0
        
        # We will use reason_fast (Sonnet) because we are processing a queue
        model_name, reasoning_func = route_reasoning(
            task_type="sparse_feature_extraction", 
            severity="low"
        )

        for row in rows:
            opp_id = int(row["opportunity_id"])
            merchant_id = row["merchant_id"]
            merchant_domain = row["merchant_domain"]
            
            # Fetch raw signals
            signal_rows = conn.execute(
                text(
                    """
                    SELECT content 
                    FROM signals
                    WHERE merchant_id = :merchant_id
                       OR (id IN (
                           SELECT signal_id FROM merchant_signals 
                           WHERE merchant_id = :merchant_id
                       ))
                    ORDER BY detected_at DESC
                    LIMIT 3
                    """
                ),
                {"merchant_id": merchant_id}
            ).fetchall()
            
            if not signal_rows:
                continue
                
            signals_text = "\n".join([f"- {r[0]}" for r in signal_rows if r[0]])
            
            prompt = f"""
You are the Agent Flux intelligence engine.
We detected merchant distress but failed to extract the exact payment processor and the category of distress.

Merchant Domain: {merchant_domain}
Signals:
{signals_text}

Task: Extract the payment processor and the core distress topic.
- Processor: Try to identify if they use Stripe, PayPal, Square, Adyen, Braintree, Shopify Payments, Authorize.net, or Checkout.com. If you cannot find a processor, return "unknown".
- Distress Topic: Map the issue to one of the following exact categories if possible:
  1. account_frozen
  2. payouts_delayed
  3. reserve_hold
  4. verification_review
  5. processor_switch_intent
  6. chargeback_issue
  7. account_terminated
  8. onboarding_rejected
  If none apply perfectly, deduce the closest match, or return "unknown".

IMPORTANT: Respond with ONLY a valid JSON object. Do not include markdown blocks like ```json.
{{
  "processor": "extracted_processor_or_unknown",
  "distress_topic": "extracted_category_or_unknown"
}}
"""
            try:
                response = reasoning_func(prompt, use_cache=False)
                
                # Check if it successfully parsed as a dict
                if isinstance(response, dict) and "processor" in response and "distress_topic" in response:
                    inferred_processor = response["processor"].lower().strip().replace(" ", "_")
                    raw_distress = response["distress_topic"]
                    
                    # Normalize distress safely
                    inferred_distress = normalize_distress_topic(raw_distress)
                    
                    # Update if we discovered something new
                    current_proc = row["processor"] or "unknown"
                    current_dist = row["distress_topic"] or "unknown"
                    
                    updates_made = False
                    update_params = {"opp_id": opp_id}
                    if inferred_processor == "unknown":
                        inferred_processor = "unclassified"
                    if inferred_distress == "unknown":
                        inferred_distress = "unclassified"

                    update_fields = []
                    
                    if current_proc in ["unknown", "unclassified"] and inferred_processor != "unknown":
                        update_fields.append("processor = :proc")
                        update_params["proc"] = inferred_processor
                        updates_made = True
                        
                    if current_dist in ["unknown", "unclassified"] and inferred_distress != "unknown":
                        update_fields.append("distress_topic = :dist")
                        update_params["dist"] = inferred_distress
                        updates_made = True

                    if updates_made:
                        update_sql = "UPDATE merchant_opportunities SET " + ", ".join(update_fields) + " WHERE id = :opp_id"
                        conn.execute(text(update_sql), update_params)
                        enriched_count += 1
                        logger.info(f"Enriched Opp #{opp_id} ({merchant_domain}): processor='{inferred_processor}', distress='{inferred_distress}'")
                        
            except Exception as e:
                errors += 1
                conn.rollback()
                logger.warning(f"Failed to enrich opp #{opp_id}: {e}")
                
        conn.commit()
        
    result = {
        "scanned_count": len(rows),
        "enriched_count": enriched_count,
        "errors": errors,
        "completed_at": utc_now_iso()
    }
    
    save_event("sparse_opportunity_enrichment_run", result)
    record_component_state(
        "sparse_opportunity_enrichment",
        ttl=3600,
        last_run_at=result["completed_at"],
        last_scanned_count=len(rows),
        last_enriched_count=enriched_count
    )
    
    logger.info(f"Sparse enrichment pass complete: {result}")
    return result

if __name__ == "__main__":
    print(run_enrich_sparse_opportunities())
