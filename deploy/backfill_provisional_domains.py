"""
Backfill script to infer provisional domains for merchants lacking one
using the new merchant_attribution fallback logic.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'runtime'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from intelligence.merchant_attribution import attribute_merchant
from intelligence.contact_discovery import discover_contacts_for_merchant
import time

logger = get_logger("backfill")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def main():
    logger.info("Starting merchant domains backfill...")
    print("Starting merchant domains backfill...")
    
    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT id, canonical_name FROM merchants 
            WHERE domain IS NULL OR domain = ''
        """)).fetchall()
        
    if not merchants:
        print("No merchants missing domains. Exiting.")
        return
        
    print(f"Found {len(merchants)} merchants without domains.")
    
    updated_count = 0
    for merchant_id, canonical_name in merchants:
        # Fetch up to 5 signals for context to guess the platform
        with engine.connect() as conn:
            signals = conn.execute(text("""
                SELECT content, source FROM signals
                WHERE merchant_id = :mid
                LIMIT 5
            """), {"mid": merchant_id}).fetchall()
            
        context_text = " ".join([r[0] for r in signals if r[0]])
        # Use canonical name as the brand instead of only relying on extraction
        context_text += f" my store {canonical_name}"
        
        result = attribute_merchant(context_text)
        
        if result and result.get("domain") and result.get("domain_confidence") == "provisional":
            provisional_domain = result["domain"]
            confidence = result.get("confidence", 0.4)
            
            with engine.connect() as conn:
                conn.execute(text("""
                    UPDATE merchants 
                    SET domain = :domain, domain_confidence = 'provisional'
                    WHERE id = :mid
                """), {"domain": provisional_domain, "mid": merchant_id})
                conn.commit()
                
            updated_count += 1
            print(f"Updated: {canonical_name} -> {provisional_domain}")
            
            # Instantly enqueue crawler for the new provisional domain
            try:
                # Fire and forget locally
                discover_contacts_for_merchant(merchant_id)
            except Exception as e:
                logger.error(f"Contact discovery failed on backfill for {canonical_name}: {e}")
                
        time.sleep(0.5)
        
    print(f"Backfill complete! Generated provisional domains for {updated_count} merchants.")

if __name__ == "__main__":
    main()
