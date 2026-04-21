"""
Sales Pipeline Audit Module
Verifies sales queue health and ensures merchants without verified emails
receive contact discovery retries.
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
    from runtime.ops.autonomous_sales import run_autonomous_sales_cycle
except ImportError:
    from autonomous_sales import run_autonomous_sales_cycle

logger = get_logger("pipeline_audit")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def audit_sales_pipeline():
    logger.info("Auditing sales pipeline health...")
    
    with engine.connect() as conn:
        # Task 4 — Pending opportunities check
        pending_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM merchant_opportunities
            WHERE status IN ('pending', 'pending_review', 'approved')
        """)).scalar() or 0
        
        logger.info(f"Pipeline Audit: {pending_count} pending opportunities.")
        
        if pending_count > 20:
            logger.info("Pending opportunities > 20. Triggering autonomous sales cycle...")
            try:
                run_autonomous_sales_cycle()
                save_event(
                    "sales_cycle_triggered",
                    {"pending_opportunities": pending_count}
                )
            except Exception as e:
                logger.error(f"Error triggering sales cycle: {e}")
            
        # Outreach deadlock detection — find merchants stuck in blocked retry loops
        try:
            conn.execute(text("ALTER TABLE sales_outreach_events ADD COLUMN IF NOT EXISTS event_type VARCHAR(20)"))
            conn.execute(text("ALTER TABLE merchant_opportunities ADD COLUMN IF NOT EXISTS retry_after TIMESTAMP"))
        except Exception:
            pass

        # Only flag permanent failure reasons — temporary blocks (warmup, low_distress) resolve on their own
        PERMANENT_BLOCK_REASONS = ('legitimacy_failed', 'domain_blacklisted', 'fraud_pattern_detected')

        stuck_merchants = conn.execute(text("""
            SELECT merchant_id, COUNT(*) as block_count, MAX(blocked_reason) as last_reason
            FROM sales_outreach_events
            WHERE event_type = 'blocked'
              AND blocked_reason IN :reasons
              AND sent_at >= NOW() - INTERVAL '7 days'
            GROUP BY merchant_id
            HAVING COUNT(*) >= 5
        """), {"reasons": PERMANENT_BLOCK_REASONS}).fetchall()

        if stuck_merchants:
            logger.warning(f"Pipeline Audit: {len(stuck_merchants)} merchants stuck in permanent block loops.")
            for merchant_id, block_count, last_reason in stuck_merchants:
                logger.warning(f"  Merchant {merchant_id}: {block_count} blocks, last reason: {last_reason}")
                conn.execute(text("""
                    UPDATE merchant_opportunities SET status = 'rejected'
                    WHERE merchant_id = :mid AND status IN ('pending_review', 'approved')
                """), {"mid": merchant_id})
                logger.info(f"  Rejected opportunities for merchant {merchant_id} ({last_reason} deadlock).")
            save_event("outreach_deadlock_detected", {"stuck_merchants": len(stuck_merchants)})

        # Task 5 — Domain verification check (retry context retrieval)
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT false"))
        conn.commit()
        
        merchants_needing_contacts = conn.execute(text("""
            SELECT id, domain
            FROM merchants
            WHERE domain IS NOT NULL
              AND id NOT IN (
                  SELECT merchant_id
                  FROM merchant_contacts
                  WHERE email_verified = true
              )
        """)).fetchall()
        
        if merchants_needing_contacts:
            logger.info(f"Pipeline Audit: Found {len(merchants_needing_contacts)} merchants needing email verification retries.")
            
            try:
                from runtime.intelligence.contact_discovery import discover_contacts_for_merchant
            except ImportError:
                discover_contacts_for_merchant = None
                
            if discover_contacts_for_merchant:
                for merchant_id, domain in merchants_needing_contacts:
                    try:
                        discover_contacts_for_merchant(merchant_id)
                        save_event("contact_discovery_retry", {"merchant_id": merchant_id, "domain": domain})
                    except Exception as e:
                        logger.error(f"Error running contact discovery for merchant {merchant_id}: {e}")
            else:
                logger.error("Could not import discover_contacts_for_merchant for pipeline audit.")
                
    logger.info("Sales pipeline audit complete.")

if __name__ == "__main__":
    audit_sales_pipeline()
