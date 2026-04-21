"""
Autonomous Sales Execution Engine
Automatically converts discovered merchants by generating outreach and closing deals.
"""
import sys, os, time, random
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.integrations.email_sender import send_outreach_email
from runtime.conversation.chat_engine import generate_chat_response

# Re-use conversation handling
from runtime.ops.operator_alerts import send_operator_alert

logger = get_logger("autonomous_sales")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_OUTREACH_PER_CYCLE = 10
AUTONOMOUS_SALES_ENABLED = True
AUTONOMOUS_SALES_MODE = "live"

# ── Entity Type Filter ────────────────────────────────────────────
# Domains and names that are banks, processors, platforms, or VCs —
# NOT real merchant targets. These appear in distress threads because
# merchants *complain about* them, not because they ARE merchants.
EXCLUDED_DOMAINS = {
    # Banks
    "scotiabank.com", "chase.com", "bankofamerica.com", "wellsfargo.com",
    "hsbc.com", "barclays.com", "citibank.com", "tdbank.com", "rbc.com",
    "capitalone.com", "usbank.com", "pnc.com",
    # Payment Processors / Fintechs
    "stripe.com", "paypal.com", "square.com", "adyen.com", "worldpay.com",
    "braintreepayments.com", "revolut.com", "wise.com", "klarna.com",
    "afterpay.com", "affirm.com", "checkout.com", "mollie.com",
    # Platforms
    "shopify.com", "bigcommerce.com", "woocommerce.com", "magento.com",
    "wix.com", "squarespace.com", "godaddy.com",
    # VC / Accelerators
    "ycombinator.com", "techstars.com", "500.co",
    # Chargeback / Fraud services
    "chargebacks911.com", "sift.com", "signifyd.com", "riskified.com",
}
EXCLUDED_NAME_KEYWORDS = {
    "stripe", "paypal", "square", "revolut", "scotiabank", "scotia",
    "chase", "visa", "mastercard", "amex", "american express",
    "ycombinator", "y combinator", "unknown stripe merchant",
    "bank of america", "wells fargo", "citibank", "hsbc", "barclays",
    "chargebacks911", "chargebacks", "adyen", "worldpay", "klarna",
}

def _is_excluded_entity(domain, merchant_name):
    """Filter out banks, processors, platforms, and VCs from outreach targets."""
    if domain and domain.lower() in EXCLUDED_DOMAINS:
        return True
    if merchant_name:
        name_lower = merchant_name.lower()
        for keyword in EXCLUDED_NAME_KEYWORDS:
            if keyword in name_lower:
                return True
    return False

RETRY_COOLDOWNS = {
    'low_distress': 24 * 3600,         # 24 hours
    'warmup_limit_exceeded': 6 * 3600, # 6 hours
    'duplicate_outreach': 30 * 86400,  # 30 days
    'legitimacy_failed': 7 * 86400,    # 7 days
    'excluded_entity': 365 * 86400,    # 1 year (effectively permanent)
}

def compute_retry_after(reason):
    """Compute a jittered retry timestamp for a given block reason."""
    base = RETRY_COOLDOWNS.get(reason, 6 * 3600)
    jitter = random.randint(0, base // 4)
    return datetime.now(timezone.utc) + timedelta(seconds=base + jitter)

def _init_sales_tables():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales_outreach_events (
                id SERIAL PRIMARY KEY,
                merchant_id INT,
                contact_email VARCHAR(255),
                subject TEXT,
                body TEXT,
                draft_body TEXT,
                simulation_mode BOOLEAN DEFAULT FALSE,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        # In case the table already exists, safely alter it to add new columns
        try:
            conn.execute(text("ALTER TABLE sales_outreach_events ADD COLUMN IF NOT EXISTS draft_body TEXT"))
            conn.execute(text("ALTER TABLE sales_outreach_events ADD COLUMN IF NOT EXISTS simulation_mode BOOLEAN DEFAULT FALSE"))
            conn.execute(text("ALTER TABLE sales_outreach_events ADD COLUMN IF NOT EXISTS blocked_reason VARCHAR(50)"))
            conn.execute(text("ALTER TABLE sales_outreach_events ADD COLUMN IF NOT EXISTS event_type VARCHAR(20)"))
            conn.execute(text("ALTER TABLE merchant_opportunities ADD COLUMN IF NOT EXISTS retry_after TIMESTAMP"))
        except Exception as e:
            logger.debug(f"Table alter info: {e}")
            conn.rollback()

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS merchant_conversations (
                id SERIAL PRIMARY KEY,
                merchant_id INT,
                contact_email VARCHAR(255),
                message TEXT,
                direction VARCHAR(50), -- 'sent' or 'received'
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()

def get_daily_outreach_limit(conn):
    """
    Calculate limits based on system age (domain reputation warmup).
    system_age < 2 days -> 3/day
    system_age < 5 days -> 5/day
    Otherwise -> 10/day
    """
    try:
        first_signal = conn.execute(text("SELECT MIN(detected_at) FROM signals")).scalar()
        if not first_signal:
            return 3
            
        if first_signal.tzinfo is None:
            first_signal = first_signal.replace(tzinfo=timezone.utc)
            
        age_days = (datetime.now(timezone.utc) - first_signal).days
        if age_days < 2:
            return 3
        elif age_days < 5:
            return 5
        else:
            return 10
    except Exception as e:
        logger.warning(f"Failed to calculate system age for warmup limits: {e}")
        return 3

def is_valid_merchant_target(domain, confidence):
    """
    Verify merchant legitimacy before drafting outreach.
    1. Must have a domain.
    2. Must have valid MX records (meaning they can receive email).
    3. Contact confidence must be >= 0.6.
    """
    if not domain:
        return False
    if float(confidence) < 0.6:
        return False
        
    try:
        from intelligence.domain_utils import get_mx_servers
        mx_servers = get_mx_servers(domain)
        if not mx_servers:
            return False
        return True
    except Exception as e:
        logger.warning(f"Legitimacy filter failed for {domain}: {e}")
        return False

def is_outreach_allowed(conn, merchant_id):
    """
    Duplicate Merchant Outreach Protection.
    Ensure we do not email the same merchant more than once every 30 days.
    """
    try:
        last_outreach = conn.execute(text("""
            SELECT MAX(sent_at) FROM sales_outreach_events
            WHERE merchant_id = :mid
              AND event_type = 'sent'
        """), {"mid": merchant_id}).scalar()
        
        if not last_outreach:
            return True

        if last_outreach.tzinfo is None:
            last_outreach = last_outreach.replace(tzinfo=timezone.utc)
            
        days_since = (datetime.now(timezone.utc) - last_outreach).days
        return days_since >= 30
    except Exception as e:
        logger.error(f"Failed checking outreach allowance: {e}")
        return False

def is_retry_allowed(conn, opp_id):
    """
    Check if a blocked opportunity has passed its retry cooldown.
    Returns True if no active retry cooldown exists.
    """
    try:
        retry_after = conn.execute(text("""
            SELECT retry_after FROM merchant_opportunities
            WHERE id = :opp_id AND retry_after IS NOT NULL
        """), {"opp_id": opp_id}).scalar()

        if not retry_after:
            return True

        if retry_after.tzinfo is None:
            retry_after = retry_after.replace(tzinfo=timezone.utc)

        return datetime.now(timezone.utc) >= retry_after
    except Exception as e:
        logger.error(f"Failed checking retry allowance: {e}")
        return True

def run_autonomous_sales_cycle(force_run=False):
    """
    Main autonomous conversion loop:
    1. Checks if autonomous operations are enabled globally.
    2. Identifies 'approved' or highly confident 'pending_review' deal opportunities.
    3. Gathers the matched contact email.
    4. Extracts or generates the outreach draft utilizing Stripe links.
    5. Transmits the payload via SMTP.
    6. Logs the outbound message directly to the conversational ledger.
    """
    logger.info(f"Initializing Autonomous Sales Cycle (Mode: {AUTONOMOUS_SALES_MODE}, Force: {force_run})...")
    _init_sales_tables()
    
    if not AUTONOMOUS_SALES_ENABLED and not force_run:
        logger.warning("AUTONOMOUS_SALES_ENABLED is false. Simulating cycle only.")

    outreach_sent_count = 0
    drafts_created = 0
    blocked_warmup = 0
    blocked_legitimacy = 0
    blocked_duplicate = 0
    blocked_low_distress = 0
    MIN_DISTRESS_SCORE = 15
    
    with engine.connect() as conn:
        daily_limit = get_daily_outreach_limit(conn)
        
        # Check how many emails were actually sent today (not simulated)
        sent_today = conn.execute(text("""
            SELECT COUNT(*) FROM sales_outreach_events
            WHERE sent_at >= NOW() - INTERVAL '24 hours'
              AND event_type = 'sent'
        """)).scalar() or 0
        
        logger.info(f"Warmup System: {sent_today}/{daily_limit} emails sent today.")
        # Fetch pending/approved un-sent opportunities ranked by priority score.
        # Priority = (distress*3) + (confidence*2) + signal_volume + recency + intent_bonus
        query = """
            SELECT mo.id as opp_id, mo.merchant_id, mo.merchant_domain, mo.processor,
                   mo.distress_topic, mo.outreach_draft, mo.status,
                   mc.contact_name, mc.email as contact_email, mc.confidence,
                   m.distress_score, COALESCE(m.opportunity_score, 0) as opportunity_score,
                   COUNT(s.id) as signal_volume,
                   CASE WHEN MAX(s.detected_at) > NOW() - INTERVAL '24 hours' THEN 5 ELSE 0 END as recency_bonus,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM signals s2 WHERE s2.merchant_id = mo.merchant_id AND (
                           s2.content ILIKE '%%looking for%%payment%%'
                           OR s2.content ILIKE '%%need%%processor%%'
                           OR s2.content ILIKE '%%alternative to%%'
                           OR s2.content ILIKE '%%switch%%from%%'
                           OR s2.content ILIKE '%%recommend%%gateway%%'
                           OR s2.content ILIKE '%%new payment%%'
                           OR s2.content ILIKE '%%high risk%%process%%'
                           OR s2.content ILIKE '%%searching for%%payment%%'
                       )
                   ) THEN 25 ELSE 0 END as intent_bonus,
                   (
                     (COALESCE(m.distress_score, 0) * 3) +
                     (COALESCE(mc.confidence, 0) * 2) +
                     COUNT(s.id) +
                     CASE WHEN MAX(s.detected_at) > NOW() - INTERVAL '24 hours' THEN 5 ELSE 0 END +
                     CASE WHEN EXISTS (
                         SELECT 1 FROM signals s3 WHERE s3.merchant_id = mo.merchant_id AND (
                             s3.content ILIKE '%%looking for%%payment%%'
                             OR s3.content ILIKE '%%need%%processor%%'
                             OR s3.content ILIKE '%%alternative to%%'
                             OR s3.content ILIKE '%%switch%%from%%'
                             OR s3.content ILIKE '%%recommend%%gateway%%'
                             OR s3.content ILIKE '%%new payment%%'
                             OR s3.content ILIKE '%%high risk%%process%%'
                             OR s3.content ILIKE '%%searching for%%payment%%'
                         )
                     ) THEN 25 ELSE 0 END
                   ) as priority_score,
                   m.canonical_name
            FROM merchant_opportunities mo
            JOIN merchant_contacts mc ON mo.merchant_id = mc.merchant_id
            JOIN merchants m ON mo.merchant_id = m.id
            LEFT JOIN signals s ON s.merchant_id = m.id
            WHERE mo.status IN ('pending_review', 'approved')
              AND mo.merchant_id NOT IN (
                  SELECT merchant_id FROM sales_outreach_events
                  WHERE sent_at >= NOW() - INTERVAL '30 days'
                    AND event_type = 'sent'
              )
              AND COALESCE(m.status, 'active') != 'rejected'
              AND mo.merchant_id NOT IN (
                  SELECT merchant_id FROM sales_outreach_events
                  WHERE event_type = 'blocked' AND blocked_reason = 'excluded_entity'
              )
        """
        if not force_run:
            query += " AND mc.confidence >= 0.8 "

        query += """
            GROUP BY mo.id, mo.merchant_id, mo.merchant_domain, mo.processor,
                     mo.distress_topic, mo.outreach_draft, mo.status,
                     mc.contact_name, mc.email, mc.confidence,
                     m.distress_score, m.opportunity_score, m.canonical_name
            ORDER BY priority_score DESC
            LIMIT :limit
        """

        opportunities = conn.execute(text(query), {"limit": MAX_OUTREACH_PER_CYCLE * 2}).fetchall()
        
        if not opportunities:
            logger.info("No actionable conversion opportunities with valid endpoints found.")
            return {"emails_sent": 0}
            
        for row in opportunities:
            opp_id = row[0]
            merchant_id = row[1]
            domain = row[2]
            processor = row[3]
            topic = row[4]
            draft = row[5]
            status = row[6]
            contact_name = row[7] or "Founder"
            contact_email = row[8]
            confidence = row[9] or 0.0
            distress_score = row[10] or 0
            # Priority scoring columns
            _opportunity_score = row[11] or 0
            signal_volume = row[12] or 0
            recency_bonus = row[13] or 0
            intent_bonus = row[14] or 0
            priority_score = row[15] or 0
            canonical_name = row[16] or ""

            logger.info(f"Opportunity queued | merchant_id={merchant_id} | domain={domain} | name={canonical_name} | priority_score={priority_score} | intent_bonus={intent_bonus} | distress={distress_score} | confidence={confidence} | signals={signal_volume} | recency={recency_bonus}")

            # ── Entity Type Filter (banks, processors, platforms, VCs) ──
            if _is_excluded_entity(domain, canonical_name):
                logger.info(f"EXCLUDED: {domain or contact_name} is a bank/processor/platform/VC. Skipping.")
                conn.execute(text("""
                    INSERT INTO sales_outreach_events (merchant_id, contact_email, blocked_reason, event_type)
                    VALUES (:mid, :email, 'excluded_entity', 'blocked')
                """), {"mid": merchant_id, "email": contact_email})
                conn.execute(text("""
                    UPDATE merchant_opportunities SET status = 'rejected', retry_after = :retry_after WHERE id = :opp_id
                """), {"retry_after": compute_retry_after('excluded_entity'), "opp_id": opp_id})
                continue

            # ── Retry Cooldown Check ──
            if not force_run and not is_retry_allowed(conn, opp_id):
                logger.info(f"Merchant {domain} still in retry cooldown. Skipping.")
                continue

            # ── 0. Distress Score Filter ──
            if not force_run and distress_score < MIN_DISTRESS_SCORE:
                logger.info(f"Merchant {domain} distress score ({distress_score}) below threshold ({MIN_DISTRESS_SCORE}). Skipping.")
                blocked_low_distress += 1
                retry_after = compute_retry_after('low_distress')
                conn.execute(text("""
                    INSERT INTO sales_outreach_events (merchant_id, contact_email, blocked_reason, event_type)
                    VALUES (:mid, :email, 'low_distress', 'blocked')
                """), {"mid": merchant_id, "email": contact_email})
                conn.execute(text("""
                    UPDATE merchant_opportunities SET retry_after = :retry_after WHERE id = :opp_id
                """), {"retry_after": retry_after, "opp_id": opp_id})
                continue
            
            # ── 1. Duplicate Cooldown Guard ──
            if not force_run and not is_outreach_allowed(conn, merchant_id):
                logger.info(f"Merchant {domain} cooled down (contacted < 30 days). Skipping.")
                blocked_duplicate += 1
                retry_after = compute_retry_after('duplicate_outreach')
                conn.execute(text("""
                    INSERT INTO sales_outreach_events (merchant_id, contact_email, blocked_reason, event_type)
                    VALUES (:mid, :email, 'duplicate_outreach', 'blocked')
                """), {"mid": merchant_id, "email": contact_email})
                conn.execute(text("""
                    UPDATE merchant_opportunities SET retry_after = :retry_after WHERE id = :opp_id
                """), {"retry_after": retry_after, "opp_id": opp_id})
                continue
                
            # ── 2. Legitimacy Filter ──
            if not force_run and not is_valid_merchant_target(domain, confidence):
                logger.info(f"Merchant {domain} failed legitimacy filter. Skipping.")
                blocked_legitimacy += 1
                retry_after = compute_retry_after('legitimacy_failed')
                conn.execute(text("""
                    INSERT INTO sales_outreach_events (merchant_id, contact_email, blocked_reason, event_type)
                    VALUES (:mid, :email, 'legitimacy_failed', 'blocked')
                """), {"mid": merchant_id, "email": contact_email})
                conn.execute(text("""
                    UPDATE merchant_opportunities SET retry_after = :retry_after WHERE id = :opp_id
                """), {"retry_after": retry_after, "opp_id": opp_id})
                
                # Update status so we don't infinitely retry a busted domain
                conn.execute(text("UPDATE merchant_opportunities SET status = 'rejected' WHERE id = :opp_id"), {"opp_id": opp_id})
                continue

            # ── 3. Warmup Limit Guard ──
            if not force_run and AUTONOMOUS_SALES_MODE == "live" and sent_today >= daily_limit:
                logger.info(f"Warmup limit reached ({sent_today}/{daily_limit}). Blocking {domain}.")
                blocked_warmup += 1
                retry_after = compute_retry_after('warmup_limit_exceeded')
                conn.execute(text("""
                    INSERT INTO sales_outreach_events (merchant_id, contact_email, blocked_reason, event_type)
                    VALUES (:mid, :email, 'warmup_limit_exceeded', 'blocked')
                """), {"mid": merchant_id, "email": contact_email})
                conn.execute(text("""
                    UPDATE merchant_opportunities SET retry_after = :retry_after WHERE id = :opp_id
                """), {"retry_after": retry_after, "opp_id": opp_id})
                # Leave status as pending/approved for tomorrow
                break

            # Use the intelligent MCP operator tools to draft and approve.
            try:
                from runtime.ops.operator_commands import (
                    draft_outreach_for_opportunity_command, 
                    approve_outreach_for_opportunity_command
                )
                
                # If not drafted yet, draft it
                if not draft or status == 'pending_review':
                    logger.info(f"Generating intelligent draft for {opp_id} ({domain})...")
                    res = draft_outreach_for_opportunity_command(opportunity_id=opp_id)
                    if res.get("error"):
                        logger.error(f"Failed drafting for {domain}: {res['error']}")
                        continue
                        
                # Approve via the autonomous critic (which automatically triggers rewrite sharpening)
                logger.info(f"Submitting {opp_id} ({domain}) for Critic approval...")
                res = approve_outreach_for_opportunity_command(opportunity_id=opp_id)
                if res.get("error"):
                    logger.error(f"Failed approving for {domain}: {res['error']}")
                    continue
                    
                drafts_created += 1
                
                # Log success
                save_event("autonomous_outreach_approved", {
                    "merchant_domain": domain,
                    "email": contact_email,
                    "opportunity_id": opp_id
                })
                
            except Exception as e:
                logger.error(f"Error in intelligent outreach pipeline for {domain}: {e}")
                continue
                    
        conn.commit()
    
    if outreach_sent_count > 0:
        send_operator_alert(f"🚀 Autonomous Sales dispatched {outreach_sent_count} live emails ({sent_today}/{daily_limit} limit).")
    elif drafts_created > 0:
        logger.info(f"DRY RUN: Generated {drafts_created} conversion message drafts natively (Force: {force_run}).")
        
    logger.info(f"Autonomous cycle finalized. Lives: {outreach_sent_count} | Drafts: {drafts_created} | Blocked Legit: {blocked_legitimacy} | Blocked Warmup: {blocked_warmup} | Blocked Dupe: {blocked_duplicate} | Blocked Low Distress: {blocked_low_distress}")
    return {
        "emails_sent": outreach_sent_count,
        "drafts_created": drafts_created,
        "blocked_legitimacy": blocked_legitimacy,
        "blocked_warmup": blocked_warmup,
        "blocked_duplicate": blocked_duplicate,
        "blocked_low_distress": blocked_low_distress
    }

if __name__ == "__main__":
    force = len(sys.argv) > 1 and sys.argv[1] == "--force"
    print(run_autonomous_sales_cycle(force_run=force))
