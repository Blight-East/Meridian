"""
Autonomous Deal-Sourcing Engine
Detects distressed merchants, generates sales strategies,
prepares outreach drafts, creates checkout links, and logs opportunities.
"""
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import requests
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OUTREACH,
    evaluate_opportunity_queue_quality,
)

logger = get_logger("deal_sourcing")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_OPPORTUNITIES_PER_CYCLE = 10
MIN_CONTACT_CONFIDENCE = 0.85


def run_deal_sourcing_cycle():
    """
    Main deal-sourcing loop:
    1. Find high-risk clusters
    2. Get merchant profiles
    3. Generate sales strategies
    4. Discover contacts
    5. Draft outreach
    6. Generate checkout links
    7. Store opportunities
    """
    logger.info("Starting deal-sourcing cycle...")
    opportunities_created = 0

    with engine.connect() as conn:
        # Step 1: Find distressed clusters with linked merchants
        clusters = conn.execute(text("""
            SELECT DISTINCT c.cluster_topic, c.cluster_size, m.id as merchant_id,
                   m.canonical_name, m.domain, m.industry, m.distress_score, m.domain_confidence,
                   CASE WHEN m.domain_confidence = 'confirmed' THEN 1 ELSE 2 END as domain_sort_order
            FROM clusters c
            JOIN signals s ON s.detected_at >= NOW() - INTERVAL '48 hours'
            JOIN merchants m ON s.merchant_id = m.id
            WHERE c.cluster_size >= 3
              AND m.domain IS NOT NULL
              AND m.id NOT IN (
                  SELECT merchant_id FROM merchant_opportunities
                  WHERE merchant_id IS NOT NULL
                  AND created_at >= NOW() - INTERVAL '7 days'
              )
            ORDER BY 
              domain_sort_order ASC,
              c.cluster_size DESC, 
              m.distress_score DESC
            LIMIT :limit
        """), {"limit": MAX_OPPORTUNITIES_PER_CYCLE}).fetchall()

        if not clusters:
            logger.info("No new deal-sourcing candidates found")
            return {"opportunities_created": 0}

        for row in clusters:
            if opportunities_created >= MAX_OPPORTUNITIES_PER_CYCLE:
                break

            cluster_topic = row[0]
            cluster_size = row[1]
            merchant_id = row[2]
            merchant_name = row[3] or "Unknown Merchant"
            merchant_domain = row[4] or ""
            industry = row[5] or "unknown"
            raw_distress = row[6] or 0
            domain_conf = row[7] or "confirmed"

            # Apply distress scoring formula:
            # Score = (cluster_size * 5) + revenue_signal_score + processor_risk_weight
            # We approximate the second half using raw_distress and the context signals inside the loop.
            revenue_score = 0
            processor_weight = 0

            # Calculate preliminary formula (updated in the processor phase below):
            computed_distress = (cluster_size * 5) + raw_distress
            
            # We will fully calculate it and filter >= 15 inside the _process_merchant_opportunity
            
            try:
                opp = _process_merchant_opportunity(
                    conn, merchant_id, merchant_name, merchant_domain,
                    industry, cluster_topic, cluster_size, computed_distress
                )
                if opp:
                    opportunities_created += 1
            except Exception as e:
                logger.warning(f"Failed to process opportunity for {merchant_name}: {e}")

        conn.commit()

    save_event("deal_sourcing_cycle", {"opportunities_created": opportunities_created})
    logger.info(f"Deal-sourcing cycle complete: {opportunities_created} opportunities created")
    return {"opportunities_created": opportunities_created}


def _process_merchant_opportunity(conn, merchant_id, merchant_name, domain, industry, topic, cluster_size, preliminary_score):
    """Process a single merchant into a deal opportunity."""

    # Step 2: Determine processor from signals
    proc_row = conn.execute(text("""
        SELECT q.processor, q.revenue_detected FROM qualified_leads q
        JOIN signals s ON q.signal_id = s.id
        WHERE s.merchant_id = :mid AND q.processor IS NOT NULL AND q.processor != 'unknown'
        ORDER BY q.created_at DESC LIMIT 1
    """), {"mid": merchant_id}).fetchone()
    
    processor = proc_row[0] if proc_row else "unknown"
    rev = proc_row[1] if proc_row else None
    
    # Calculate final distress score
    rev_score = 5 if rev else 0
    proc_weight = 5 if processor in ['stripe', 'paypal', 'square'] else 0
    calculated_score = preliminary_score + rev_score + proc_weight

    if calculated_score < 15:
        return None

    # Gather distress signals
    distress_rows = conn.execute(text("""
        SELECT content FROM signals
        WHERE merchant_id = :mid
        ORDER BY detected_at DESC LIMIT 5
    """), {"mid": merchant_id}).fetchall()
    distress_signals = [r[0][:200] for r in distress_rows if r[0]]

    if not distress_signals:
        return None

    merchant_row = conn.execute(
        text(
            """
            SELECT canonical_name, domain, normalized_domain, domain_confidence, confidence_score
            FROM merchants
            WHERE id = :mid
            LIMIT 1
            """
        ),
        {"mid": merchant_id},
    ).mappings().first()
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_id": merchant_id,
            "merchant_domain": domain,
            "merchant_name": merchant_name,
            "processor": processor,
            "distress_topic": topic,
        },
        merchant=dict(merchant_row or {}),
        signal={"content": distress_signals[0]},
    )
    if quality["eligibility_class"] != ELIGIBILITY_OUTREACH:
        save_event(
            "merchant_opportunity_suppressed",
            {
                "merchant_id": merchant_id,
                "merchant_domain": domain,
                "processor": processor,
                "distress_topic": topic,
                "eligibility_class": quality["eligibility_class"],
                "eligibility_reason": quality["eligibility_reason"],
                "quality_score": quality["quality_score"],
            },
        )
        logger.info(
            f"Suppressed opportunity for {merchant_name} ({domain}) "
            f"as {quality['eligibility_class']}: {quality['eligibility_reason']}"
        )
        return None
    if not bool(quality.get("high_conviction_prospect")) or int(quality.get("quality_score") or 0) < 65:
        save_event(
            "merchant_opportunity_suppressed",
            {
                "merchant_id": merchant_id,
                "merchant_domain": domain,
                "processor": processor,
                "distress_topic": topic,
                "eligibility_class": "review_only",
                "eligibility_reason": "merchant opportunity is not yet a high-conviction prospect with strong enough queue quality",
                "quality_score": quality["quality_score"],
            },
        )
        return None
    canonical_distress_topic = (
        quality.get("parsed_distress_type")
        if quality.get("parsed_distress_type") not in {None, "", "unknown"}
        else topic
    )

    # Step 3: Generate sales strategy
    sales_strategy = None
    try:
        from runtime.ops.sales_reasoning import generate_sales_strategy
        sales_strategy = generate_sales_strategy(domain, processor, distress_signals, industry)
    except Exception as e:
        logger.warning(f"Sales strategy generation failed for {domain}: {e}")
        sales_strategy = {
            "pain_point": f"{processor} payout issues",
            "pitch_angle": "Earlier visibility into processor pressure and what to do next",
            "recommended_message": f"We help merchants facing {processor} disruptions see what changed and what to do next before cash flow gets squeezed.",
            "objection_handling": []
        }

    # Step 4: Check for contacts with sufficient confidence
    contact_row = conn.execute(text("""
        SELECT email, contact_name, confidence FROM merchant_contacts
        WHERE merchant_id = :mid AND confidence >= :min_conf
        ORDER BY confidence DESC LIMIT 1
    """), {"mid": merchant_id, "min_conf": MIN_CONTACT_CONFIDENCE}).fetchone()

    contact_email = contact_row[0] if contact_row else None
    contact_name = contact_row[1] if contact_row else merchant_name
    if not contact_email:
        save_event(
            "merchant_opportunity_suppressed",
            {
                "merchant_id": merchant_id,
                "merchant_domain": domain,
                "processor": processor,
                "distress_topic": canonical_distress_topic,
                "eligibility_class": "no_reachable_contact",
                "eligibility_reason": "No reachable verified merchant contact exists yet, so this is not real pipeline.",
                "quality_score": quality["quality_score"],
            },
        )
        logger.info(f"Suppressed opportunity for {merchant_name} ({domain}) due to no reachable verified contact")
        return None

    # Step 5: Draft outreach
    from outreach_generator import generate_outreach
    outreach_draft = generate_outreach(
        merchant_name=contact_name or merchant_name,
        processor=processor,
        domain=domain,
        topic=topic,
        checkout_url="{{checkout_url}}"  # Placeholder, replaced after link generation
    )

    # Step 6: Generate checkout link
    checkout_url = None
    try:
        checkout_resp = requests.post(
            "http://127.0.0.1:8000/create_checkout_session",
            json={"email": contact_email} if contact_email else {},
            timeout=10
        )
        if checkout_resp.status_code == 200:
            checkout_data = checkout_resp.json()
            checkout_url = checkout_data.get("checkout_url", "")
            outreach_draft = outreach_draft.replace("{{checkout_url}}", checkout_url)
    except Exception as e:
        logger.warning(f"Checkout link generation failed: {e}")
        outreach_draft = outreach_draft.replace("{{checkout_url}}", "https://payflux.dev")

    # Step 7: Store opportunity
    conn.execute(text("""
        INSERT INTO merchant_opportunities
            (merchant_id, merchant_domain, processor, distress_topic,
             sales_strategy, outreach_draft, checkout_url, status)
        VALUES (:mid, :domain, :proc, :topic, :strategy, :draft, :url, 'pending_review')
    """), {
        "mid": merchant_id,
        "domain": domain,
        "proc": processor,
        "topic": canonical_distress_topic,
        "strategy": json.dumps(sales_strategy) if isinstance(sales_strategy, dict) else str(sales_strategy),
        "draft": outreach_draft,
        "url": checkout_url or "",
    })

    save_event("opportunity_generated", {
        "merchant_id": merchant_id,
        "merchant_domain": domain,
        "processor": processor,
        "topic": canonical_distress_topic,
        "has_contact": contact_email is not None,
    })

    logger.info(f"Opportunity created for {merchant_name} ({domain}) — {canonical_distress_topic}")
    return True


# ---------------------------------------------------------------------------
# Opportunity-to-Pipeline Bridge
# ---------------------------------------------------------------------------
# The `opportunities` table (populated by opportunity_extractor from signals)
# and `merchant_opportunities` (consumed by the outreach pipeline) were never
# connected.  This function bridges the gap: it reads scored opportunities,
# joins through signals → merchants → merchant_contacts, and promotes those
# with a verified contact and identifiable distress into merchant_opportunities
# so the auto-send monitor can draft and send outreach autonomously.
# ---------------------------------------------------------------------------

BRIDGE_BATCH_SIZE = 20
BRIDGE_MIN_OPPORTUNITY_SCORE = 50
BRIDGE_MIN_CONTACT_CONFIDENCE = 0.85

_BRIDGE_DISTRESS_PATTERNS = {
    "account_frozen": (
        "account frozen", "accounts frozen", "froze", "frozen account",
        "payments disabled", "processing disabled", "funds frozen", "funds held",
    ),
    "payouts_delayed": (
        "payout delay", "payouts delayed", "payout paused", "payouts paused",
        "delayed payout",
    ),
    "reserve_hold": (
        "reserve", "funds held", "funds on hold", "rolling reserve",
    ),
    "verification_review": (
        "under review", "kyc", "kyb", "compliance review",
        "verification review", "website verification",
    ),
    "chargeback_issue": (
        "chargeback", "dispute spike", "negative balance",
    ),
    "processor_switch_intent": (
        "new processor", "processor alternative", "alternative to stripe",
        "alternative to paypal", "need a processor",
    ),
    "account_terminated": (
        "account terminated", "account closed", "terminated account",
        "stopped accepting",
    ),
    "onboarding_rejected": (
        "onboarding rejected", "application denied", "application rejected",
    ),
}

_BRIDGE_PROCESSOR_PATTERNS = {
    "stripe": ("stripe",),
    "paypal": ("paypal",),
    "square": ("square", "squareup"),
    "braintree": ("braintree",),
    "adyen": ("adyen",),
}


def _infer_distress_from_content(content):
    lowered = (content or "").lower()
    for distress_type, keywords in _BRIDGE_DISTRESS_PATTERNS.items():
        if any(kw in lowered for kw in keywords):
            return distress_type
    return None


def _infer_processor_from_content(content):
    lowered = (content or "").lower()
    for processor, keywords in _BRIDGE_PROCESSOR_PATTERNS.items():
        if any(kw in lowered for kw in keywords):
            return processor
    return "unknown"


def promote_opportunities_to_pipeline(limit=BRIDGE_BATCH_SIZE):
    """
    Bridge scored opportunities into the outreach pipeline.

    Reads from `opportunities` (populated by opportunity_extractor), joins
    through signals → merchants → merchant_contacts, and creates
    merchant_opportunities rows for entries that have:
      1. A merchant with a confirmed domain
      2. At least one verified contact above confidence threshold
      3. Identifiable distress from signal content

    Returns dict with promotion stats.
    """
    promoted = 0
    skipped_no_merchant = 0
    skipped_no_domain = 0
    skipped_no_contact = 0
    skipped_no_distress = 0
    skipped_already_exists = 0

    with engine.connect() as conn:
        # Find opportunities not yet in merchant_opportunities
        candidates = conn.execute(text("""
            SELECT o.id AS opp_id, o.signal_id, o.opportunity_score,
                   s.content, s.source, s.merchant_id,
                   m.canonical_name, m.domain, m.normalized_domain,
                   m.industry, m.distress_score, m.domain_confidence,
                   m.confidence_score, m.status AS merchant_status
            FROM opportunities o
            JOIN signals s ON o.signal_id = s.id
            LEFT JOIN merchants m ON s.merchant_id = m.id
            WHERE o.opportunity_score >= :min_score
              AND s.merchant_id IS NOT NULL
              AND m.domain IS NOT NULL
              AND m.domain != ''
              AND COALESCE(m.status, 'active') NOT IN ('provisional', 'rejected')
              AND m.id NOT IN (
                  SELECT merchant_id FROM merchant_opportunities
                  WHERE merchant_id IS NOT NULL
                    AND created_at >= NOW() - INTERVAL '14 days'
              )
            ORDER BY o.opportunity_score DESC
            LIMIT :limit
        """), {"min_score": BRIDGE_MIN_OPPORTUNITY_SCORE, "limit": limit}).fetchall()

        for row in candidates:
            opp_id = row[0]
            signal_id = row[1]
            opp_score = row[2]
            content = row[3] or ""
            merchant_id = row[5]
            merchant_name = row[6] or "Unknown"
            domain = row[7] or ""
            industry = row[9] or "unknown"
            merchant_status = row[13]

            if not merchant_id:
                skipped_no_merchant += 1
                continue
            if not domain:
                skipped_no_domain += 1
                continue

            # Check for verified contact
            contact = conn.execute(text("""
                SELECT email, contact_name, confidence
                FROM merchant_contacts
                WHERE merchant_id = :mid
                  AND confidence >= :min_conf
                  AND email IS NOT NULL
                  AND email != ''
                ORDER BY confidence DESC
                LIMIT 1
            """), {"mid": merchant_id, "min_conf": BRIDGE_MIN_CONTACT_CONFIDENCE}).fetchone()

            if not contact:
                skipped_no_contact += 1
                continue

            # Infer distress type from signal content
            distress_type = _infer_distress_from_content(content)
            if not distress_type:
                skipped_no_distress += 1
                continue

            # Infer processor
            processor = _infer_processor_from_content(content)

            # Build sales strategy from signal context
            sales_strategy = json.dumps({
                "pain_point": f"{processor} {distress_type.replace('_', ' ')} affecting operations",
                "pitch_angle": "Early visibility into processor pressure before cash flow gets squeezed",
                "source_signal": content[:200],
                "promoted_from_opportunity": opp_id,
            })

            # Insert into merchant_opportunities
            try:
                new_opp = conn.execute(text("""
                    INSERT INTO merchant_opportunities
                        (merchant_id, merchant_domain, processor, distress_topic,
                         sales_strategy, outreach_draft, checkout_url,
                         opportunity_score, status)
                    VALUES (:mid, :domain, :proc, :topic, :strategy, '', '',
                            :score, 'pending_review')
                    RETURNING id
                """), {
                    "mid": merchant_id,
                    "domain": domain,
                    "proc": processor,
                    "topic": distress_type,
                    "strategy": sales_strategy,
                    "score": opp_score,
                }).fetchone()

                if new_opp:
                    promoted += 1
                    save_event("opportunity_promoted_to_pipeline", {
                        "opportunity_id": opp_id,
                        "merchant_opportunity_id": new_opp[0],
                        "merchant_id": merchant_id,
                        "merchant_name": merchant_name,
                        "domain": domain,
                        "processor": processor,
                        "distress_type": distress_type,
                        "contact_email": contact[0],
                        "opportunity_score": opp_score,
                    })
                    logger.info(
                        f"Promoted opportunity {opp_id} -> merchant_opp {new_opp[0]}: "
                        f"{merchant_name} ({domain}) — {distress_type}"
                    )
            except Exception as exc:
                logger.warning(f"Failed to promote opportunity {opp_id} for {merchant_name}: {exc}")
                skipped_already_exists += 1

        conn.commit()

    result = {
        "candidates_checked": len(candidates),
        "promoted": promoted,
        "skipped_no_merchant": skipped_no_merchant,
        "skipped_no_domain": skipped_no_domain,
        "skipped_no_contact": skipped_no_contact,
        "skipped_no_distress": skipped_no_distress,
        "skipped_already_exists": skipped_already_exists,
    }
    save_event("opportunity_pipeline_bridge_run", result)
    logger.info(f"Opportunity pipeline bridge complete: {result}")
    return result
