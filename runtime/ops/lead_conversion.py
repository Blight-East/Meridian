import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from runtime.ops.alerts import send_operator_alert
from memory.structured.db import save_event
from config.logging_config import get_logger
from entity_taxonomy import classify_entity, is_valid_lead
from merchant_identity import resolve_merchant_identity, get_merchant_profile
from contact_discovery import get_best_contact

logger = get_logger("lead_conversion")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
API = "http://localhost:8000"

COOLDOWN_DAYS = 30


def check_conversion_opportunities():
    """Main entry point: check individual leads then cluster-level opportunities."""
    _check_lead_opportunities()
    _check_cluster_opportunities()


def _check_lead_opportunities():
    """Generate conversion opportunities from individual qualified leads."""
    with engine.connect() as conn:
        leads = conn.execute(text("""
            SELECT ql.id, ql.qualification_score, ql.processor, ql.revenue_detected,
                   ql.merchant_name, ql.merchant_website, ql.industry, ql.location,
                   ql.signal_id
            FROM qualified_leads ql
            WHERE ql.qualification_score >= 70
              AND ql.revenue_detected = true
              AND ql.lifecycle_status = 'qualified'
              AND ql.id NOT IN (
                  SELECT co.lead_id FROM conversion_opportunities co
                  WHERE co.lead_id IS NOT NULL
                    AND (co.cooldown_until IS NULL OR co.cooldown_until > NOW())
              )
            ORDER BY ql.qualification_score DESC
            LIMIT 10
        """)).fetchall()

    if not leads:
        logger.info("No new conversion-ready leads found")
        return

    logger.info(f"Found {len(leads)} conversion-ready leads")

    # Fetch signal content for entity classification
    signal_ids = [lead[8] for lead in leads if lead[8]]
    signal_content_map = {}
    if signal_ids:
        with engine.connect() as conn:
            placeholders = ",".join(str(int(sid)) for sid in signal_ids)
            rows = conn.execute(text(f"""
                SELECT id, content FROM signals WHERE id IN ({placeholders})
            """)).fetchall()
            signal_content_map = {r[0]: r[1] for r in rows}

    filtered_count = 0
    for lead in leads:
        lead_id = lead[0]
        score = lead[1]
        processor = lead[2] or "unknown"
        merchant_name = lead[4] or "Unknown Merchant"
        merchant_website = lead[5] or ""
        industry = lead[6] or "unknown"
        location = lead[7] or "unknown"
        signal_id = lead[8]

        # ── Lead validity gate ──
        signal_text = signal_content_map.get(signal_id, "")
        entity_info = classify_entity(signal_text)
        if not is_valid_lead(entity_info["classification"], entity_info["entity_type"]):
            logger.info(
                f"Signal excluded — {entity_info['classification'] or entity_info['entity_type']} "
                f"(entity: {entity_info['entity_name']}, lead_id: {lead_id})"
            )
            filtered_count += 1
            continue

        _create_opportunity(
            lead_id=lead_id,
            merchant_name=merchant_name,
            processor=processor,
            industry=industry,
            score=score,
            source_type="lead",
            merchant_domain=merchant_website or None,
        )

    if filtered_count:
        logger.info(f"Lead validity gate filtered {filtered_count} consumer/non-merchant signals")


def _check_cluster_opportunities():
    """Generate conversion opportunities from high-severity clusters."""
    with engine.connect() as conn:
        clusters = conn.execute(text("""
            SELECT c.id, c.cluster_topic, c.cluster_size, c.signal_ids,
                   ca.risk_level,
                   ci.processor, ci.industry
            FROM clusters c
            JOIN cluster_analysis ca ON ca.cluster_id = CAST(c.id AS TEXT)
            LEFT JOIN cluster_intelligence ci ON ci.cluster_id = CAST(c.id AS TEXT)
            WHERE c.cluster_size >= 10
              AND ca.risk_level IN ('high', 'critical')
            ORDER BY c.cluster_size DESC
            LIMIT 5
        """)).fetchall()

    if not clusters:
        logger.info("No high-severity clusters found for conversion")
        return

    logger.info(f"Found {len(clusters)} high-severity clusters")

    for cluster in clusters:
        cluster_id = cluster[0]
        cluster_topic = cluster[1]
        cluster_size = cluster[2]
        signal_ids_raw = cluster[3] or ""
        risk_level = cluster[4]
        processor = cluster[5] or "unknown"
        industry = cluster[6] or "unknown"

        signal_ids = [s.strip() for s in signal_ids_raw.split(",") if s.strip()]
        if not signal_ids:
            continue

        with engine.connect() as conn:
            merchants = conn.execute(text("""
                SELECT DISTINCT s.merchant_name, s.content
                FROM signals s
                WHERE s.id IN :ids
                  AND s.merchant_name IS NOT NULL
                  AND s.merchant_name != ''
                  AND s.merchant_name NOT IN (
                      SELECT co.merchant_name FROM conversion_opportunities co
                      WHERE co.merchant_name IS NOT NULL
                        AND (co.cooldown_until IS NULL OR co.cooldown_until > NOW())
                  )
            """), {"ids": tuple(int(i) for i in signal_ids[:100])}).fetchall()

        if not merchants:
            logger.info(f"Cluster {cluster_id}: no new merchants to convert")
            continue

        logger.info(f"Cluster {cluster_id} ({cluster_topic}): {len(merchants)} merchants to convert")

        for merchant_row in merchants:
            merchant_name = merchant_row[0]
            merchant_content = merchant_row[1] or ""

            # ── Lead validity gate (cluster path) ──
            entity_info = classify_entity(merchant_content)
            if not is_valid_lead(entity_info["classification"], entity_info["entity_type"]):
                logger.info(
                    f"Cluster signal excluded — {entity_info['classification'] or entity_info['entity_type']} "
                    f"(entity: {entity_info['entity_name']}, cluster: {cluster_id})"
                )
                continue

            _create_opportunity(
                lead_id=None,
                merchant_name=merchant_name,
                processor=processor,
                industry=industry,
                score=None,
                source_type="cluster",
                cluster_id=cluster_id,
                cluster_topic=cluster_topic,
                risk_level=risk_level,
            )


def _create_opportunity(*, lead_id, merchant_name, processor, industry, score,
                        source_type, cluster_id=None, cluster_topic=None, risk_level=None,
                        merchant_domain=None):
    """Create a single conversion opportunity with checkout link and Telegram alert."""
    try:
        # Look up best contact from merchant_contacts
        contact_email = None
        contact_info = None
        if merchant_domain:
            with engine.connect() as conn:
                mid_row = conn.execute(text(
                    "SELECT id FROM merchants WHERE domain = :d OR normalized_domain = :d LIMIT 1"
                ), {"d": merchant_domain}).fetchone()
                if mid_row:
                    contact_info = get_best_contact(mid_row[0])
                    if contact_info:
                        contact_email = contact_info.get("email")

        payload = {}
        res = requests.post(f"{API}/create_checkout_session", json=payload, timeout=15)
        data = res.json()
        checkout_url = data.get("checkout_url")
        session_id = data.get("session_id")

        if not checkout_url:
            logger.error(f"No checkout URL returned for {merchant_name}")
            return

        if source_type == "cluster":
            outreach = _build_cluster_outreach(merchant_name, processor, industry,
                                               cluster_topic, risk_level, checkout_url)
        else:
            outreach = _build_outreach(merchant_name, processor, industry, checkout_url)

        cooldown = datetime.utcnow() + timedelta(days=COOLDOWN_DAYS)

        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO conversion_opportunities
                    (lead_id, merchant_name, merchant_email, checkout_url,
                     checkout_session_id, outreach_message, status, cooldown_until)
                VALUES (:lead_id, :name, :email, :url,
                        :sess_id, :msg, 'pending', :cooldown)
            """), {
                "lead_id": lead_id,
                "name": merchant_name,
                "email": contact_email or "",
                "url": checkout_url,
                "sess_id": session_id,
                "msg": outreach,
                "cooldown": cooldown,
            })
            conn.commit()

        if source_type == "cluster":
            alert = (
                f"🔴 Cluster Conversion Opportunity\n\n"
                f"Merchant: {merchant_name}\n"
                f"Cluster: {cluster_topic}\n"
                f"Risk Level: {risk_level}\n"
                f"Processor: {processor}\n"
            )
        else:
            alert = (
                f"New Conversion Opportunity\n\n"
                f"Merchant: {merchant_name}\n"
                f"Industry: {industry}\n"
                f"Score: {score}/100\n"
                f"Processor: {processor}\n"
            )

        # Append contact details if discovered
        if contact_email:
            alert += f"Contact: {contact_email}\n"
        if contact_info and contact_info.get("linkedin_url"):
            alert += f"LinkedIn: {contact_info['linkedin_url']}\n"

        alert += (
            f"\nCheckout: {checkout_url}\n\n"
            f"Outreach draft ready for review."
        )
        send_operator_alert(alert)

        save_event("conversion_opportunity_created", {
            "lead_id": lead_id,
            "merchant_name": merchant_name,
            "score": score,
            "source_type": source_type,
            "cluster_id": cluster_id,
            "session_id": session_id,
        })

        logger.info(f"Conversion opportunity created for {merchant_name} (source: {source_type})")

    except Exception as e:
        logger.error(f"Failed to create opportunity for {merchant_name}: {e}")


def _build_outreach(merchant_name, processor, industry, checkout_url):
    return (
        f"Hi {merchant_name},\n\n"
        f"We've been seeing processor pressure across {industry} merchants using {processor}, "
        f"and your situation looks worth a closer read.\n\n"
        f"PayFlux is not the processor. It helps your team spot risk earlier, understand "
        f"what changed, and keep a clear record when payout or reserve pressure starts building.\n\n"
        f"Activate your Pro subscription here:\n{checkout_url}\n\n"
        f"PayFlux Team"
    )


def _build_cluster_outreach(merchant_name, processor, industry, cluster_topic,
                            risk_level, checkout_url):
    return (
        f"Hi {merchant_name},\n\n"
        f"We're reaching out because we picked up a {risk_level} signal around "
        f"{cluster_topic} affecting {processor} merchants in {industry}.\n\n"
        f"If that kind of pressure is starting to build, PayFlux helps your team see it earlier, "
        f"understand what changed, and keep a clear record. It is not the processor itself.\n\n"
        f"Activate your Pro subscription here:\n{checkout_url}\n\n"
        f"PayFlux Team"
    )
