"""
Lead Conversation Sales Assistant

Generates personalized outreach drafts for qualified merchant leads.
NEVER auto-sends or impersonates a human — all messages are drafts for operator review.

Integrates with the chat engine as operator tools.
"""

import os
import json
import requests
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine, text
from runtime.ops.alerts import send_operator_alert
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("sales_assistant")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

ANTHROPIC_API_KEY = None
HIGH_URGENCY_KEYWORDS = (
    "payout frozen",
    "account closed",
    "funds held",
    "payment processor shutdown",
)


def _get_api_key():
    global ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        env_file_values = dotenv_values(ENV_FILE_PATH)
        ANTHROPIC_API_KEY = env_file_values.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    return ANTHROPIC_API_KEY


def generate_sales_message(lead_id):
    """
    Generate a personalized outreach draft for a qualified lead.

    Returns: dict with subject, body, checkout_url, merchant_name
    """
    logger.info(f"Generating sales message for lead {lead_id}")

    # Fetch lead context
    lead = _get_lead_context(lead_id)
    if not lead:
        return {"error": f"Lead {lead_id} not found"}

    # Check for existing draft (avoid duplicates)
    existing = _get_latest_draft(lead_id)
    if existing:
        logger.info(f"Existing draft found for lead {lead_id}")
        return {
            "subject": existing.get("subject", ""),
            "body": existing.get("body", ""),
            "checkout_url": lead.get("checkout_url", ""),
            "merchant_name": lead["merchant_name"],
            "note": "Returning existing draft. Use /new_draft to generate fresh.",
        }

    # Generate checkout link if not already present
    checkout_url = lead.get("checkout_url", "")
    session_id = None
    if not checkout_url:
        try:
            res = requests.post("http://localhost:8000/create_checkout_session", json={}, timeout=15)
            data = res.json()
            checkout_url = data.get("checkout_url", "")
            session_id = data.get("session_id", "")
        except Exception as e:
            logger.warning(f"Failed to generate checkout link: {e}")

    if _is_high_urgency_checkout_trigger(lead):
        draft = _high_urgency_outreach(lead, checkout_url)
    else:
        draft = _generate_with_claude(lead, checkout_url)
        if not draft:
            draft = _template_outreach(lead, checkout_url)

    # Store the draft in lead_conversations
    _store_conversation(
        lead_id=lead_id,
        merchant_name=lead["merchant_name"],
        message=json.dumps(draft),
        role="assistant",
        channel="draft",
    )

    save_event("sales_draft_generated", {
        "lead_id": lead_id,
        "merchant_name": lead["merchant_name"],
        "processor": lead.get("processor", "unknown"),
    })

    return {
        "subject": draft.get("subject", ""),
        "body": draft.get("body", ""),
        "checkout_url": checkout_url,
        "merchant_name": lead["merchant_name"],
        "lead_id": lead_id,
    }


def get_sales_context(lead_id):
    """
    Get full merchant intelligence context for operator review.

    Returns comprehensive context: lead data, signals, clusters, contacts, conversations.
    """
    lead = _get_lead_context(lead_id)
    if not lead:
        return {"error": f"Lead {lead_id} not found"}

    context = {
        "lead": lead,
        "conversations": _get_conversation_history(lead_id),
        "opportunity": _get_opportunity(lead_id),
    }

    return context


def list_pending_outreach():
    """List all conversion opportunities with pending status."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT co.id, co.lead_id, co.merchant_name, co.merchant_email,
                   co.checkout_url, co.status, co.created_at
            FROM conversion_opportunities co
            WHERE co.status = 'pending'
            ORDER BY co.created_at DESC
            LIMIT 20
        """)).fetchall()

    return [{
        "opportunity_id": r[0],
        "lead_id": r[1],
        "merchant_name": r[2],
        "merchant_email": r[3] or "not discovered",
        "checkout_url": r[4],
        "status": r[5],
        "created_at": r[6].isoformat() if r[6] else None,
    } for r in rows]


def approve_outreach(opportunity_id):
    """Mark an outreach opportunity as approved for sending."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE conversion_opportunities
            SET status = 'approved'
            WHERE id = :id AND status = 'pending'
            RETURNING merchant_name
        """), {"id": opportunity_id})
        row = result.fetchone()
        conn.commit()

    if row:
        merchant_name = row[0]
        save_event("outreach_approved", {
            "opportunity_id": opportunity_id,
            "merchant_name": merchant_name,
        })
        logger.info(f"Outreach approved for opportunity {opportunity_id} ({merchant_name})")
        return {"status": "approved", "merchant_name": merchant_name, "opportunity_id": opportunity_id}
    else:
        return {"error": f"Opportunity {opportunity_id} not found or not in pending status"}


# ── Internal helpers ──

def _get_lead_context(lead_id):
    """Fetch comprehensive lead data from the database."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT ql.id, ql.qualification_score, ql.processor, ql.revenue_detected,
                   ql.merchant_name, ql.merchant_website, ql.industry, ql.location,
                   ql.signal_id, ql.lifecycle_status,
                   s.content as signal_content, s.source as signal_source,
                   s.merchant_id,
                   m.distress_score,
                   m.domain
            FROM qualified_leads ql
            LEFT JOIN signals s ON s.id = ql.signal_id
            LEFT JOIN merchants m ON m.id = s.merchant_id
            WHERE ql.id = :lead_id
        """), {"lead_id": lead_id}).fetchone()

    if not row:
        return None

    lead = {
        "id": row[0],
        "qualification_score": row[1],
        "processor": row[2] or "unknown",
        "revenue_detected": bool(row[3]),
        "merchant_name": row[4] or "Unknown Merchant",
        "merchant_website": row[5] or "",
        "industry": row[6] or "unknown",
        "location": row[7] or "unknown",
        "signal_id": row[8],
        "lifecycle_status": row[9],
        "signal_content": row[10] or "",
        "signal_source": row[11] or "unknown",
        "merchant_id": row[12],
        "distress_score": row[13] or 0,
        "merchant_domain": row[14] or "",
    }

    # Get contact info if available
    if lead["merchant_website"]:
        with engine.connect() as conn:
            contact = conn.execute(text("""
                SELECT mc.email, mc.linkedin_url
                FROM merchant_contacts mc
                JOIN merchants m ON m.id = mc.merchant_id
                WHERE m.domain = :domain OR m.normalized_domain = :domain
                LIMIT 1
            """), {"domain": lead["merchant_website"]}).fetchone()
            if contact:
                lead["contact_email"] = contact[0]
                lead["contact_linkedin"] = contact[1]

    # Get existing opportunity
    with engine.connect() as conn:
        opp = conn.execute(text("""
            SELECT checkout_url, status FROM conversion_opportunities
            WHERE lead_id = :lid
            ORDER BY created_at DESC LIMIT 1
        """), {"lid": lead_id}).fetchone()
        if opp:
            lead["checkout_url"] = opp[0]
            lead["opportunity_status"] = opp[1]

    return lead


def _get_latest_draft(lead_id):
    """Check if there's a recent draft for this lead."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT message FROM lead_conversations
            WHERE lead_id = :lid AND role = 'assistant'
            ORDER BY created_at DESC LIMIT 1
        """), {"lid": lead_id}).fetchone()

    if row and row[0]:
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return None
    return None


def _generate_with_claude(lead, checkout_url):
    """Use Claude to generate a personalized outreach message."""
    api_key = _get_api_key()
    if not api_key:
        logger.warning("No API key for sales message generation")
        return None

    urgency_rule = (
        "5. Put the checkout URL above fold immediately after the empathy line.\n"
        "6. The subject should convey an urgent fix for payout disruption.\n"
    ) if _is_high_urgency_checkout_trigger(lead) else (
        "5. Include the checkout URL naturally at the end.\n"
        "6. Keep the subject calm and specific.\n"
    )

    prompt = f"""You are a sales intelligence agent for PayFlux, a payment risk infrastructure company.

Generate a personalized cold outreach email for a merchant who is experiencing payment processor distress.

MERCHANT CONTEXT:
- Name: {lead['merchant_name']}
- Processor: {lead['processor']}
- Industry: {lead['industry']}
- Qualification Score: {lead['qualification_score']}/100
- Revenue Detected: {lead['revenue_detected']}
- Distress Signal: {lead['signal_content'][:400]}
- Signal Source: {lead['signal_source']}
- Location: {lead['location']}
- Distress Score: {lead.get('distress_score', 0)}

CHECKOUT URL: {checkout_url}

RULES:
1. Be empathetic and professional — this merchant is going through a difficult situation
2. Reference their specific situation without being creepy
3. Position PayFlux as a tool for visibility and protection, not a sales pitch
4. Keep it concise — under 150 words for the body
{urgency_rule}7. Never claim to be a person — this is from "PayFlux Intelligence"

Return JSON only:
{{
    "subject": "Short, specific email subject line",
    "body": "The full email body text"
}}"""

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-3-haiku-20240307",
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()

        if "content" in result and result["content"]:
            import re
            raw = result["content"][0]["text"].strip()
            if raw.startswith("```"):
                raw = re.sub(r'^```(?:json)?\s*', '', raw)
                raw = re.sub(r'\s*```$', '', raw)
            return json.loads(raw)

    except Exception as e:
        logger.error(f"Claude sales message generation failed: {e}")

    return None


def _template_outreach(lead, checkout_url):
    """Fallback template-based outreach when Claude is unavailable."""
    return {
        "subject": f"Payment disruption alert — {lead['processor']} merchants in {lead['industry']}",
        "body": (
            f"Hi {lead['merchant_name']},\n\n"
            f"We detected signals indicating payment processor disruptions "
            f"affecting merchants in your sector ({lead['processor']}, {lead['industry']}).\n\n"
            f"PayFlux provides early detection and reserve projection tools "
            f"to help merchants anticipate and avoid revenue freezes.\n\n"
            f"You can activate PayFlux Pro here:\n{checkout_url}\n\n"
            f"— PayFlux Intelligence"
        ),
    }


def _high_urgency_outreach(lead, checkout_url):
    return {
        "subject": "Quick fix for your payout freeze",
        "body": (
            f"Hi {lead['merchant_name']},\n\n"
            f"I saw your post about payouts being frozen.\n\n"
            f"We built PayFlux specifically for merchants in this situation.\n\n"
            f"You can instantly route payments through our infrastructure here:\n\n"
            f"{checkout_url}\n\n"
            f"If you'd rather talk first, just reply.\n\n"
            f"— PayFlux"
        ),
    }


def _is_high_urgency_checkout_trigger(lead):
    if (lead.get("distress_score") or 0) >= 25:
        return True

    signal_text = (lead.get("signal_content") or "").lower()
    return any(keyword in signal_text for keyword in HIGH_URGENCY_KEYWORDS)


def _store_conversation(*, lead_id, merchant_name, message, role, channel="draft"):
    """Store a message in the lead_conversations table."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO lead_conversations (lead_id, merchant_name, message, role, channel)
            VALUES (:lid, :name, :msg, :role, :channel)
        """), {
            "lid": lead_id,
            "name": merchant_name,
            "msg": message,
            "role": role,
            "channel": channel,
        })
        conn.commit()


def _get_conversation_history(lead_id):
    """Fetch conversation history for a lead."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT role, message, channel, created_at
            FROM lead_conversations
            WHERE lead_id = :lid
            ORDER BY created_at ASC
        """), {"lid": lead_id}).fetchall()

    return [{
        "role": r[0],
        "message": r[1][:500],
        "channel": r[2],
        "created_at": r[3].isoformat() if r[3] else None,
    } for r in rows]


def _get_opportunity(lead_id):
    """Fetch the conversion opportunity for a lead."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, merchant_name, checkout_url, checkout_session_id, status, created_at
            FROM conversion_opportunities
            WHERE lead_id = :lid
            ORDER BY created_at DESC LIMIT 1
        """), {"lid": lead_id}).fetchone()

    if row:
        return {
            "opportunity_id": row[0],
            "merchant_name": row[1],
            "checkout_url": row[2],
            "session_id": row[3],
            "status": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
        }
    return None
