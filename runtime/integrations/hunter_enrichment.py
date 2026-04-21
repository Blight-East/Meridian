"""
Hunter.io email enrichment integration for contact discovery.

Provides domain_search and email_finder as fallback contact sources
when site crawling fails to find merchant emails.
"""

import os
import logging
import requests

logger = logging.getLogger("agent_flux.hunter_enrichment")

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
HUNTER_BASE_URL = "https://api.hunter.io/v2"

# Map Hunter verification statuses to confidence scores
VERIFICATION_CONFIDENCE = {
    "valid": 0.92,
    "accept_all": 0.75,
    "webmail": 0.60,
    "unknown": 0.55,
    "invalid": 0.10,
}

# Map Hunter seniority to role hints
SENIORITY_ROLE_MAP = {
    "senior": "decision_maker",
    "executive": "executive",
    "junior": "staff",
    "": "unknown",
}


def _get_api_key():
    key = HUNTER_API_KEY or os.environ.get("HUNTER_API_KEY", "")
    if not key:
        logger.warning("HUNTER_API_KEY not configured; skipping Hunter enrichment")
    return key


def domain_search(domain, limit=10):
    """
    Search Hunter.io for all known email addresses at a domain.

    Returns list of candidate dicts compatible with contact_discovery's
    _persist_candidate format.
    """
    api_key = _get_api_key()
    if not api_key:
        return []

    # Strip myshopify.com subdomains — Hunter won't have results for these
    if domain.endswith(".myshopify.com"):
        logger.info(f"Hunter: skipping myshopify.com subdomain {domain}")
        return []

    try:
        resp = requests.get(
            f"{HUNTER_BASE_URL}/domain-search",
            params={
                "domain": domain,
                "api_key": api_key,
                "limit": limit,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
    except Exception as e:
        logger.error(f"Hunter domain_search failed for {domain}: {e}")
        return []

    candidates = []
    for email_entry in data.get("emails", []):
        email = email_entry.get("value", "").strip().lower()
        if not email or "@" not in email:
            continue

        verification = email_entry.get("verification", {})
        verification_status = verification.get("status", "unknown")
        confidence = VERIFICATION_CONFIDENCE.get(verification_status, 0.55)

        # Boost confidence for emails with names (more likely real people)
        first_name = email_entry.get("first_name", "") or ""
        last_name = email_entry.get("last_name", "") or ""
        contact_name = f"{first_name} {last_name}".strip()
        if contact_name:
            confidence = min(confidence + 0.05, 0.95)

        seniority = email_entry.get("seniority", "") or ""
        department = email_entry.get("department", "") or ""
        role_hint = SENIORITY_ROLE_MAP.get(seniority, "unknown")
        if department in ("executive", "management"):
            role_hint = "decision_maker"

        local_part, email_domain = email.split("@", 1)

        candidates.append({
            "email": email,
            "contact_name": contact_name,
            "source": "hunter_io:domain_search",
            "confidence": confidence,
            "email_verified": verification_status == "valid",
            "local_part": local_part,
            "email_domain": email_domain,
            "page_type": "hunter_io",
            "page_url": f"https://hunter.io/search/{domain}",
            "role_hint": role_hint,
            "same_domain": email_domain.lower() == domain.lower(),
            "explicit_on_site": False,
            "crawl_step": 0,
            "render_mode": None,
            "source_quality_tier": 2,  # Hunter is a trusted enrichment source
            "extraction_confidence": confidence,
        })

    logger.info(f"Hunter domain_search for {domain}: found {len(candidates)} candidates")
    return candidates


def email_finder(domain, first_name, last_name):
    """
    Find the most likely email for a specific person at a domain.

    Returns a single candidate dict or None.
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    if domain.endswith(".myshopify.com"):
        return None

    try:
        resp = requests.get(
            f"{HUNTER_BASE_URL}/email-finder",
            params={
                "domain": domain,
                "first_name": first_name,
                "last_name": last_name,
                "api_key": api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
    except Exception as e:
        logger.error(f"Hunter email_finder failed for {first_name} {last_name}@{domain}: {e}")
        return None

    email = data.get("email", "").strip().lower()
    if not email or "@" not in email:
        return None

    verification_status = data.get("verification", {}).get("status", "unknown")
    confidence = VERIFICATION_CONFIDENCE.get(verification_status, 0.55)
    local_part, email_domain = email.split("@", 1)

    return {
        "email": email,
        "contact_name": f"{first_name} {last_name}".strip(),
        "source": "hunter_io:email_finder",
        "confidence": confidence,
        "email_verified": verification_status == "valid",
        "local_part": local_part,
        "email_domain": email_domain,
        "page_type": "hunter_io",
        "page_url": f"https://hunter.io/search/{domain}",
        "role_hint": "unknown",
        "same_domain": email_domain.lower() == domain.lower(),
        "explicit_on_site": False,
        "crawl_step": 0,
        "render_mode": None,
        "source_quality_tier": 2,
        "extraction_confidence": confidence,
    }


def verify_email(email):
    """
    Verify a single email address via Hunter.io.

    Returns dict with 'status' (valid/invalid/accept_all/webmail/unknown)
    and 'score' (0-100).
    """
    api_key = _get_api_key()
    if not api_key:
        return {"status": "unknown", "score": 0}

    try:
        resp = requests.get(
            f"{HUNTER_BASE_URL}/email-verifier",
            params={
                "email": email,
                "api_key": api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        return {
            "status": data.get("status", "unknown"),
            "score": data.get("score", 0),
        }
    except Exception as e:
        logger.error(f"Hunter email verification failed for {email}: {e}")
        return {"status": "unknown", "score": 0}
