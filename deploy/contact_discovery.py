"""
Automated Merchant Contact Discovery
Crawls merchant websites for emails and LinkedIn profiles,
scores contacts by confidence, and stores them in merchant_contacts.
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

import re
import time
import requests
from urllib.parse import urljoin, urlparse
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
try:
    from runtime.intelligence.domain_utils import generate_email_patterns, validate_email_domain, verify_email_smtp, get_mx_servers
except ImportError:
    from intelligence.domain_utils import generate_email_patterns, validate_email_domain, verify_email_smtp, get_mx_servers

logger = get_logger("contact_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

# ── Safety limits ──────────────────────────────────────────────────
MAX_PAGES_PER_MERCHANT = 5
PAGE_TIMEOUT = 5
CRAWL_DELAY = 1.0  # seconds between requests to same domain
BATCH_SIZE = 20     # merchants per scheduler run

# ── Contact pages to crawl ────────────────────────────────────────
CONTACT_PATHS = ["/contact", "/about", "/team", "/about-us", "/contact-us"]

# ── Email extraction ──────────────────────────────────────────────
_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Emails to ignore (tracking pixels, automated systems)
_IGNORE_EMAIL_PATTERNS = {
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "mailer-daemon", "postmaster", "webmaster",
    "wixpress.com", "sentry.io", "cloudflare.com",
    "googleapis.com", "google.com", "facebook.com",
    "example.com", "test.com", "localhost", "sentry@", "noreply@",
}

# ── LinkedIn extraction ───────────────────────────────────────────
_LINKEDIN_PATTERN = re.compile(
    r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[a-zA-Z0-9\-_%]+/?',
    re.IGNORECASE
)

# ── Confidence scoring ────────────────────────────────────────────
_HIGH_VALUE_PREFIXES = {"ceo", "founder", "owner", "cofounder", "co-founder", "director", "president", "team"}
_PERSONAL_INDICATORS = re.compile(r'^[a-z]+(\.[a-z]+)?@', re.IGNORECASE)
_GENERIC_PREFIXES = {"info", "support", "hello", "contact", "sales", "help", "admin", "team"}
_NOREPLY_PREFIXES = {"noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon", "sentry"}

# ── User agent ────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "PayFlux-ContactBot/1.0 (contact discovery; respect robots.txt)",
    "Accept": "text/html,application/xhtml+xml",
}


def _build_base_url(domain):
    if domain.startswith("etsy.com/shop/"):
        return f"https://www.{domain}"
    return f"https://{domain}"


def _supports_pattern_enumeration(domain):
    return not (
        "/" in domain
        or domain.endswith(".myshopify.com")
        or domain.endswith(".substack.com")
        or domain.endswith(".kajabi.com")
        or domain.endswith(".mybigcommerce.com")
    )


def extract_emails_from_html(html):
    """
    Extract email addresses from HTML content.
    Filters out tracking/system emails and deduplicates.
    """
    if not html:
        return []

    emails = _EMAIL_PATTERN.findall(html)
    cleaned = []
    for email in emails:
        email_lower = email.lower()
        # Skip ignored patterns
        if any(pat in email_lower for pat in _IGNORE_EMAIL_PATTERNS):
            continue
        # Skip very long emails (probably not real)
        if len(email) > 60:
            continue
        # Skip emails with encoded characters
        if "%" in email or "&" in email:
            continue
        cleaned.append(email_lower)

    return list(dict.fromkeys(cleaned))


def extract_linkedin_from_html(html):
    """
    Extract LinkedIn profile and company URLs from HTML.
    """
    if not html:
        return []

    urls = _LINKEDIN_PATTERN.findall(html)
    # Dedupe and clean
    cleaned = []
    for url in urls:
        url = url.rstrip("/")
        if url not in cleaned:
            cleaned.append(url)

    return cleaned


def score_contact(email=None, linkedin_url=None):
    """
    Score a contact by confidence (0.0 - 1.0).
    Higher scores = more likely to reach a decision maker.
    """
    if email:
        prefix = email.split("@")[0].lower()

        if prefix in _NOREPLY_PREFIXES:
            return 0.1

        if prefix in _HIGH_VALUE_PREFIXES:
            return 0.8

        if prefix in _GENERIC_PREFIXES:
            return 0.3

        # Personal email pattern (firstname or firstname.lastname)
        if _PERSONAL_INDICATORS.match(email):
            return 0.7

        return 0.5

    if linkedin_url:
        url_lower = linkedin_url.lower()
        if "/in/" in url_lower:
            return 0.8  # Personal profile
        if "/company/" in url_lower:
            return 0.5  # Company page

    return 0.3


def _fetch_page(url):
    """Fetch a page with timeout and error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
            return resp.text
    except Exception:
        pass
    return None


def discover_contacts_for_merchant(merchant_id):
    """
    Main entry point: discover contacts for a merchant by crawling their domain.
    Returns number of contacts discovered.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT domain, normalized_domain, canonical_name FROM merchants WHERE id = :mid
        """), {"mid": merchant_id}).fetchone()

    if not row or not row[0]:
        return 0

    domain = row[0]
    merchant_name = row[2] or domain

    base_url = _build_base_url(domain)

    # Check if we already have contacts
    with engine.connect() as conn:
        existing = conn.execute(text(
            "SELECT COUNT(*) FROM merchant_contacts WHERE merchant_id = :mid"
        ), {"mid": merchant_id}).scalar()
        if existing > 0:
            logger.debug(f"Merchant {merchant_name} already has {existing} contacts, skipping")
            return 0

    # Build URLs to crawl
    urls_to_crawl = [base_url]
    for path in CONTACT_PATHS:
        urls_to_crawl.append(urljoin(base_url, path))

    # Crawl pages and collect contacts
    all_emails = []
    all_linkedin = []
    pages_crawled = 0

    for url in urls_to_crawl:
        if pages_crawled >= MAX_PAGES_PER_MERCHANT:
            break

        html = _fetch_page(url)
        pages_crawled += 1

        if html:
            emails = extract_emails_from_html(html)
            linkedin = extract_linkedin_from_html(html)
            all_emails.extend(emails)
            all_linkedin.extend(linkedin)

        time.sleep(CRAWL_DELAY)

    # Deduplicate
    all_emails = list(dict.fromkeys(all_emails))
    all_linkedin = list(dict.fromkeys(all_linkedin))

    email_records = []
    for email in all_emails:
        email_records.append({
            "email": email,
            "source": f"website:{domain}",
            "conf": score_contact(email=email)
        })
        
    if not email_records:
        # Fallback to pattern enumeration
        if _supports_pattern_enumeration(domain) and validate_email_domain(domain):
            logger.info(f"0 scraped emails for {merchant_name}. Attempting Pattern Enumeration on {domain}")
            candidate_emails = generate_email_patterns(domain)
            mx_servers = get_mx_servers(domain)
            
            for email in candidate_emails:
                is_valid = verify_email_smtp(email, mx_servers)
                conf = 0.8 if is_valid else 0.5
                email_records.append({
                    "email": email,
                    "source": "pattern_enumeration",
                    "conf": conf
                })

    if not email_records and not all_linkedin:
        logger.info(f"No contacts found for {merchant_name} ({domain})")
        return 0

    # Score and store contacts
    contacts_stored = 0
    with engine.connect() as conn:
        for record in email_records:
            email = record["email"]
            confidence = record["conf"]
            source = record["source"]
            # Derive contact name from email prefix
            prefix = email.split("@")[0]
            contact_name = prefix.replace(".", " ").replace("-", " ").replace("_", " ").title()
            if prefix in _GENERIC_PREFIXES or prefix in _NOREPLY_PREFIXES:
                contact_name = None

            conn.execute(text("""
                INSERT INTO merchant_contacts (merchant_id, contact_name, email, source, confidence)
                VALUES (:mid, :name, :email, :source, :conf)
            """), {
                "mid": merchant_id,
                "name": contact_name,
                "email": email,
                "source": source,
                "conf": confidence,
            })
            contacts_stored += 1

        for url in all_linkedin:
            confidence = score_contact(linkedin_url=url)
            # Derive name from LinkedIn URL
            slug = url.split("/")[-1]
            contact_name = slug.replace("-", " ").title() if "/in/" in url else None

            conn.execute(text("""
                INSERT INTO merchant_contacts (merchant_id, contact_name, linkedin_url, source, confidence)
                VALUES (:mid, :name, :url, :source, :conf)
            """), {
                "mid": merchant_id,
                "name": contact_name,
                "url": url,
                "source": f"website:{domain}",
                "conf": confidence,
            })
            contacts_stored += 1

        conn.commit()

    logger.info(
        f"Discovered {contacts_stored} contacts for {merchant_name} "
        f"({len(all_emails)} emails, {len(all_linkedin)} linkedin)"
    )
    return contacts_stored


def get_best_contact(merchant_id):
    """
    Get the highest-confidence contact for a merchant.
    Returns dict with email/linkedin or None.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT contact_name, email, linkedin_url, confidence
            FROM merchant_contacts
            WHERE merchant_id = :mid
            ORDER BY confidence DESC
            LIMIT 1
        """), {"mid": merchant_id}).fetchone()

    if not row:
        return None

    return {
        "contact_name": row[0],
        "email": row[1],
        "linkedin_url": row[2],
        "confidence": row[3],
    }


def get_all_contacts(merchant_id):
    """Get all contacts for a merchant, ordered by confidence."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, contact_name, email, linkedin_url, source, confidence, created_at
            FROM merchant_contacts
            WHERE merchant_id = :mid
            ORDER BY confidence DESC
        """), {"mid": merchant_id}).fetchall()

    return [{
        "id": r[0],
        "contact_name": r[1],
        "email": r[2],
        "linkedin_url": r[3],
        "source": r[4],
        "confidence": r[5],
        "created_at": r[6].isoformat() if r[6] else None,
    } for r in rows]


def run_contact_discovery():
    """
    Scheduler entry point: discover contacts for merchants that have domains
    but no contacts yet. Processes up to BATCH_SIZE merchants per run.
    """
    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT m.id, m.canonical_name, m.domain
            FROM merchants m
            WHERE m.domain IS NOT NULL AND m.domain != ''
              AND m.id NOT IN (
                  SELECT DISTINCT merchant_id FROM merchant_contacts
              )
            ORDER BY m.distress_score DESC
            LIMIT :limit
        """), {"limit": BATCH_SIZE}).fetchall()

    if not merchants:
        logger.info("No merchants pending contact discovery")
        return {"merchants_processed": 0, "contacts_found": 0}

    total_contacts = 0
    failures = 0

    for merchant in merchants:
        mid, name, domain = merchant[0], merchant[1], merchant[2]
        try:
            found = discover_contacts_for_merchant(mid)
            total_contacts += found
        except Exception as e:
            logger.error(f"Contact discovery failed for {name} ({domain}): {e}")
            failures += 1

    result = {
        "merchants_processed": len(merchants),
        "contacts_found": total_contacts,
        "failures": failures,
    }
    logger.info(f"Contact discovery run: {result}")
    return result
