"""
Targeted merchant contact deepening.

This pass only runs for high-value outreach-blocked leads and only deepens
merchant-owned same-domain website contacts. It preserves operator-visible
attribution for page type, page URL, role hints, and deepening outcomes.
"""
import os
import re
import sys
import time
from html import unescape
from urllib.parse import unquote, urljoin, urlparse

import requests
import redis
from sqlalchemy import create_engine, text

# Dynamically find project root
_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.logging_config import get_logger
from memory.structured.db import save_event, save_learning_feedback

try:
    from intelligence.distress_normalization import normalize_distress_topic, operator_action_for_distress
    from intelligence.domain_utils import get_mx_servers, validate_email_domain, verify_email_smtp
    from intelligence.opportunity_queue_quality import (
        ELIGIBILITY_OPERATOR_REVIEW_ONLY,
        ELIGIBILITY_OUTREACH,
        evaluate_opportunity_queue_quality,
    )
except ImportError:
    from runtime.intelligence.distress_normalization import normalize_distress_topic, operator_action_for_distress
    from runtime.intelligence.domain_utils import get_mx_servers, validate_email_domain, verify_email_smtp
    from runtime.intelligence.opportunity_queue_quality import (
        ELIGIBILITY_OPERATOR_REVIEW_ONLY,
        ELIGIBILITY_OUTREACH,
        evaluate_opportunity_queue_quality,
    )

logger = get_logger("contact_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
_SCHEMA_READY = False

# Safety limits
MAX_DEEPENING_REQUESTS = 10
PAGE_TIMEOUT = 5
CRAWL_DELAY = 1.0
BATCH_SIZE = 20
HIGH_VALUE_OPPORTUNITY_SCORE = float(os.getenv("AGENT_FLUX_HIGH_VALUE_OPPORTUNITY_SCORE", "25.0"))
HIGH_VALUE_DISTRESS_SCORE = float(os.getenv("AGENT_FLUX_HIGH_VALUE_DISTRESS_SCORE", "8.0"))
HIGH_CONFIDENCE_CONTACT_THRESHOLD = float(os.getenv("AGENT_FLUX_HIGH_CONFIDENCE_CONTACT_THRESHOLD", "0.85"))
USABLE_FALLBACK_CONTACT_THRESHOLD = float(os.getenv("AGENT_FLUX_USABLE_CONTACT_THRESHOLD", "0.64"))
MAX_PERSISTED_CANDIDATES = 5
# Hard per-merchant deadline for a full discovery pass (site crawl + Hunter + fallback).
# Prevents one slow merchant from consuming the 300s scheduler task budget.
MERCHANT_DISCOVERY_DEADLINE_SECONDS = int(os.getenv("AGENT_FLUX_CONTACT_DISCOVERY_DEADLINE", "45"))
# Role-account prefixes used as a last-resort pattern fallback when no real
# contact can be discovered. Persisted at low confidence; email_verified=False.
ROLE_EMAIL_FALLBACK_PREFIXES = ("info", "support", "hello", "contact", "sales")
ROLE_EMAIL_FALLBACK_CONFIDENCE = float(os.getenv("AGENT_FLUX_ROLE_FALLBACK_CONFIDENCE", "0.30"))

TRUSTED_UPGRADE_METHODS = {
    "website:contact_page",
    "website:finance_page",
    "website:about_page",
    "website:team_page",
}

# Page priority
PAGE_TYPE_PRIORITY = {
    "contact_page": 1,
    "about_page": 2,
    "team_page": 2,
    "footer_page": 3,
    "legal_page": 3,
    "support_page": 4,
    "finance_page": 4,
    "payment_page": 4,
    "careers_page": 99,
}
PAGE_TYPE_SOURCE = {
    "contact_page": "website:contact_page",
    "about_page": "website:about_page",
    "team_page": "website:team_page",
    "footer_page": "website:footer_page",
    "legal_page": "website:legal_page",
    "support_page": "website:support_page",
    "finance_page": "website:finance_page",
    "payment_page": "website:payment_page",
}
PAGE_TYPE_FALLBACKS = {
    "contact_page": ("/contact", "/contact-us", "/pages/contact", "/pages/contact-us"),
    "about_page": ("/about", "/about-us", "/pages/about", "/pages/about-us"),
    "team_page": ("/team", "/leadership", "/our-team"),
    "legal_page": ("/legal", "/privacy", "/terms", "/imprint", "/policies/privacy-policy", "/policies/terms-of-service"),
    "support_page": ("/support", "/help", "/faq", "/customer-service"),
    "finance_page": ("/finance", "/billing", "/payouts", "/settlements"),
    "payment_page": ("/payment", "/payments", "/checkout", "/merchant-services"),
}
PAGE_TYPE_HINTS = {
    "contact_page": ("contact", "reach", "talk", "touch"),
    "about_page": ("about", "story", "company", "mission"),
    "team_page": ("team", "leadership", "staff", "founder", "management"),
    "legal_page": ("legal", "privacy", "terms", "imprint", "notice", "disclosure", "policy"),
    "support_page": ("support", "help", "faq", "service", "customer"),
    "finance_page": ("finance", "billing", "invoice", "payout", "settlement", "reserve"),
    "payment_page": ("payment", "payments", "checkout", "merchant"),
    "careers_page": ("career", "careers", "jobs", "hiring", "join", "work-with-us", "vacancies"),
}
PAGE_PRIORITY_ORDER = (
    "contact_page",
    "about_page",
    "team_page",
    "footer_page",
    "legal_page",
    "support_page",
    "finance_page",
    "payment_page",
)

# Extraction
_EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)
_LINKEDIN_PATTERN = re.compile(
    r"https?://(?:www\.)?linkedin\.com/(?:in|company)/[a-zA-Z0-9\-_%]+/?",
    re.IGNORECASE,
)
_ANCHOR_PATTERN = re.compile(
    r"<a\b[^>]*href=(['\"])(?P<href>.*?)\1[^>]*>(?P<label>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_MAILTO_PATTERN = re.compile(
    r"<a\b[^>]*href=(['\"])mailto:(?P<href>.*?)\1[^>]*>(?P<label>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)

# Contact quality
_HIGH_VALUE_PREFIXES = {
    "ceo",
    "founder",
    "owner",
    "cofounder",
    "co-founder",
    "director",
    "president",
    "finance",
    "payments",
    "ops",
    "operations",
    "risk",
    "compliance",
}
_PERSONAL_INDICATORS = re.compile(r"^[a-z]+(\.[a-z]+)?$", re.IGNORECASE)
_GENERIC_PREFIXES = {"info", "support", "hello", "contact", "sales", "help", "admin", "team", "billing"}
_NOREPLY_PREFIXES = {"noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon", "sentry"}
_FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}
_INTERNAL_CONTACT_DOMAINS = {"payflux.dev", "reach.payflux.dev"}
_BLOCKED_LOCAL_PARTS = {
    "admin",
    "billing",
    "example",
    "hr",
    "jobs",
    "legal",
    "mailer-daemon",
    "marketing",
    "media",
    "no-reply",
    "noreply",
    "office",
    "postmaster",
    "press",
    "test",
    "user",
}
_IGNORE_EMAIL_PATTERNS = {
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "webmaster",
    "wixpress.com",
    "sentry.io",
    "cloudflare.com",
    "googleapis.com",
    "google.com",
    "facebook.com",
    "example.com",
    "test.com",
    "localhost",
    "sentry@",
    "noreply@",
}
_PLAY_CONTACT_PREFIXES = {
    "urgent_processor_migration": ("finance", "payments", "operations", "founder"),
    "payout_acceleration": ("finance", "payments", "operations", "founder"),
    "reserve_negotiation": ("finance", "risk", "compliance", "operations"),
    "compliance_remediation": ("finance", "risk", "compliance", "operations"),
    "chargeback_mitigation": ("risk", "operations", "support"),
    "onboarding_assistance": ("founder", "operations", "finance"),
    "clarify_distress": ("finance", "operations"),
}
_HIGH_VALUE_DISTRESS_TOPICS = {
    "account_frozen",
    "payouts_delayed",
    "reserve_hold",
    "verification_review",
    "chargeback_issue",
}
_ROLE_KEYWORDS = {
    "finance": ("finance", "financial", "controller", "cfo", "accounting", "billing"),
    "payments": ("payment", "payments", "merchant services", "settlement", "payout"),
    "operations": ("operations", "ops", "operator", "fulfillment", "revops"),
    "founder": ("founder", "owner", "ceo", "co-founder", "cofounder", "president"),
    "risk": ("risk", "fraud", "chargeback", "dispute"),
    "compliance": ("compliance", "kyc", "aml", "underwriting", "verification"),
    "support": ("support", "customer success", "customer service", "help"),
}

# User agent
HEADERS = {
    "User-Agent": "PayFlux-ContactBot/1.0 (merchant contact deepening; respect robots.txt)",
    "Accept": "text/html,application/xhtml+xml",
}

VERIFY_BATCH_SIZE = 25
ROLE_ACCOUNTS = {"info", "support", "contact", "team", "sales", "hello", "admin", "help", "finance", "payments"}
FALLBACK_GENERIC_PAGE_TYPES = {"contact_page", "about_page", "team_page", "support_page", "finance_page", "payment_page"}


def _build_base_url(domain):
    return f"https://{domain}"


def _domain_usable(domain):
    cleaned = str(domain or "").strip().lower()
    if not cleaned or " " in cleaned or "/" in cleaned:
        return False
    return "." in cleaned and cleaned not in {"unknown", "example.com", "test.com"}


def _same_domain(email_domain, merchant_domain):
    email_domain = str(email_domain or "").strip().lower()
    merchant_domain = str(merchant_domain or "").strip().lower()
    return bool(
        email_domain
        and merchant_domain
        and (email_domain == merchant_domain or email_domain.endswith(f".{merchant_domain}"))
    )


def _split_email(email):
    lowered = str(email or "").strip().lower()
    if "@" not in lowered:
        return ("", "")
    return tuple(lowered.split("@", 1))


def _contact_prefix(email):
    return _split_email(email)[0]


def _clean_anchor_text(raw):
    cleaned = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", raw or "")
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _strip_html_noise(html):
    cleaned = re.sub(r"(?is)<script.*?</script>|<style.*?</style>|<!--.*?-->", " ", html or "")
    return unescape(cleaned)


def _visible_text_from_html(html):
    visible = re.sub(r"(?is)<[^>]+>", " ", _strip_html_noise(html))
    return re.sub(r"\s+", " ", visible).strip()


def _footer_html(html):
    match = re.search(r"(?is)<footer\b.*?</footer>", html or "")
    return match.group(0) if match else ""


def _normalize_same_domain_url(href, base_url, merchant_domain):
    if not href:
        return ""
    candidate = urljoin(base_url, unescape(href).strip())
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if not _same_domain(parsed.netloc.lower(), merchant_domain):
        return ""
    path = parsed.path or "/"
    normalized = parsed._replace(query="", fragment="", path=path.rstrip("/") or "/")
    return normalized.geturl()


def _classify_page_type(url, anchor_text=""):
    blob = f"{urlparse(url).path.lower()} {str(anchor_text or '').lower()}"
    best_match = ""
    best_priority = 99
    for page_type, hints in PAGE_TYPE_HINTS.items():
        if any(token in blob for token in hints):
            priority = PAGE_TYPE_PRIORITY.get(page_type, 99)
            if priority < best_priority or not best_match:
                best_match = page_type
                best_priority = priority
    return best_match


def _extract_internal_page_links(html, *, base_url, merchant_domain):
    grouped = {page_type: [] for page_type in PAGE_PRIORITY_ORDER}
    seen = set()
    for match in _ANCHOR_PATTERN.finditer(html or ""):
        href = match.group("href")
        label = _clean_anchor_text(match.group("label"))
        if str(href or "").lower().startswith("mailto:"):
            continue
        normalized = _normalize_same_domain_url(href, base_url, merchant_domain)
        if not normalized or normalized == base_url or normalized in seen:
            continue
        page_type = _classify_page_type(normalized, label)
        if not page_type:
            continue
        grouped.setdefault(page_type, []).append(normalized)
        seen.add(normalized)
    for page_type, fallbacks in PAGE_TYPE_FALLBACKS.items():
        for fallback in fallbacks:
            normalized = _normalize_same_domain_url(fallback, base_url, merchant_domain)
            if normalized and normalized != base_url and normalized not in grouped.setdefault(page_type, []):
                grouped[page_type].append(normalized)
    return grouped


def _extract_mailto_records(html):
    records = []
    for match in _MAILTO_PATTERN.finditer(html or ""):
        href = str(match.group("href") or "")
        label = _clean_anchor_text(match.group("label"))
        email_part = unquote(href.split("?", 1)[0]).split(",", 1)[0].strip().lower()
        if email_part:
            records.append({"email": email_part, "label": label})
    return records


def _extract_context_window(text, token, size=140):
    lowered = str(text or "").lower()
    target = str(token or "").lower()
    idx = lowered.find(target)
    if idx < 0:
        return str(text or "")[: size * 2]
    start = max(0, idx - size)
    end = min(len(text or ""), idx + len(target) + size)
    return str(text or "")[start:end]


def _role_hint_from_context(local_part, context_text, page_type):
    lowered = f"{local_part} {str(context_text or '').lower()} {page_type.replace('_', ' ')}"
    for role, hints in _ROLE_KEYWORDS.items():
        if any(token in lowered for token in hints):
            return role
    if local_part in {"ops", "operations"}:
        return "operations"
    return ""


def _infer_contact_name(local_part, context_text):
    context = str(context_text or "").strip()
    if context and "@" not in context:
        matches = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", context)
        for candidate in matches:
            if len(candidate.split()) >= 2:
                return candidate.strip()
    lowered = str(local_part or "").strip().lower()
    if not lowered or lowered in _GENERIC_PREFIXES or lowered in _HIGH_VALUE_PREFIXES or lowered in _BLOCKED_LOCAL_PARTS:
        return ""
    if _PERSONAL_INDICATORS.match(lowered):
        return lowered.replace(".", " ").replace("-", " ").replace("_", " ").title()
    return ""


def _role_aligned(local_part, role_hint, target_prefixes):
    clean_local = str(local_part or "").strip().lower()
    clean_hint = str(role_hint or "").strip().lower()
    normalized_targets = {str(role or "").strip().lower() for role in target_prefixes or []}
    if clean_local in normalized_targets or clean_hint in normalized_targets:
        return True
    alias_map = {
        "ops": "operations",
        "operation": "operations",
        "support leadership": "support",
        "payment": "payments",
    }
    if alias_map.get(clean_local) in normalized_targets or alias_map.get(clean_hint) in normalized_targets:
        return True
    return False


def extract_emails_from_html(html):
    """
    Extract email addresses from text content.

    The caller should pass visible text when mailto links need to stay separate.
    """
    if not html:
        return []

    emails = _EMAIL_PATTERN.findall(html)
    cleaned = []
    for email in emails:
        email_lower = email.lower()
        if any(pat in email_lower for pat in _IGNORE_EMAIL_PATTERNS):
            continue
        if len(email_lower) > 80 or "%" in email_lower or "&" in email_lower:
            continue
        if email_lower not in cleaned:
            cleaned.append(email_lower)
    return cleaned


def extract_linkedin_from_html(html):
    """Retained for backwards-compatible tests and existing read paths."""
    if not html:
        return []
    urls = _LINKEDIN_PATTERN.findall(html)
    cleaned = []
    for url in urls:
        url = url.rstrip("/")
        if url not in cleaned:
            cleaned.append(url)
    return cleaned


def score_contact(email=None, linkedin_url=None):
    """Base confidence score before page and role modifiers."""
    if email:
        prefix = _contact_prefix(email)
        if prefix in _NOREPLY_PREFIXES:
            return 0.1
        if prefix in _HIGH_VALUE_PREFIXES:
            return 0.8
        if prefix in _GENERIC_PREFIXES:
            return 0.3
        if _PERSONAL_INDICATORS.match(prefix):
            return 0.7
        return 0.5
    if linkedin_url:
        url_lower = str(linkedin_url or "").lower()
        if "/in/" in url_lower:
            return 0.8
        if "/company/" in url_lower:
            return 0.5
    return 0.3


def _email_allowed_for_merchant(email, merchant_domain):
    if not email or "@" not in email:
        return False
    local, domain = _split_email(email)
    if not _same_domain(domain, merchant_domain):
        return False
    if domain in _FREE_EMAIL_DOMAINS or domain in _INTERNAL_CONTACT_DOMAINS:
        return False
    if local in _NOREPLY_PREFIXES or local in _BLOCKED_LOCAL_PARTS:
        return False
    return True


def _is_named_contact(local_part):
    clean = str(local_part or "").strip().lower()
    return bool(
        clean
        and clean not in _GENERIC_PREFIXES
        and clean not in _HIGH_VALUE_PREFIXES
        and clean not in _BLOCKED_LOCAL_PARTS
        and _PERSONAL_INDICATORS.match(clean)
    )


def _candidate_confidence(email, *, page_type, email_verified, explicit_on_site, role_aligned, named_contact):
    confidence = score_contact(email=email)
    page_bonus = {
        "contact_page": 0.18,
        "about_page": 0.12,
        "team_page": 0.14,
        "footer_page": 0.08,
        "legal_page": 0.07,
        "support_page": 0.08,
        "finance_page": 0.14,
        "payment_page": 0.14,
    }.get(page_type, 0.05)
    if explicit_on_site:
        confidence += 0.16
    confidence += page_bonus
    if role_aligned:
        confidence += 0.16
    if named_contact:
        confidence += 0.06
    if email_verified:
        confidence = max(confidence + 0.12, 0.94)
    local_part = _contact_prefix(email)
    if local_part in _GENERIC_PREFIXES and not role_aligned:
        confidence = min(confidence, 0.76)
    return round(max(0.05, min(confidence, 0.99)), 3)


def _candidate_rank_key(candidate):
    local_part = candidate.get("local_part") or ""
    page_priority = int(candidate.get("page_priority") or 99)
    crawl_step = int(candidate.get("crawl_step") or 99)
    return (
        1 if candidate.get("same_domain") else 0,
        1 if candidate.get("email_verified") else 0,
        1 if candidate.get("role_aligned") and candidate.get("contact_name") else 0,
        1 if candidate.get("role_aligned") else 0,
        1 if candidate.get("explicit_on_site") else 0,
        1 if candidate.get("contact_name") else 0,
        1 if local_part in _HIGH_VALUE_PREFIXES else 0,
        100 - page_priority,
        100 - crawl_step,
        float(candidate.get("confidence") or 0.0),
    )


def _candidate_is_high_confidence(candidate):
    local_part = str(candidate.get("local_part") or "").strip().lower()
    if not candidate.get("same_domain") or not candidate.get("explicit_on_site"):
        return False
    confidence = float(candidate.get("confidence") or 0.0)
    page_type = str(candidate.get("page_type") or "").strip().lower()
    source = str(candidate.get("source") or "").strip().lower()
    if local_part in _NOREPLY_PREFIXES or local_part in _BLOCKED_LOCAL_PARTS:
        return False
    if local_part in _GENERIC_PREFIXES:
        return bool(
            confidence >= USABLE_FALLBACK_CONTACT_THRESHOLD
            and page_type in FALLBACK_GENERIC_PAGE_TYPES
            and (candidate.get("explicit_on_site") or source.startswith("website:"))
        )
    if not candidate.get("email_verified") and confidence < HIGH_CONFIDENCE_CONTACT_THRESHOLD:
        return False
    return bool(
        candidate.get("email_verified")
        or candidate.get("role_aligned")
        or candidate.get("contact_name")
        or local_part in _HIGH_VALUE_PREFIXES
    )


def _merge_candidate(bucket, candidate):
    key = str(candidate.get("email") or "").strip().lower()
    if not key:
        return
    existing = bucket.get(key)
    if not existing or _candidate_rank_key(candidate) > _candidate_rank_key(existing):
        bucket[key] = candidate


def _fetch_page(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT, allow_redirects=True)
        if response.status_code == 200 and "text/html" in response.headers.get("content-type", ""):
            return response.text
    except Exception:
        return None
    return None


def _merchant_contact_target_context(conn, merchant_id, opportunity_id=None, *, distress_topic="", selected_action=""):
    query = text(
        """
        SELECT
            COALESCE(ooa.selected_action, '') AS selected_action,
            COALESCE(mo.distress_topic, '') AS distress_topic
        FROM merchant_opportunities mo
        LEFT JOIN opportunity_operator_actions ooa ON ooa.opportunity_id = mo.id
        WHERE mo.merchant_id = :merchant_id
          AND (:opportunity_id <= 0 OR mo.id = :opportunity_id)
        ORDER BY
            CASE mo.status
                WHEN 'pending_review' THEN 0
                WHEN 'approved' THEN 1
                WHEN 'outreach_sent' THEN 2
                ELSE 3
            END,
            mo.created_at DESC
        LIMIT 1
        """
    )
    row = conn.execute(query, {"merchant_id": merchant_id, "opportunity_id": int(opportunity_id or 0)}).fetchone()
    selected_action_value = (row[0] if row else "") or str(selected_action or "")
    distress_topic_value = (row[1] if row else "") or str(distress_topic or "")
    distress = normalize_distress_topic(distress_topic_value)
    action = str(selected_action_value or operator_action_for_distress(distress)).strip().lower().replace("-", "_").replace(" ", "_")
    target_roles = list(_PLAY_CONTACT_PREFIXES.get(action, _PLAY_CONTACT_PREFIXES["clarify_distress"]))
    target_prefixes = []
    for role in target_roles:
        clean = str(role or "").strip().lower()
        if clean and clean not in target_prefixes:
            target_prefixes.append(clean)
        if clean == "operations" and "ops" not in target_prefixes:
            target_prefixes.append("ops")
    return {"action": action or "clarify_distress", "target_roles": target_roles, "target_prefixes": target_prefixes}


def _existing_contact_rows(conn, merchant_id):
    _ensure_contact_discovery_schema()
    return conn.execute(
        text(
            """
            SELECT
                id,
                COALESCE(contact_name, '') AS contact_name,
                LOWER(COALESCE(email, '')) AS email,
                LOWER(COALESCE(source, '')) AS source,
                COALESCE(confidence, 0) AS confidence,
                COALESCE(email_verified, FALSE) AS email_verified,
                COALESCE(local_part, '') AS local_part,
                COALESCE(email_domain, '') AS email_domain,
                COALESCE(page_type, '') AS page_type,
                COALESCE(page_url, '') AS page_url,
                COALESCE(role_hint, '') AS role_hint,
                COALESCE(same_domain, FALSE) AS same_domain,
                COALESCE(explicit_on_site, FALSE) AS explicit_on_site,
                COALESCE(crawl_step, 0) AS crawl_step,
                created_at
            FROM merchant_contacts
            WHERE merchant_id = :merchant_id
            """
        ),
        {"merchant_id": int(merchant_id)},
    ).mappings().fetchall()


def _contact_send_ready(row, merchant_domain, target_prefixes):
    email = str(row.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return False
    local_part, domain = _split_email(email)
    if not _same_domain(domain or row.get("email_domain"), merchant_domain):
        return False
    if domain in _FREE_EMAIL_DOMAINS or domain in _INTERNAL_CONTACT_DOMAINS:
        return False
    if local_part in _NOREPLY_PREFIXES or local_part in _BLOCKED_LOCAL_PARTS:
        return False
    source = str(row.get("source") or "").strip().lower()
    if "validation" in source:
        return False
    named_contact = _is_named_contact(local_part) or bool(str(row.get("contact_name") or "").strip())
    role_hint = str(row.get("role_hint") or "").strip().lower()
    role_aligned = _role_aligned(local_part, role_hint, target_prefixes)
    confidence = float(row.get("confidence") or 0.0)
    explicit_on_site = bool(row.get("explicit_on_site")) or source.startswith("website:")
    page_type = str(row.get("page_type") or "").strip().lower()
    if local_part in _GENERIC_PREFIXES:
        return bool(
            explicit_on_site
            and confidence >= USABLE_FALLBACK_CONTACT_THRESHOLD
            and page_type in FALLBACK_GENERIC_PAGE_TYPES
        )
    if not (bool(row.get("email_verified")) or confidence >= HIGH_CONFIDENCE_CONTACT_THRESHOLD):
        return False
    return bool(named_contact or role_aligned or local_part in _HIGH_VALUE_PREFIXES)


def _existing_contact_coverage(conn, merchant_id, merchant_domain, target_prefixes):
    rows = _existing_contact_rows(conn, merchant_id)
    emails = {row["email"] for row in rows if row.get("email")}
    same_domain_rows = [
        row for row in rows if _same_domain(row.get("email_domain") or _split_email(row.get("email"))[1], merchant_domain)
    ]
    best_same_domain = max(
        same_domain_rows,
        key=lambda row: (
            1 if row.get("email_verified") else 0,
            float(row.get("confidence") or 0.0),
            1 if row.get("explicit_on_site") else 0,
        ),
        default=None,
    )
    has_send_ready = any(_contact_send_ready(row, merchant_domain, target_prefixes) for row in same_domain_rows)
    return {
        "rows": rows,
        "emails": emails,
        "same_domain_rows": same_domain_rows,
        "best_same_domain": best_same_domain,
        "has_send_ready": has_send_ready,
    }


def _high_value_context(opportunity_score, distress_score, distress_topic):
    normalized_distress = normalize_distress_topic(distress_topic)
    return bool(
        float(opportunity_score or 0.0) >= HIGH_VALUE_OPPORTUNITY_SCORE
        or float(distress_score or 0.0) >= HIGH_VALUE_DISTRESS_SCORE
        or normalized_distress in _HIGH_VALUE_DISTRESS_TOPICS
    )


def _load_deepening_context(
    conn,
    merchant_id,
    opportunity_id=None,
    *,
    allow_without_opportunity=False,
    distress_topic="",
    selected_action="",
):
    merchant = conn.execute(
        text(
            """
            SELECT
                m.id,
                COALESCE(m.canonical_name, '') AS canonical_name,
                COALESCE(m.domain, '') AS domain,
                COALESCE(m.opportunity_score, 0) AS opportunity_score,
                COALESCE(m.distress_score, 0) AS distress_score
            FROM merchants m
            WHERE m.id = :merchant_id
            LIMIT 1
            """
        ),
        {"merchant_id": int(merchant_id)},
    ).mappings().first()
    opportunity = conn.execute(
        text(
            """
            SELECT
                mo.id,
                mo.status,
                COALESCE(mo.distress_topic, '') AS distress_topic,
                COALESCE(mo.merchant_domain, '') AS merchant_domain,
                mo.created_at
            FROM merchant_opportunities mo
            WHERE mo.merchant_id = :merchant_id
              AND mo.status IN ('pending_review', 'approved', 'outreach_sent')
              AND (:opportunity_id <= 0 OR mo.id = :opportunity_id)
            ORDER BY
                CASE mo.status
                    WHEN 'pending_review' THEN 0
                    WHEN 'approved' THEN 1
                    WHEN 'outreach_sent' THEN 2
                    ELSE 3
                END,
                mo.created_at DESC
            LIMIT 1
            """
        ),
        {"merchant_id": int(merchant_id), "opportunity_id": int(opportunity_id or 0)},
    ).mappings().first()
    if not merchant:
        return {"eligible": False, "reason": "No live blocked outreach opportunity is available for deepening."}

    merchant_domain = str(merchant.get("domain") or "").strip().lower()
    seed_distress = normalize_distress_topic(distress_topic)
    seed_mode = False
    if not opportunity and allow_without_opportunity:
        if seed_distress == "unknown":
            return {"eligible": False, "reason": "Pattern-seed deepening requires a concrete distress topic."}
        seed_mode = True
        opportunity = {
            "id": 0,
            "status": "pattern_seed",
            "distress_topic": seed_distress,
            "merchant_domain": merchant_domain,
            "created_at": None,
        }
    elif not opportunity:
        return {"eligible": False, "reason": "No live blocked outreach opportunity is available for deepening."}

    latest_signal = conn.execute(
        text(
            """
            SELECT s.content
            FROM signals s
            LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
            WHERE s.merchant_id = :merchant_id OR ms.merchant_id = :merchant_id
            ORDER BY s.detected_at DESC
            LIMIT 1
            """
        ),
        {"merchant_id": int(merchant["id"])},
    ).mappings().first()
    queue_quality = evaluate_opportunity_queue_quality(
        opportunity=opportunity,
        merchant=merchant,
        signal=dict(latest_signal or {}),
    )
    if (
        not seed_mode
        and queue_quality["eligibility_class"] not in (ELIGIBILITY_OUTREACH, ELIGIBILITY_OPERATOR_REVIEW_ONLY)
    ):
        return {
            "eligible": False,
            "reason": queue_quality["eligibility_reason"],
            "queue_quality": queue_quality,
        }

    merchant_domain = str(merchant_domain or opportunity.get("merchant_domain") or "").strip().lower()
    if not _domain_usable(merchant_domain):
        return {"eligible": False, "reason": "Merchant domain is missing or not usable for same-domain deepening."}

    target_context = _merchant_contact_target_context(
        conn,
        merchant_id,
        opportunity.get("id"),
        distress_topic=seed_distress if seed_mode else opportunity.get("distress_topic"),
        selected_action=selected_action,
    )
    coverage = _existing_contact_coverage(conn, merchant_id, merchant_domain, target_context["target_prefixes"])
    if coverage["has_send_ready"]:
        return {"eligible": False, "reason": "A trusted same-domain send-eligible contact already exists."}

    if not _high_value_context(merchant.get("opportunity_score"), merchant.get("distress_score"), opportunity.get("distress_topic")):
        return {"eligible": False, "reason": "Lead is not above the current high-value deepening threshold."}

    best_same_domain = coverage["best_same_domain"]
    if not coverage["same_domain_rows"]:
        eligibility_reason = (
            "High-value pattern-seed merchant with no trusted same-domain contact."
            if seed_mode
            else "High-value blocked lead with no trusted same-domain contact."
        )
        blocked_state = "blocked_no_contact"
    else:
        eligibility_reason = (
            "Pattern-seed merchant has only weak same-domain contacts below send threshold."
            if seed_mode
            else "Weak same-domain contact exists, but confidence is below send threshold."
        )
        blocked_state = "blocked_weak_contact"

    return {
        "eligible": True,
        "merchant_id": int(merchant["id"]),
        "merchant_name": merchant.get("canonical_name") or merchant_domain,
        "merchant_domain": merchant_domain,
        "opportunity_id": int(opportunity["id"]),
        "opportunity_status": opportunity.get("status") or "pending_review",
        "distress_topic": normalize_distress_topic(opportunity.get("distress_topic")),
        "opportunity_score": float(merchant.get("opportunity_score") or 0.0),
        "distress_score": float(merchant.get("distress_score") or 0.0),
        "eligibility_reason": eligibility_reason,
        "blocked_state": blocked_state,
        "best_same_domain": dict(best_same_domain) if best_same_domain else {},
        "existing_emails": set(coverage["emails"]),
        "target_context": target_context,
        "queue_quality": queue_quality,
        "seed_mode": seed_mode,
    }


def _build_candidate(
    *,
    email,
    merchant_domain,
    page_type,
    page_url,
    crawl_step,
    source,
    context_text,
    email_verified,
    target_prefixes,
    render_mode="http",
    source_quality_tier=0,
    extraction_confidence=None,
):
    email = str(email or "").strip().lower()
    if not _email_allowed_for_merchant(email, merchant_domain):
        return {}
    local_part, domain = _split_email(email)
    role_hint = _role_hint_from_context(local_part, context_text, page_type)
    contact_name = _infer_contact_name(local_part, context_text)
    named_contact = bool(contact_name) or _is_named_contact(local_part)
    role_aligned = _role_aligned(local_part, role_hint, target_prefixes)
    confidence = _candidate_confidence(
        email,
        page_type=page_type,
        email_verified=email_verified,
        explicit_on_site=True,
        role_aligned=role_aligned,
        named_contact=named_contact,
    )
    return {
        "email": email,
        "local_part": local_part,
        "email_domain": domain,
        "page_type": page_type,
        "page_url": page_url,
        "page_priority": int(PAGE_TYPE_PRIORITY.get(page_type, 5)),
        "crawl_step": int(crawl_step),
        "source": source,
        "contact_name": contact_name,
        "role_hint": role_hint,
        "same_domain": True,
        "explicit_on_site": True,
        "email_verified": bool(email_verified),
        "confidence": confidence,
        "role_aligned": bool(role_aligned),
        "render_mode": render_mode,
        "source_quality_tier": int(source_quality_tier),
        "extraction_confidence": float(extraction_confidence) if extraction_confidence is not None else None,
    }


def _visible_email_candidates(
    *,
    html,
    merchant_domain,
    page_type,
    page_url,
    crawl_step,
    existing_emails,
    target_prefixes,
    mx_servers,
):
    text_blob = _visible_text_from_html(html)
    candidates = []
    for email in extract_emails_from_html(text_blob):
        if email in existing_emails:
            continue
        context_text = _extract_context_window(text_blob, email)
        email_verified = quick_verify(email, mx_servers) if mx_servers else False
        candidate = _build_candidate(
            email=email,
            merchant_domain=merchant_domain,
            page_type=page_type,
            page_url=page_url,
            crawl_step=crawl_step,
            source=PAGE_TYPE_SOURCE.get(page_type, "website"),
            context_text=context_text,
            email_verified=email_verified,
            target_prefixes=target_prefixes,
        )
        if candidate:
            candidates.append(candidate)
    return candidates


def _mailto_candidates(
    *,
    html,
    merchant_domain,
    page_type,
    page_url,
    existing_emails,
    target_prefixes,
    mx_servers,
):
    candidates = []
    for record in _extract_mailto_records(html):
        email = record.get("email") or ""
        if email in existing_emails:
            continue
        context_text = record.get("label") or ""
        email_verified = quick_verify(email, mx_servers) if mx_servers else False
        candidate = _build_candidate(
            email=email,
            merchant_domain=merchant_domain,
            page_type=page_type,
            page_url=page_url,
            crawl_step=5,
            source="website:mailto",
            context_text=context_text,
            email_verified=email_verified,
            target_prefixes=target_prefixes,
        )
        if candidate:
            candidates.append(candidate)
    return candidates


def _build_priority_plan(*, homepage_html, base_url, merchant_domain):
    grouped = _extract_internal_page_links(homepage_html, base_url=base_url, merchant_domain=merchant_domain)
    plan = []
    footer = _footer_html(homepage_html)
    if footer:
        plan.append({"page_type": "footer_page", "page_url": base_url, "html": footer, "prefetched": True})
    for page_type in PAGE_PRIORITY_ORDER:
        if page_type == "footer_page":
            continue
        candidates = grouped.get(page_type, [])
        if candidates:
            plan.append({"page_type": page_type, "page_url": candidates[0], "prefetched": False})
    ranked = sorted(plan, key=lambda item: (PAGE_TYPE_PRIORITY.get(item["page_type"], 99), item["page_url"]))
    if not footer:
        return ranked[:MAX_DEEPENING_REQUESTS]
    footer_item = ranked[:1] if ranked and ranked[0]["page_type"] == "footer_page" else []
    others = [item for item in ranked if item["page_type"] != "footer_page"]
    return footer_item + others[:MAX_DEEPENING_REQUESTS]


def _persist_params(candidate):
    """Build params dict for SQL, ensuring new fields have safe defaults."""
    params = dict(candidate)
    params.setdefault("render_mode", None)
    params.setdefault("source_quality_tier", 0)
    params.setdefault("extraction_confidence", None)
    return params


def _persist_candidate(conn, merchant_id, candidate):
    existing = conn.execute(
        text(
            """
            SELECT
                id,
                COALESCE(confidence, 0) AS confidence,
                COALESCE(email_verified, FALSE) AS email_verified,
                COALESCE(explicit_on_site, FALSE) AS explicit_on_site,
                COALESCE(crawl_step, 0) AS crawl_step
            FROM merchant_contacts
            WHERE merchant_id = :merchant_id
              AND LOWER(COALESCE(email, '')) = :email
            ORDER BY COALESCE(email_verified, FALSE) DESC, COALESCE(confidence, 0) DESC, created_at DESC
            LIMIT 1
            """
        ),
        {"merchant_id": int(merchant_id), "email": candidate["email"]},
    ).mappings().first()
    if existing:
        existing_key = (
            1 if existing.get("email_verified") else 0,
            float(existing.get("confidence") or 0.0),
            1 if existing.get("explicit_on_site") else 0,
            100 - int(existing.get("crawl_step") or 99),
        )
        candidate_key = (
            1 if candidate.get("email_verified") else 0,
            float(candidate.get("confidence") or 0.0),
            1 if candidate.get("explicit_on_site") else 0,
            100 - int(candidate.get("crawl_step") or 99),
        )
        if candidate_key <= existing_key:
            return
        conn.execute(
            text(
                """
                UPDATE merchant_contacts
                SET
                    contact_name = :contact_name,
                    source = :source,
                    confidence = :confidence,
                    email_verified = :email_verified,
                    local_part = :local_part,
                    email_domain = :email_domain,
                    page_type = :page_type,
                    page_url = :page_url,
                    role_hint = :role_hint,
                    same_domain = :same_domain,
                    explicit_on_site = :explicit_on_site,
                    crawl_step = :crawl_step,
                    render_mode = :render_mode,
                    source_quality_tier = :source_quality_tier,
                    extraction_confidence = :extraction_confidence
                WHERE id = :id
                """
            ),
            {**_persist_params(candidate), "merchant_id": int(merchant_id), "id": int(existing["id"])},
        )
        return
    conn.execute(
        text(
            """
            INSERT INTO merchant_contacts (
                merchant_id,
                contact_name,
                email,
                source,
                confidence,
                email_verified,
                local_part,
                email_domain,
                page_type,
                page_url,
                role_hint,
                same_domain,
                explicit_on_site,
                crawl_step,
                status,
                discovery_method,
                render_mode,
                source_quality_tier,
                extraction_confidence
            )
            VALUES (
                :merchant_id,
                :contact_name,
                :email,
                :source,
                :confidence,
                :email_verified,
                :local_part,
                :email_domain,
                :page_type,
                :page_url,
                :role_hint,
                :same_domain,
                :explicit_on_site,
                :crawl_step,
                'candidate',
                :discovery_method,
                :render_mode,
                :source_quality_tier,
                :extraction_confidence
            )
            """
        ),
        {**_persist_params(candidate), "merchant_id": int(merchant_id), "discovery_method": candidate.get("source")},
    )
    _maybe_upgrade_candidate_merchant(conn, merchant_id, candidate)


def _maybe_upgrade_candidate_merchant(conn, merchant_id, candidate):
    """
    Automatically promote 'candidate' merchants to 'verified' if a strong,
    same-domain contact is found from a trusted source.
    """
    confidence = float(candidate.get("confidence") or 0.0)
    same_domain = bool(candidate.get("same_domain") or candidate.get("email_domain")) # check if it matches merchant domain
    method = candidate.get("source") or candidate.get("discovery_method")

    # Double check same-domain if same_domain flag is missing or false
    if not same_domain and candidate.get("email"):
        # We need the merchant domain to verify
        merchant = conn.execute(
            text("SELECT domain, status, canonical_name FROM merchants WHERE id = :mid"),
            {"mid": int(merchant_id)},
        ).mappings().first()
        if merchant and merchant["domain"] and candidate["email"].endswith(f"@{merchant['domain']}"):
            same_domain = True
    else:
        # We still need the merchant to check status
        merchant = conn.execute(
            text("SELECT status, canonical_name FROM merchants WHERE id = :mid"),
            {"mid": int(merchant_id)},
        ).mappings().first()

    if not merchant or merchant["status"] != "candidate":
        return

    if not same_domain:
        return
    if confidence < HIGH_CONFIDENCE_CONTACT_THRESHOLD:
        return
    if method not in TRUSTED_UPGRADE_METHODS:
        return

    # Upgrade
    conn.execute(
        text(
            """
            UPDATE merchants
            SET status = 'verified',
                verification_reason = 'same_domain_contact_discovery',
                last_seen = CURRENT_TIMESTAMP
            WHERE id = :mid
            """
        ),
        {"mid": int(merchant_id)},
    )
    logger.info(f"Merchant '{merchant['canonical_name']}' (ID {merchant_id}) AUTO-UPGRADED to verified via contact discovery ({method})")
    save_event("merchant_auto_upgraded_from_discovery", {
        "merchant_id": int(merchant_id),
        "merchant_name": merchant["canonical_name"],
        "discovery_method": method,
        "contact_email": candidate.get("email")
    })


def _persist_candidates(conn, merchant_id, candidates):
    ranked = sorted(candidates, key=_candidate_rank_key, reverse=True)
    limit = min(MAX_PERSISTED_CANDIDATES, len(ranked))
    for candidate in ranked[:limit]:
        _persist_candidate(conn, merchant_id, candidate)


def _deepening_summary(context, candidates, pages_checked, page_types_checked, best_candidate):
    outcome = context.get("blocked_state") or "blocked_no_contact"
    summary = ""
    promoted = 0
    if best_candidate and _candidate_is_high_confidence(best_candidate):
        outcome = "promoted_contact"
        promoted = 1
        summary = (
            f"A trusted same-domain {best_candidate.get('role_hint') or best_candidate.get('local_part') or 'merchant'} "
            f"contact was found on the {str(best_candidate.get('page_type') or 'merchant').replace('_', ' ')}."
        )
    elif candidates:
        lead = sorted(candidates, key=_candidate_rank_key, reverse=True)[0]
        summary = (
            f"Only a weak {lead.get('local_part') or 'merchant'} inbox was found on the "
            f"{str(lead.get('page_type') or 'merchant').replace('_', ' ')}."
        )
        outcome = "blocked_weak_contact"
    else:
        checked = ", ".join(page_types_checked) if page_types_checked else "contact, about, legal, and support pages"
        summary = f"No same-domain contact was found on {checked}."
        outcome = "blocked_no_contact"
    return {
        "merchant_id": int(context["merchant_id"]),
        "opportunity_id": int(context["opportunity_id"]),
        "merchant_domain": context["merchant_domain"],
        "merchant_name": context["merchant_name"],
        "eligibility_reason": context["eligibility_reason"],
        "blocked_state": context["blocked_state"],
        "outcome": outcome,
        "summary": summary,
        "candidates_found": len(candidates),
        "promoted_contacts": promoted,
        "top_page_type": str((best_candidate or {}).get("page_type") or ""),
        "top_source": str((best_candidate or {}).get("source") or ""),
        "top_email": str((best_candidate or {}).get("email") or ""),
        "pages_checked": int(pages_checked),
        "page_types_checked": page_types_checked,
        "target_roles": list(context["target_context"]["target_roles"]),
    }


def _build_pattern_fallback_candidates(merchant_domain, existing_emails):
    """Last-resort role-account guesses so outreach isn't blocked on zero contacts.

    Returns a list of low-confidence candidate dicts (confidence=0.30,
    email_verified=False). Never returns emails already present in
    `existing_emails`.
    """
    if not merchant_domain:
        return []
    existing_lower = {(e or "").lower() for e in existing_emails or []}
    out = []
    for prefix in ROLE_EMAIL_FALLBACK_PREFIXES:
        email = f"{prefix}@{merchant_domain}".lower()
        if email in existing_lower:
            continue
        out.append({
            "email": email,
            "local_part": prefix,
            "email_domain": merchant_domain,
            "page_type": "pattern_fallback",
            "page_url": None,
            "page_priority": 9,
            "crawl_step": 99,
            "source": "pattern_guess",
            "contact_name": None,
            "role_hint": "role_account",
            "same_domain": True,
            "explicit_on_site": False,
            "email_verified": False,
            "confidence": ROLE_EMAIL_FALLBACK_CONFIDENCE,
            "role_aligned": True,
            "render_mode": None,
            "source_quality_tier": 0,
            "extraction_confidence": None,
        })
    return out


def discover_contacts_for_merchant(
    merchant_id,
    opportunity_id=None,
    *,
    allow_without_opportunity=False,
    distress_topic="",
    selected_action="",
):
    """
    Run targeted merchant_contact_deepening for an eligible merchant.

    Returns the number of candidates persisted.
    """
    _ensure_contact_discovery_schema()

    with engine.connect() as conn:
        context = _load_deepening_context(
            conn,
            merchant_id,
            opportunity_id=opportunity_id,
            allow_without_opportunity=allow_without_opportunity,
            distress_topic=distress_topic,
            selected_action=selected_action,
        )
        if not context.get("eligible"):
            return 0

    merchant_domain = context["merchant_domain"]
    merchant_name = context["merchant_name"] or merchant_domain

    started_at = time.time()
    deadline_ts = started_at + MERCHANT_DISCOVERY_DEADLINE_SECONDS
    logger.info(
        f"[contact_discovery] start merchant_id={merchant_id} domain={merchant_domain} "
        f"deadline={MERCHANT_DISCOVERY_DEADLINE_SECONDS}s"
    )

    # ── PayFlux Site Discovery Engine ────────────────────────────────────
    try:
        from intelligence.site_discovery_engine import run_site_discovery
    except ImportError:
        from runtime.intelligence.site_discovery_engine import run_site_discovery

    try:
        crawl_result = run_site_discovery(
            domain=merchant_domain,
            merchant_domain=merchant_domain,
            existing_emails=set(context["existing_emails"]),
            target_prefixes=context["target_context"]["target_prefixes"],
            mx_servers=get_mx_servers(merchant_domain) if validate_email_domain(merchant_domain) else [],
            deadline_ts=deadline_ts,
        )
    except TypeError:
        # Older signature without deadline_ts — fall back gracefully.
        crawl_result = run_site_discovery(
            domain=merchant_domain,
            merchant_domain=merchant_domain,
            existing_emails=set(context["existing_emails"]),
            target_prefixes=context["target_context"]["target_prefixes"],
            mx_servers=get_mx_servers(merchant_domain) if validate_email_domain(merchant_domain) else [],
        )

    if crawl_result is None:
        # ── Hunter.io fallback when site crawl completely fails ──────────
        try:
            from runtime.integrations.hunter_enrichment import domain_search as hunter_domain_search
        except ImportError:
            try:
                from integrations.hunter_enrichment import domain_search as hunter_domain_search
            except ImportError:
                hunter_domain_search = None
        hunter_candidates = hunter_domain_search(merchant_domain) if hunter_domain_search else []
        if hunter_candidates:
            logger.info(f"Hunter.io rescued {len(hunter_candidates)} candidates for uncrawlable {merchant_domain}")
            existing_emails_lower = {e.lower() for e in context["existing_emails"]}
            hunter_filtered = [hc for hc in hunter_candidates if hc["email"].lower() not in existing_emails_lower]
            if hunter_filtered:
                with engine.connect() as conn:
                    _persist_candidates(conn, merchant_id, hunter_filtered)
                    conn.commit()
                summary = _deepening_summary(context, hunter_filtered, 0, ["hunter_io"], hunter_filtered[0])
                save_event("merchant_contact_deepening_run", summary)
                elapsed = time.time() - started_at
                logger.info(
                    f"[contact_discovery] done merchant_id={merchant_id} domain={merchant_domain} "
                    f"source=hunter_io found={len(hunter_filtered)} elapsed={elapsed:.1f}s"
                )
                return min(MAX_PERSISTED_CANDIDATES, len(hunter_filtered))

        # ── Pattern fallback when both site crawl and Hunter.io fail ─────
        pattern_candidates = _build_pattern_fallback_candidates(
            merchant_domain, context["existing_emails"]
        )
        if pattern_candidates:
            with engine.connect() as conn:
                _persist_candidates(conn, merchant_id, pattern_candidates)
                conn.commit()
            summary = _deepening_summary(
                context, pattern_candidates, 0, ["pattern_fallback"], pattern_candidates[0]
            )
            save_event("merchant_contact_deepening_run", summary)
            elapsed = time.time() - started_at
            logger.info(
                f"[contact_discovery] done merchant_id={merchant_id} domain={merchant_domain} "
                f"source=pattern_fallback found={len(pattern_candidates)} elapsed={elapsed:.1f}s"
            )
            return min(MAX_PERSISTED_CANDIDATES, len(pattern_candidates))

        summary = {
            "merchant_id": int(context["merchant_id"]),
            "opportunity_id": int(context["opportunity_id"]),
            "merchant_domain": merchant_domain,
            "merchant_name": merchant_name,
            "eligibility_reason": context["eligibility_reason"],
            "blocked_state": context["blocked_state"],
            "outcome": context["blocked_state"],
            "summary": "No same-domain page could be fetched for merchant_contact_deepening.",
            "candidates_found": 0,
            "promoted_contacts": 0,
            "top_page_type": "",
            "top_source": "",
            "top_email": "",
            "pages_checked": 1,
            "page_types_checked": [],
            "target_roles": list(context["target_context"]["target_roles"]),
        }
        save_event("merchant_contact_deepening_run", summary)
        elapsed = time.time() - started_at
        logger.info(
            f"[contact_discovery] done merchant_id={merchant_id} domain={merchant_domain} "
            f"source=none found=0 elapsed={elapsed:.1f}s (homepage fetch failed)"
        )
        return 0

    pages_checked = crawl_result["pages_checked"]
    page_types_checked = crawl_result["page_types_checked"]
    candidate_bucket = {}
    for candidate in crawl_result["email_candidates"]:
        _merge_candidate(candidate_bucket, candidate)

    # ── Hunter.io Enrichment Fallback ────────────────────────────────────
    # If site crawl found no same-domain contacts, try Hunter.io domain search
    existing_emails_lower = {e.lower() for e in context["existing_emails"]}
    site_found_emails = {c["email"].lower() for c in candidate_bucket.values() if c.get("email")}
    has_usable_site_contact = any(
        c.get("same_domain") and float(c.get("confidence") or 0) >= 0.5
        for c in candidate_bucket.values()
    )
    if not has_usable_site_contact:
        try:
            from runtime.integrations.hunter_enrichment import domain_search as hunter_domain_search
        except ImportError:
            try:
                from integrations.hunter_enrichment import domain_search as hunter_domain_search
            except ImportError:
                hunter_domain_search = None
        if hunter_domain_search:
            hunter_candidates = hunter_domain_search(merchant_domain)
            hunter_added = 0
            for hc in hunter_candidates:
                if hc["email"].lower() not in existing_emails_lower and hc["email"].lower() not in site_found_emails:
                    _merge_candidate(candidate_bucket, hc)
                    hunter_added += 1
            if hunter_added:
                logger.info(f"Hunter.io added {hunter_added} new candidates for {merchant_domain}")
                page_types_checked = list(set(page_types_checked + ["hunter_io"]))

    # ── Pattern fallback when crawl + Hunter still produced nothing ──────
    if not candidate_bucket:
        for pc in _build_pattern_fallback_candidates(merchant_domain, context["existing_emails"]):
            _merge_candidate(candidate_bucket, pc)
        if candidate_bucket and "pattern_fallback" not in page_types_checked:
            page_types_checked = list(page_types_checked) + ["pattern_fallback"]

    ranked_candidates = sorted(candidate_bucket.values(), key=_candidate_rank_key, reverse=True)
    best_candidate = ranked_candidates[0] if ranked_candidates else {}

    final_candidates = sorted(candidate_bucket.values(), key=_candidate_rank_key, reverse=True)
    summary = _deepening_summary(context, final_candidates, pages_checked, page_types_checked, best_candidate)
    with engine.connect() as conn:
        _persist_candidates(conn, merchant_id, final_candidates)
        conn.commit()
    save_event("merchant_contact_deepening_run", summary)
    
    # Pass 3: Instrument decision
    try:
        top_cand = final_candidates[0] if final_candidates else {}
        save_learning_feedback({
            "merchant_id_candidate": int(merchant_id),
            "opportunity_id": int(context.get("opportunity_id") or 0),
            "source": "contact_discovery",
            "decision_type": "contact_selection",
            "decision_payload_json": {
                "pages_checked": pages_checked,
                "page_types_checked": page_types_checked,
                "candidates_found_count": len(final_candidates),
                "outcome_state": summary.get("outcome"),
                "best_page_type": top_cand.get("page_type"),
                "best_confidence": float(top_cand.get("confidence") or 0.0)
            },
            "decision_score": float(top_cand.get("confidence") or 0.0),
            "decision_model_version": "v1-deterministic"
        }, hook_name="contact_selection")
    except Exception as le:
        logger.error(f"Reward hook failed [contact_selection] merchant_id={merchant_id}: {type(le).__name__}: {le}")

    elapsed = time.time() - started_at
    top_source = (best_candidate or {}).get("source") or (page_types_checked[0] if page_types_checked else "none")
    logger.info(
        f"[contact_discovery] done merchant_id={merchant_id} domain={merchant_domain} "
        f"source={top_source} found={len(final_candidates)} elapsed={elapsed:.1f}s "
        f"outcome={summary['outcome']}"
    )
    return min(MAX_PERSISTED_CANDIDATES, len(final_candidates))


def get_best_contact(merchant_id):
    """Return the strongest stored contact candidate for a merchant."""
    _ensure_contact_discovery_schema()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COALESCE(contact_name, '') AS contact_name,
                    email,
                    linkedin_url,
                    COALESCE(source, '') AS source,
                    COALESCE(confidence, 0) AS confidence,
                    COALESCE(email_verified, FALSE) AS email_verified,
                    COALESCE(page_type, '') AS page_type,
                    COALESCE(page_url, '') AS page_url,
                    COALESCE(role_hint, '') AS role_hint,
                    COALESCE(explicit_on_site, FALSE) AS explicit_on_site,
                    COALESCE(crawl_step, 0) AS crawl_step
                FROM merchant_contacts
                WHERE merchant_id = :merchant_id
                ORDER BY
                    COALESCE(email_verified, FALSE) DESC,
                    COALESCE(explicit_on_site, FALSE) DESC,
                    COALESCE(confidence, 0) DESC,
                    created_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": int(merchant_id)},
        ).mappings().first()
    return dict(row) if row else None


def get_all_contacts(merchant_id):
    """Return structured contacts for API and operator visibility."""
    _ensure_contact_discovery_schema()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    id,
                    COALESCE(contact_name, '') AS contact_name,
                    email,
                    linkedin_url,
                    COALESCE(source, '') AS source,
                    COALESCE(confidence, 0) AS confidence,
                    COALESCE(email_verified, FALSE) AS email_verified,
                    COALESCE(local_part, '') AS local_part,
                    COALESCE(email_domain, '') AS email_domain,
                    COALESCE(page_type, '') AS page_type,
                    COALESCE(page_url, '') AS page_url,
                    COALESCE(role_hint, '') AS role_hint,
                    COALESCE(same_domain, FALSE) AS same_domain,
                    COALESCE(explicit_on_site, FALSE) AS explicit_on_site,
                    COALESCE(crawl_step, 0) AS crawl_step,
                    created_at
                FROM merchant_contacts
                WHERE merchant_id = :merchant_id
                ORDER BY
                    COALESCE(email_verified, FALSE) DESC,
                    COALESCE(confidence, 0) DESC,
                    COALESCE(explicit_on_site, FALSE) DESC,
                    created_at DESC
                """
            ),
            {"merchant_id": int(merchant_id)},
        ).mappings().fetchall()
    contacts = []
    for row in rows:
        local_part, domain = _split_email(row.get("email") or "")
        contacts.append(
            {
                "id": row["id"],
                "contact_name": row.get("contact_name") or "",
                "email": row.get("email") or "",
                "linkedin_url": row.get("linkedin_url") or "",
                "source": row.get("source") or "",
                "confidence": float(row.get("confidence") or 0.0),
                "email_verified": bool(row.get("email_verified")),
                "local_part": row.get("local_part") or local_part,
                "domain": row.get("email_domain") or domain,
                "page_type": row.get("page_type") or "",
                "page_url": row.get("page_url") or "",
                "role_hint": row.get("role_hint") or "",
                "same_domain": bool(row.get("same_domain")) if row.get("email") else False,
                "explicit_on_site": bool(row.get("explicit_on_site")),
                "crawl_step": int(row.get("crawl_step") or 0),
                "discovery_method": row.get("source"),
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            }
        )
    return contacts


def run_contact_discovery():
    """
    Scheduler entry point for targeted merchant_contact_deepening.

    It only runs for high-value leads blocked on contact quality.
    """
    _ensure_contact_discovery_schema()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT ON (mo.merchant_id)
                    mo.merchant_id,
                    mo.id AS opportunity_id,
                    COALESCE(dl.priority_score, 0) AS lifecycle_priority,
                    COALESCE(dl.contact_trust_score, 0) AS lifecycle_contact_trust
                FROM merchant_opportunities mo
                JOIN merchants m ON m.id = mo.merchant_id
                LEFT JOIN deal_lifecycle dl ON dl.opportunity_id = mo.id
                WHERE mo.merchant_id IS NOT NULL
                  AND mo.status IN ('pending_review', 'approved', 'outreach_pending', 'outreach_sent')
                  AND COALESCE(m.domain, '') != ''
                ORDER BY
                    mo.merchant_id,
                    CASE
                        WHEN COALESCE(dl.contact_trust_score, 0) = 0 THEN 0
                        ELSE 1
                    END,
                    CASE mo.status
                        WHEN 'pending_review' THEN 0
                        WHEN 'approved' THEN 1
                        WHEN 'outreach_pending' THEN 2
                        WHEN 'outreach_sent' THEN 3
                        ELSE 4
                    END,
                    COALESCE(dl.priority_score, 0) DESC,
                    COALESCE(m.opportunity_score, 0) DESC,
                    COALESCE(m.distress_score, 0) DESC,
                    mo.created_at DESC
                LIMIT 300
                """
            )
        ).mappings().fetchall()

        eligible = []
        for row in rows:
            context = _load_deepening_context(conn, row["merchant_id"], opportunity_id=row["opportunity_id"])
            if context.get("eligible"):
                context["lifecycle_priority"] = float(row.get("lifecycle_priority") or 0.0)
                context["lifecycle_contact_trust"] = int(row.get("lifecycle_contact_trust") or 0)
                eligible.append(context)

    if not eligible:
        logger.info("No merchants eligible for merchant_contact_deepening")
        return {"merchants_processed": 0, "contacts_found": 0, "failures": 0}

    eligible.sort(
        key=lambda item: (
            float(item.get("lifecycle_priority") or 0.0),
            1 if int(item.get("lifecycle_contact_trust") or 0) == 0 else 0,
            float(item.get("opportunity_score") or 0.0),
            float(item.get("distress_score") or 0.0),
        ),
        reverse=True,
    )
    total_contacts = 0
    failures = 0
    processed = 0
    batch_started_at = time.time()
    logger.info(
        f"[contact_discovery] batch start: {min(len(eligible), BATCH_SIZE)} merchants "
        f"(per-merchant deadline={MERCHANT_DISCOVERY_DEADLINE_SECONDS}s)"
    )
    for context in eligible[:BATCH_SIZE]:
        processed += 1
        merchant_started = time.time()
        try:
            total_contacts += discover_contacts_for_merchant(
                context["merchant_id"],
                opportunity_id=context["opportunity_id"],
            )
        except Exception as exc:
            logger.error(
                f"[contact_discovery] merchant_id={context['merchant_id']} "
                f"domain={context['merchant_domain']} failed after "
                f"{time.time() - merchant_started:.1f}s: {exc}",
                exc_info=True,
            )
            r.incr("contact_discovery_failures_24h")
            failures += 1
    
    if total_contacts > 0:
        r.incrby("contacts_found_24h", total_contacts)

    batch_elapsed = time.time() - batch_started_at
    result = {
        "merchants_processed": processed,
        "contacts_found": total_contacts,
        "failures": failures,
        "elapsed_seconds": round(batch_elapsed, 1),
    }
    logger.info(
        f"[contact_discovery] batch done: processed={processed} contacts_found={total_contacts} "
        f"failures={failures} elapsed={batch_elapsed:.1f}s"
    )
    return result


def _ensure_contact_discovery_schema(force=False):
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return
    # DB hardening (Phase 1): serialize this batch across the fleet with a
    # fleet-wide advisory lock so two cold-starting PM2 processes cannot
    # race on the same 14 ALTERs.  When MERIDIAN_SCHEMA_MIGRATIONS_ENABLED
    # is false the helper is a pass-through and behaviour is identical
    # to before.
    from runtime.ops.schema_migrations import with_advisory_lock as _with_lock

    with _with_lock("contact_discovery_schema"), engine.connect() as conn:
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS local_part TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS email_domain TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS page_type TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS page_url TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS role_hint TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS same_domain BOOLEAN DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS explicit_on_site BOOLEAN DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS crawl_step INTEGER DEFAULT 0"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'candidate'"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS discovery_method TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS render_mode TEXT"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS source_quality_tier INTEGER DEFAULT 0"))
        conn.execute(text("ALTER TABLE merchant_contacts ADD COLUMN IF NOT EXISTS extraction_confidence REAL DEFAULT 0"))
        conn.commit()
    _SCHEMA_READY = True


def verify_email_hunter(email: str) -> tuple[bool, float]:
    import os
    import requests
    api_key = os.getenv("HUNTER_API_KEY")
    if not api_key:
        return False, 0.0
    try:
        response = requests.get(
            "https://api.hunter.io/v2/email-verifier",
            params={"email": email, "api_key": api_key},
            timeout=10
        )
        if response.status_code != 200:
            return False, 0.0
        data = response.json()
        result = data.get("data", {})
        status = result.get("status")
        score = result.get("score", 0)
        return status in ("valid", "accept_all"), score / 100.0
    except Exception as e:
        logger.error(f"Hunter verification failed for {email}: {e}")
        return False, 0.0


def quick_verify(email, mx_servers):
    """
    Use a role-account shortcut before Hunter.io probe.
    """
    if not mx_servers:
        return False
    local = _contact_prefix(email)
    if local in ROLE_ACCOUNTS:
        return True
    is_valid, _score = verify_email_hunter(email)
    return is_valid


def run_contact_verification():
    """
    Verify existing same-domain contacts using Hunter.io.
    """
    _ensure_contact_discovery_schema()
    with engine.connect() as conn:
        contacts = conn.execute(
            text(
                """
                SELECT mc.id, mc.email, mc.confidence
                FROM merchant_contacts mc
                WHERE mc.email_verified IS NOT TRUE
                  AND mc.email IS NOT NULL
                  AND mc.email LIKE '%%@%%.%%'
                ORDER BY mc.confidence DESC
                LIMIT :limit
                """
            ),
            {"limit": VERIFY_BATCH_SIZE},
        ).fetchall()

    if not contacts:
        logger.info("No unverified contacts to process")
        return {"checked": 0, "verified": 0, "failed": 0}

    logger.info(f"Verifying {len(contacts)} contacts (MX + Hunter.io)...")
    domain_mx = {}
    verified = 0
    failed = 0
    with engine.connect() as conn:
        for contact_id, email, _confidence in contacts:
            _local, domain = _split_email(email)
            if domain not in domain_mx:
                domain_mx[domain] = get_mx_servers(domain)
            mx = domain_mx[domain]
            if not mx:
                failed += 1
                continue
            
            is_valid, score = verify_email_hunter(email)
            if is_valid:
                new_confidence = max(0.9, score) if score > 0 else 0.9
                conn.execute(
                    text("UPDATE merchant_contacts SET confidence = :conf, email_verified = TRUE WHERE id = :contact_id"),
                    {"conf": new_confidence, "contact_id": int(contact_id)},
                )
                r.incr("contacts_verified_24h")
                verified += 1
            else:
                failed += 1
        conn.commit()

    result = {"checked": len(contacts), "verified": verified, "failed": failed}
    logger.info(f"Contact verification run: {result}")
    return result
