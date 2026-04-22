"""
PayFlux Site Discovery Engine

Focused internal crawler for merchant contact discovery.
Crawls one merchant domain at a time, discovers contact-bearing pages,
fetches content robustly, extracts structured contact candidates,
and labels source/page quality.

This module is called by contact_discovery.discover_contacts_for_merchant()
and returns structured results consumed by the existing ranking pipeline.
"""
import json
import os
import re
import sys
import time
from html import unescape
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

import requests

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.logging_config import get_logger

logger = get_logger("site_discovery_engine")

# ── Hard Limits ──────────────────────────────────────────────────────────────
MAX_CRAWL_PAGES = 10
MAX_SITEMAP_PAGES = 3
MAX_BROWSER_RENDERS = 3
MAX_SITEMAP_URLS_PARSED = 100
MAX_EMAIL_CANDIDATES_PER_PAGE = 10
MAX_SITEMAP_BYTES = 1_000_000  # 1MB
PAGE_TIMEOUT = 5
BROWSER_TIMEOUT = 30000
CRAWL_DELAY = 1.0

HEADERS = {
    "User-Agent": "PayFlux-ContactBot/1.0 (merchant contact deepening; respect robots.txt)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
}

# ── Page Type Classification ────────────────────────────────────────────────
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

PAGE_TYPE_FALLBACKS = {
    "contact_page": ("/contact", "/contact-us", "/pages/contact", "/pages/contact-us"),
    "about_page": ("/about", "/about-us", "/pages/about", "/pages/about-us"),
    "team_page": ("/team", "/leadership", "/our-team", "/staff", "/people"),
    "legal_page": ("/legal", "/privacy", "/terms", "/imprint", "/policies/privacy-policy", "/policies/terms-of-service"),
    "support_page": ("/support", "/help", "/faq", "/customer-service"),
    "finance_page": ("/finance", "/billing", "/payouts", "/settlements"),
    "payment_page": ("/payment", "/payments", "/checkout", "/merchant-services"),
}

CONTACT_RELEVANT_KEYWORDS = (
    "contact", "about", "team", "leadership", "staff", "people",
    "legal", "privacy", "terms", "support", "help",
    "finance", "billing", "payment", "payments", "partners",
)

# ── Careers noise detection ──────────────────────────────────────────────────
_CAREERS_PRIMARY_SEGMENTS = {
    "careers", "career", "jobs", "hiring", "vacancies",
    "openings", "recruitment", "work-with-us", "join-us",
    "join-our-team",
}

# ── Source quality tiers ─────────────────────────────────────────────────────
_TIER_3_SOURCES = {
    "website:contact_page",
    "website:about_page",
    "website:team_page",
    "website:finance_page",
    "website:payment_page",
}
_TIER_2_SOURCES = {
    "website:footer_page",
    "website:legal_page",
    "website:support_page",
    "website:json_ld",
    "website:mailto",
}
_TIER_1_SOURCES = {
    "role_inbox_same_domain",
    "website:sitemap_discovered",
}

# ── Extraction patterns ─────────────────────────────────────────────────────
_ANCHOR_PATTERN = re.compile(
    r"<a\b[^>]*href=(['\"])(?P<href>.*?)\1[^>]*>(?P<label>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_JSON_LD_PATTERN = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)
_PHONE_PATTERN = re.compile(
    r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}"
)
_TEL_LINK_PATTERN = re.compile(
    r"<a\b[^>]*href=[\"']tel:(?P<number>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
_SPA_MARKERS = re.compile(
    r'id=["\'](?:root|app|__next)["\']|data-reactroot|ng-app|v-app',
    re.IGNORECASE,
)
_NOSCRIPT_JS_REQUIRED = re.compile(
    r"<noscript[^>]*>.*?(?:enable\s+javascript|javascript\s+is\s+required|javascript\s+must\s+be\s+enabled).*?</noscript>",
    re.IGNORECASE | re.DOTALL,
)
_FORM_PATTERN = re.compile(
    r"<form\b[^>]*>.*?</form>",
    re.IGNORECASE | re.DOTALL,
)

# ── Extraction confidence heuristic defaults ─────────────────────────────────
_EXTRACTION_CONFIDENCE = {
    "visible_high": 0.9,   # visible same-domain email on contact/about/team/finance/payment page
    "mailto": 0.8,         # mailto same-domain email
    "json_ld": 0.7,        # JSON-LD same-domain email
    "visible_mid": 0.6,    # visible email on support/legal/footer page
    "generic": 0.4,        # generic/role inbox on any page
}
_HIGH_CONFIDENCE_PAGE_TYPES = {"contact_page", "about_page", "team_page", "finance_page", "payment_page"}


# ═════════════════════════════════════════════════════════════════════════════
# URL Utilities
# ═════════════════════════════════════════════════════════════════════════════

def _same_domain(host, merchant_domain):
    host = str(host or "").strip().lower()
    merchant_domain = str(merchant_domain or "").strip().lower()
    return bool(
        host and merchant_domain
        and (host == merchant_domain or host.endswith(f".{merchant_domain}"))
    )


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


def _is_careers_noise(url, anchor_text=""):
    """Block pages that are primarily careers-oriented.

    Matches on primary path segments, not substring in full URL.
    Does NOT block ambiguous pages like /company/careers-and-benefits
    or team pages that mention jobs.
    """
    parsed = urlparse(str(url or ""))
    path_segments = [s.lower() for s in parsed.path.strip("/").split("/") if s]
    if not path_segments:
        return False
    # Only block if a primary segment (first or second) is a careers keyword
    primary = path_segments[:2]
    for segment in primary:
        if segment in _CAREERS_PRIMARY_SEGMENTS:
            return True
    # Check anchor text only if strongly careers-oriented
    anchor_lower = str(anchor_text or "").strip().lower()
    if anchor_lower in _CAREERS_PRIMARY_SEGMENTS:
        return True
    return False


def _is_blog_or_archive(url):
    """Reject blog, article, archive, and pagination URLs."""
    path = urlparse(str(url or "")).path.lower()
    reject_segments = {"blog", "news", "article", "articles", "press", "media",
                       "archive", "archives", "page", "category", "tag", "posts"}
    segments = [s for s in path.strip("/").split("/") if s]
    if segments and segments[0] in reject_segments:
        return True
    # Reject pagination patterns
    if re.search(r"/page/\d+", path):
        return True
    return False


def _strip_html_noise(html):
    cleaned = re.sub(r"(?is)<script.*?</script>|<style.*?</style>|<!--.*?-->", " ", html or "")
    return unescape(cleaned)


def _visible_text_from_html(html):
    visible = re.sub(r"(?is)<[^>]+>", " ", _strip_html_noise(html))
    return re.sub(r"\s+", " ", visible).strip()


def _clean_anchor_text(raw):
    cleaned = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", raw or "")
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


# ═════════════════════════════════════════════════════════════════════════════
# Page Discovery
# ═════════════════════════════════════════════════════════════════════════════

def _parse_robots_txt_sitemaps(base_url):
    """Extract Sitemap: directives from robots.txt."""
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        resp = requests.get(robots_url, headers=HEADERS, timeout=PAGE_TIMEOUT)
        if resp.status_code != 200:
            return []
        sitemaps = []
        for line in resp.text.splitlines():
            stripped = line.strip()
            if stripped.lower().startswith("sitemap:"):
                url = stripped.split(":", 1)[1].strip()
                if url:
                    sitemaps.append(url)
        return sitemaps
    except Exception:
        return []


def _parse_sitemap(sitemap_url, merchant_domain):
    """Parse sitemap XML and return contact-relevant same-domain URLs.

    Handles sitemap index files (one level only).
    Caps at MAX_SITEMAP_URLS_PARSED total and MAX_SITEMAP_PAGES relevant.
    """
    urls_parsed = 0
    relevant = []

    def _fetch_and_parse(url):
        nonlocal urls_parsed
        try:
            resp = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT)
            if resp.status_code != 200 or len(resp.content) > MAX_SITEMAP_BYTES:
                return []
            root = ElementTree.fromstring(resp.content)
            # Strip namespace
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"
            entries = []
            # Check if this is a sitemap index
            for sitemap_el in root.findall(f"{ns}sitemap"):
                loc = sitemap_el.find(f"{ns}loc")
                if loc is not None and loc.text:
                    entries.append(("index", loc.text.strip()))
            # Check for regular URL entries
            for url_el in root.findall(f"{ns}url"):
                loc = url_el.find(f"{ns}loc")
                if loc is not None and loc.text:
                    entries.append(("url", loc.text.strip()))
            return entries
        except Exception:
            return []

    entries = _fetch_and_parse(sitemap_url)
    child_sitemaps = [e[1] for e in entries if e[0] == "index"]
    direct_urls = [e[1] for e in entries if e[0] == "url"]

    # Process child sitemaps (one level only)
    for child_url in child_sitemaps:
        if urls_parsed >= MAX_SITEMAP_URLS_PARSED:
            break
        child_entries = _fetch_and_parse(child_url)
        for entry_type, entry_url in child_entries:
            if entry_type == "url":
                direct_urls.append(entry_url)

    # Filter to same-domain, contact-relevant URLs
    for url in direct_urls:
        if urls_parsed >= MAX_SITEMAP_URLS_PARSED:
            break
        urls_parsed += 1
        parsed = urlparse(url)
        if not _same_domain(parsed.netloc.lower(), merchant_domain):
            continue
        path_lower = parsed.path.lower()
        if _is_careers_noise(url) or _is_blog_or_archive(url):
            continue
        if any(kw in path_lower for kw in CONTACT_RELEVANT_KEYWORDS):
            page_type = _classify_page_type(url)
            if page_type and page_type != "careers_page":
                relevant.append({"url": url, "page_type": page_type, "discovery_method": "sitemap"})
                if len(relevant) >= MAX_SITEMAP_PAGES:
                    break

    return relevant, urls_parsed


def _extract_homepage_links(html, base_url, merchant_domain):
    """Extract and classify same-domain links from homepage HTML.

    Parses all <a> tags plus <nav> and <footer> sections specifically.
    Filters out careers noise and blog/archive pages.
    """
    pages = []
    seen = set()

    for match in _ANCHOR_PATTERN.finditer(html or ""):
        href = match.group("href")
        label = _clean_anchor_text(match.group("label"))
        if str(href or "").lower().startswith("mailto:"):
            continue
        normalized = _normalize_same_domain_url(href, base_url, merchant_domain)
        if not normalized or normalized == base_url or normalized in seen:
            continue
        if _is_careers_noise(normalized, label):
            continue
        if _is_blog_or_archive(normalized):
            continue
        page_type = _classify_page_type(normalized, label)
        if not page_type or page_type == "careers_page":
            continue
        pages.append({"url": normalized, "page_type": page_type, "discovery_method": "homepage_link"})
        seen.add(normalized)

    return pages


def _build_fallback_probes(base_url, merchant_domain, already_found_urls):
    """Generate fallback URL probes for paths not already discovered."""
    probes = []
    for page_type, paths in PAGE_TYPE_FALLBACKS.items():
        if page_type == "careers_page":
            continue
        for path in paths:
            normalized = _normalize_same_domain_url(path, base_url, merchant_domain)
            if normalized and normalized != base_url and normalized not in already_found_urls:
                probes.append({"url": normalized, "page_type": page_type, "discovery_method": "fallback_probe"})
                already_found_urls.add(normalized)
    return probes


def _discover_pages(base_url, homepage_html, merchant_domain):
    """Build priority-ordered page list from 3 discovery layers.

    Returns list of page dicts, each with url, page_type, discovery_method.
    Capped at MAX_CRAWL_PAGES total (including homepage as page 1).
    """
    stats = {
        "sitemap_urls_considered": 0,
        "sitemap_urls_accepted": 0,
        "careers_pages_rejected": 0,
    }

    # Layer 1: Sitemap discovery
    sitemap_pages = []
    sitemap_urls = _parse_robots_txt_sitemaps(base_url)
    if not sitemap_urls:
        # Try default sitemap.xml
        sitemap_urls = [urljoin(base_url, "/sitemap.xml")]

    for sitemap_url in sitemap_urls:
        pages, urls_parsed = _parse_sitemap(sitemap_url, merchant_domain)
        stats["sitemap_urls_considered"] += urls_parsed
        for page in pages:
            if len(sitemap_pages) < MAX_SITEMAP_PAGES:
                sitemap_pages.append(page)
                stats["sitemap_urls_accepted"] += 1

    # Layer 2: Homepage link extraction
    homepage_pages = _extract_homepage_links(homepage_html, base_url, merchant_domain)

    # Layer 3: Fallback path probing
    already_found = {p["url"] for p in sitemap_pages + homepage_pages}
    already_found.add(base_url)
    fallback_pages = _build_fallback_probes(base_url, merchant_domain, already_found)

    # Merge and dedupe
    all_pages = []
    seen_urls = {base_url}
    # Priority: sitemap and homepage links first, then fallbacks
    for page in sitemap_pages + homepage_pages + fallback_pages:
        if page["url"] not in seen_urls:
            seen_urls.add(page["url"])
            all_pages.append(page)

    # Sort by page type priority, prefer discovered over fallback
    discovery_order = {"homepage_link": 0, "sitemap": 1, "fallback_probe": 2}
    all_pages.sort(key=lambda p: (
        PAGE_TYPE_PRIORITY.get(p["page_type"], 99),
        discovery_order.get(p["discovery_method"], 3),
    ))

    # Ensure page type diversity: take the best URL per page type first,
    # then fill remaining slots with extras. This prevents e.g. 5 contact_page
    # variants from crowding out legal_page and support_page.
    best_per_type = []
    extras = []
    types_seen = set()
    for page in all_pages:
        if page["page_type"] not in types_seen:
            best_per_type.append(page)
            types_seen.add(page["page_type"])
        else:
            extras.append(page)
    diversified = best_per_type + extras

    # Cap at MAX_CRAWL_PAGES - 1 (homepage is page 1)
    capped = diversified[:MAX_CRAWL_PAGES - 1]
    stats["careers_pages_rejected"] = _count_careers_rejected(homepage_html, base_url, merchant_domain)

    return capped, stats


def _count_careers_rejected(html, base_url, merchant_domain):
    """Count how many careers links were on homepage but rejected."""
    count = 0
    for match in _ANCHOR_PATTERN.finditer(html or ""):
        href = match.group("href")
        label = _clean_anchor_text(match.group("label"))
        normalized = _normalize_same_domain_url(href, base_url, merchant_domain)
        if normalized and _is_careers_noise(normalized, label):
            count += 1
    return count


# ═════════════════════════════════════════════════════════════════════════════
# Smart Fetch
# ═════════════════════════════════════════════════════════════════════════════

def _needs_browser_render(html):
    """Heuristic: does this page need JavaScript rendering?"""
    if not html:
        return True
    # Shell/loader page
    if len(html) < 2048:
        body_match = re.search(r"<body\b", html, re.IGNORECASE)
        if body_match:
            return True
    # SPA markers
    if _SPA_MARKERS.search(html):
        visible = _visible_text_from_html(html)
        if len(visible) < 200:
            return True
    # Noscript warning
    if _NOSCRIPT_JS_REQUIRED.search(html):
        return True
    # Body exists but very little visible text
    if re.search(r"<body\b", html, re.IGNORECASE):
        visible = _visible_text_from_html(html)
        if len(visible) < 200:
            return True
    return False


def _smart_fetch(url, browser_budget_remaining):
    """HTTP-first fetch with smart browser fallback.

    Returns (page_dict, browser_budget_remaining) where page_dict has:
    url, html, render_mode, fetch_ok
    """
    render_mode = "http"
    html = None
    fetch_ok = False

    # Try HTTP first
    try:
        resp = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
            html = resp.text
            fetch_ok = True
    except Exception:
        pass

    # Check if we need browser rendering
    if html and _needs_browser_render(html) and browser_budget_remaining > 0:
        browser_html = _browser_fetch(url)
        if browser_html:
            html = browser_html
            render_mode = "browser"
            browser_budget_remaining -= 1
    elif not fetch_ok and browser_budget_remaining > 0:
        # HTTP failed entirely, try browser as fallback
        browser_html = _browser_fetch(url)
        if browser_html:
            html = browser_html
            render_mode = "browser"
            fetch_ok = True
            browser_budget_remaining -= 1

    return {
        "url": url,
        "html": html or "",
        "render_mode": render_mode,
        "fetch_ok": fetch_ok or bool(html),
    }, browser_budget_remaining


def _browser_fetch(url):
    """Attempt browser fetch via tools/browser_tool.py."""
    try:
        from tools.browser_tool import browser_fetch
        result = browser_fetch(url, timeout=BROWSER_TIMEOUT)
        if result.get("status") == "ok" and result.get("html"):
            return result["html"]
    except Exception as e:
        logger.debug(f"Browser fetch failed for {url}: {e}")
    return None


# ═════════════════════════════════════════════════════════════════════════════
# Extraction
# ═════════════════════════════════════════════════════════════════════════════

def _extract_json_ld_contacts(html, merchant_domain):
    """Conservative JSON-LD extraction.

    Same-domain emails only. Organization/Person/ContactPoint only.
    Ignores third-party schema blobs.
    """
    accepted = []
    rejected = 0

    for match in _JSON_LD_PATTERN.finditer(html or ""):
        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            continue

        items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for item in items:
            if not isinstance(item, dict):
                continue
            item_type = item.get("@type", "")
            if isinstance(item_type, list):
                item_type = item_type[0] if item_type else ""
            if item_type not in ("Organization", "LocalBusiness", "Person", "ContactPoint"):
                continue

            contacts = _extract_contacts_from_schema(item, merchant_domain)
            for contact in contacts:
                if contact.get("email"):
                    accepted.append(contact)
                else:
                    rejected += 1

            # Check nested contactPoint
            contact_points = item.get("contactPoint", [])
            if isinstance(contact_points, dict):
                contact_points = [contact_points]
            for cp in contact_points:
                if isinstance(cp, dict):
                    cp_contacts = _extract_contacts_from_schema(cp, merchant_domain)
                    for contact in cp_contacts:
                        if contact.get("email"):
                            accepted.append(contact)
                        else:
                            rejected += 1

    return accepted, rejected


def _extract_contacts_from_schema(item, merchant_domain):
    """Extract contacts from a single schema.org item. Same-domain only."""
    contacts = []
    email = item.get("email", "")
    if isinstance(email, str):
        email = email.strip().lower().replace("mailto:", "")
    else:
        email = ""

    if email and "@" in email:
        _, domain = email.split("@", 1)
        if _same_domain(domain, merchant_domain):
            name = ""
            title = ""
            # Only trust name/title if clearly present
            raw_name = item.get("name", "")
            if isinstance(raw_name, str) and len(raw_name) < 100 and "@" not in raw_name:
                name = raw_name.strip()
            raw_title = item.get("jobTitle", "")
            if isinstance(raw_title, str) and len(raw_title) < 100:
                title = raw_title.strip()
            contacts.append({
                "email": email,
                "name": name,
                "title": title,
                "telephone": str(item.get("telephone", "") or ""),
                "contact_type": str(item.get("contactType", "") or ""),
            })
        else:
            contacts.append({"email": ""})  # rejected (not same domain)
    return contacts


def _extract_phone_numbers(html):
    """Extract phone numbers from HTML. In-memory only, not persisted."""
    phones = set()
    # Tel links
    for match in _TEL_LINK_PATTERN.finditer(html or ""):
        number = match.group("number").strip()
        if number:
            phones.add(number)
    # Visible text phone patterns
    text = _visible_text_from_html(html)
    for match in _PHONE_PATTERN.finditer(text):
        candidate = match.group(0).strip()
        # Filter obvious non-phones (too short, likely a date or ID)
        digits_only = re.sub(r"\D", "", candidate)
        if 7 <= len(digits_only) <= 15:
            # Check context isn't "fax"
            start = max(0, match.start() - 20)
            context = text[start:match.start()].lower()
            if "fax" not in context:
                phones.add(candidate)
    return list(phones)


def _detect_contact_forms(html, base_url):
    """Detect contact forms. In-memory only, not persisted."""
    forms = []
    for match in _FORM_PATTERN.finditer(html or ""):
        form_html = match.group(0).lower()
        # Check for contact-form indicators
        has_email_field = 'type="email"' in form_html or 'type=\'email\'' in form_html or 'name="email"' in form_html
        has_message_field = "message" in form_html or "textarea" in form_html or "subject" in form_html
        if has_email_field or has_message_field:
            # Extract action
            action_match = re.search(r'action=["\']([^"\']+)["\']', match.group(0), re.IGNORECASE)
            action = action_match.group(1) if action_match else ""
            if action:
                action = urljoin(base_url, action)
            method_match = re.search(r'method=["\']([^"\']+)["\']', match.group(0), re.IGNORECASE)
            method = (method_match.group(1) if method_match else "POST").upper()
            forms.append({"form_url": base_url, "form_action": action, "method": method})
    return forms


def _get_extraction_confidence(page_type, extraction_source, local_part, generic_prefixes):
    """Heuristic extraction confidence. Not calibrated, not used in ranking."""
    if local_part in generic_prefixes:
        return _EXTRACTION_CONFIDENCE["generic"]
    if extraction_source == "json_ld":
        return _EXTRACTION_CONFIDENCE["json_ld"]
    if extraction_source == "mailto":
        return _EXTRACTION_CONFIDENCE["mailto"]
    if page_type in _HIGH_CONFIDENCE_PAGE_TYPES:
        return _EXTRACTION_CONFIDENCE["visible_high"]
    return _EXTRACTION_CONFIDENCE["visible_mid"]


# ═════════════════════════════════════════════════════════════════════════════
# Source Quality Tiers
# ═════════════════════════════════════════════════════════════════════════════

def _assign_source_quality_tier(source, same_domain):
    """Map source string to Tier 0-3. Advisory metadata only."""
    if not same_domain:
        return 0
    if source in _TIER_3_SOURCES:
        return 3
    if source in _TIER_2_SOURCES:
        return 2
    if source in _TIER_1_SOURCES:
        return 1
    return 0


# ═════════════════════════════════════════════════════════════════════════════
# Main Entry Point
# ═════════════════════════════════════════════════════════════════════════════

def run_site_discovery(*, domain, merchant_domain, existing_emails, target_prefixes, mx_servers, deadline_ts=None):
    """
    Crawl a single merchant domain for contact discovery.

    Returns a CrawlResult dict with structured candidates compatible with
    contact_discovery._merge_candidate, or None if homepage fetch fails.

    `deadline_ts`, when provided, is an absolute `time.time()` cutoff. The
    per-page crawl loop checks it before each fetch and bails early rather
    than blowing the scheduler's task-level SIGALRM timeout.
    """
    # Import contact_discovery functions for candidate building
    try:
        from intelligence.contact_discovery import (
            _build_candidate,
            _visible_email_candidates,
            _mailto_candidates,
            _merge_candidate,
            _candidate_is_high_confidence,
            _candidate_rank_key,
            quick_verify,
            PAGE_TYPE_SOURCE,
            PAGE_TYPE_PRIORITY as CD_PAGE_TYPE_PRIORITY,
            _footer_html,
        )
    except ImportError:
        from runtime.intelligence.contact_discovery import (
            _build_candidate,
            _visible_email_candidates,
            _mailto_candidates,
            _merge_candidate,
            _candidate_is_high_confidence,
            _candidate_rank_key,
            quick_verify,
            PAGE_TYPE_SOURCE,
            PAGE_TYPE_PRIORITY as CD_PAGE_TYPE_PRIORITY,
            _footer_html,
        )

    base_url = f"https://{domain}"
    browser_budget = MAX_BROWSER_RENDERS

    # ── Step 1: Fetch homepage ──────────────────────────────────────────────
    homepage_result, browser_budget = _smart_fetch(base_url, browser_budget)
    if not homepage_result["fetch_ok"] or not homepage_result["html"]:
        logger.info(f"Site discovery: homepage fetch failed for {domain}")
        return None

    homepage_html = homepage_result["html"]
    homepage_render_mode = homepage_result["render_mode"]

    # ── Step 2: Discover pages ──────────────────────────────────────────────
    page_plan, discovery_stats = _discover_pages(base_url, homepage_html, merchant_domain)

    # ── Step 3: Crawl pages and extract contacts ────────────────────────────
    candidate_bucket = {}
    mailto_bucket = {}
    pages_fetched = [{
        "url": base_url,
        "page_type": "homepage",
        "render_mode": homepage_render_mode,
        "discovery_method": "homepage",
    }]
    pages_checked = 1
    page_types_checked = []
    phone_candidates = []
    contact_forms = []
    json_ld_accepted = 0
    json_ld_rejected = 0
    browser_renders_used = 1 if homepage_render_mode == "browser" else 0
    discovery_source_counts = {"homepage": 1}

    # Extract from homepage footer
    footer = _footer_html(homepage_html)
    if footer:
        for candidate in _visible_email_candidates(
            html=footer,
            merchant_domain=merchant_domain,
            page_type="footer_page",
            page_url=base_url,
            crawl_step=CD_PAGE_TYPE_PRIORITY.get("footer_page", 3),
            existing_emails=existing_emails,
            target_prefixes=target_prefixes,
            mx_servers=mx_servers,
        ):
            candidate["render_mode"] = homepage_render_mode
            candidate["source_quality_tier"] = _assign_source_quality_tier(candidate.get("source", ""), candidate.get("same_domain", False))
            candidate["extraction_confidence"] = _get_extraction_confidence("footer_page", "visible", candidate.get("local_part", ""), {"info", "support", "hello", "contact", "sales", "help", "admin", "team", "billing"})
            _merge_candidate(candidate_bucket, candidate)
        if "footer_page" not in page_types_checked:
            page_types_checked.append("footer_page")

    # Homepage mailto extraction
    for candidate in _mailto_candidates(
        html=homepage_html,
        merchant_domain=merchant_domain,
        page_type="contact_page",
        page_url=base_url,
        existing_emails=existing_emails,
        target_prefixes=target_prefixes,
        mx_servers=mx_servers,
    ):
        candidate["render_mode"] = homepage_render_mode
        candidate["source_quality_tier"] = _assign_source_quality_tier("website:mailto", candidate.get("same_domain", False))
        candidate["extraction_confidence"] = _get_extraction_confidence("contact_page", "mailto", candidate.get("local_part", ""), {"info", "support", "hello", "contact", "sales", "help", "admin", "team", "billing"})
        _merge_candidate(mailto_bucket, candidate)

    # Homepage JSON-LD extraction
    jl_contacts, jl_rejected = _extract_json_ld_contacts(homepage_html, merchant_domain)
    json_ld_rejected += jl_rejected
    for jl_contact in jl_contacts:
        candidate = _build_candidate(
            email=jl_contact["email"],
            merchant_domain=merchant_domain,
            page_type="contact_page",
            page_url=base_url,
            crawl_step=1,
            source="website:json_ld",
            context_text=f"{jl_contact.get('name', '')} {jl_contact.get('title', '')}".strip(),
            email_verified=quick_verify(jl_contact["email"], mx_servers) if mx_servers else False,
            target_prefixes=target_prefixes,
        )
        if candidate:
            candidate["render_mode"] = homepage_render_mode
            candidate["source_quality_tier"] = _assign_source_quality_tier("website:json_ld", True)
            candidate["extraction_confidence"] = _EXTRACTION_CONFIDENCE["json_ld"]
            _merge_candidate(candidate_bucket, candidate)
            json_ld_accepted += 1

    # Homepage phones and forms
    phone_candidates.extend(_extract_phone_numbers(homepage_html))
    contact_forms.extend(_detect_contact_forms(homepage_html, base_url))

    # ── Step 4: Crawl discovered pages ──────────────────────────────────────
    for page_info in page_plan:
        if deadline_ts is not None and time.time() >= deadline_ts:
            logger.info(
                f"Site discovery: deadline reached for {domain} after "
                f"{pages_checked} pages — returning partial result"
            )
            break
        page_url = page_info["url"]
        page_type = page_info["page_type"]
        discovery_method = page_info["discovery_method"]

        time.sleep(CRAWL_DELAY)
        fetch_result, browser_budget = _smart_fetch(page_url, browser_budget)
        pages_checked += 1

        if fetch_result["render_mode"] == "browser":
            browser_renders_used += 1

        discovery_source_counts[discovery_method] = discovery_source_counts.get(discovery_method, 0) + 1
        pages_fetched.append({
            "url": page_url,
            "page_type": page_type,
            "render_mode": fetch_result["render_mode"],
            "discovery_method": discovery_method,
        })

        if not fetch_result["fetch_ok"] or not fetch_result["html"]:
            continue

        html = fetch_result["html"]
        render_mode = fetch_result["render_mode"]

        if page_type not in page_types_checked:
            page_types_checked.append(page_type)

        page_source = PAGE_TYPE_SOURCE.get(page_type, "website")
        crawl_step = CD_PAGE_TYPE_PRIORITY.get(page_type, 5)
        generic_prefixes = {"info", "support", "hello", "contact", "sales", "help", "admin", "team", "billing"}

        # Visible email candidates
        page_email_count = 0
        for candidate in _visible_email_candidates(
            html=html,
            merchant_domain=merchant_domain,
            page_type=page_type,
            page_url=page_url,
            crawl_step=crawl_step,
            existing_emails=existing_emails,
            target_prefixes=target_prefixes,
            mx_servers=mx_servers,
        ):
            if page_email_count >= MAX_EMAIL_CANDIDATES_PER_PAGE:
                break
            candidate["render_mode"] = render_mode
            candidate["source_quality_tier"] = _assign_source_quality_tier(page_source, candidate.get("same_domain", False))
            candidate["extraction_confidence"] = _get_extraction_confidence(page_type, "visible", candidate.get("local_part", ""), generic_prefixes)
            _merge_candidate(candidate_bucket, candidate)
            page_email_count += 1

        # Mailto candidates
        for candidate in _mailto_candidates(
            html=html,
            merchant_domain=merchant_domain,
            page_type=page_type,
            page_url=page_url,
            existing_emails=existing_emails,
            target_prefixes=target_prefixes,
            mx_servers=mx_servers,
        ):
            if page_email_count >= MAX_EMAIL_CANDIDATES_PER_PAGE:
                break
            candidate["render_mode"] = render_mode
            candidate["source_quality_tier"] = _assign_source_quality_tier("website:mailto", candidate.get("same_domain", False))
            candidate["extraction_confidence"] = _get_extraction_confidence(page_type, "mailto", candidate.get("local_part", ""), generic_prefixes)
            _merge_candidate(mailto_bucket, candidate)
            page_email_count += 1

        # JSON-LD
        jl_contacts, jl_rejected = _extract_json_ld_contacts(html, merchant_domain)
        json_ld_rejected += jl_rejected
        for jl_contact in jl_contacts:
            if page_email_count >= MAX_EMAIL_CANDIDATES_PER_PAGE:
                break
            candidate = _build_candidate(
                email=jl_contact["email"],
                merchant_domain=merchant_domain,
                page_type=page_type,
                page_url=page_url,
                crawl_step=crawl_step,
                source="website:json_ld",
                context_text=f"{jl_contact.get('name', '')} {jl_contact.get('title', '')}".strip(),
                email_verified=quick_verify(jl_contact["email"], mx_servers) if mx_servers else False,
                target_prefixes=target_prefixes,
            )
            if candidate:
                candidate["render_mode"] = render_mode
                candidate["source_quality_tier"] = _assign_source_quality_tier("website:json_ld", True)
                candidate["extraction_confidence"] = _EXTRACTION_CONFIDENCE["json_ld"]
                _merge_candidate(candidate_bucket, candidate)
                json_ld_accepted += 1
                page_email_count += 1

        # Phones and forms (in-memory only)
        phone_candidates.extend(_extract_phone_numbers(html))
        contact_forms.extend(_detect_contact_forms(html, page_url))

        # Early exit if high-confidence candidate found
        ranked = sorted(candidate_bucket.values(), key=_candidate_rank_key, reverse=True)
        if ranked and _candidate_is_high_confidence(ranked[0]):
            break

    # ── Step 5: Merge mailto bucket ─────────────────────────────────────────
    ranked = sorted(candidate_bucket.values(), key=_candidate_rank_key, reverse=True)
    best = ranked[0] if ranked else {}
    if not best or not _candidate_is_high_confidence(best):
        for candidate in mailto_bucket.values():
            _merge_candidate(candidate_bucket, candidate)

    all_candidates = list(candidate_bucket.values())

    # Dedupe phones
    phone_candidates = list(set(phone_candidates))

    # ── Log discovery summary ───────────────────────────────────────────────
    logger.info(
        f"Site discovery complete: domain={domain} "
        f"pages={pages_checked} browser_renders={browser_renders_used}/{MAX_BROWSER_RENDERS} "
        f"candidates={len(all_candidates)} phones={len(phone_candidates)} forms={len(contact_forms)} "
        f"sitemap_considered={discovery_stats['sitemap_urls_considered']} "
        f"sitemap_accepted={discovery_stats['sitemap_urls_accepted']} "
        f"careers_rejected={discovery_stats['careers_pages_rejected']} "
        f"json_ld_accepted={json_ld_accepted} json_ld_rejected={json_ld_rejected}"
    )

    return {
        "pages_fetched": pages_fetched,
        "email_candidates": all_candidates,
        "phone_candidates": phone_candidates,
        "contact_forms": contact_forms,
        "pages_checked": pages_checked,
        "page_types_checked": page_types_checked,
        "browser_renders_used": browser_renders_used,
        "sitemap_urls_considered": discovery_stats["sitemap_urls_considered"],
        "sitemap_urls_accepted": discovery_stats["sitemap_urls_accepted"],
        "careers_pages_rejected": discovery_stats["careers_pages_rejected"],
        "json_ld_contacts_accepted": json_ld_accepted,
        "json_ld_contacts_rejected": json_ld_rejected,
        "homepage_fetch_ok": True,
        "discovery_source_counts": discovery_source_counts,
    }
