"""
Deterministic merchant domain discovery without LLMs.

Resolves merchant brands to domains by:
1. Expanding hosted-platform patterns when the platform is known.
2. Searching the web for the official site.
3. Validating discovered domains via DNS.
"""
import os
import re
import sys
from urllib.parse import urlparse

import dns.resolver
from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event
from tools.web_search import web_search

logger = get_logger("domain_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

DISCOVERY_BATCH_SIZE = 50
SEARCH_CONFIDENCE = 0.7
PLATFORM_CONFIDENCE = 0.8
MX_BONUS = 0.1
REJECTED_RESULT_DOMAINS = {
    # Social / discussion
    "facebook.com", "instagram.com", "reddit.com", "redd.it",
    "trustpilot.com", "twitter.com", "x.com", "yelp.com",
    "linkedin.com", "tiktok.com", "pinterest.com", "tumblr.com",
    "discord.com", "discord.gg", "slack.com", "telegram.org",
    "medium.com", "substack.com", "wordpress.com", "blogger.com",
    "quora.com", "producthunt.com", "news.ycombinator.com",
    # Code / dev
    "github.com", "gitlab.com", "bitbucket.org", "stackoverflow.com",
    "stackexchange.com", "npmjs.com", "pypi.org",
    # Big tech / platforms
    "google.com", "youtube.com", "apple.com", "microsoft.com",
    "amazon.com", "ebay.com", "etsy.com", "walmart.com", "target.com",
    "docs.google.com", "drive.google.com", "support.google.com",
    # Payment processors (not merchants)
    "stripe.com", "paypal.com", "squareup.com", "authorize.net",
    "braintreegateway.com", "adyen.com", "2checkout.com",
    "wise.com", "revolut.com", "cash.app",
    # E-commerce platforms
    "shopify.com", "bigcommerce.com", "woocommerce.com",
    "squarespace.com", "wix.com", "godaddy.com",
    # Archives / reference
    "archive.org", "web.archive.org", "wikipedia.org", "wikimedia.org",
    "en.wikipedia.org", "fandom.com",
    # URL shorteners
    "t.co", "bit.ly", "goo.gl", "tinyurl.com", "youtu.be",
    # Government / education
    "usa.gov", "gov.uk", "canada.ca", "europa.eu",
    # Media / news
    "nytimes.com", "bbc.com", "cnn.com", "reuters.com",
    "techcrunch.com", "theverge.com", "arstechnica.com",
    # Misc large platforms
    "patreon.com", "ko-fi.com", "gumroad.com", "itch.io",
    "booking.com", "airbnb.com", "hulu.com", "netflix.com",
    "mint.com", "nerdwallet.com", "capitalone.com",
    "indeed.com", "glassdoor.com", "crunchbase.com",
}

# Patterns that indicate a non-merchant domain
_NON_MERCHANT_DOMAIN_PATTERNS = re.compile(
    r'(?:'
    r'\.gov(?:\.[a-z]{2})?$'       # Government (.gov, .gov.uk)
    r'|\.edu(?:\.[a-z]{2})?$'      # Education
    r'|\.mil$'                      # Military
    r'|^(?:preview|staging|test|dev|api|cdn|static|docs|help|support|blog|news|mail|email)\.'
    r'|redirecttocheckout'          # Stripe test
    r'|^(?:sellercentral|seller)\.' # Amazon seller
    r'|\.jobs$'                     # Job sites
    r')',
    re.IGNORECASE,
)
BRAND_REJECT_TOKENS = {
    "account",
    "accounts",
    "amazon",
    "bigcommerce",
    "business",
    "ebay",
    "etsy",
    "gateway",
    "gumroad",
    "kajabi",
    "mastercard",
    "merchant",
    "merchants",
    "news",
    "online",
    "payment",
    "payments",
    "paypal",
    "shopify",
    "square",
    "stripe",
    "substack",
    "visa",
    "woocommerce",
}

PLATFORM_PATTERNS = {
    "shopify": lambda slug: f"{slug}.myshopify.com",
    "etsy": lambda slug: f"etsy.com/shop/{slug}",
    "gumroad": lambda slug: f"gumroad.com/{slug}",
    "substack": lambda slug: f"{slug}.substack.com",
    "kajabi": lambda slug: f"{slug}.kajabi.com",
    "bigcommerce": lambda slug: f"{slug}.mybigcommerce.com",
}


def discover_domain_for_brand(brand, platform=None):
    """
    Resolve a merchant brand to a domain deterministically.

    Returns:
    {
      "domain": str,
      "confidence": float,
      "source": "platform_pattern" | "search_result"
    }
    """
    if not brand:
        return None
    if not _is_viable_brand(brand):
        return None

    normalized_platform = (platform or "").strip().lower() or None
    for candidate in _platform_candidates(brand, normalized_platform):
        verified = _verify_candidate(candidate, PLATFORM_CONFIDENCE)
        if verified:
            verified["source"] = "platform_pattern"
            return verified

    queries = [
        f"\"{brand}\" official site",
        brand,
        f"{brand} store",
        f"{brand} company",
        f"{brand} shop"
    ]

    for query in queries:
        try:
            results = web_search(query)
        except Exception as exc:
            logger.warning(f"Search lookup failed for query '{query}': {exc}")
            continue

        for candidate in _search_result_candidates(results, brand):
            verified = _verify_candidate(candidate, SEARCH_CONFIDENCE)
            if verified:
                verified["source"] = "search_result"
                return verified

    return None


def persist_discovered_domain(conn, merchant_id, brand, platform=None):
    """
    Discover and persist a missing merchant domain.
    """
    discovery = discover_domain_for_brand(brand, platform)
    if not discovery:
        return None

    normalized = normalize_discovered_domain(discovery["domain"])
    domain_confidence = "confirmed" if discovery["confidence"] >= 0.8 else "provisional"
    updated = conn.execute(text("""
        UPDATE merchants
        SET domain = :domain,
            normalized_domain = :normalized_domain,
            domain_confidence = :domain_confidence,
            last_seen = CURRENT_TIMESTAMP
        WHERE id = :merchant_id
          AND (domain IS NULL OR domain = '')
    """), {
        "domain": discovery["domain"],
        "normalized_domain": normalized,
        "domain_confidence": domain_confidence,
        "merchant_id": merchant_id,
    }).rowcount

    if not updated:
        return None

    discovery["domain_confidence"] = domain_confidence
    save_event("domain_discovered", {
        "merchant_id": merchant_id,
        "brand": brand,
        "domain": discovery["domain"],
        "confidence": discovery["confidence"],
        "source": discovery["source"],
        "platform": platform,
        "domain_confidence": domain_confidence,
    })
    logger.info(
        f"Domain discovered for merchant {merchant_id}: "
        f"{brand} -> {discovery['domain']} ({discovery['source']}, {discovery['confidence']})"
    )
    return discovery


def run_domain_discovery(limit=DISCOVERY_BATCH_SIZE):
    """
    Scheduler entry point for merchants missing domains.
    """
    resolved = 0
    platform_resolved = 0
    search_resolved = 0
    failures = 0

    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT id, canonical_name
            FROM merchants
            WHERE (domain IS NULL OR domain = '')
              AND distress_score >= 5
            ORDER BY distress_score DESC, last_seen DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

        for merchant_id, brand in merchants:
            try:
                platform = infer_platform_for_merchant(conn, merchant_id)
                inferred_brand = infer_brand_for_merchant(conn, merchant_id, brand)
                discovery = persist_discovered_domain(conn, merchant_id, inferred_brand, platform)
                if discovery:
                    resolved += 1
                    if discovery["source"] == "platform_pattern":
                        platform_resolved += 1
                    elif discovery["source"] == "search_result":
                        search_resolved += 1
            except Exception as exc:
                logger.warning(f"Domain discovery failed for merchant {merchant_id}: {exc}")
                failures += 1

        conn.commit()

    result = {
        "merchants_processed": len(merchants),
        "domains_discovered": resolved,
        "platform_domains_resolved": platform_resolved,
        "search_domains_resolved": search_resolved,
        "failures": failures,
    }
    logger.info(f"Domain discovery run complete: {result}")
    return result


def infer_platform_for_merchant(conn, merchant_id):
    """
    Infer platform from recent merchant-linked signals.
    """
    try:
        from runtime.intelligence.merchant_attribution import detect_platform
    except ImportError:
        from merchant_attribution import detect_platform

    try:
        from runtime.intelligence.merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )
    except ImportError:
        from merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )

    rows = conn.execute(text("""
        SELECT content
        FROM signals
        WHERE merchant_id = :merchant_id
        ORDER BY detected_at DESC
        LIMIT 5
    """), {"merchant_id": merchant_id}).fetchall()
    combined = "\n".join(
        row[0]
        for row in rows
        if row[0] and classify_merchant_signal(row[0])["classification"] == CLASS_MERCHANT_OPERATOR
    )
    if not combined:
        return None
    return detect_platform(combined)


def infer_brand_for_merchant(conn, merchant_id, fallback_brand=None):
    """
    Re-extract a likely merchant brand from recent signals before falling back
    to the stored canonical name.
    """
    try:
        from runtime.intelligence.brand_extraction import extract_brand
    except ImportError:
        from brand_extraction import extract_brand
    try:
        from runtime.intelligence.merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )
    except ImportError:
        from merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )

    rows = conn.execute(text("""
        SELECT content, merchant_name
        FROM signals
        WHERE merchant_id = :merchant_id
        ORDER BY detected_at DESC
        LIMIT 5
    """), {"merchant_id": merchant_id}).fetchall()

    for row in rows:
        content = row[0] or ""
        if classify_merchant_signal(content)["classification"] != CLASS_MERCHANT_OPERATOR:
            continue
        brand = extract_brand(content, merchant_name=row[1])
        if brand:
            return brand
    return fallback_brand


def normalize_discovered_domain(domain):
    """
    Normalize a discovered domain or hosted-platform path.
    """
    if not domain:
        return None

    cleaned = domain.lower().strip()
    if "://" in cleaned:
        parsed = urlparse(cleaned)
        cleaned = parsed.netloc or parsed.path

    host, _, path = cleaned.partition("/")
    host = host.rstrip(".")

    parts = host.split(".")
    while len(parts) > 2 and parts[0] in {"www", "shop", "app", "store", "my", "checkout", "pay", "m"}:
        parts.pop(0)
    normalized_host = ".".join(parts)

    if path:
        return f"{normalized_host}/{path.strip('/')}"
    return normalized_host


def _platform_candidates(brand, platform):
    if not platform or platform not in PLATFORM_PATTERNS:
        return []

    candidates = []
    for slug in _brand_slugs(brand):
        candidates.append(PLATFORM_PATTERNS[platform](slug))
    return list(dict.fromkeys(candidates))


_GENERIC_SINGLE_WORDS = {
    "about", "account", "across", "admin", "after", "again", "alert",
    "alternative", "alternatives", "anyone", "archive", "back", "been",
    "before", "below", "beware", "blog", "booking", "canada", "capital",
    "checkout", "click", "context", "credit", "demo", "docs", "down",
    "download", "dropship", "dropshipping", "each", "ecommerce", "email",
    "entrepreneur", "error", "every", "first", "from", "getting", "going",
    "hello", "help", "here", "home", "hosting", "however", "india", "info",
    "into", "issue", "issues", "jobs", "just", "keep", "last", "lessons",
    "like", "link", "login", "looking", "made", "make", "many", "model",
    "money", "more", "most", "much", "need", "never", "next", "none",
    "nothing", "one", "only", "other", "over", "page", "please", "presents",
    "preview", "pricing", "problem", "problems", "question", "questions",
    "recently", "review", "reviews", "said", "same", "search", "seller",
    "sells", "service", "services", "side", "since", "small", "software",
    "someone", "something", "staging", "started", "startups", "status",
    "still", "store", "stores", "support", "test", "their", "there",
    "through", "today", "tools", "turning", "under", "update", "upon",
    "usa", "using", "very", "want", "warning", "web", "while", "zero",
}


def _is_viable_brand(brand):
    tokens = re.findall(r"[a-z0-9]+", brand.lower())
    if not tokens:
        return False

    meaningful_tokens = [token for token in tokens if token not in BRAND_REJECT_TOKENS]
    if not meaningful_tokens or len("".join(meaningful_tokens)) < 3:
        return False

    # Reject single generic words that are never real brand names
    if len(meaningful_tokens) == 1 and meaningful_tokens[0] in _GENERIC_SINGLE_WORDS:
        return False

    return True


def _brand_slugs(brand):
    cleaned = re.sub(r"[&+]", " and ", brand.lower())
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned).strip()
    if not cleaned:
        return []

    words = [word for word in cleaned.split() if word]
    if not words:
        return []

    joined = "".join(words)
    hyphenated = "-".join(words)
    return list(dict.fromkeys([joined, hyphenated]))


def _search_result_candidates(results, brand):
    items = []
    if isinstance(results, dict):
        web_results = results.get("web", {})
        if isinstance(web_results, dict):
            items = web_results.get("results", []) or []
        elif isinstance(results.get("results"), list):
            items = results["results"]

    allowed_slugs = {slug.replace("-", "") for slug in _brand_slugs(brand)}
    candidates = []
    for item in items:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        candidate = _candidate_from_url(url, allowed_slugs)
        if candidate:
            candidates.append(candidate)
    return list(dict.fromkeys(candidates))


def _candidate_from_url(url, allowed_slugs):
    if not url:
        return None

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower().strip()
    if not host:
        return None

    if host.startswith("www."):
        host = host[4:]

    # Reject known non-merchant domains
    if any(host == blocked or host.endswith(f".{blocked}") for blocked in REJECTED_RESULT_DOMAINS):
        return None

    # Reject domains matching non-merchant patterns (.gov, .edu, preview.*, etc.)
    if _NON_MERCHANT_DOMAIN_PATTERNS.search(host):
        return None

    host_slug = re.sub(r"[^a-z0-9]", "", host)
    if allowed_slugs and not any(slug and slug in host_slug for slug in allowed_slugs):
        return None

    return host


_WILDCARD_HOSTED_SUFFIXES = {
    "myshopify.com",
    "mybigcommerce.com",
    "kajabi.com",
    "substack.com",
}


def _verify_candidate(domain, base_confidence):
    host = _host_for_dns(domain)
    if not host:
        return None

    # For hosted platforms with wildcard DNS, DNS alone proves nothing.
    # We need to check if the store actually exists via HTTP.
    if any(host.endswith(f".{suffix}") or host == suffix for suffix in _WILDCARD_HOSTED_SUFFIXES):
        if not _hosted_store_exists(domain):
            return None
    else:
        if not _has_a_record(host):
            return None

    confidence = base_confidence
    if _has_mx_record(host):
        confidence += MX_BONUS

    return {
        "domain": domain,
        "confidence": round(min(confidence, 1.0), 2),
    }


def _hosted_store_exists(domain):
    """Check if a hosted-platform store actually exists (not a wildcard 404)."""
    import httpx

    url = f"https://{domain}"
    try:
        with httpx.Client(timeout=6, follow_redirects=True, verify=False) as client:
            resp = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            # Shopify returns 200 for real stores, but also 200 for their
            # "store not found" page. Check for Shopify 404 indicators.
            if resp.status_code == 404:
                return False
            if resp.status_code >= 500:
                return False
            # Check for password-protected / placeholder stores
            final_url = str(resp.url).lower()
            if "/password" in final_url:
                return False
            body = resp.text[:3000].lower()
            # Shopify dead-store indicators
            if "store is currently unavailable" in body:
                return False
            if "only store staff can see this" in body:
                return False
            if "sorry, this shop is currently unavailable" in body:
                return False
            if "this store is unavailable" in body:
                return False
            # Shopify password / placeholder pages
            if "please log in" in body and "password" in body:
                return False
            if "opening soon" in body and "password" in body:
                return False
            if "coming soon" in body:
                return False
            # Shopify 404 title
            if "create an ecommerce website" in body and "shopify" in body:
                return False
            # Substack non-existent
            if "page not found" in body and "substack" in body:
                return False
            return True
    except Exception:
        return False


def _host_for_dns(domain):
    if not domain:
        return None

    cleaned = domain
    if "://" in cleaned:
        parsed = urlparse(cleaned)
        cleaned = parsed.netloc or parsed.path

    host, _, _ = cleaned.partition("/")
    if host.startswith("www."):
        host = host[4:]
    return host.rstrip(".").lower() or None


def _has_a_record(host):
    try:
        answers = dns.resolver.resolve(host, "A")
        return len(answers) > 0
    except Exception:
        return False


def _has_mx_record(host):
    try:
        answers = dns.resolver.resolve(host, "MX")
        return len(answers) > 0
    except Exception:
        return False
