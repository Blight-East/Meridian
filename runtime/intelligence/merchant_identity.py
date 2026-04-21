"""
Merchant Identity Resolution Module
Extracts domains, company names, and resolves canonical merchant identities
from distress signal text. Maintains the merchants table and links signals.
"""
import sys, os

# Dynamically find project root (walk up until we find config/)
_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, 'config')):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import re
import threading
import json
from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event, save_learning_feedback

logger = get_logger("merchant_identity")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
_SCHEMA_READY = False

try:
    from merchant_canonicalization import check_and_log_canonical_match
except ImportError:
    try:
        from runtime.intelligence.merchant_canonicalization import check_and_log_canonical_match
    except ImportError:
        check_and_log_canonical_match = None

# Import attribution module for contextual inference fallback
try:
    from merchant_attribution import attribute_merchant, detect_platform, detect_processor
    _HAS_ATTRIBUTION = True
except ImportError:
    _HAS_ATTRIBUTION = False
    detect_platform = None
    detect_processor = None

try:
    from domain_discovery import discover_domain_for_brand
except ImportError:
    from runtime.intelligence.domain_discovery import discover_domain_for_brand

try:
    from brand_extraction import extract_brand_candidates
except ImportError:
    from runtime.intelligence.brand_extraction import extract_brand_candidates

try:
    from merchant_slug import build_merchant_slug, ensure_merchant_slug_guard
except ImportError:
    from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard

try:
    from runtime.intelligence.merchant_signal_classifier import (
        CLASS_MERCHANT_OPERATOR,
        classify_merchant_signal,
    )
except ImportError:
    from merchant_signal_classifier import CLASS_MERCHANT_OPERATOR, classify_merchant_signal

try:
    from runtime.intelligence.merchant_validation import is_valid_merchant_name, compute_merchant_confidence
except ImportError:
    from merchant_validation import is_valid_merchant_name, compute_merchant_confidence

try:
    from runtime.intelligence.reranker import score_identity_candidates
except ImportError:
    try:
        from reranker import score_identity_candidates
    except ImportError:
        score_identity_candidates = None

# Identity confidence thresholds
IDENTITY_CANDIDATE_THRESHOLD = 25
IDENTITY_FULL_THRESHOLD = 60

# Import contact discovery to run background jobs
try:
    from contact_discovery import discover_contacts_for_merchant
    _HAS_CONTACT_DISCOVERY = True
except ImportError:
    try:
        import intelligence.contact_discovery as contact_discovery
        discover_contacts_for_merchant = contact_discovery.discover_contacts_for_merchant
        _HAS_CONTACT_DISCOVERY = True
    except ImportError:
        _HAS_CONTACT_DISCOVERY = False

def enqueue_contact_discovery(merchant_id):
    """Run contact discovery asynchronously to prevent blocking the worker thread."""
    if _HAS_CONTACT_DISCOVERY:
        threading.Thread(target=discover_contacts_for_merchant, args=(merchant_id,), daemon=True).start()


# ── Domain extraction ──────────────────────────────────────────────
_DOMAIN_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?'
    r'(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,})',
    re.IGNORECASE
)

# Domains to ignore — not merchant sites
_IGNORE_DOMAINS = {
    # Social / discussion (including subdomains)
    "reddit.com", "redd.it", "old.reddit.com", "preview.redd.it",
    "np.reddit.com", "i.reddit.com", "v.redd.it",
    "news.ycombinator.com", "ycombinator.com", "hackernews.com",
    "github.com", "gitlab.com", "bitbucket.org", "gist.github.com",
    "google.com", "youtube.com", "twitter.com", "x.com",
    "facebook.com", "instagram.com", "linkedin.com", "tiktok.com",
    "discord.com", "discord.gg", "slack.com", "telegram.org",
    "medium.com", "substack.com", "wordpress.com", "blogger.com",
    "stackoverflow.com", "stackexchange.com", "quora.com",
    "imgur.com", "pastebin.com", "producthunt.com",
    # Payment processors (not merchants)
    "stripe.com", "paypal.com", "squareup.com", "authorize.net",
    "braintreegateway.com", "adyen.com", "wise.com", "revolut.com",
    # E-commerce platforms
    "shopify.com", "bigcommerce.com", "woocommerce.com",
    "squarespace.com", "wix.com",
    # Big tech
    "amazon.com", "ebay.com", "etsy.com",
    "apple.com", "microsoft.com",
    "docs.google.com", "drive.google.com", "support.google.com",
    "sellercentral.amazon.com", "services.amazon.fr", "amazon.jobs",
    # Reference / archives
    "archive.org", "web.archive.org", "wikipedia.org",
    "en.wikipedia.org", "donate.wikimedia.org", "fandom.com",
    # Government
    "usa.gov", "gov.uk", "canada.ca", "parks.canada.ca",
    # URL shorteners
    "t.co", "bit.ly", "goo.gl", "tinyurl.com", "youtu.be",
    # Misc non-merchants
    "patreon.com", "ko-fi.com", "itch.io", "mint.com",
    "capitalone.com", "booking.com", "hulu.com",
    "niso.org", "about.youtube", "paypal.me",
}

# Pattern-based domain rejection (catches subdomains dynamically)
_IGNORE_DOMAIN_PATTERNS = re.compile(
    r'(?:'
    r'\.gov(?:\.[a-z]{2})?$'
    r'|\.edu(?:\.[a-z]{2})?$'
    r'|\.mil$'
    r'|^(?:preview|staging|test|dev|api|cdn|static|docs|help|support|blog|news|mail|email|accounts|hello|alert|click|tracking)\.'
    r'|redirecttocheckout'
    r'|^(?:sellercentral|seller|services)\.'
    r')',
    re.IGNORECASE,
)

# Subdomains to strip during normalization
_STRIP_SUBDOMAINS = {"www", "shop", "app", "store", "my", "checkout", "pay", "m"}
_TRACKING_SUBDOMAIN_LABELS = {
    "alert",
    "alerts",
    "click",
    "clicks",
    "email",
    "em",
    "link",
    "links",
    "mail",
    "mailer",
    "mkt",
    "news",
    "notify",
    "public",
    "public-usa",
    "tracking",
    "trk",
}
_HOSTED_PLATFORM_SUFFIXES = {
    "myshopify.com",
    "substack.com",
    "kajabi.com",
    "mybigcommerce.com",
}
_GENERIC_DOMAIN_LABELS = {
    "account",
    "alert",
    "alerts",
    "business",
    "click",
    "clicks",
    "company",
    "customer",
    "email",
    "mail",
    "merchant",
    "newsletter",
    "notification",
    "payments",
    "public",
    "shop",
    "store",
    "support",
}

# ── Company name extraction ────────────────────────────────────────
# Pattern: 2-3 capitalized words (require at least 2 to reduce false positives)
_COMPANY_PATTERN = re.compile(
    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b'
)

# Individual words to ignore when ALL words in the match belong to this set
_IGNORE_WORDS = {
    "The", "This", "That", "They", "Their", "There", "Then", "When",
    "What", "Where", "Which", "After", "Before", "During", "Since",
    "Because", "Just", "Still", "Also", "Even", "Already", "Been",
    "Have", "Has", "Had", "Got", "Get", "Can", "Could", "Would",
    "Should", "Will", "May", "Might", "Must", "Need", "Want",
    "Like", "Some", "Any", "All", "Every", "Each", "Many", "Much",
    "More", "Most", "Very", "Really", "Now", "Today", "Yesterday",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "June", "July",
    "August", "September", "October", "November", "December",
    "Update", "Edit", "Rant", "Help", "Question", "Anyone",
    "Does", "How", "Why", "Please", "Thanks", "Sorry",
    "Reddit", "Stripe", "PayPal", "Square", "Shopify", "Adyen",
    "Braintree", "Worldpay", "Rakuten",
    # Additional common non-merchant words
    "Here", "For", "Ask", "Tell", "Don", "About", "Sort",
    "Are", "Not", "But", "And", "Or", "Was", "Were", "Being",
    "New", "Old", "My", "Your", "Our", "Me", "We", "You",
    "His", "Her", "Its", "Who", "Whom", "Whose",
    "Over", "Under", "With", "From", "Into", "Through",
    "Seems", "Looks", "Says", "Said", "Think", "Thought",
    "Dark", "Side", "Power", "Wearing", "Alarm", "Bell",
    "Delayed", "Banned", "Frozen", "Closed", "Disabled",
    "Account", "Payment", "Payout", "Merchant", "Bank",
    "Credit", "Card", "Processing", "Processor", "Processors",
    "Services", "Service", "Digital", "Online",
    "Preview", "Design", "Facial", "Recognition", "Verification",
    "Humans", "Memes", "Dog", "Tea", "Brick", "Break",
    "Domain", "Lovers", "Blacklisted", "Payments",
}

# Full phrase blacklist
_IGNORE_PHRASES = {
    "Payment Processors", "Credit Card Processing", "Merchant Services",
    "Digital Services", "Payment Services", "Merchant Account",
    "Bank Account", "Shopify Payments", "Delayed Payout",
    "Design Preview", "Power Armor",
}


def extract_domains(text_content):
    """
    Extract merchant domains from signal text.
    Returns list of normalized domain strings, filtered for merchant relevance.
    """
    if not text_content:
        return []

    raw_matches = _DOMAIN_PATTERN.findall(text_content)
    domains = []
    for match in raw_matches:
        normalized = normalize_domain(match)
        if not normalized or normalized in _IGNORE_DOMAINS:
            continue
        # Reject domains matching non-merchant patterns
        if _IGNORE_DOMAIN_PATTERNS.search(normalized):
            continue
        # Must have at least one dot and look like a real domain
        if "." in normalized and len(normalized) > 4:
            domains.append(normalized)

    return list(dict.fromkeys(domains))  # dedupe preserving order


def extract_company_names(text_content):
    """
    Extract potential company names from signal text.
    Returns list of candidate company name strings (2+ word phrases only).
    """
    if not text_content:
        return []

    matches = _COMPANY_PATTERN.findall(text_content)
    names = []
    for name in matches:
        # Filter out known non-merchant phrases
        if name in _IGNORE_PHRASES:
            continue
        # Filter out if ALL words are in the ignore set
        words = name.split()
        if all(w in _IGNORE_WORDS for w in words):
            continue
        # Filter out very short names
        if len(name) < 5:
            continue
        names.append(name)

    return list(dict.fromkeys(names))  # dedupe preserving order


def normalize_domain(domain):
    """
    Normalize a domain: lowercase, strip known subdomains, remove trailing dots.
    """
    if not domain:
        return None

    domain = domain.lower().strip().rstrip(".")

    # Remove protocol if somehow still present
    domain = re.sub(r'^https?://', '', domain)
    host, _, path = domain.partition("/")

    # Strip known subdomains
    parts = host.split(".")
    while len(parts) > 2 and parts[0] in _STRIP_SUBDOMAINS:
        parts.pop(0)

    normalized_host = ".".join(parts)
    if path:
        return f"{normalized_host}/{path.strip('/')}"
    return normalized_host


def _domain_to_name(domain):
    """
    Derive a readable canonical name from a domain.
    e.g. 'example-store.com' → 'Example Store'
    """
    if not domain:
        return None
    base = domain.split("/")[0].split(".")[0]
    # Replace hyphens/underscores with spaces, title-case
    name = base.replace("-", " ").replace("_", " ").title()
    return name


def _root_host(domain):
    host = str(domain or "").split("/")[0].strip().lower()
    labels = [label for label in host.split(".") if label]
    if len(labels) <= 2:
        return host
    return ".".join(labels[-2:])


def _hosted_platform_domain(domain: str | None) -> bool:
    host = str(domain or "").split("/")[0].strip().lower()
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in _HOSTED_PLATFORM_SUFFIXES)


def _looks_like_tracking_subdomain(domain: str | None) -> bool:
    host = str(domain or "").split("/")[0].strip().lower()
    labels = [label for label in host.split(".") if label]
    if len(labels) < 3 or _hosted_platform_domain(host):
        return False
    return any(label in _TRACKING_SUBDOMAIN_LABELS for label in labels[:-2])


def _generic_domain_name(domain: str | None) -> bool:
    base = str(domain or "").split("/")[0].split(".")[0].strip().lower()
    return base in _GENERIC_DOMAIN_LABELS


def _identity_candidate_domains(domains: list[str]) -> list[str]:
    normalized_domains: list[str] = []
    seen: set[str] = set()
    for domain in domains or []:
        normalized = normalize_domain(domain)
        if not normalized:
            continue
        candidate = normalized
        if _looks_like_tracking_subdomain(candidate):
            root = _root_host(candidate)
            if root and root != candidate:
                candidate = root
        if _generic_domain_name(candidate):
            continue
        if candidate not in seen:
            seen.add(candidate)
            normalized_domains.append(candidate)
    return normalized_domains


_PROCESSOR_DISPLAY_NAMES = {
    "2checkout": "2Checkout",
    "authorize.net": "Authorize.net",
    "paypal": "PayPal",
    "square": "Square",
    "stripe": "Stripe",
}
_GENERIC_DISTRESSED_MERCHANT = "Unknown Distressed Merchant"


def _unknown_processor_merchant_name(processor):
    if not processor:
        return None
    display = _PROCESSOR_DISPLAY_NAMES.get(processor.lower(), processor.replace("_", " ").title())
    return f"Unknown {display} Merchant"


def _ensure_merchant_identity_schema(force: bool = False) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS evidence JSONB DEFAULT '{}'::jsonb"))
        conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS verification_reason TEXT"))
        conn.commit()
    _SCHEMA_READY = True


def resolve_merchant_identity(
    signal_id,
    content,
    priority_score=0,
    author=None,
    merchant_name=None,
    *,
    allow_generic_fallback: bool = False,
):
    """
    Main entry point: resolve a signal's text to a canonical merchant identity.

    1. Extract domains and company names from content
    2. Look up or create merchant in DB
    3. Update distress_score
    4. Link signal via merchant_signals

    Returns: merchant_id (int) or None
    """
    if not content:
        return None

    _ensure_merchant_identity_schema()
    ensure_merchant_slug_guard()

    domains = _identity_candidate_domains(extract_domains(content))
    brand_candidates = extract_brand_candidates(content, merchant_name=merchant_name, author=author)
    company_names = [candidate["brand"] for candidate in brand_candidates]
    platform = detect_platform(content) if detect_platform else None
    attribution = None
    discovered_domain = None
    discovered_domain_source = None
    discovered_domain_confidence = None
    primary_domain = None
    reranker_scores = {"domain_scores": {}, "brand_scores": {}, "recommendation": None}
    discovered_domain_confidence = "confirmed"
    fallback_merchant_name = None
    signal_classification = classify_merchant_signal(content)
    stored_signal_classification = None

    if signal_id:
        with engine.connect() as conn:
            stored_signal_classification = conn.execute(text("""
                SELECT classification
                FROM signals
                WHERE id = :sid
            """), {"sid": signal_id}).scalar()

    # Strategy 0: Attribution fallback — infer from context when no explicit identifiers
    if not domains and not company_names and _HAS_ATTRIBUTION:
        try:
            attribution = attribute_merchant(content, author=author)
            if attribution and attribution.get("confidence", 0) >= 0.3:
                # If attribution resolved a domain, use it as if extracted
                if attribution.get("domain"):
                    domains = _identity_candidate_domains([attribution["domain"]])
                # If attribution found a brand, use it as a company name
                if attribution.get("brand"):
                    company_names = [attribution["brand"]]
                if attribution.get("platform"):
                    platform = attribution["platform"]
                    
                # Capture domain confidence if provided
                discovered_domain_confidence = attribution.get("domain_confidence", "confirmed")
                
                logger.info(
                    f"Attribution fallback for signal {signal_id}: "
                    f"brand={attribution.get('brand')}, domain={attribution.get('domain')}, "
                    f"platform={attribution.get('platform')}, confidence={attribution.get('confidence')}"
                )
        except Exception as e:
            logger.warning(f"Attribution fallback failed for signal {signal_id}: {e}")

    if not domains and company_names:
        for brand in company_names:
            discovery = discover_domain_for_brand(brand, platform)
            if not discovery:
                continue
            discovered_domain = discovery["domain"]
            discovered_domain_source = discovery["source"]
            discovered_domain_confidence = "confirmed" if discovery["confidence"] >= 0.8 else "provisional"
            domains = _identity_candidate_domains([discovered_domain])
            logger.info(
                f"Domain discovery for signal {signal_id}: "
                f"brand={brand}, domain={discovered_domain}, source={discovery['source']}"
            )
            break

    if not domains and not company_names:
        processor = detect_processor(content) if detect_processor else None
        is_merchant_distress = (
            signal_classification["classification"] == CLASS_MERCHANT_OPERATOR
            or stored_signal_classification == "merchant_distress"
        )
        if is_merchant_distress and processor:
            fallback_merchant_name = _unknown_processor_merchant_name(processor)
        elif is_merchant_distress:
            fallback_merchant_name = _GENERIC_DISTRESSED_MERCHANT

        if fallback_merchant_name:
            if not allow_generic_fallback:
                logger.info(
                    f"Skipping generic fallback merchant creation for signal {signal_id}: "
                    f"{fallback_merchant_name}"
                )
                return None
            company_names = [fallback_merchant_name]
            logger.info(
                f"Fallback merchant for signal {signal_id}: "
                f"{fallback_merchant_name}"
            )

    if not domains and not company_names:
        return None

    merchant_id = None

    try:
        with engine.connect() as conn:
            signal_row = conn.execute(text("""
                SELECT merchant_name FROM signals WHERE id = :sid
            """), {"sid": signal_id}).fetchone()
            current_signal_name = signal_row[0] if signal_row else None

            if company_names:
                chosen_brand = company_names[0]
                if not current_signal_name or current_signal_name in ("", "unknown"):
                    conn.execute(text("""
                        UPDATE signals SET merchant_name = :brand WHERE id = :sid
                    """), {"brand": chosen_brand, "sid": signal_id})
                    if fallback_merchant_name and chosen_brand == fallback_merchant_name:
                        save_event("merchant_created_from_processor_fallback", {
                            "signal_id": signal_id,
                            "merchant_name": chosen_brand,
                            "processor": detect_processor(content) if detect_processor else None,
                        })
                    else:
                        save_event("merchant_brand_extracted", {
                            "signal_id": signal_id,
                            "brand": chosen_brand,
                            "platform": platform,
                        })

            # Pass 5: Reranker advisor (Read-only)
            reranker_scores = None
            if score_identity_candidates:
                try:
                    reranker_scores = score_identity_candidates(signal_id, domains, company_names)
                    if reranker_scores.get("recommendation"):
                        logger.info(f"Reranker advisory for signal {signal_id}: {reranker_scores['recommendation']}")
                except Exception as re:
                    logger.warning(f"Reranker advisory failed: {re}")

            # Strategy 1: Match by normalized domain
            for domain in domains:
                normalized = normalize_domain(domain)
                row = conn.execute(text("""
                    SELECT id FROM merchants
                    WHERE normalized_domain = :nd OR domain = :d
                    LIMIT 1
                """), {"nd": normalized, "d": domain}).fetchone()

                if row:
                    merchant_id = row[0]
                    break

            # Strategy 2: Match by canonical name (from company name extraction)
            if not merchant_id and company_names:
                for name in company_names:
                    merchant_slug = build_merchant_slug(name)
                    row = conn.execute(text("""
                        SELECT id FROM merchants
                        WHERE slug = :slug OR LOWER(canonical_name) = LOWER(:name)
                        LIMIT 1
                    """), {"name": name, "slug": merchant_slug}).fetchone()
                    if row:
                        merchant_id = row[0]
                        break

            # Strategy 3: Create new merchant — if we found a domain OR a confident brand
            if not merchant_id and (domains or company_names):
                primary_domain = domains[0] if domains else None
                normalized = normalize_domain(primary_domain) if primary_domain else None
                canonical_name = _domain_to_name(primary_domain) if primary_domain else (company_names[0] if company_names else None)
                merchant_slug = build_merchant_slug(canonical_name)

                if not canonical_name:
                    return None
                if primary_domain and _generic_domain_name(primary_domain) and canonical_name not in company_names:
                    save_event("merchant_validation_rejected", {"name": canonical_name, "domain": primary_domain, "reason": "generic_domain_name"})
                    return None

                # Canonical duplicate check before creating new merchant
                if check_and_log_canonical_match:
                    canonical_match = check_and_log_canonical_match(conn, canonical_name, primary_domain)
                    if canonical_match:
                        merchant_id = canonical_match
                        logger.info(f"Canonical match found for '{canonical_name}' → merchant_id={merchant_id}")
                        # Skip to signal linking below
                        conn.execute(text("""
                            UPDATE merchants SET last_seen = CURRENT_TIMESTAMP WHERE id = :mid
                        """), {"mid": merchant_id})

                if not merchant_id:
                    _proc_for_valid = detect_processor(content) if detect_processor else None
                    if not is_valid_merchant_name(canonical_name, domain=primary_domain, processor=_proc_for_valid):
                        save_event("merchant_validation_rejected", {"name": canonical_name})
                        return None

                    # Default domain confidence to 'confirmed' unless attribution passed 'provisional'
                    conf_val = "confirmed"
                    if _HAS_ATTRIBUTION and 'attribution' in locals() and attribution:
                        conf_val = attribution.get('domain_confidence', conf_val)
                    if discovered_domain:
                        conf_val = discovered_domain_confidence

                    val_source = "manual"
                    if domains:
                        val_source = "domain_extraction"
                    elif canonical_name in company_names:
                        val_source = "brand_extraction"
                    elif fallback_merchant_name and canonical_name == fallback_merchant_name:
                        val_source = "processor_context"

                    computed_confidence = compute_merchant_confidence(
                        domain=primary_domain,
                        processor_mentions=1 if detect_processor and detect_processor(content) else 0,
                        contact_email=None,
                        signal_count=1
                    )
                    save_event("merchant_confidence_calculated", {"merchant": canonical_name, "score": computed_confidence})

                    # Provisional status logic
                    identity_status = "active"
                    if computed_confidence < IDENTITY_FULL_THRESHOLD:
                        identity_status = "candidate"
                    
                    evidence_payload = {
                        "domains": domains,
                        "company_names": company_names,
                        "strategy_used": val_source,
                        "domain_confidence": conf_val,
                        "discovery_source": discovered_domain_source if 'discovered_domain_source' in locals() else None,
                        "reranker_scores": reranker_scores
                    }

                    row = conn.execute(text("""
                        INSERT INTO merchants (
                            canonical_name, slug, domain, normalized_domain, 
                            detected_from, domain_confidence, validation_source, 
                            confidence_score, status, evidence
                        )
                        VALUES (
                            :name, :slug, :domain, :nd, 
                            :source, :conf_val, :val_source, 
                            :conf_score, :status, :evidence
                        )
                        ON CONFLICT (slug) DO UPDATE SET
                            canonical_name = EXCLUDED.canonical_name,
                            domain = COALESCE(NULLIF(merchants.domain, ''), EXCLUDED.domain),
                            normalized_domain = COALESCE(NULLIF(merchants.normalized_domain, ''), EXCLUDED.normalized_domain),
                            detected_from = COALESCE(NULLIF(merchants.detected_from, ''), EXCLUDED.detected_from),
                            domain_confidence = COALESCE(NULLIF(merchants.domain_confidence, ''), EXCLUDED.domain_confidence),
                            validation_source = COALESCE(NULLIF(merchants.validation_source, ''), EXCLUDED.validation_source),
                            confidence_score = GREATEST(merchants.confidence_score, EXCLUDED.confidence_score),
                            status = CASE 
                                WHEN merchants.status = 'active' THEN 'active'
                                ELSE EXCLUDED.status
                            END,
                            evidence = jsonb_concat(COALESCE(merchants.evidence, '{}'::jsonb), EXCLUDED.evidence),
                            last_seen = CURRENT_TIMESTAMP
                        RETURNING id
                    """), {
                        "name": canonical_name,
                        "slug": merchant_slug,
                        "domain": primary_domain or "",
                        "nd": normalized or "",
                        "source": "signal_pipeline",
                        "conf_val": conf_val,
                        "val_source": val_source,
                        "conf_score": computed_confidence,
                        "status": identity_status,
                        "evidence": json.dumps(evidence_payload)
                    }).fetchone()
                    merchant_id = row[0]
                    logger.info(f"Merchant created/synced: {canonical_name} (status: {identity_status}, domain: {primary_domain or 'inferred'})")

                    if not primary_domain and company_names and canonical_name != fallback_merchant_name:
                        save_event("merchant_created_from_brand", {
                            "signal_id": signal_id,
                            "merchant_id": merchant_id,
                            "brand": canonical_name,
                            "platform": platform,
                        })

                    # Section 6: Trigger Contact Discovery Immediately
                    if primary_domain:
                        enqueue_contact_discovery(merchant_id)

            if merchant_id and discovered_domain:
                updated = conn.execute(text("""
                    UPDATE merchants
                    SET domain = :domain,
                        normalized_domain = :normalized_domain,
                        domain_confidence = :domain_confidence,
                        last_seen = CURRENT_TIMESTAMP
                    WHERE id = :mid
                      AND (domain IS NULL OR domain = '')
                """), {
                    "domain": discovered_domain,
                    "normalized_domain": normalize_domain(discovered_domain),
                    "domain_confidence": discovered_domain_confidence,
                    "mid": merchant_id,
                }).rowcount
                if updated:
                    enqueue_contact_discovery(merchant_id)

            # Update distress score
            # Age weight: recent signals weight more
            age_weight = 1.0
            score_delta = (priority_score or 0) * age_weight

            conn.execute(text("""
                UPDATE merchants
                SET distress_score = distress_score + :delta,
                    last_seen = CURRENT_TIMESTAMP
                WHERE id = :mid
            """), {"delta": score_delta, "mid": merchant_id})

            # Link signal → merchant (idempotent via unique constraint)
            conn.execute(text("""
                INSERT INTO merchant_signals (merchant_id, signal_id)
                VALUES (:mid, :sid)
                ON CONFLICT (merchant_id, signal_id) DO NOTHING
            """), {"mid": merchant_id, "sid": signal_id})

            # Update signal's merchant_id for direct lookup
            conn.execute(text("""
                UPDATE signals SET merchant_id = :mid WHERE id = :sid
            """), {"mid": merchant_id, "sid": signal_id})

            save_event("merchant_attributed", {
                "signal_id": int(signal_id or 0),
                "merchant_id": int(merchant_id),
                "merchant_name": company_names[0] if company_names else None,
                "domain": primary_domain or discovered_domain or "",
                "strategy_used": strategy_used if 'strategy_used' in locals() else "",
                "fallback_used": bool(fallback_merchant_name),
            })

            conn.commit()

    except Exception as e:
        logger.error(f"Merchant identity resolution error for signal {signal_id}: {e}")
        return None

    # Pass 2: Instrument decision
    try:
        strategy_used = "none"
        if merchant_id:
            if domains and primary_domain and any(normalize_domain(d) == normalize_domain(primary_domain) for d in domains):
                strategy_used = "domain_match"
            elif company_names:
                strategy_used = "brand_match"
        
        save_learning_feedback({
            "signal_id": signal_id,
            "source": "merchant_identity",
            "merchant_id_candidate": merchant_id,
            "merchant_domain_candidate": domains[0] if domains else None,
            "decision_type": "merchant_identity_resolution",
            "decision_payload_json": {
                "domains": domains,
                "company_names": company_names,
                "strategy_used": strategy_used,
                "fallback_used": bool(fallback_merchant_name),
                "attribution_used": bool(attribution) if 'attribution' in locals() else False,
                "reranker_scores": reranker_scores
            },
            "decision_score": float(priority_score or 0.0),
            "decision_model_version": "v1-deterministic"
        }, hook_name="merchant_identity_resolution")
    except Exception as le:
        logger.error(f"Reward hook failed [merchant_identity_resolution] signal_id={signal_id}: {type(le).__name__}: {le}")

    return merchant_id


def get_merchant_profile(merchant_id):
    """
    Build a complete merchant profile with signal count and processor breakdown.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT m.id, m.canonical_name, m.domain, m.normalized_domain,
                   m.industry, m.distress_score, m.first_seen, m.last_seen,
                   (SELECT COUNT(*) FROM merchant_signals ms WHERE ms.merchant_id = m.id) as signal_count
            FROM merchants m WHERE m.id = :mid
        """), {"mid": merchant_id}).fetchone()

        if not row:
            return None

        # Get processors involved
        procs = conn.execute(text("""
            SELECT DISTINCT ql.processor
            FROM qualified_leads ql
            JOIN merchant_signals ms ON ms.signal_id = ql.signal_id
            WHERE ms.merchant_id = :mid
              AND ql.processor IS NOT NULL AND ql.processor != 'unknown'
        """), {"mid": merchant_id}).fetchall()

        return {
            "id": row[0],
            "merchant": row[1],
            "domain": row[2],
            "normalized_domain": row[3],
            "industry": row[4],
            "distress_score": row[5],
            "first_seen": row[6].isoformat() if row[6] else None,
            "last_seen": row[7].isoformat() if row[7] else None,
            "signal_count": row[8],
            "processors_involved": [p[0] for p in procs],
        }


def get_merchant_signals(merchant_id, limit=20):
    """
    Get signals linked to a specific merchant.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.id, s.content, s.source, s.priority_score,
                   s.detected_at, s.entity_type, s.classification
            FROM signals s
            JOIN merchant_signals ms ON ms.signal_id = s.id
            WHERE ms.merchant_id = :mid
            ORDER BY s.detected_at DESC
            LIMIT :lim
        """), {"mid": merchant_id, "lim": limit}).fetchall()

        return [{
            "id": r[0],
            "content": r[1],
            "source": r[2],
            "priority_score": r[3],
            "detected_at": r[4].isoformat() if r[4] else None,
            "entity_type": r[5],
            "classification": r[6],
        } for r in rows]
