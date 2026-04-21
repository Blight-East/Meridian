"""
Deterministic merchant brand extraction from distress signals.

This module exists to create merchant records even when a signal has no
explicit domain. It avoids LLM calls and relies on phrase patterns,
merchant-name fallbacks, and lightweight signal backfill.
"""
import os
import re
import sys

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger

logger = get_logger("brand_extraction")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

BRAND_EXTRACTION_BATCH_SIZE = 100

_PLATFORM_PREFIX = r"(?:shopify|etsy|gumroad|substack|kajabi|bigcommerce|woocommerce|wix|squarespace)\s+"
_BRAND_TOKEN = r"[A-Z][A-Za-z0-9&'\-]{1,29}"
_BRAND_PHRASE = rf"({_BRAND_TOKEN}(?:\s+{_BRAND_TOKEN}){{0,2}})"

_SIGNAL_BRAND_PATTERNS = [
    re.compile(
        rf"(?:my|our|the)\s+(?:{_PLATFORM_PREFIX})?(?:store|shop|brand|business|company|site|website)\s+"
        rf"(?:called\s+|named\s+|is\s+)?[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:my|our)\s+(?:{_PLATFORM_PREFIX})?(?:brand|store|shop)\s+{_BRAND_PHRASE}(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:i|we)\s+(?:run|own|operate|built|started|founded)\s+(?:a\s+)?[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:brand|business|company|store|shop)\s+(?:called|named|is)\s+[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"[\"'\u201c]{_BRAND_PHRASE}[\"'\u201d](?:\s+(?:store|shop|brand|business))?",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:froze|held|paused|locked|limited|suspended|closed|banned|terminated)\s+(?:my|our)\s+"
        rf"(?:{_PLATFORM_PREFIX})?(?:store|shop|brand|business)\s+{_BRAND_PHRASE}(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
]

_GENERIC_WORDS = {
    "account", "again", "alone", "anyone", "bank", "brand", "business", "closed",
    "company", "complaint", "delayed", "disabled", "frozen", "funds", "gateway",
    "held", "help", "just", "limited", "locked", "merchant", "money", "news",
    "online", "paused", "payment", "payments", "payout", "payouts", "platform",
    "processor", "processors", "secure", "shop", "site", "small", "square",
    "store", "stripe", "support", "suspended", "terminated", "unknown", "update",
    "urban", "warning", "website", "with",
}
_BRAND_STOP_WORDS = {
    "amazon", "bigcommerce", "ebay", "etsy", "facebook", "gumroad", "instagram",
    "kajabi", "paypal", "reddit", "shopify", "square", "stripe", "substack",
    "trustpilot", "twitter", "woocommerce", "x", "yelp", "youtube",
}
_TRAILING_CUTOFF_WORDS = {
    "again", "after", "and", "are", "because", "been", "but", "for", "from",
    "got", "had", "has", "have", "is", "just", "locked", "paused", "since",
    "suspended", "terminated", "that", "the", "was", "were", "with",
}
_LEADING_NOISE_WORDS = {
    "and", "because", "however", "if", "nobody", "something", "that", "the",
    "then", "this", "those", "though", "what", "when", "where", "which",
    "who", "why",
}
_NOISE_BRAND_BLOCKLIST = {
    "and nobody", "because", "however", "holding", "nobody", "something",
    "stressing", "uk registered", "uk-registered", "wearing",
}
_NOISE_TOKENS = {
    "because", "however", "holding", "nobody", "registered", "something",
    "stressing", "wearing",
}
_PROCESSOR_OR_PLATFORM_HINTS = (
    "adyen", "bigcommerce", "braintree", "etsy", "gumroad", "kajabi", "magento",
    "paypal", "shopify", "square", "stripe", "substack", "woocommerce", "worldpay",
)
_MERCHANT_CONTEXT_HINTS = (
    " my store ", " our store ", " my shop ", " our shop ", " my brand ", " our brand ",
    " my business ", " our business ", " my company ", " our company ", " website ",
)
_TITLECASE_GERUND_PATTERN = re.compile(r"^[A-Z][a-z]+ing$")


def extract_brand_candidates(text, merchant_name=None, author=None):
    """
    Return ranked brand candidates extracted from a distress signal.
    """
    if not text and not merchant_name and not author:
        return []

    candidates = []
    seen = set()

    for pattern in _SIGNAL_BRAND_PATTERNS:
        for match in pattern.findall(text or ""):
            candidate = _coerce_candidate(match)
            cleaned = normalize_brand(candidate)
            if cleaned and _is_valid_brand(cleaned, text=text, source="signal_pattern") and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                candidates.append({
                    "brand": cleaned,
                    "confidence": _score_brand(cleaned, "pattern"),
                    "source": "signal_pattern",
                })

    for candidate, source in ((merchant_name, "signal_field"), (author, "author")):
        cleaned = normalize_brand(candidate)
        if cleaned and _is_valid_brand(cleaned, text=text, source=source) and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            candidates.append({
                "brand": cleaned,
                "confidence": _score_brand(cleaned, source),
                "source": source,
            })

    candidates.sort(key=lambda item: (-item["confidence"], len(item["brand"])))
    return candidates


def extract_brand(text, merchant_name=None, author=None):
    """
    Return the strongest merchant brand candidate from a signal.
    """
    try:
        from runtime.intelligence.merchant_validation import is_valid_merchant_name
    except ImportError:
        from merchant_validation import is_valid_merchant_name

    candidates = extract_brand_candidates(text, merchant_name=merchant_name, author=author)
    if not candidates:
        return None
        
    best_candidate = candidates[0]["brand"]
    if not is_valid_merchant_name(best_candidate):
        return None
        
    return best_candidate


def run_brand_extraction(limit=BRAND_EXTRACTION_BATCH_SIZE):
    """
    Scheduler entrypoint that revisits unresolved signals and tries to create
    merchants directly from extracted brand names.
    """
    try:
        from runtime.intelligence.merchant_identity import resolve_merchant_identity
    except ImportError:
        from merchant_identity import resolve_merchant_identity

    processed = 0
    extracted = 0
    resolved = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content, priority_score, merchant_name, source
            FROM signals
            WHERE merchant_id IS NULL
              AND (merchant_name IS NULL OR merchant_name = '' OR merchant_name = 'unknown')
            ORDER BY priority_score DESC, detected_at DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()

    for signal_id, content, priority_score, merchant_name, source in rows:
        processed += 1
        if not should_attempt_brand_extraction(content or ""):
            continue
        brand = extract_brand(content or "", merchant_name=merchant_name)
        if not brand:
            continue
        extracted += 1
        try:
            merchant_id = resolve_merchant_identity(
                signal_id,
                content or "",
                priority_score or 0,
                merchant_name=brand,
            )
            if merchant_id:
                resolved += 1
        except Exception as exc:
            logger.warning(f"Brand extraction resolution failed for signal {signal_id}: {exc}")

    result = {
        "signals_processed": processed,
        "brands_extracted": extracted,
        "signals_resolved": resolved,
    }
    logger.info(f"Brand extraction run complete: {result}")
    return result


def normalize_brand(candidate):
    if not candidate:
        return None

    candidate = candidate.strip().strip("\"'.,!?():;[]{}")
    candidate = re.sub(r"\s+", " ", candidate).strip()
    if not candidate:
        return None

    # Trim leading possessives or generic determiners that leaked through.
    candidate = re.sub(r"^(?:my|our|the)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s+(?:store|shop|brand|business|company|site|website)$", "", candidate, flags=re.IGNORECASE)
    words = candidate.split()
    trimmed = []
    for word in words:
        bare = re.sub(r"[^A-Za-z0-9]", "", word)
        if not bare:
            continue
        if trimmed and (bare.islower() or bare.lower() in _TRAILING_CUTOFF_WORDS):
            break
        trimmed.append(word)

    candidate = " ".join(trimmed).strip()
    return candidate or None


def is_valid_brand_candidate(name, text=None, source=None):
    return _is_valid_brand(name, text=text, source=source)


def should_attempt_brand_extraction(text):
    if not text:
        return False

    lower = f" {text.lower()} "
    if any(hint in lower for hint in _MERCHANT_CONTEXT_HINTS):
        return True
    return any(hint in lower for hint in _PROCESSOR_OR_PLATFORM_HINTS)


def _coerce_candidate(match):
    if isinstance(match, tuple):
        for value in reversed(match):
            if value:
                return value
        return ""
    return match or ""


def _has_supporting_context(text):
    if not text:
        return False

    lower = f" {text.lower()} "
    if any(hint in lower for hint in _MERCHANT_CONTEXT_HINTS):
        return True
    return any(hint in lower for hint in _PROCESSOR_OR_PLATFORM_HINTS)


def _is_valid_brand(name, text=None, source=None):
    if not name or len(name) < 3 or len(name) > 50:
        return False

    tokens = re.findall(r"[A-Za-z0-9]+", name.lower())
    if not tokens:
        return False
    if len("".join(tokens)) < 4:
        return False

    normalized = " ".join(tokens)
    if normalized in _NOISE_BRAND_BLOCKLIST:
        return False
    if _TITLECASE_GERUND_PATTERN.fullmatch(name):
        return False
    if tokens[0] in _LEADING_NOISE_WORDS:
        return False
    if len(tokens) <= 2 and any(token in _NOISE_TOKENS for token in tokens):
        return False

    has_context = _has_supporting_context(text)
    alpha_only = re.sub(r"[^A-Za-z]", "", name)
    if alpha_only and alpha_only == alpha_only.lower() and not has_context:
        return False
    if source != "signal_field" and not has_context and len(tokens) == 1:
        return False

    if all(token in _GENERIC_WORDS or token in _BRAND_STOP_WORDS for token in tokens):
        return False
    if any(token in _BRAND_STOP_WORDS for token in tokens) and len(tokens) == 1:
        return False
    if len(tokens) == 1 and tokens[0] in _GENERIC_WORDS:
        return False
    if len(tokens) <= 2 and all(token in _GENERIC_WORDS for token in tokens):
        return False
    return True


def _score_brand(name, source):
    score = 0.55
    if source == "signal_pattern":
        score += 0.2
    elif source == "signal_field":
        score += 0.1
    elif source == "author":
        score += 0.05

    if re.search(r"[A-Z].*[A-Z]", name):
        score += 0.05
    if re.search(r"[a-z][A-Z]", name):
        score += 0.05
    if " " in name:
        score += 0.03

    return round(min(score, 0.95), 2)
