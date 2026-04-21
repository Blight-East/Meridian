"""
Merchant name validation layer.

Validates whether a candidate merchant name is legitimate before a merchant
record is created. Prevents junk merchants from entering the opportunity
pipeline.
"""
import re

STOP_WORDS = {
    "something", "someone", "nobody", "because", "however", "about",
    "wearing", "holding", "stressing", "talking", "thinking",
    "merchant", "account", "news", "issue", "problem", "story",
    "article", "thread", "discussion",
    "alert", "alerts", "click", "customer", "email", "mail",
    "newsletter", "notification", "public", "support",
    "recommend", "recommended", "immediately", "description", "obviously",
    "yesterday", "later", "historically", "fortunately", "questions",
    "signed", "other", "recently", "started", "thoughts", "things",
    "better", "complete", "surprise", "trouble", "personal", "there",
    "around", "replace", "pasted", "quick", "worth", "orders",
    "chargeback", "beware", "advice", "happy", "split", "details",
    "recommendation", "recommendations",
    "last", "october", "depend", "funds",
}

LEADING_NOISE_WORDS = {
    "avoid", "best", "can", "does", "has", "how", "should", "through",
    "using", "what", "when", "where", "why", "your",
}

GENERIC_PHRASES = {
    "amazon seller account",
    "app development",
    "best payment processor",
    "best payments processing system",
    "best strategy",
    "can anyone process",
    "canadian adult drop shipping",
    "cannabis local",
    "chase paymenttech issues",
    "citizen needed",
    "credit card processing merchant account news",
    "credit card processors",
    "cryptocurrency payment",
    "early termination fees",
    "founder conflict",
    "funds being held",
    "long post",
    "just depend",
    "last october",
    "mercury bank",
    "neurogenre research",
    "nightmare client",
    "reserve bank",
    "sponsored ads best practices",
    "through stripe atlas",
    "using paypal",
    "your stripe",
}

GENERIC_HOSTED_STORE_LABELS = {
    "across", "advice", "additionally", "anyalternativeslike", "around",
    "based", "bible", "budget", "confirm", "details", "dutch", "europe",
    "handmade", "issues", "klarna", "last", "multi", "native", "newshopify",
    "partial", "pokemon", "razorpay", "recently", "should", "since",
    "strictly", "thingswrong", "dropshipped", "founders", "shopifitutionalized",
}

PROVIDER_DOMAINS = {
    "authorize.net",
    "github.com",
    "paypal.com",
    "squareup.com",
    "stripe.com",
    "www.paypal.com",
    "x.com",
    "youtube.com",
}

INVALID_PATTERNS = [
    re.compile(r"^[A-Z][a-z]+ing$"),   # Wearing, Stressing
    re.compile(r"^[a-z]+$"),            # all lowercase
    re.compile(r"^\d+$"),               # numbers only
    re.compile(r"^(?:www\.)?[a-z0-9-]+\.[a-z]{2,}$", re.IGNORECASE),
]

HOSTED_PLATFORM_SUFFIXES = {
    "myshopify.com",
    "mybigcommerce.com",
}


def _root_label(domain: str | None) -> str:
    host = str(domain or "").strip().lower()
    if "/" in host:
        host = host.split("/", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    labels = [label for label in host.split(".") if label]
    return labels[0] if labels else ""


def is_valid_merchant_name(name: str, domain: str | None = None, processor: str | None = None) -> bool:
    """
    Validate whether a candidate merchant name is legitimate.

    Reject if:
      - len(name) < 4
      - name.lower() in STOP_WORDS
      - matches INVALID_PATTERNS

    Allow if ANY of the following is true:
      - domain exists
      - processor mention exists
      - name contains two capitalized words

    Returns True only if valid.
    """
    if not name:
        return False

    # ── Rejection rules ──────────────────────────────────────────────
    if len(name) < 4:
        return False

    normalized_name = name.lower().strip()
    if normalized_name in STOP_WORDS or normalized_name in GENERIC_PHRASES:
        return False

    for pattern in INVALID_PATTERNS:
        if pattern.match(name.strip()):
            return False

    words = [w for w in re.split(r"\s+", name.strip()) if w]
    normalized_words = [re.sub(r"[^a-z0-9]", "", w.lower()) for w in words]
    normalized_words = [w for w in normalized_words if w]
    if not normalized_words:
        return False
    if normalized_words[0] in LEADING_NOISE_WORDS:
        return False
    if len(normalized_words) <= 3 and normalized_words[0] in STOP_WORDS:
        return False
    if all(word in STOP_WORDS for word in normalized_words):
        return False

    if domain:
        host = domain.strip().lower()
        if host in PROVIDER_DOMAINS:
            return False
        root = _root_label(host)
        if any(host == suffix or host.endswith(f".{suffix}") for suffix in HOSTED_PLATFORM_SUFFIXES):
            if root in STOP_WORDS or root in GENERIC_HOSTED_STORE_LABELS:
                return False

    # ── Allow rules (any one is sufficient) ──────────────────────────
    if domain:
        return True

    # Minimum entropy rule: reject names that are too generic
    letters = [c for c in name.lower() if c.isalpha()]
    if letters:
        unique_letters = len(set(letters))
        if unique_letters / len(letters) < 0.4:
            return False

    if processor:
        return True

    capitalized_words = sum(1 for w in words if w.istitle() and len(w) > 1)
    return capitalized_words >= 2


def compute_merchant_confidence(
    domain: str | None,
    processor_mentions: int,
    contact_email: str | None,
    signal_count: int
) -> int:
    score = 0
    if domain:
        score += 40
    if processor_mentions > 0:
        score += 30
    if contact_email:
        score += 20
    if signal_count >= 2:
        score += 10
    return min(score, 100)
