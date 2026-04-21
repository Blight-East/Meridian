import re

# Words that commonly appear titlecased in Reddit/forum posts but are never
# merchant names. This set is checked case-insensitively against each candidate
# word's first token.
_IGNORE_FIRST_WORDS = {
    # Pronouns / articles / prepositions
    "a", "an", "at", "i", "if", "in", "it", "my", "on", "the", "this",
    "we", "they", "what", "how", "why", "when", "where", "who",
    # Payment processors / platforms (not merchants)
    "stripe", "paypal", "shopify", "square", "braintree", "adyen",
    "authorize", "worldpay", "klarna", "afterpay",
    # Generic payment / business terms
    "account", "accounts", "business", "card", "cards", "chargeback",
    "chargebacks", "checkout", "commerce", "customer", "customers",
    "dispute", "disputes", "does", "funds", "gateway", "merchant", "merchants",
    "money", "order", "orders", "payment", "payments", "payout", "payouts",
    "processor", "processors", "refund", "refunds", "removes", "reserve",
    "revenue", "sales", "selecting", "seller", "sellers", "settlement",
    "small", "snapped", "subscription", "transaction", "transactions",
    "transfer", "transfers", "turning", "verification", "wallet",
}

# Full candidate phrases that should never be treated as merchant names.
# Checked case-insensitively.
_IGNORE_PHRASES = {
    # Subreddit names that appear titlecased in [reddit/SubName] format
    "entrepreneur", "dropship", "dropshipping", "startups", "smallbusiness",
    "ecommerce", "shopify", "stripe", "paypal", "personalfinance",
    "legaladvice", "accounting", "marketing", "webdev", "saas",
    "cryptocurrency", "bitcoin", "fintech", "freelance", "digitalnomad",
    "tiktokshop",
    # Common English words that appear titlecased at sentence starts
    "after", "again", "also", "already", "always", "another", "anyone",
    "anything", "around", "back", "been", "before", "being", "below",
    "between", "both", "cannot", "context", "could", "currently",
    "does", "doing", "done", "down", "during", "each", "even",
    "every", "everyone", "everything", "first", "from", "getting",
    "going", "have", "having", "here", "however", "into", "just",
    "keep", "last", "like", "long", "looking", "made", "make",
    "many", "more", "most", "much", "need", "never", "next",
    "none", "nothing", "only", "other", "over", "please", "presents",
    "recently", "right", "same", "sells", "should", "since", "some",
    "someone", "something", "still", "such", "sure", "take", "than",
    "that", "their", "them", "then", "there", "these", "think",
    "those", "through", "today", "trying", "under", "until", "upon",
    "very", "want", "well", "were", "which", "while", "will",
    "with", "without", "would", "zero", "across",
    # Generic business / tech terms
    "beware", "catch", "company", "complaint", "complaints", "credit",
    "debit", "digital", "discuss", "discussing", "does", "experience",
    "found", "issue", "issues", "lessons", "monthly", "online",
    "platform", "problem", "problems", "question", "questions",
    "recently", "registered", "removes", "removes", "review", "reviews",
    "selecting", "service", "services", "small", "snapped", "software",
    "solution", "solutions", "started", "starting", "store", "stores",
    "support", "system", "turning", "update", "using", "warning",
}

# Source-tag pattern: [reddit/...], [twitter/...], etc. — we strip these before
# trying to extract brands so the subreddit name isn't misread.
_SOURCE_TAG_RE = re.compile(r"\[(?:reddit|twitter|trustpilot|stackoverflow|shopify)[^\]]*\]", re.IGNORECASE)

# Domain regex — unchanged
_DOMAIN_RE = re.compile(
    r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:com|net|org|co|io)\b",
    re.IGNORECASE,
)

# Brand candidate regex — titlecased words
_BRAND_RE = re.compile(r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)*\b")


def extract_merchant(text):
    """Extract a merchant name or domain from signal text."""
    if not text:
        return "unknown"

    # Try explicit domain first
    domain_match = _DOMAIN_RE.search(text)
    if domain_match:
        return domain_match.group(0)

    # Strip source tags so [reddit/Entrepreneur] doesn't yield "Entrepreneur"
    cleaned = _SOURCE_TAG_RE.sub("", text)

    brands = _BRAND_RE.findall(cleaned)
    for b in brands:
        first_word = b.split()[0].lower()
        full_lower = b.lower()

        # Skip if first word is in the ignore set
        if first_word in _IGNORE_FIRST_WORDS:
            continue

        # Skip if full phrase is a known non-merchant
        if full_lower in _IGNORE_PHRASES:
            continue

        # Skip single words that are too short or too generic
        if " " not in b and len(b) <= 4:
            continue

        # Skip single common English words (gerunds ending in -ing, etc.)
        if " " not in b and full_lower.endswith("ing") and len(b) <= 10:
            continue

        return b

    return "unknown"


def classify_industry(text, merchant_name):
    lower_text = text.lower()
    if any(w in lower_text for w in ["store", "ecommerce", "woocommerce", "cart", "shop", "physical"]):
        return "ecommerce"
    if any(w in lower_text for w in ["saas", "software", "api", "app", "dashboard"]):
        return "saas"
    if any(w in lower_text for w in ["marketplace", "vendors", "platform"]):
        return "marketplace"
    if any(w in lower_text for w in ["subscription", "monthly", "mrr"]):
        return "subscription"
    if any(w in lower_text for w in ["digital", "download", "course", "ebook", "crypto"]):
        return "digital goods"
    return "unknown"


def classify_region(text):
    t = text.lower()
    if 'usd' in t or '$' in t: return 'US'
    if 'gbp' in t or '£' in t: return 'UK'
    if 'eur' in t or '€' in t: return 'EU'
    return 'global'
