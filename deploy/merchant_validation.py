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
}

INVALID_PATTERNS = [
    re.compile(r"^[A-Z][a-z]+ing$"),   # Wearing, Stressing
    re.compile(r"^[a-z]+$"),            # all lowercase
    re.compile(r"^\d+$"),               # numbers only
]


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

    if name.lower().strip() in STOP_WORDS:
        return False

    for pattern in INVALID_PATTERNS:
        if pattern.match(name.strip()):
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

    words = [w for w in name.split() if w.strip()]
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
