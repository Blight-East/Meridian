import re, math
from datetime import datetime, timedelta, timezone

SEVERITY_KEYWORDS = [
    "froze my account", "holding funds", "reserve", "terminated account",
    "shut down payments", "account locked", "funds held", "account frozen",
    "account suspended", "withheld", "seized", "blocked payments",
    "gateway shutdown merchant account", "payout withheld"
]

FIRST_PERSON = ["i ", "i'm ", "i've ", "my ", "we ", "our "]

BUSINESS_TERMS = [
    "store", "customers", "orders", "sales", "revenue",
    "transactions", "payout", "merchant", "business",
]

CONSUMER_TERMS = [
    "cashback", "refund delay", "consumer support complaint", "bought", "purchased"
]

REVENUE_PATTERN = re.compile(
    r'\$[\d,]+[kK]?|\$[\d,]+(?:\.\d+)?|six figures|monthly revenue|annual revenue',
    re.IGNORECASE
)

def first_person_signal(text):
    t = text.lower()
    return any(p in t for p in FIRST_PERSON)

def business_context(text):
    t = text.lower()
    return any(term in t for term in BUSINESS_TERMS)

def merchant_relevance(text):
    return first_person_signal(text) and business_context(text)

def detect_revenue(text):
    return bool(REVENUE_PATTERN.search(text))

def score_signal(content, score=0, comments=0, created_at=None):
    priority = 0.0
    metadata = {"revenue_detected": False, "merchant_relevant": False, "age_weight": 1.0}
    content_lower = content.lower()

    for keyword in SEVERITY_KEYWORDS:
        if keyword in content_lower:
            priority += 20

    priority += min(score, 50) * 0.4
    priority += min(comments, 20) * 0.8

    if merchant_relevance(content):
        priority += 30
        metadata["merchant_relevant"] = True
    else:
        priority -= 20

    if detect_revenue(content):
        priority += 40
        metadata["revenue_detected"] = True
        
    for ct in CONSUMER_TERMS:
        if ct in content_lower:
            priority -= 20

    if created_at:
        if isinstance(created_at, str):
            try: created_at = datetime.fromisoformat(created_at)
            except Exception: created_at = None
        if created_at:
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            else:
                created_at = created_at.astimezone(timezone.utc)
            delta = datetime.now(timezone.utc) - created_at
            days_old = delta.total_seconds() / 86400.0
            metadata["age_weight"] = round(math.exp(-max(days_old, 0) / 7.0), 4)

    adjusted = priority * metadata["age_weight"]
    return round(max(adjusted, 0), 2), metadata
