import re

PROCESSORS = ["stripe", "shopify payments", "paypal", "square", "adyen"]

URGENCY_KEYWORDS = ["froze", "holding funds", "terminated", "locked", "shut down",
                     "frozen", "seized", "withheld", "suspended", "blocked"]

REVENUE_PATTERN = re.compile(
    r'\$[\d,]+[kK]?|\$[\d,]+(?:\.\d+)?|six figures|monthly revenue|annual revenue',
    re.IGNORECASE
)
LIVE_PRESSURE_PATTERN = re.compile(
    r"\b(right now|currently|today|this week|ongoing|still|active|under review|on hold|frozen|paused|delayed|holding funds|cash flow|getting worse|escalating)\b",
    re.IGNORECASE,
)
DISTRESS_PATTERN = re.compile(
    r"\b(chargeback|dispute|reserve|held funds|holding funds|payout|settlement|account frozen|account closed|under review|verification review|processing disabled|payments disabled)\b",
    re.IGNORECASE,
)
MERCHANT_OPERATOR_PATTERN = re.compile(
    r"\b(my store|our store|our business|our company|we sell|we run|we operate|merchant candidate|merchant domain candidate|orders|checkout|revenue|sales)\b",
    re.IGNORECASE,
)
CONSUMER_NOISE_PATTERN = re.compile(
    r"\b(my order|my purchase|my package|money back|refund denied|customer service|return policy|buyer complaint)\b",
    re.IGNORECASE,
)
HYPOTHETICAL_OR_HISTORICAL_PATTERN = re.compile(
    r"\b(what if|hypothetical|in theory|for example|case study|historical|history of|years ago|last year|used to|previously|tutorial|guide)\b",
    re.IGNORECASE,
)
GENERIC_TOOLING_PATTERN = re.compile(
    r"\b(seo|ad spend|crm|customer analytics|generic analytics|marketing automation|growth tooling|lead gen software)\b",
    re.IGNORECASE,
)

def qualify_lead(content, priority_score=0):
    qualification_score = 0.0
    processor = None
    revenue_detected = False
    content_lower = content.lower()
    icp_fit_score = 0.0
    disqualifier_reason = ""

    # Revenue detection
    if REVENUE_PATTERN.search(content):
        qualification_score += 40
        revenue_detected = True
        icp_fit_score += 15

    # Processor detection
    for p in PROCESSORS:
        if p in content_lower:
            qualification_score += 30
            icp_fit_score += 20
            processor = p
            break

    # Urgency detection
    for u in URGENCY_KEYWORDS:
        if u in content_lower:
            qualification_score += 30
            icp_fit_score += 20
            break

    if DISTRESS_PATTERN.search(content):
        icp_fit_score += 20
    if LIVE_PRESSURE_PATTERN.search(content):
        icp_fit_score += 20
    if MERCHANT_OPERATOR_PATTERN.search(content):
        icp_fit_score += 15

    if CONSUMER_NOISE_PATTERN.search(content):
        icp_fit_score -= 35
        disqualifier_reason = disqualifier_reason or "consumer_noise"
    if HYPOTHETICAL_OR_HISTORICAL_PATTERN.search(content):
        icp_fit_score -= 40
        disqualifier_reason = disqualifier_reason or "historical_or_hypothetical"
    if GENERIC_TOOLING_PATTERN.search(content):
        icp_fit_score -= 40
        disqualifier_reason = disqualifier_reason or "generic_tooling_mismatch"

    icp_fit_score = max(0.0, min(100.0, icp_fit_score))
    icp_fit_label = "strong" if icp_fit_score >= 70 else "medium" if icp_fit_score >= 45 else "weak"
    eligible_for_pipeline = bool(qualification_score >= 60 and icp_fit_score >= 45 and not disqualifier_reason)

    return {
        "qualification_score": round(qualification_score, 2),
        "processor": processor,
        "revenue_detected": revenue_detected,
        "icp_fit_score": round(icp_fit_score, 2),
        "icp_fit_label": icp_fit_label,
        "eligible_for_pipeline": eligible_for_pipeline,
        "disqualifier_reason": disqualifier_reason,
    }
