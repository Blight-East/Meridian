import re

PROCESSOR_TOKENS = {
    "stripe": ["stripe", "connect", "stripe payout", "stripe reserve"],
    "paypal": ["paypal", "paypal hold", "paypal reserve"],
    "shopify": ["shopify payments"],
    "square": ["square payouts"],
    "adyen": ["adyen"]
}

DISTRESS_TOKENS = [
    "payout",
    "reserve",
    "gateway",
    "chargeback",
    "account frozen",
    "merchant account terminated"
]

def classify_processor(text_content):
    if not text_content: return "unknown", 0.0
    text_lower = text_content.lower()
    
    distress_count = sum(1 for d in DISTRESS_TOKENS if d in text_lower)
    
    scores = {}
    for proc, tokens in PROCESSOR_TOKENS.items():
        match_count = sum(1 for t in tokens if t in text_lower)
        if match_count > 0:
            scores[proc] = match_count
            
    if not scores:
        return "unknown", 0.0
        
    best_proc = max(scores, key=scores.get)
    best_count = scores[best_proc]
    
    confidence = min(1.0, (best_count * 0.4) + (distress_count * 0.2))
    return best_proc, round(confidence, 2)
