import sys, os, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from tools.web_fetch import web_fetch
from config.logging_config import get_logger

logger = get_logger("investigation")

INDUSTRIES = {
    "ecommerce": ["store", "shop", "ecommerce", "marketplace", "retail", "product", "orders", "shopify"],
    "saas": ["saas", "software", "platform", "subscription", "app", "service"],
    "fintech": ["fintech", "payment", "banking", "financial", "lending", "crypto"],
    "freelance": ["freelance", "contractor", "client", "invoice", "gig"],
    "creator": ["creator", "content", "youtube", "course", "digital product"],
    "agency": ["agency", "marketing", "consulting", "clients"],
}

COUNTRIES = [
    "united states", "usa", "uk", "united kingdom", "canada", "australia",
    "germany", "france", "india", "netherlands", "singapore", "ireland",
]

PROCESSORS = ["stripe", "shopify payments", "paypal", "square", "adyen", "wise", "braintree", "rakuten"]

def _extract_merchant_ref(content):
    content_lower = content.lower()
    # Remove common prefixes
    clean = re.sub(r'\[reddit/[^\]]+\]\s*', '', content)
    clean = re.sub(r'\[hackernews[^\]]*\]\s*', '', clean)
    clean = re.sub(r'\[score:\d+\|comments:\d+\]', '', clean)
    # Extract first meaningful phrase (likely the complaint title)
    clean = clean.strip()
    # Try to find a company/service name reference
    name_match = re.search(r'(?:at|from|by|with)\s+([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?)', content)
    if name_match:
        return name_match.group(1)
    return clean[:60] if clean else None

def _detect_processor(content):
    content_lower = content.lower()
    for p in PROCESSORS:
        if p in content_lower:
            return p
    return None

def _detect_industry(content):
    content_lower = content.lower()
    for industry, keywords in INDUSTRIES.items():
        if any(kw in content_lower for kw in keywords):
            return industry
    return "unknown"

def _detect_location(text):
    text_lower = text.lower()
    for country in COUNTRIES:
        if country in text_lower:
            return country.title()
    return None

def _extract_website(text):
    urls = re.findall(r'https?://[^\s<>"\']+', text)
    for url in urls:
        if 'reddit.com' not in url and 'ycombinator.com' not in url:
            return url
    return None

def investigate_lead(content, source_url=None):
    processor = _detect_processor(content)
    industry = _detect_industry(content)
    merchant_ref = _extract_merchant_ref(content)
    location = None
    merchant_website = None
    notes_parts = []

    # Try to fetch the source post for more context
    if source_url and 'reddit.com' in source_url:
        try:
            json_url = source_url.rstrip('/') + '.json'
            result = web_fetch(json_url)
            if result.get("status") == 200:
                page_text = result.get("content", "")
                loc = _detect_location(page_text)
                if loc:
                    location = loc
                site = _extract_website(page_text)
                if site:
                    merchant_website = site
                notes_parts.append("Source post fetched and analyzed")
        except Exception as e:
            notes_parts.append(f"Source fetch failed: {str(e)[:50]}")

    # Detect location from original content if not found
    if not location:
        location = _detect_location(content)

    # Build merchant name
    merchant_name = None
    if merchant_ref and len(merchant_ref) > 3:
        merchant_name = merchant_ref

    # Build notes
    if processor:
        notes_parts.append(f"Processor: {processor}")
    if industry != "unknown":
        notes_parts.append(f"Industry signals: {industry}")
    notes_parts.append(f"Original: {content[:100]}")

    return {
        "merchant_name": merchant_name,
        "merchant_website": merchant_website,
        "industry": industry,
        "location": location,
        "notes": " | ".join(notes_parts),
    }
