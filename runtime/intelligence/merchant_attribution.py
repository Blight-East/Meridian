"""
Merchant Attribution Module
Infers merchant platform, brand, and domain from contextual signal text
when no explicit domain or company name is present.

Three-layer inference:
  Layer 1 — Platform Detection (Shopify, WooCommerce, etc.)
  Layer 2 — Brand Extraction (NLP patterns, author heuristics)
  Layer 3 — Domain Resolution (DNS probing)
"""
import sys, os, re

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, 'config')):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event
try:
    from runtime.intelligence.brand_extraction import extract_brand
except ImportError:
    from brand_extraction import extract_brand

try:
    from runtime.intelligence.domain_discovery import discover_domain_for_brand
except ImportError:
    from domain_discovery import discover_domain_for_brand

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

logger = get_logger("merchant_attribution")


# ── Layer 1: Platform Detection ──────────────────────────────────

PLATFORM_MAP = {
    "shopify": ["shopify", "shopify store", "shopify payments", "myshopify"],
    "woocommerce": ["woocommerce", "woo commerce", "woo store"],
    "bigcommerce": ["bigcommerce", "big commerce"],
    "magento": ["magento", "adobe commerce"],
    "squarespace": ["squarespace"],
    "wix": ["wix store", "wix payments", "wix ecommerce"],
    "etsy": ["etsy shop", "etsy store", "my etsy"],
    "amazon": ["amazon seller", "amazon fba", "amazon store"],
    "ebay": ["ebay store", "ebay seller", "ebay shop"],
    "gumroad": ["gumroad"],
    "stripe_direct": ["stripe account", "stripe connect"],
    "paypal_direct": ["paypal business", "paypal merchant"],
    "square": ["square pos", "square payments", "square store"],
}

# Processor detection (separate from platform)
PROCESSOR_MAP = {
    "stripe": ["stripe", "stripe froze", "stripe held", "stripe suspended", "stripe closed"],
    "paypal": ["paypal", "paypal froze", "paypal held", "paypal limited"],
    "square": ["square", "square payments"],
    "adyen": ["adyen"],
    "braintree": ["braintree"],
    "worldpay": ["worldpay"],
    "authorize.net": ["authorize.net", "authorize net", "authnet"],
    "2checkout": ["2checkout", "2co"],
}


def detect_platform(text):
    """Detect merchant ecommerce platform from signal text."""
    lower = text.lower()
    for platform, keywords in PLATFORM_MAP.items():
        for kw in keywords:
            if kw in lower:
                return platform
    return None


def detect_processor(text):
    """Detect payment processor from signal text."""
    lower = text.lower()
    for processor, keywords in PROCESSOR_MAP.items():
        for kw in keywords:
            if kw in lower:
                return processor
    return None


# ── Layer 3: Domain Resolution ───────────────────────────────────

def resolve_domain(brand, platform=None):
    """
    Attempt to resolve a brand name to a real domain deterministically.
    Returns (domain, confidence) or (None, 0).
    """
    discovery = discover_domain_for_brand(brand, platform)
    if not discovery:
        return None, 0

    logger.info(
        f"Domain resolved: {brand} -> {discovery['domain']} "
        f"(confidence: {discovery['confidence']})"
    )
    return discovery["domain"], discovery["confidence"]


# ── Main Attribution Function ────────────────────────────────────

def attribute_merchant(content, author=None, source=None):
    """
    Main entry point: infer merchant identity from contextual signal text.

    Returns dict:
    {
        "platform": str or None,
        "processor": str or None,
        "brand": str or None,
        "domain": str or None,
        "confidence": float (0-1),
        "attribution_method": str
    }
    """
    if not content:
        return None

    signal_info = classify_merchant_signal(content)
    if signal_info["classification"] != CLASS_MERCHANT_OPERATOR:
        return None

    platform = detect_platform(content)
    processor = detect_processor(content)
    brand = extract_brand(content, author=author)

    result = {
        "platform": platform,
        "processor": processor,
        "brand": brand,
        "domain": None,
        "confidence": 0,
        "attribution_method": "none",
        "domain_confidence": None,
    }

    # If we have a brand, try to resolve domain
    if brand:
        domain, conf = resolve_domain(brand, platform)
        if domain:
            result["domain"] = domain
            result["confidence"] = conf
            result["domain_confidence"] = "confirmed"
            result["attribution_method"] = "brand_dns"
            save_event("merchant_attributed", {
                "brand": brand, "domain": domain,
                "platform": platform, "processor": processor,
                "confidence": conf, "method": "brand_dns",
                "domain_confidence": "confirmed",
            })
            return result

        # No DNS domain resolved, attempt provisional domain building
        provisional_domain = None
        domain_confidence_score = 0.4
        
        slug = re.sub(r'[^a-z0-9]', '', brand.lower())
        if slug and len(slug) >= 3:
            if platform == "shopify":
                provisional_domain = f"{slug}.myshopify.com"
                domain_confidence_score = 0.6
            elif platform in ("woocommerce", "magento"):
                provisional_domain = f"{slug}.com"
                domain_confidence_score = 0.6
            elif platform == "bigcommerce":
                provisional_domain = f"{slug}.mybigcommerce.com"
                domain_confidence_score = 0.6
            elif platform == "etsy":
                provisional_domain = f"etsy.com/shop/{slug}"
                domain_confidence_score = 0.6
            elif platform == "gumroad":
                provisional_domain = f"{slug}.gumroad.com"
                domain_confidence_score = 0.6
            else:
                provisional_domain = f"{slug}.com"
                domain_confidence_score = 0.4

        if provisional_domain:
            result["domain"] = provisional_domain
            result["confidence"] = domain_confidence_score
            result["domain_confidence"] = "provisional"
            result["attribution_method"] = "platform_inferred"
            save_event("merchant_attributed", {
                "brand": brand, "domain": provisional_domain,
                "platform": platform, "processor": processor,
                "confidence": domain_confidence_score,
                "method": "platform_inferred",
                "domain_confidence": "provisional",
            })
            return result

        # No domain resolved and no provisional built, but brand + platform is still useful
        result["confidence"] = 0.4 if platform else 0.3
        result["attribution_method"] = "brand_context"
        save_event("merchant_attributed", {
            "brand": brand, "platform": platform,
            "processor": processor, "confidence": result["confidence"],
            "method": "brand_context",
        })
        return result

    # No brand found, but platform detection alone provides some value
    if platform:
        result["confidence"] = 0.2
        result["attribution_method"] = "platform_only"
        return result

    return None
