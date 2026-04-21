"""
Entity Taxonomy Module
Central registry for classifying entities by type and determining
signal classification for the lead validity gate.
"""

# ── Entity type constants ──────────────────────────────────────────
ENTITY_TYPE_PROCESSOR = "processor"
ENTITY_TYPE_BANK = "bank"
ENTITY_TYPE_MARKETPLACE = "marketplace"
ENTITY_TYPE_AFFILIATE_NETWORK = "affiliate_network"
ENTITY_TYPE_PLATFORM = "platform"
ENTITY_TYPE_CONSUMER_SERVICE = "consumer_service"
ENTITY_TYPE_UNKNOWN = "unknown"

VALID_ENTITY_TYPES = {
    ENTITY_TYPE_PROCESSOR,
    ENTITY_TYPE_BANK,
    ENTITY_TYPE_MARKETPLACE,
    ENTITY_TYPE_AFFILIATE_NETWORK,
    ENTITY_TYPE_PLATFORM,
    ENTITY_TYPE_CONSUMER_SERVICE,
    ENTITY_TYPE_UNKNOWN,
}

# ── Classification constants ───────────────────────────────────────
CLASS_CONSUMER_COMPLAINT = "consumer_complaint"
CLASS_AFFILIATE_DISTRESS = "affiliate_distress"
CLASS_PROCESSOR_DISTRESS = "processor_distress"
CLASS_MERCHANT_DISTRESS = "merchant_distress"
CLASS_NON_MERCHANT = "non_merchant"

try:
    from runtime.intelligence.merchant_signal_classifier import (
        CLASS_CONSUMER_OR_IRRELEVANT,
        CLASS_MERCHANT_OPERATOR,
        classify_merchant_signal,
    )
except ImportError:
    from merchant_signal_classifier import (
        CLASS_CONSUMER_OR_IRRELEVANT,
        CLASS_MERCHANT_OPERATOR,
        classify_merchant_signal,
    )

# ── Entity registry ───────────────────────────────────────────────
# Maps lowercase entity name → (display_name, entity_type)
ENTITY_REGISTRY = {
    # Processors
    "stripe":           ("Stripe",          ENTITY_TYPE_PROCESSOR),
    "paypal":           ("PayPal",          ENTITY_TYPE_PROCESSOR),
    "square":           ("Square",          ENTITY_TYPE_PROCESSOR),
    "adyen":            ("Adyen",           ENTITY_TYPE_PROCESSOR),
    "braintree":        ("Braintree",       ENTITY_TYPE_PROCESSOR),
    "authorize.net":    ("Authorize.net",   ENTITY_TYPE_PROCESSOR),
    "worldpay":         ("Worldpay",        ENTITY_TYPE_PROCESSOR),
    "checkout.com":     ("Checkout.com",    ENTITY_TYPE_PROCESSOR),
    "mollie":           ("Mollie",          ENTITY_TYPE_PROCESSOR),
    "2checkout":        ("2Checkout",       ENTITY_TYPE_PROCESSOR),
    "payoneer":         ("Payoneer",        ENTITY_TYPE_PROCESSOR),
    "klarna":           ("Klarna",          ENTITY_TYPE_PROCESSOR),
    "afterpay":         ("Afterpay",        ENTITY_TYPE_PROCESSOR),

    # Banks
    "wells fargo":      ("Wells Fargo",     ENTITY_TYPE_BANK),
    "chase":            ("Chase",           ENTITY_TYPE_BANK),
    "bank of america":  ("Bank of America", ENTITY_TYPE_BANK),
    "citibank":         ("Citibank",        ENTITY_TYPE_BANK),
    "hsbc":             ("HSBC",            ENTITY_TYPE_BANK),
    "barclays":         ("Barclays",        ENTITY_TYPE_BANK),

    # Affiliate networks
    "rakuten":          ("Rakuten",         ENTITY_TYPE_AFFILIATE_NETWORK),
    "cj affiliate":     ("CJ Affiliate",   ENTITY_TYPE_AFFILIATE_NETWORK),
    "shareasale":       ("ShareASale",      ENTITY_TYPE_AFFILIATE_NETWORK),
    "impact":           ("Impact",          ENTITY_TYPE_AFFILIATE_NETWORK),
    "awin":             ("Awin",            ENTITY_TYPE_AFFILIATE_NETWORK),

    # Platforms
    "shopify":          ("Shopify",         ENTITY_TYPE_PLATFORM),
    "woocommerce":      ("WooCommerce",     ENTITY_TYPE_PLATFORM),
    "bigcommerce":      ("BigCommerce",     ENTITY_TYPE_PLATFORM),
    "magento":          ("Magento",         ENTITY_TYPE_PLATFORM),
    "wix":              ("Wix",             ENTITY_TYPE_PLATFORM),
    "squarespace":      ("Squarespace",     ENTITY_TYPE_PLATFORM),

    # Marketplaces
    "amazon":           ("Amazon",          ENTITY_TYPE_MARKETPLACE),
    "ebay":             ("eBay",            ENTITY_TYPE_MARKETPLACE),
    "etsy":             ("Etsy",            ENTITY_TYPE_MARKETPLACE),
    "walmart marketplace": ("Walmart Marketplace", ENTITY_TYPE_MARKETPLACE),

    # Consumer services
    "venmo":            ("Venmo",           ENTITY_TYPE_CONSUMER_SERVICE),
    "cash app":         ("Cash App",        ENTITY_TYPE_CONSUMER_SERVICE),
    "zelle":            ("Zelle",           ENTITY_TYPE_CONSUMER_SERVICE),
    "apple pay":        ("Apple Pay",       ENTITY_TYPE_CONSUMER_SERVICE),
    "google pay":       ("Google Pay",      ENTITY_TYPE_CONSUMER_SERVICE),
}

# Sorted longest-first so "bank of america" matches before "chase" etc.
_REGISTRY_KEYS_SORTED = sorted(ENTITY_REGISTRY.keys(), key=len, reverse=True)

# ── Consumer complaint keyword signals ─────────────────────────────
_CONSUMER_KEYWORDS = [
    "cashback", "cash back", "refund not received", "refund denied",
    "my order", "my purchase", "customer service", "consumer complaint",
    "didn't get my", "never received", "want my money back",
    "as a customer", "buyer complaint", "return policy",
]

# ── Affiliate distress keyword signals ─────────────────────────────
_AFFILIATE_DISTRESS_KEYWORDS = [
    "affiliate payout", "commission withheld", "commission delayed",
    "affiliate earnings", "publisher payout", "referral commission",
    "affiliate account suspended",
]

# ── Processor distress override keywords ───────────────────────────
_PROCESSOR_DISTRESS_KEYWORDS = [
    "rakuten pay", "processing failed", "payment processing",
    "merchant account", "payout frozen", "funds held",
]


def classify_entity(text):
    """
    Classify a signal's text to extract entity information.

    Returns:
        dict with keys:
            entity_name:    str or None
            entity_type:    str (one of VALID_ENTITY_TYPES)
            classification: str or None (consumer_complaint, affiliate_distress,
                           processor_distress, merchant_distress)
    """
    if not text:
        return {
            "entity_name": None,
            "entity_type": ENTITY_TYPE_UNKNOWN,
            "classification": None,
        }

    text_lower = text.lower()
    signal_info = classify_merchant_signal(text)

    # ── Step 1: Identify entity from registry ──
    entity_name = None
    entity_type = ENTITY_TYPE_UNKNOWN

    for key in _REGISTRY_KEYS_SORTED:
        if key in text_lower:
            display_name, etype = ENTITY_REGISTRY[key]
            entity_name = display_name
            entity_type = etype
            break

    # ── Step 2: Determine classification ──
    classification = None

    if entity_type == ENTITY_TYPE_AFFILIATE_NETWORK:
        # Rakuten and similar: sub-classify based on context
        if any(kw in text_lower for kw in _PROCESSOR_DISTRESS_KEYWORDS):
            classification = CLASS_PROCESSOR_DISTRESS
        elif any(kw in text_lower for kw in _AFFILIATE_DISTRESS_KEYWORDS):
            classification = CLASS_AFFILIATE_DISTRESS
        elif any(kw in text_lower for kw in _CONSUMER_KEYWORDS):
            classification = CLASS_CONSUMER_COMPLAINT
        else:
            # Default affiliate network signal without clear context → consumer
            classification = CLASS_CONSUMER_COMPLAINT

    elif entity_type == ENTITY_TYPE_CONSUMER_SERVICE:
        # Venmo, Cash App, etc. — nearly always consumer complaints
        if any(kw in text_lower for kw in _CONSUMER_KEYWORDS):
            classification = CLASS_CONSUMER_COMPLAINT
        else:
            classification = CLASS_CONSUMER_COMPLAINT

    elif entity_type in (ENTITY_TYPE_PROCESSOR, ENTITY_TYPE_BANK):
        if signal_info["classification"] == CLASS_CONSUMER_OR_IRRELEVANT:
            classification = CLASS_CONSUMER_COMPLAINT
        elif signal_info["classification"] == CLASS_MERCHANT_OPERATOR:
            classification = CLASS_MERCHANT_DISTRESS
        else:
            classification = CLASS_NON_MERCHANT

    elif entity_type in (ENTITY_TYPE_PLATFORM, ENTITY_TYPE_MARKETPLACE):
        if signal_info["classification"] == CLASS_CONSUMER_OR_IRRELEVANT:
            classification = CLASS_CONSUMER_COMPLAINT
        elif signal_info["classification"] == CLASS_MERCHANT_OPERATOR:
            classification = CLASS_MERCHANT_DISTRESS
        else:
            classification = CLASS_NON_MERCHANT

    else:
        if signal_info["classification"] == CLASS_CONSUMER_OR_IRRELEVANT:
            classification = CLASS_CONSUMER_COMPLAINT
        elif signal_info["classification"] == CLASS_MERCHANT_OPERATOR:
            classification = CLASS_MERCHANT_DISTRESS
        else:
            classification = CLASS_NON_MERCHANT

    return {
        "entity_name": entity_name,
        "entity_type": entity_type,
        "classification": classification,
        "merchant_signal_score": signal_info["merchant_score"],
        "consumer_signal_score": signal_info["consumer_score"],
        "signal_reasons": signal_info["reasons"],
    }


def score_entity_type(entity_type):
    """
    Returns a qualification score modifier based on entity type.
    Higher values mean the entity is more likely to be a valid PayFlux lead.
    """
    modifiers = {
        ENTITY_TYPE_PROCESSOR:         40,
        ENTITY_TYPE_BANK:              35,
        ENTITY_TYPE_PLATFORM:          25,
        ENTITY_TYPE_MARKETPLACE:       15,
        ENTITY_TYPE_AFFILIATE_NETWORK: 10,
        ENTITY_TYPE_CONSUMER_SERVICE: -50,
        ENTITY_TYPE_UNKNOWN:            0,
    }
    return modifiers.get(entity_type, 0)


def is_valid_lead(classification, entity_type):
    """
    Lead validity gate. Returns True if the signal should proceed
    to conversion opportunity creation.
    """
    # Hard block: consumer complaints never become leads
    if classification in (CLASS_CONSUMER_COMPLAINT, CLASS_NON_MERCHANT):
        return False

    # Soft block: affiliate distress is low-value but not blocked
    # Consumer services are blocked regardless of classification
    if entity_type == ENTITY_TYPE_CONSUMER_SERVICE:
        return False

    return True
