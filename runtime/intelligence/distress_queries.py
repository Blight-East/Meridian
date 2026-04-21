"""
Distress Query Library for Historical Signal Expansion.

Curated keyword queries targeting merchant payment distress signals
across Reddit, HackerNews, and forum platforms.
"""

# ── Core distress queries (high signal density) ─────────────────────────────
DISTRESS_QUERIES = [
    # Account actions
    "stripe froze my account",
    "stripe banned my store",
    "stripe account terminated",
    "stripe holding my funds",
    "paypal holding funds",
    "paypal account limited",
    "paypal froze my business account",
    "payment processor shut down my account",
    "merchant account terminated",
    "payment gateway holding funds",

    # Seeking alternatives
    "need high risk payment processor",
    "looking for payment gateway",
    "alternative to stripe",
    "alternative to paypal",
    "payment gateway recommendations",
    "best payment processor for high risk",
    "stripe alternative for high risk",

    # Specific distress signals
    "payment processor problems",
    "payout delayed stripe",
    "reserve hold payment processor",
    "chargeback rate too high merchant",
    "180 day reserve stripe",
    "funds held by payment processor",
    "merchant account shut down no warning",
    "payment gateway closed my account",

    # Platform-specific
    "shopify payments disabled",
    "square froze my account",
    "adyen terminated merchant",
    "braintree account suspended",
    "woocommerce payment issues",

    # Industry context
    "high risk merchant processing",
    "CBD payment processing problems",
    "dropshipping payment processor banned",
    "subscription business payment gateway",
]

# ── Reddit-specific subreddits to target ────────────────────────────────────
REDDIT_SUBREDDITS = [
    "Entrepreneur",
    "smallbusiness",
    "ecommerce",
    "shopify",
    "stripe",
    "paypal",
    "dropship",
    "FulfillmentByAmazon",
    "startups",
]

# ── HackerNews tags for filtering ───────────────────────────────────────────
HN_SEARCH_TAGS = ["story", "comment"]
