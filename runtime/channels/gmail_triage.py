from __future__ import annotations

import hashlib
import os
import re
from functools import lru_cache
from datetime import datetime, timezone

from sqlalchemy import text

from config.logging_config import get_logger
from memory.structured.db import engine, save_event, save_learning_feedback
from runtime.channels.base import ChannelTarget
from runtime.channels.store import get_gmail_thread_intelligence, upsert_gmail_thread_intelligence
from runtime.channels.templates import normalized_text
from runtime.intelligence.brand_extraction import extract_brand_candidates
from runtime.intelligence.merchant_identity import extract_domains, normalize_domain, resolve_merchant_identity
from runtime.intelligence.merchant_signal_classifier import CLASS_MERCHANT_OPERATOR, classify_merchant_signal
from runtime.ranking.signal_ranker import score_signal
from runtime.reasoning.control_plane import run_reasoning_task

logger = get_logger("gmail_triage")

THREAD_CATEGORIES = {"merchant_distress", "customer_support", "noise_system"}
DISTRESS_TYPES = {
    "account_frozen",
    "payouts_delayed",
    "reserve_hold",
    "verification_review",
    "processor_switch_intent",
    "chargeback_issue",
    "account_terminated",
    "onboarding_rejected",
    "unknown",
}
SECONDARY_DISTRESS_TYPES = {
    "frozen_funds",
    "payout_paused",
    "reserve_increase",
    "compliance_review",
    "website_verification",
    "kyc_kyb_review",
    "rolling_reserve",
    "processing_disabled",
    "looking_for_alternative",
    "negative_balance",
    "dispute_spike",
    "unknown",
}
PROCESSORS = {
    "stripe",
    "paypal",
    "square",
    "adyen",
    "shopify_payments",
    "braintree",
    "authorize_net",
    "worldpay",
    "checkout_com",
    "unknown",
}
INDUSTRIES = {
    "ecommerce",
    "saas",
    "agency",
    "creator",
    "marketplace",
    "health_wellness",
    "high_risk",
    "unknown",
}

AUTOMATED_SENDER_TOKENS = (
    "no-reply",
    "noreply",
    "do-not-reply",
    "donotreply",
    "mailer-daemon",
    "postmaster",
)
MARKETING_SENDER_TOKENS = (
    "alert",
    "alerts",
    "digest",
    "marketing",
    "news",
    "newsletter",
    "notify",
    "offers",
    "updates",
)
SYSTEM_DOMAIN_TOKENS = (
    "google.com",
    "googleworkspace.com",
    "calendar.google.com",
    "amazonses.com",
    "mailchimp.com",
    "sendgrid.net",
    "amazonses.com",
)
HARD_NOISE_PATTERNS = (
    "security alert",
    "google workspace billing",
    "verification code",
    "password reset",
    "calendar",
    "invoice available",
    "subscription receipt",
    "payment receipt",
    "delivery status notification",
    "undeliverable",
    "sandbox",
    "test import validation",
    "agent flux gmail sandbox",
)
PROMOTIONAL_NOISE_PATTERNS = (
    "% off",
    "discount code",
    "newsletter",
    "new arrivals",
    "arrivals",
    "sale",
    "spring newsletter",
    "personalized card recommendations",
    "perfect new card",
    "cordially invited",
    "you are cordially invited",
    "all dressed up",
    "new kicks",
    "peek at",
    "plan the perfect",
    "aegean escape",
    "needs a nap",
    "job might be right for you",
)
PROMOTIONAL_SENDER_DOMAIN_TOKENS = (
    "brief.barchart.com",
    "silversea.com",
    "myjobhelper.com",
    "onelink.me",
    "ziprecruiter.com",
)
HIGH_INTENT_REPLY_PHRASES = (
    "interested",
    "tell me more",
    "send more info",
    "can you send",
    "let's talk",
    "lets talk",
    "book a call",
    "jump on a call",
    "how much",
    "pricing",
    "what does it cost",
    "this is live",
    "we need this",
    "we need help",
)
OBJECTION_REPLY_PHRASES = (
    "already working with",
    "already have",
    "already using",
    "we're fine",
    "we are fine",
    "not a fit",
    "don't need",
    "do not need",
    "too expensive",
    "skeptical",
    "how did you get",
)
DEFER_REPLY_PHRASES = (
    "not now",
    "later",
    "circle back",
    "follow up next week",
    "follow up later",
    "not this quarter",
    "timing is bad",
)
AUTHORITY_MISMATCH_PHRASES = (
    "wrong person",
    "not the right person",
    "not my area",
    "i'm not the right",
    "not the owner",
    "talk to",
    "reach out to",
)
NEGATIVE_REPLY_PHRASES = (
    "not interested",
    "stop emailing",
    "remove me",
    "unsubscribe",
    "leave me alone",
)
STRONG_DISTRESS_PHRASES = (
    "stripe froze",
    "account frozen",
    "account was frozen",
    "account closed",
    "account terminated",
    "payout delayed",
    "payouts delayed",
    "funds held",
    "funds on hold",
    "holding funds",
    "rolling reserve",
    "payment processor",
    "merchant account",
    "processor alternative",
    "looking for a new processor",
    "need a processor",
    "under review",
    "verification review",
    "website verification",
    "kyc",
    "kyb",
    "payments disabled",
    "cannot process payments",
    "cannot accept payments",
    "chargeback spike",
)
BUSINESS_CONTEXT_TERMS = (
    "store",
    "shop",
    "business",
    "sales",
    "revenue",
    "customers",
    "checkout",
    "payouts",
    "processor",
    "merchant account",
    "transactions",
    "orders",
)
BUSINESS_TITLE_TERMS = (
    "founder",
    "owner",
    "operator",
    "ceo",
    "coo",
    "finance",
    "operations",
    "ops",
    "cfo",
)
SUPPORT_TERMS = (
    "help",
    "question",
    "can you",
    "how do i",
    "how can i",
    "demo",
    "pricing",
    "onboarding",
    "docs",
    "feature request",
)
AUTOMATED_BODY_MARKERS = (
    "unsubscribe",
    "manage preferences",
    "delivery failed",
    "automated message",
    "do not reply",
    "this is a system generated",
)
FREEMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "live.com",
    "msn.com",
}
GENERIC_NAME_TOKENS = {"team", "support", "billing", "notifications", "security", "payflux"}
NOISY_MAIL_SUBDOMAIN_LABELS = {
    "alert",
    "alerts",
    "click",
    "clicks",
    "email",
    "em",
    "link",
    "links",
    "mail",
    "mailer",
    "mkt",
    "news",
    "newsletter",
    "notify",
    "public",
    "tracking",
    "trk",
}

PROCESSOR_PATTERNS = {
    "stripe": ("stripe", "stripe connect"),
    "paypal": ("paypal",),
    "square": ("square", "squareup"),
    "adyen": ("adyen",),
    "shopify_payments": ("shopify payments",),
    "braintree": ("braintree",),
    "authorize_net": ("authorize.net", "authorize net"),
    "worldpay": ("worldpay",),
    "checkout_com": ("checkout.com", "checkout com"),
}
DISTRESS_TYPE_PATTERNS = {
    "account_frozen": ("stripe froze", "account frozen", "account was frozen", "frozen account"),
    "payouts_delayed": ("payout delayed", "payouts delayed", "funds held", "funds on hold", "holding funds"),
    "reserve_hold": ("rolling reserve", "reserve hold", "reserve increase", "reserve requirement", "funds reserve"),
    "verification_review": ("under review", "verification review", "website verification", "kyc", "kyb", "compliance review"),
    "processor_switch_intent": ("processor alternative", "looking for a new processor", "need a processor", "alternative to stripe", "alternative to paypal"),
    "chargeback_issue": ("chargeback spike", "dispute spike", "negative balance"),
    "account_terminated": ("account terminated", "terminated account", "account closed"),
    "onboarding_rejected": ("onboarding rejected", "application rejected", "unable to onboard"),
}
SECONDARY_SIGNAL_PATTERNS = {
    "frozen_funds": ("funds held", "funds on hold", "frozen funds"),
    "payout_paused": ("payout paused", "payout delayed", "payouts delayed"),
    "reserve_increase": ("reserve increase",),
    "compliance_review": ("compliance review",),
    "website_verification": ("website verification",),
    "kyc_kyb_review": ("kyc", "kyb"),
    "rolling_reserve": ("rolling reserve",),
    "processing_disabled": ("payments disabled", "cannot process payments", "cannot accept payments"),
    "looking_for_alternative": ("looking for a new processor", "processor alternative", "need a processor", "alternative to stripe", "alternative to paypal"),
    "negative_balance": ("negative balance",),
    "dispute_spike": ("dispute spike", "chargeback spike"),
}
INDUSTRY_PATTERNS = {
    "ecommerce": ("store", "shop", "checkout", "orders", "cart", "products"),
    "saas": ("saas", "subscription software", "api", "platform"),
    "agency": ("agency", "client work", "client payments"),
    "creator": ("creator", "newsletter", "audience", "subscribers"),
    "marketplace": ("marketplace", "sellers", "vendors"),
    "health_wellness": ("wellness", "health", "supplement", "fitness"),
    "high_risk": ("cbd", "adult", "gambling", "vape", "firearms", "crypto"),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lower(value: str | None) -> str:
    return (value or "").strip().lower()


def _email_parts(email: str) -> tuple[str, str]:
    normalized = _lower(email)
    if "@" not in normalized:
        return normalized, ""
    local, domain = normalized.rsplit("@", 1)
    return local, domain


def _internal_test_mailboxes() -> set[str]:
    values = {os.getenv("GMAIL_SENDER_EMAIL", "").strip().lower()}
    for item in os.getenv("GMAIL_INTERNAL_TEST_MAILBOXES", "").split(","):
        if item.strip():
            values.add(item.strip().lower())
    return {value for value in values if value}


def _internal_sender_domains() -> set[str]:
    domains = set()
    for key in ("GMAIL_SENDER_EMAIL", "SMTP_FROM"):
        _local, domain = _email_parts(os.getenv(key, ""))
        if domain:
            domains.add(domain)
    return domains


def _contains_any(text: str, patterns) -> list[str]:
    return [pattern for pattern in patterns if pattern in text]


@lru_cache(maxsize=8)
def _table_columns(table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {row[0] for row in rows}


def _is_business_domain(domain: str) -> bool:
    lowered = _lower(domain)
    if not lowered or lowered in FREEMAIL_DOMAINS:
        return False
    if any(token in lowered for token in SYSTEM_DOMAIN_TOKENS):
        return False
    return "." in lowered


def _base_domain(domain: str) -> str:
    normalized = normalize_domain(domain or "")
    labels = [label for label in normalized.split(".") if label]
    if len(labels) < 2:
        return normalized
    return ".".join(labels[-2:])


def _has_noisy_mail_subdomain(domain: str) -> bool:
    normalized = normalize_domain(domain or "")
    labels = [label for label in normalized.split(".") if label]
    if len(labels) < 3:
        return False
    return any(label in NOISY_MAIL_SUBDOMAIN_LABELS for label in labels[:-2])


def _looks_like_sender_campaign_domain(candidate_domain: str, sender_domain: str) -> bool:
    candidate = normalize_domain(candidate_domain or "")
    sender = normalize_domain(sender_domain or "")
    if not candidate or not sender:
        return False
    if candidate == sender:
        return False
    if _base_domain(candidate) != _base_domain(sender):
        return False
    return _has_noisy_mail_subdomain(candidate) or candidate.endswith(f".{sender}")


def _derive_name_from_domain(domain: str) -> str | None:
    normalized = normalize_domain(domain or "")
    if not normalized:
        return None
    base = normalized.split("/")[0].split(".")[0]
    if not base:
        return None
    return base.replace("-", " ").replace("_", " ").title()


def _human_written(text: str) -> bool:
    lowered = _lower(text)
    if not lowered:
        return False
    if any(marker in lowered for marker in AUTOMATED_BODY_MARKERS):
        return False
    return bool(re.search(r"\b(i|we|my|our|help|thanks|question)\b", lowered))


def _real_operator_sender(sender_name: str, sender_email: str) -> bool:
    local_part, domain = _email_parts(sender_email)
    if any(token in local_part for token in AUTOMATED_SENDER_TOKENS):
        return False
    if sender_name and sender_name.strip().lower() not in GENERIC_NAME_TOKENS:
        return True
    return _is_business_domain(domain)


def _looks_like_marketing_sender(sender_email: str, sender_domain: str) -> bool:
    local_part, domain = _email_parts(sender_email)
    labels = [label for label in normalize_domain(domain or "").split(".") if label]
    pre_root = labels[:-2] if len(labels) >= 3 else []
    if any(token in local_part for token in MARKETING_SENDER_TOKENS):
        return True
    if any(label in MARKETING_SENDER_TOKENS for label in pre_root):
        return True
    if any(label in {"offer", "offers", "promo", "promos", "promotion", "promotions", "broadcast", "broadcasting"} for label in pre_root):
        return True
    return any(domain.endswith(token) for token in PROMOTIONAL_SENDER_DOMAIN_TOKENS)


def _strict_opportunity_gate(intelligence: dict) -> tuple[bool, list[str]]:
    metadata = dict(intelligence.get("metadata") or {})
    sender_email = _lower(intelligence.get("sender_email"))
    sender_domain = normalize_domain(intelligence.get("sender_domain") or "")
    merchant_domain = normalize_domain(intelligence.get("merchant_domain_candidate") or "")
    distress_type = str(intelligence.get("distress_type") or "").strip().lower()
    confidence = float(intelligence.get("confidence") or 0.0)
    identity_confidence = float(intelligence.get("merchant_identity_confidence") or 0.0)
    reasons: list[str] = []

    if intelligence.get("thread_category") != "merchant_distress":
        reasons.append("not_merchant_distress")
    if bool(intelligence.get("test_thread")):
        reasons.append("test_thread")
    if confidence < 0.85:
        reasons.append("confidence_below_strict_bar")
    if distress_type in {"", "unknown", "none"}:
        reasons.append("distress_unclassified")
    if identity_confidence < 0.8:
        reasons.append("merchant_identity_too_weak")
    if not merchant_domain and not intelligence.get("merchant_id"):
        reasons.append("merchant_domain_missing")
    if metadata.get("sender_is_automated"):
        reasons.append("automated_sender")
    if metadata.get("promotional_noise_hits"):
        reasons.append("promotional_noise_detected")
    if _looks_like_marketing_sender(sender_email, sender_domain):
        reasons.append("marketing_sender_pattern")
    if _has_noisy_mail_subdomain(sender_domain):
        reasons.append("campaign_sender_subdomain")
    if sender_domain and not _is_business_domain(sender_domain):
        reasons.append("non_business_sender_domain")

    return not reasons, reasons


def _processor_from_text(text: str) -> str:
    lowered = _lower(text)
    for processor, patterns in PROCESSOR_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return processor
    return "unknown"


def _distress_type_from_text(text: str) -> tuple[str, list[str], list[str]]:
    lowered = _lower(text)
    primary = "unknown"
    primary_hits = []
    for distress_type, patterns in DISTRESS_TYPE_PATTERNS.items():
        hits = [pattern for pattern in patterns if pattern in lowered]
        if hits:
            primary = distress_type
            primary_hits = hits
            break
    secondary = []
    for secondary_signal, patterns in SECONDARY_SIGNAL_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            secondary.append(secondary_signal)
    if not secondary and primary != "unknown":
        secondary = ["unknown"]
    return primary, secondary[:4], primary_hits


def _industry_from_text(text: str) -> str:
    lowered = _lower(text)
    for industry, patterns in INDUSTRY_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return industry
    return "unknown"


def _classify_reply_intent(
    *,
    subject: str,
    body: str,
    snippet: str,
    sender_is_automated: bool,
    thread_category: str,
) -> dict:
    text_blob = " ".join(part for part in (subject, body, snippet) if part).lower()
    if sender_is_automated:
        return {
            "reply_intent": "automated",
            "buying_intent": "none",
            "confidence": 0.95,
            "reason": "The sender looks automated, so this is not a live buyer signal.",
        }
    if any(phrase in text_blob for phrase in NEGATIVE_REPLY_PHRASES):
        return {
            "reply_intent": "negative",
            "buying_intent": "none",
            "confidence": 0.93,
            "reason": "The reply is explicitly negative or asks to stop contact.",
        }
    if any(phrase in text_blob for phrase in AUTHORITY_MISMATCH_PHRASES):
        return {
            "reply_intent": "authority_mismatch",
            "buying_intent": "low",
            "confidence": 0.84,
            "reason": "The reply suggests this contact is not the decision-maker.",
        }
    if any(phrase in text_blob for phrase in HIGH_INTENT_REPLY_PHRASES):
        return {
            "reply_intent": "interested",
            "buying_intent": "high",
            "confidence": 0.86,
            "reason": "The reply shows active interest, urgency, or willingness to continue the conversation.",
        }
    if any(phrase in text_blob for phrase in DEFER_REPLY_PHRASES):
        return {
            "reply_intent": "defer",
            "buying_intent": "medium",
            "confidence": 0.78,
            "reason": "The reply does not reject the offer, but pushes the timing out.",
        }
    if any(phrase in text_blob for phrase in OBJECTION_REPLY_PHRASES):
        return {
            "reply_intent": "objection",
            "buying_intent": "medium",
            "confidence": 0.76,
            "reason": "The reply raises a real objection rather than ignoring the conversation entirely.",
        }
    if thread_category == "merchant_distress":
        return {
            "reply_intent": "engaged",
            "buying_intent": "medium",
            "confidence": 0.68,
            "reason": "The reply still looks merchant-operated and tied to a live processor issue.",
        }
    return {
        "reply_intent": "unknown",
        "buying_intent": "low",
        "confidence": 0.45,
        "reason": "The reply does not cleanly resolve to interest, objection, or a dead end yet.",
    }


def _business_context_flags(target: ChannelTarget, normalized: str, sender_domain: str) -> list[str]:
    flags = []
    if any(term in normalized for term in BUSINESS_CONTEXT_TERMS):
        flags.append("business_context")
    if _is_business_domain(sender_domain):
        flags.append("business_domain")
    sender_name = _lower(target.metadata.get("sender_name", ""))
    if any(term in sender_name or term in normalized for term in BUSINESS_TITLE_TERMS):
        flags.append("operator_title")
    classifier = classify_merchant_signal(normalized)
    if classifier.get("classification") == CLASS_MERCHANT_OPERATOR:
        flags.append("merchant_operator")
    return list(dict.fromkeys(flags))


def _is_test_thread(target: ChannelTarget, normalized: str, sender_email: str) -> bool:
    subject = _lower(target.title)
    local_part, _domain = _email_parts(sender_email)
    if "agent flux gmail sandbox" in subject:
        return True
    if "sandbox" in normalized or "test import validation" in normalized:
        return True
    if "internal test" in normalized or "validation" in normalized:
        return True
    if sender_email in _internal_test_mailboxes():
        return True
    if local_part.startswith("test") or local_part.endswith("+test"):
        return True
    if sender_email and sender_email == _lower(target.account_identity):
        return True
    return False


def _hard_noise_match(target: ChannelTarget, normalized: str, sender_email: str, sender_domain: str) -> tuple[bool, list[str]]:
    subject = _lower(target.title)
    sender_name = _lower(target.metadata.get("sender_name", ""))
    local_part, _domain = _email_parts(sender_email)
    hits = []
    if any(token in local_part or token in sender_name for token in AUTOMATED_SENDER_TOKENS):
        hits.append("automated_sender")
    if sender_domain and any(token in sender_domain for token in SYSTEM_DOMAIN_TOKENS):
        hits.append("system_domain")
    pattern_hits = _contains_any(f"{subject} {normalized}", HARD_NOISE_PATTERNS)
    hits.extend(pattern_hits)
    return bool(hits), hits


def _promotional_noise_hits(target: ChannelTarget, normalized: str, sender_email: str) -> list[str]:
    subject = _lower(target.title)
    sender_name = _lower(target.metadata.get("sender_name", ""))
    _local, sender_domain = _email_parts(sender_email)
    sender_blob = " ".join(part for part in [subject, normalized, sender_name, sender_email, sender_domain] if part)
    hits = _contains_any(sender_blob, PROMOTIONAL_NOISE_PATTERNS)
    if sender_domain and any(token in sender_domain for token in PROMOTIONAL_SENDER_DOMAIN_TOKENS):
        hits.append("promotional_sender_domain")
    return hits


def _extract_merchant_identity(
    target: ChannelTarget,
    normalized: str,
    sender_email: str,
    sender_domain: str,
    test_thread: bool,
    category: str,
    promotional_hits: list[str] | None = None,
):
    if test_thread or category != "merchant_distress":
        return (None, None, None)
    promotional_hits = list(promotional_hits or [])
    if promotional_hits or any(token in (sender_domain or "") for token in PROMOTIONAL_SENDER_DOMAIN_TOKENS):
        return (None, None, None)
    sender_local, _ = _email_parts(sender_email)
    marketing_sender = any(token in sender_local for token in MARKETING_SENDER_TOKENS) or _has_noisy_mail_subdomain(sender_domain)

    explicit_domains = [
        normalize_domain(domain)
        for domain in extract_domains(f"{target.title}\n{target.body}")
    ]
    explicit_domains = [
        domain
        for domain in explicit_domains
        if domain
        and domain != normalize_domain(sender_domain)
        and domain not in _internal_sender_domains()
        and not _looks_like_sender_campaign_domain(domain, sender_domain)
    ]
    if explicit_domains:
        domain_candidate = explicit_domains[0]
        return (_derive_name_from_domain(domain_candidate), domain_candidate, 0.9)

    if (
        _is_business_domain(sender_domain)
        and sender_domain not in _internal_sender_domains()
        and not marketing_sender
        and not _has_noisy_mail_subdomain(sender_domain)
    ):
        domain_candidate = normalize_domain(sender_domain)
        return (_derive_name_from_domain(domain_candidate), domain_candidate, 0.9)

    if marketing_sender:
        return (None, None, None)

    brand_candidates = extract_brand_candidates(
        f"{target.title}\n{target.body}",
        merchant_name=None,
        author=target.metadata.get("sender_name", ""),
    )
    for candidate in brand_candidates:
        brand = candidate.get("brand")
        if brand and brand.lower() not in {"stripe", "paypal", "square", "adyen"}:
            return (brand, None, 0.75)

    sender_name = (target.metadata.get("sender_name") or "").strip()
    if sender_name and sender_name.lower() not in GENERIC_NAME_TOKENS:
        return (sender_name, None, 0.6)
    return (None, None, None)


def build_thread_fingerprint(target: ChannelTarget) -> str:
    normalized = normalized_text(target.title, target.body)
    raw = "|".join(
        [
            target.thread_id,
            _lower(target.metadata.get("message_id", "")),
            _lower(target.metadata.get("sender_email", target.author)),
            normalized,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _gmail_signal_hash(thread_id: str, sender_email: str, subject: str) -> str:
    raw = f"gmail:{thread_id}:{_lower(sender_email)}:{normalized_text(subject)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_signal_content(intelligence: dict, target: ChannelTarget) -> str:
    parts = [
        f"[gmail][thread:{intelligence['thread_id']}][processor:{intelligence['processor']}][distress:{intelligence.get('distress_type') or 'unknown'}]",
        f"Subject: {intelligence['subject']}",
        f"Sender: {intelligence.get('sender_name') or intelligence['sender_email']}",
    ]
    if intelligence.get("merchant_name_candidate"):
        parts.append(f"Merchant candidate: {intelligence['merchant_name_candidate']}")
    if intelligence.get("merchant_domain_candidate"):
        parts.append(f"Merchant domain candidate: {intelligence['merchant_domain_candidate']}")
    sender_domain = intelligence.get("sender_domain") or "unknown"
    if sender_domain == "unknown" or sender_domain not in _internal_sender_domains():
        parts.append(f"Sender domain: {sender_domain}")
    if intelligence.get("distress_signals"):
        parts.append("Signals: " + ", ".join(intelligence["distress_signals"]))
    body = (target.body or intelligence.get("snippet") or "").strip()
    if body:
        parts.append(body[:3000])
    return "\n".join(parts)


def _rank_signal_metadata(content: str, intelligence: dict) -> tuple[float, bool, bool]:
    priority, metadata = score_signal(content, created_at=datetime.now(timezone.utc))
    priority = max(priority, round(float(intelligence.get("confidence", 0.0)) * 100, 2))
    merchant_relevant = intelligence["thread_category"] == "merchant_distress" or bool(metadata.get("merchant_relevant"))
    revenue_detected = bool(
        metadata.get("revenue_detected")
        or any(term in _lower(content) for term in ("sales", "revenue", "orders", "checkout", "customers", "payout"))
    )
    return priority, merchant_relevant, revenue_detected


def _link_signal_to_cluster(conn, signal_id: int, intelligence: dict) -> int | None:
    if "cluster_id" not in _table_columns("signals"):
        return None

    distress_type = intelligence.get("distress_type") or "unknown"
    processor = intelligence.get("processor") or "unknown"
    topic_hints = {
        "account_frozen": ("frozen", "locked", "disabled"),
        "payouts_delayed": ("payout", "funds", "delayed"),
        "reserve_hold": ("reserve",),
        "verification_review": ("review", "verification", "kyc"),
        "processor_switch_intent": ("alternative", "processor", "switch"),
        "chargeback_issue": ("chargeback", "dispute"),
        "account_terminated": ("terminated", "closed"),
        "onboarding_rejected": ("onboarding", "rejected"),
        "unknown": tuple(),
    }
    rows = conn.execute(
        text(
            """
            SELECT id, cluster_topic
            FROM clusters
            WHERE created_at >= NOW() - INTERVAL '30 days'
            ORDER BY created_at DESC
            LIMIT 100
            """
        )
    ).fetchall()
    for cluster_id, cluster_topic in rows:
        lowered_topic = _lower(cluster_topic)
        topic_match = any(token in lowered_topic for token in topic_hints.get(distress_type, tuple()))
        processor_match = processor == "unknown" or processor in lowered_topic
        if processor_match and (topic_match or processor != "unknown"):
            conn.execute(
                text("UPDATE signals SET cluster_id = :cluster_id WHERE id = :signal_id"),
                {"cluster_id": cluster_id, "signal_id": signal_id},
            )
            return int(cluster_id)
    return None


def _linked_merchant_id(conn, signal_id: int) -> int | None:
    if not signal_id:
        return None
    row = conn.execute(
        text(
            """
            SELECT merchant_id
            FROM merchant_signals
            WHERE signal_id = :signal_id
            ORDER BY merchant_id ASC
            LIMIT 1
            """
        ),
        {"signal_id": signal_id},
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _backfill_signal_merchant_id(conn, signal_id: int, merchant_id: int | None) -> bool:
    if not signal_id or not merchant_id or "merchant_id" not in _table_columns("signals"):
        return False
    updated = conn.execute(
        text(
            """
            UPDATE signals
            SET merchant_id = :merchant_id
            WHERE id = :signal_id
              AND (merchant_id IS NULL OR merchant_id != :merchant_id)
            """
        ),
        {"signal_id": signal_id, "merchant_id": merchant_id},
    ).rowcount
    return bool(updated)


def repair_gmail_signal_merchant_ids(limit: int | None = None) -> dict:
    if "merchant_id" not in _table_columns("signals"):
        return {"status": "skipped", "reason": "signals_missing_merchant_id", "repaired_count": 0, "signal_ids": []}

    params = {}
    query = """
        SELECT s.id AS signal_id, ms.merchant_id
        FROM signals s
        JOIN merchant_signals ms ON ms.signal_id = s.id
        WHERE s.source = 'gmail'
          AND s.merchant_id IS NULL
        ORDER BY s.id ASC
    """
    if limit is not None:
        query += " LIMIT :limit"
        params["limit"] = int(limit)

    repaired = []
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        for signal_id, merchant_id in rows:
            if _backfill_signal_merchant_id(conn, int(signal_id), int(merchant_id)):
                repaired.append(int(signal_id))
        conn.commit()

    if repaired:
        save_event("gmail_signal_merchant_backfill", {"signal_ids": repaired, "count": len(repaired)})
    return {"status": "ok", "repaired_count": len(repaired), "signal_ids": repaired}


def _upsert_signal(target: ChannelTarget, intelligence: dict) -> tuple[int | None, int | None]:
    signal_hash = _gmail_signal_hash(intelligence["thread_id"], intelligence["sender_email"], intelligence["subject"])
    content = _build_signal_content(intelligence, target)
    priority_score, merchant_relevant, revenue_detected = _rank_signal_metadata(content, intelligence)
    signal_id = None
    cluster_id = None
    signal_columns = _table_columns("signals")
    signal_params = {
        "content": content,
        "signal_hash": signal_hash,
        "confidence": float(intelligence["confidence"]),
        "merchant_name": intelligence.get("merchant_name_candidate") or "unknown",
        "industry": intelligence.get("industry") or "unknown",
        "priority_score": priority_score,
        "classification": intelligence.get("thread_category"),
        "entity_type": "unknown",
        "region": "unknown",
    }

    with engine.connect() as conn:
        existing = conn.execute(
            text(
                """
                SELECT id
                FROM signals
                WHERE signal_hash = :signal_hash
                ORDER BY id ASC
                LIMIT 1
                """
            ),
            {"signal_hash": signal_hash},
        ).fetchone()
        if existing:
            signal_id = int(existing[0])
            assignments = ["source = 'gmail'"]
            for column in ("content", "confidence", "merchant_name", "industry", "signal_hash", "priority_score", "classification", "entity_type", "region"):
                if column in signal_columns:
                    assignments.append(f"{column} = :{column}")
            if "age_weight" in signal_columns:
                assignments.append("age_weight = 1.0")
            if "ranked_at" in signal_columns:
                assignments.append("ranked_at = NOW()")
            signal_params["signal_id"] = signal_id
            conn.execute(
                text(
                    f"""
                    UPDATE signals
                    SET {', '.join(assignments)}
                    WHERE id = :signal_id
                    """
                ),
                signal_params,
            )
        else:
            insert_columns = ["source"]
            insert_values = ["'gmail'"]
            for column in ("content", "detected_at", "signal_hash", "confidence", "merchant_name", "industry", "region", "priority_score", "classification", "entity_type", "age_weight", "ranked_at"):
                if column not in signal_columns:
                    continue
                insert_columns.append(column)
                if column in {"detected_at", "ranked_at"}:
                    insert_values.append("NOW()")
                elif column == "age_weight":
                    insert_values.append("1.0")
                else:
                    insert_values.append(f":{column}")
            signal_id = int(
                conn.execute(
                    text(
                        f"""
                        INSERT INTO signals ({', '.join(insert_columns)})
                        VALUES ({', '.join(insert_values)})
                        RETURNING id
                        """
                    ),
                    signal_params,
                ).scalar_one()
            )
        conn.commit()

    if signal_id:
        with engine.connect() as conn:
            resolved_merchant_id = _linked_merchant_id(conn, signal_id)
            if resolved_merchant_id:
                merchant_backfilled = _backfill_signal_merchant_id(conn, signal_id, resolved_merchant_id)
                cluster_id = _link_signal_to_cluster(conn, signal_id, intelligence) if resolved_merchant_id else None
                conn.commit()
            else:
                conn.commit()
                merchant_id = None
                try:
                    merchant_id = resolve_merchant_identity(
                        signal_id,
                        content,
                        priority_score=priority_score,
                        author=intelligence.get("sender_name"),
                        merchant_name=intelligence.get("merchant_name_candidate"),
                    )
                except Exception as exc:
                    logger.warning(f"merchant identity resolution failed for gmail signal {signal_id}: {exc}")
                with engine.connect() as write_conn:
                    resolved_merchant_id = merchant_id or _linked_merchant_id(write_conn, signal_id)
                    merchant_backfilled = _backfill_signal_merchant_id(write_conn, signal_id, resolved_merchant_id)
                    cluster_id = _link_signal_to_cluster(write_conn, signal_id, intelligence) if resolved_merchant_id else None
                    write_conn.commit()
        if merchant_backfilled:
            save_event(
                "gmail_signal_merchant_backfilled",
                {"thread_id": intelligence["thread_id"], "signal_id": signal_id, "merchant_id": resolved_merchant_id},
            )

    save_event(
        "gmail_signal_upserted",
        {
            "thread_id": intelligence["thread_id"],
            "signal_id": signal_id,
            "processor": intelligence["processor"],
            "distress_type": intelligence.get("distress_type"),
            "merchant_name_candidate": intelligence.get("merchant_name_candidate"),
        },
    )
    return signal_id, cluster_id


def _upsert_opportunity(signal_id: int, intelligence: dict) -> int | None:
    if not signal_id or not intelligence.get("opportunity_eligible"):
        return None
    refinement = run_reasoning_task(
        "opportunity_scoring_refinement",
        {
            "signal_id": signal_id,
            "thread_id": intelligence.get("thread_id"),
            "processor": intelligence.get("processor"),
            "distress_type": intelligence.get("distress_type"),
            "merchant_name_candidate": intelligence.get("merchant_name_candidate"),
            "merchant_domain_candidate": intelligence.get("merchant_domain_candidate"),
        },
        {
            "should_create_or_keep_opportunity": True,
            "opportunity_score_adjustment": 0.0,
            "urgency_level": "high" if intelligence.get("reply_priority") == "high" else "medium",
            "reason_codes": ["deterministic_opportunity_eligible"],
            "confidence": float(intelligence.get("confidence", 0.0) or 0.0),
        },
        context={
            "item_type": "signal",
            "item_id": str(signal_id),
            "channel": "gmail",
            "thread_category": intelligence.get("thread_category"),
            "deterministic_confidence": float(intelligence.get("confidence", 0.0) or 0.0),
            "reply_priority": intelligence.get("reply_priority"),
            "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
            "merchant_known": bool(intelligence.get("merchant_id")),
            "merchant_id": intelligence.get("merchant_id"),
            "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
            "test_thread": bool(intelligence.get("test_thread")),
        },
    )
    opportunity_decision = refinement["final_decision"]
    if not opportunity_decision.get("should_create_or_keep_opportunity", True):
        return None
    with engine.connect() as conn:
        existing = conn.execute(
            text(
                """
                SELECT id
                FROM opportunities
                WHERE signal_id = :signal_id
                ORDER BY id ASC
                LIMIT 1
                """
            ),
            {"signal_id": signal_id},
        ).fetchone()
        if existing:
            return int(existing[0])
        score = max(
            70.0,
            min(
                100.0,
                round(
                    float(intelligence["confidence"]) * 100
                    + float(opportunity_decision.get("opportunity_score_adjustment", 0.0) or 0.0),
                    2,
                ),
            ),
        )
        opportunity_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO opportunities (signal_id, opportunity_score, created_at)
                    VALUES (:signal_id, :score, NOW())
                    RETURNING id
                    """
                ),
                {"signal_id": signal_id, "score": score},
            ).scalar_one()
        )
        conn.commit()
    save_event(
        "gmail_opportunity_created",
        {
            "thread_id": intelligence["thread_id"],
            "signal_id": signal_id,
            "opportunity_id": opportunity_id,
            "score": score,
        },
    )
    return opportunity_id


def triage_gmail_target(target: ChannelTarget) -> dict:
    sender_email = _lower(target.metadata.get("sender_email", target.author))
    sender_name = (target.metadata.get("sender_name") or "").strip() or None
    _local, sender_domain = _email_parts(sender_email)
    snippet = (target.metadata.get("snippet") or target.body or "").strip()
    normalized = normalized_text(target.title, target.body, snippet)
    test_thread = _is_test_thread(target, normalized, sender_email)
    hard_noise, hard_noise_hits = _hard_noise_match(target, normalized, sender_email, sender_domain)
    promotional_hits = _promotional_noise_hits(target, normalized, sender_email)

    processor = _processor_from_text(normalized)
    distress_type, distress_signals, distress_hits = _distress_type_from_text(normalized)
    business_flags = _business_context_flags(target, normalized, sender_domain)
    direct_distress_hits = _contains_any(normalized, STRONG_DISTRESS_PHRASES)

    if test_thread or hard_noise:
        category = "noise_system"
    elif direct_distress_hits or (processor != "unknown" and len(business_flags) >= 2):
        category = "merchant_distress"
    else:
        category = "customer_support" if _human_written(normalized) else "noise_system"

    if category not in THREAD_CATEGORIES:
        category = "customer_support"

    sender_is_automated = any(token in _email_parts(sender_email)[0] for token in AUTOMATED_SENDER_TOKENS)
    subject_system_match = bool(_contains_any(_lower(target.title), HARD_NOISE_PATTERNS))
    clear_business_context = bool(business_flags)
    human_written = _human_written(normalized)
    likely_promotional_noise = bool(
        promotional_hits
        and processor == "unknown"
        and distress_type in {"reserve_hold", "unknown"}
        and not clear_business_context
    )

    if category == "merchant_distress" and likely_promotional_noise and (sender_is_automated or not human_written):
        category = "noise_system"
        hard_noise = True
        hard_noise_hits = list(dict.fromkeys([*hard_noise_hits, *promotional_hits]))

    if category == "noise_system":
        confidence = 0.7
        if sender_is_automated or hard_noise:
            confidence += 0.1
        if subject_system_match:
            confidence += 0.1
        confidence = min(confidence, 0.99)
    elif category == "merchant_distress":
        confidence = 0.55
        if processor != "unknown":
            confidence += 0.15
        if direct_distress_hits:
            confidence += 0.1
        if len(direct_distress_hits) >= 2:
            confidence += 0.05
        if _is_business_domain(sender_domain):
            confidence += 0.05
        if clear_business_context:
            confidence += 0.05
        confidence = min(confidence, 0.95)
    else:
        confidence = 0.5
        if _human_written(normalized):
            confidence += 0.1
        if _real_operator_sender(sender_name or "", sender_email):
            confidence += 0.05
        confidence = min(confidence, 0.85)

    if category == "merchant_distress" and confidence >= 0.75:
        reply_priority = "high"
    elif category == "merchant_distress" and confidence >= 0.6:
        reply_priority = "medium"
    elif category == "customer_support" and confidence >= 0.6:
        reply_priority = "low"
    else:
        reply_priority = "none"

    industry = _industry_from_text(normalized)
    merchant_name_candidate, merchant_domain_candidate, merchant_identity_confidence = _extract_merchant_identity(
        target,
        normalized,
        sender_email,
        sender_domain,
        test_thread,
        category,
        promotional_hits,
    )
    if category != "merchant_distress":
        distress_type = None
        distress_signals = []

    signal_eligible = category == "merchant_distress" and not test_thread
    deterministic_eligible, deterministic_reasons = _strict_opportunity_gate(
        {
            "thread_category": category,
            "test_thread": test_thread,
            "confidence": confidence,
            "distress_type": distress_type,
            "merchant_domain_candidate": merchant_domain_candidate,
            "merchant_identity_confidence": merchant_identity_confidence,
            "sender_email": sender_email,
            "sender_domain": sender_domain,
            "metadata": {
                "promotional_noise_hits": promotional_hits,
                "sender_is_automated": sender_is_automated,
            },
        }
    )
    opportunity_eligible = deterministic_eligible
    reply_allowed = category in {"merchant_distress", "customer_support"}
    draft_recommended = category == "merchant_distress"

    reason_codes = []
    if category == "merchant_distress":
        reason_codes.append("merchant_distress_detected")
        if distress_type:
            reason_codes.append(distress_type)
        if "looking_for_alternative" in distress_signals or distress_type == "processor_switch_intent":
            reason_codes.append("processor_switch_intent")
    elif category == "noise_system" and test_thread:
        reason_codes.append("test_thread")
    elif category == "noise_system":
        reason_codes.extend(hard_noise_hits[:2])

    intelligence = {
        "thread_id": target.thread_id,
        "message_id": target.metadata.get("message_id"),
        "source": "gmail",
        "sender_email": sender_email,
        "sender_name": sender_name,
        "sender_domain": sender_domain or None,
        "subject": target.title or "",
        "snippet": snippet[:500],
        "thread_category": category,
        "confidence": round(confidence, 2),
        "processor": processor if processor in PROCESSORS else "unknown",
        "distress_type": distress_type if distress_type in DISTRESS_TYPES else None,
        "distress_signals": [signal for signal in distress_signals if signal in SECONDARY_DISTRESS_TYPES],
        "industry": industry if industry in INDUSTRIES else "unknown",
        "merchant_name_candidate": merchant_name_candidate,
        "merchant_domain_candidate": merchant_domain_candidate,
        "merchant_identity_confidence": merchant_identity_confidence,
        "reply_priority": reply_priority,
        "reply_allowed": reply_allowed,
        "draft_recommended": draft_recommended,
        "signal_eligible": signal_eligible,
        "opportunity_eligible": opportunity_eligible,
        "test_thread": test_thread,
        "triaged_at": _utc_now_iso(),
        "metadata": {
            "reason_codes": reason_codes,
            "hard_noise_hits": hard_noise_hits,
            "promotional_noise_hits": promotional_hits,
            "direct_distress_hits": direct_distress_hits[:4],
            "business_flags": business_flags,
            "human_written": human_written,
            "sender_is_automated": sender_is_automated,
            "strict_opportunity_gate_reasons": deterministic_reasons,
        },
    }
    classification_result = run_reasoning_task(
        "gmail_thread_classification_refinement",
        {
            "thread_id": target.thread_id,
            "subject": target.title or "",
            "body": (target.body or snippet)[:2500],
            "sender_email": sender_email,
            "sender_name": sender_name,
        },
        intelligence,
        context={
            "item_type": "gmail_thread",
            "item_id": target.thread_id,
            "channel": "gmail",
            "thread_category": intelligence.get("thread_category"),
            "deterministic_confidence": float(intelligence.get("confidence", 0.0) or 0.0),
            "reply_priority": intelligence.get("reply_priority"),
            "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
            "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
            "test_thread": test_thread,
            "hard_suppression": hard_noise,
            "sender_is_system": sender_is_automated,
        },
    )
    intelligence = classification_result["final_decision"]
    metadata = dict(intelligence.get("metadata") or {})
    metadata["classification_reasoning_decision_id"] = classification_result.get("decision_id")
    metadata["classification_reasoning_route"] = classification_result.get("routing", {}).get("routing_decision")

    if intelligence.get("thread_category") != "merchant_distress":
        intelligence["distress_type"] = None
        intelligence["distress_signals"] = []

    intelligence["signal_eligible"] = intelligence.get("thread_category") == "merchant_distress" and not test_thread
    strict_eligible, strict_reasons = _strict_opportunity_gate(intelligence)
    intelligence["opportunity_eligible"] = strict_eligible
    intelligence["reply_allowed"] = intelligence.get("thread_category") in {"merchant_distress", "customer_support"}
    intelligence["draft_recommended"] = intelligence.get("thread_category") == "merchant_distress"
    intelligence["test_thread"] = test_thread
    intelligence["triaged_at"] = _utc_now_iso()

    refined_reason_codes = list(metadata.get("reason_codes") or [])
    if intelligence.get("thread_category") == "merchant_distress":
        if "merchant_distress_detected" not in refined_reason_codes:
            refined_reason_codes.insert(0, "merchant_distress_detected")
        if intelligence.get("distress_type") and intelligence["distress_type"] not in refined_reason_codes:
            refined_reason_codes.append(intelligence["distress_type"])
    elif intelligence.get("thread_category") == "noise_system" and test_thread and "test_thread" not in refined_reason_codes:
        refined_reason_codes.append("test_thread")
    metadata["reason_codes"] = refined_reason_codes
    metadata["strict_opportunity_gate_reasons"] = strict_reasons
    reply_intent = _classify_reply_intent(
        subject=target.title or "",
        body=(target.body or "")[:2500],
        snippet=snippet[:500],
        sender_is_automated=bool(metadata.get("sender_is_automated")),
        thread_category=str(intelligence.get("thread_category") or ""),
    )
    metadata["reply_intent"] = reply_intent.get("reply_intent") or "unknown"
    metadata["buying_intent"] = reply_intent.get("buying_intent") or "low"
    metadata["reply_intent_confidence"] = float(reply_intent.get("confidence") or 0.0)
    metadata["reply_intent_reason"] = str(reply_intent.get("reason") or "").strip()
    intelligence["metadata"] = metadata

    if intelligence.get("signal_eligible") and (
        intelligence.get("merchant_identity_confidence") is None
        or float(intelligence.get("merchant_identity_confidence") or 0.0) < 0.75
    ):
        identity_result = run_reasoning_task(
            "merchant_identity_refinement",
            {
                "thread_id": target.thread_id,
                "subject": target.title or "",
                "body": (target.body or snippet)[:2500],
                "sender_email": sender_email,
                "sender_name": sender_name,
            },
            {
                "merchant_name_candidate": intelligence.get("merchant_name_candidate"),
                "merchant_domain_candidate": intelligence.get("merchant_domain_candidate"),
                "merchant_identity_confidence": float(intelligence.get("merchant_identity_confidence") or 0.0),
                "metadata": metadata,
            },
            context={
                "item_type": "gmail_thread",
                "item_id": target.thread_id,
                "channel": "gmail",
                "thread_category": intelligence.get("thread_category"),
                "deterministic_confidence": float(intelligence.get("confidence", 0.0) or 0.0),
                "reply_priority": intelligence.get("reply_priority"),
                "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
                "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
                "test_thread": test_thread,
            },
        )
        identity_decision = identity_result["final_decision"]
        intelligence["merchant_name_candidate"] = identity_decision.get("merchant_name_candidate")
        intelligence["merchant_domain_candidate"] = identity_decision.get("merchant_domain_candidate")
        intelligence["merchant_identity_confidence"] = identity_decision.get("merchant_identity_confidence")
        metadata = dict(intelligence.get("metadata") or {})
        metadata["merchant_identity_reasoning_decision_id"] = identity_result.get("decision_id")
        metadata["merchant_identity_reasoning_route"] = identity_result.get("routing", {}).get("routing_decision")
        intelligence["metadata"] = metadata

    return intelligence


def sync_gmail_thread_intelligence(
    target: ChannelTarget,
    *,
    create_signal: bool = True,
    force: bool = False,
) -> dict:
    fingerprint = build_thread_fingerprint(target)
    cached = get_gmail_thread_intelligence(target.thread_id)
    if cached and cached.get("content_fingerprint") == fingerprint and not force:
        cached["cached"] = True
        return cached

    intelligence = triage_gmail_target(target)
    signal_id = cached.get("signal_id") if cached else None
    opportunity_id = cached.get("opportunity_id") if cached else None
    cluster_id = None

    if intelligence["signal_eligible"] and create_signal:
        try:
            signal_id, cluster_id = _upsert_signal(target, intelligence)
            opportunity_id = _upsert_opportunity(signal_id, intelligence)
        except Exception as exc:
            logger.error(f"gmail signal sync failed for thread {target.thread_id}: {exc}")
            save_event("gmail_signal_sync_error", {"thread_id": target.thread_id, "error": str(exc)})
            intelligence["metadata"]["signal_sync_error"] = str(exc)

    record = upsert_gmail_thread_intelligence(
        intelligence,
        content_fingerprint=fingerprint,
        signal_id=signal_id,
        opportunity_id=opportunity_id,
        metadata={
            **intelligence.get("metadata", {}),
            "cluster_id": cluster_id,
        },
    )
    record["cached"] = False

    # Pass 4: Reward hook
    try:
        labels = target.metadata.get("labels") or []
        # If this is a reply to a thread we already engaged with
        if "bot-sent" in labels or "bot-replied" in labels:
            outcome_type = None
            reward = 0.0
            
            # Logic: If it's a merchant distress reply, it's a win (+0.5)
            if intelligence.get("thread_category") == "merchant_distress":
                outcome_type = "outreach_reply"
                reward = 0.5
            
            # Logic: If it's a bounce (automated noise), it's a penalty (-0.5)
            elif intelligence.get("thread_category") == "noise_system" and intelligence.get("metadata", {}).get("sender_is_automated"):
                outcome_type = "outreach_bounce"
                reward = -0.5
            
            if outcome_type:
                save_learning_feedback({
                    "signal_id": signal_id,
                    "opportunity_id": opportunity_id,
                    "source": "gmail_outreach_outcome",
                    "decision_type": "outreach_performance",
                    "outcome_type": outcome_type,
                    "outcome_value": reward,
                    "reward_score": reward,
                    "notes": f"Gmail triage detected {outcome_type} in thread {target.thread_id}"
                }, hook_name="gmail_outreach_outcome")
    except Exception as le:
        logger.error(f"Reward hook failed [gmail_outreach_outcome] signal_id={signal_id} opportunity_id={opportunity_id}: {type(le).__name__}: {le}")

    return record
