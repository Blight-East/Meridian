from __future__ import annotations

import re
from collections import Counter

from runtime.intelligence.distress_normalization import normalize_distress_topic
from runtime.intelligence.entity_taxonomy import (
    CLASS_CONSUMER_COMPLAINT,
    CLASS_MERCHANT_DISTRESS,
    classify_entity,
)
from runtime.intelligence.merchant_quality import is_valid_domain
from runtime.intelligence.merchant_signal_classifier import (
    CLASS_CONSUMER_OR_IRRELEVANT,
    CLASS_MERCHANT_OPERATOR,
    classify_merchant_signal,
)
from runtime.intelligence.merchant_validation import is_valid_merchant_name


ELIGIBILITY_OUTREACH = "outreach_eligible"
ELIGIBILITY_RESEARCH_ONLY = "research_only"
ELIGIBILITY_CONSUMER_NOISE = "consumer_noise"
ELIGIBILITY_IDENTITY_WEAK = "identity_weak"
ELIGIBILITY_DOMAIN_UNPROMOTABLE = "domain_unpromotable"
ELIGIBILITY_OPERATOR_REVIEW_ONLY = "operator_review_only"

QUEUE_ELIGIBILITY_CLASSES = {
    ELIGIBILITY_OUTREACH,
    ELIGIBILITY_RESEARCH_ONLY,
    ELIGIBILITY_CONSUMER_NOISE,
    ELIGIBILITY_IDENTITY_WEAK,
    ELIGIBILITY_DOMAIN_UNPROMOTABLE,
    ELIGIBILITY_OPERATOR_REVIEW_ONLY,
}

_DISTRESS_ANNOTATION_RE = re.compile(r"\[distress:([^\]]+)\]", re.IGNORECASE)
_PROCESSOR_ANNOTATION_RE = re.compile(r"\[processor:([^\]]+)\]", re.IGNORECASE)
_GENERIC_NAME_RE = re.compile(r"\b(?:unknown|merchant|customer|consumer|seller|buyer|payments?)\b", re.IGNORECASE)
_CONSUMER_LANGUAGE_RE = re.compile(
    r"\b(?:my order|my purchase|my package|my refund|my card|never received|money back|customer service|return policy|buyer complaint|refund denied)\b",
    re.IGNORECASE,
)
_GENERAL_DISCUSSION_RE = re.compile(
    r"\b(?:article|news|reported|according to|press release|discussion|someone looking for|check out|looking for an alternative|probably|i think|commentary)\b",
    re.IGNORECASE,
)
_HYPOTHETICAL_OR_HISTORICAL_RE = re.compile(
    r"\b(?:what if|hypothetical|in theory|for example|case study|historical|history of|years ago|last year|used to|once had|previously|tutorial|guide)\b",
    re.IGNORECASE,
)
_GENERIC_TOOLING_RE = re.compile(
    r"\b(?:seo|ad spend|growth hack|crm|customer analytics|business intelligence|generic analytics|marketing automation|fraud prevention platform|growth tooling|lead gen software)\b",
    re.IGNORECASE,
)
_MERCHANT_DISTRESS_RE = re.compile(
    r"\b(?:payouts?|reserve|chargeback|dispute|settlement|merchant account|account frozen|account closed|funds held|funds frozen|cash flow|checkout|payments disabled|processing disabled|under review|verification review|compliance review|escalation)\b",
    re.IGNORECASE,
)
_MERCHANT_OPERATOR_RE = re.compile(
    r"\b(?:my store|our store|our business|our company|we sell|we run|we operate|merchant candidate:|merchant domain candidate:|orders|checkout|revenue|sales)\b",
    re.IGNORECASE,
)
_UNPROMOTABLE_DOMAIN_PATTERNS = (
    re.compile(r"(^|\.)(reuters|thomsonreuters|apnews|nytimes|wsj|bloomberg|forbes|medium|substack|wordpress|blogspot|tumblr|github|notion|readthedocs|wikipedia)\.", re.IGNORECASE),
    re.compile(r"\.(gov|edu)$", re.IGNORECASE),
    re.compile(r"(^|\.)(googleapis|gstatic|googleusercontent|googlesyndication)\.", re.IGNORECASE),
)
_PROCESSOR_PLATFORM_DOMAIN_PATTERNS = (
    re.compile(r"(^|\.)(stripe|paypal|braintreepayments|squareup|shopify|woocommerce|bigcommerce|magento|envato|mastercard|visa)\.", re.IGNORECASE),
    re.compile(r"(^|\.)(onelink\.me|app\.link|linktr\.ee|lnk\.bio)$", re.IGNORECASE),
)
_CONTENT_DOMAIN_PATTERNS = (
    re.compile(r"(^|\.)(news|media|press|blog|magazine|journal|chronicle|bulletin|forum|docs)\.", re.IGNORECASE),
    re.compile(r"(^|\.)(alerts?|brief|newsletter|mailers?)\.", re.IGNORECASE),
)
_TRACKING_SUBDOMAIN_LABELS = {
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
    "notify",
    "public",
    "public-usa",
    "tracking",
    "trk",
}
_EXPLICIT_UNPROMOTABLE_SUFFIXES = {
    "onelink.me",
    "app.link",
    "linktr.ee",
    "lnk.bio",
}
_EXPLICIT_CONTENT_SUFFIXES = {
    "googleapis.com",
    "gstatic.com",
    "googleusercontent.com",
    "brief.barchart.com",
    "myjobhelper.com",
}
_HOSTED_STOREFRONT_SUFFIXES = {
    "myshopify.com",
    "mybigcommerce.com",
}
_MERCHANTISH_DOMAIN_HINTS = {
    "shop",
    "store",
    "cart",
    "checkout",
    "pay",
    "payments",
    "boutique",
    "market",
    "supply",
    "goods",
    "retail",
    "studio",
}
_LIVE_URGENCY_RE = re.compile(
    r"\b(?:right now|currently|today|this week|ongoing|still|active|under review|on hold|frozen|paused|delayed|holding funds|cash flow|getting worse|escalating)\b",
    re.IGNORECASE,
)
_BUYER_ROLE_RE = re.compile(
    r"\b(?:founder|owner|finance|payments?|risk|operations?|compliance|support)\b",
    re.IGNORECASE,
)


def evaluate_opportunity_queue_quality(
    *,
    opportunity: dict | None,
    merchant: dict | None = None,
    signal: dict | None = None,
) -> dict:
    opportunity = dict(opportunity or {})
    merchant = dict(merchant or {})
    signal = dict(signal or {})

    merchant_name = str(
        merchant.get("canonical_name")
        or opportunity.get("merchant_name")
        or signal.get("merchant_name")
        or ""
    ).strip()
    merchant_domain = normalize_merchant_domain(
        merchant.get("domain")
        or merchant.get("normalized_domain")
        or opportunity.get("merchant_domain")
        or signal.get("merchant_domain")
        or ""
    )
    text = " ".join(
        part
        for part in [
            str(signal.get("content") or ""),
            str(opportunity.get("distress_topic") or ""),
            str(opportunity.get("sales_strategy") or ""),
        ]
        if part
    ).strip()
    raw_distress_topic = str(opportunity.get("distress_topic") or "").strip()
    distress_annotation = str(
        signal.get("distress_type")
        or _match_annotation(_DISTRESS_ANNOTATION_RE, text)
        or ""
    ).strip()
    parsed_distress = normalize_distress_topic(raw_distress_topic or distress_annotation)
    if parsed_distress == "unknown":
        parsed_distress = normalize_distress_topic(text)
    parsed_processor = str(
        opportunity.get("processor")
        or signal.get("processor")
        or _match_annotation(_PROCESSOR_ANNOTATION_RE, text)
        or ""
    ).strip().lower()
    signal_info = classify_merchant_signal(text)
    entity_info = classify_entity(text)
    merchant_name_valid = _merchant_name_valid(merchant_name, merchant_domain, parsed_processor)
    merchant_name_generic = _merchant_name_generic(merchant_name)
    domain_quality = _domain_quality(
        merchant_domain=merchant_domain,
        merchant_name=merchant_name,
        parsed_processor=parsed_processor,
        text=text,
        domain_confidence=str(merchant.get("domain_confidence") or ""),
    )
    distress_quality = _distress_quality(
        text=text,
        parsed_distress=parsed_distress,
        parsed_processor=parsed_processor,
        merchant_name=merchant_name,
        signal_info=signal_info,
        entity_info=entity_info,
    )
    icp_fit = _icp_fit(
        text=text,
        parsed_distress=parsed_distress,
        parsed_processor=parsed_processor,
        merchant_name=merchant_name,
        merchant_domain=merchant_domain,
        signal_info=signal_info,
        domain_quality=domain_quality,
        distress_quality=distress_quality,
        contact_email=str(
            opportunity.get("contact_email")
            or signal.get("contact_email")
            or merchant.get("contact_email")
            or ""
        ).strip(),
    )

    reason_codes: list[str] = []
    eligibility_class = ELIGIBILITY_OUTREACH

    consumer_like = bool(
        entity_info.get("classification") == CLASS_CONSUMER_COMPLAINT
        or signal_info.get("classification") == CLASS_CONSUMER_OR_IRRELEVANT
        or _CONSUMER_LANGUAGE_RE.search(text)
    )
    merchant_distress_clear = bool(
        distress_quality["score"] >= 45
        and parsed_distress != "unknown"
        and domain_quality["label"] == "merchant_like"
        and merchant_name_valid
        and (parsed_processor != "unknown" or signal_info.get("classification") == CLASS_MERCHANT_OPERATOR)
    )
    contact_discovered_generic = bool(
        raw_distress_topic.strip().lower() == "contact_discovered"
        and parsed_distress == "unknown"
    )

    if consumer_like and distress_quality["score"] < 55:
        eligibility_class = ELIGIBILITY_CONSUMER_NOISE
        reason_codes.append("consumer_language_detected")
    elif domain_quality["label"] == "unpromotable":
        eligibility_class = ELIGIBILITY_DOMAIN_UNPROMOTABLE
        reason_codes.append(domain_quality["primary_reason"] or "domain_unpromotable")
    elif merchant.get("status") == "candidate":
        eligibility_class = ELIGIBILITY_OPERATOR_REVIEW_ONLY
        reason_codes.append("provisional_identity_candidate")
    elif not merchant_domain or not is_valid_domain(merchant_domain):
        eligibility_class = ELIGIBILITY_IDENTITY_WEAK
        reason_codes.append("merchant_domain_missing_or_invalid")
    elif not merchant_name_valid or merchant_name_generic:
        eligibility_class = ELIGIBILITY_IDENTITY_WEAK
        reason_codes.append("merchant_identity_too_weak")
    elif merchant_distress_clear:
        eligibility_class = ELIGIBILITY_OUTREACH
        reason_codes.append("clear_merchant_payment_distress")
    elif contact_discovered_generic:
        eligibility_class = ELIGIBILITY_RESEARCH_ONLY
        reason_codes.append("generic_contact_discovered_without_clear_distress")
    elif distress_quality["score"] >= 45 and domain_quality["score"] >= 40:
        eligibility_class = ELIGIBILITY_OPERATOR_REVIEW_ONLY
        reason_codes.append("merchant_distress_needs_operator_review")
    elif distress_quality["score"] >= 35:
        eligibility_class = ELIGIBILITY_RESEARCH_ONLY
        reason_codes.append("merchant_distress_not_outreach_ready")
    else:
        eligibility_class = ELIGIBILITY_RESEARCH_ONLY if domain_quality["score"] >= 45 else ELIGIBILITY_IDENTITY_WEAK
        reason_codes.append("insufficient_merchant_distress")

    if eligibility_class == ELIGIBILITY_OUTREACH and icp_fit["label"] == "weak":
        if distress_quality["score"] >= 50 and domain_quality["score"] >= 50:
            eligibility_class = ELIGIBILITY_OPERATOR_REVIEW_ONLY
            reason_codes.append("icp_fit_requires_operator_review")
        else:
            eligibility_class = ELIGIBILITY_RESEARCH_ONLY
            reason_codes.append("icp_fit_too_weak_for_outreach")

    quality_score = max(
        0,
        min(
            100,
            int(round((distress_quality["score"] * 0.45) + (domain_quality["score"] * 0.25) + (icp_fit["score"] * 0.30))),
        ),
    )
    result = {
        "eligibility_class": eligibility_class,
        "eligibility_reason": _eligibility_reason(
            eligibility_class,
            domain_quality_label=domain_quality["label"],
            domain_reason=domain_quality["reason"],
            distress_reason=distress_quality["reason"],
            icp_fit_reason=icp_fit["reason"],
        ),
        "reason_codes": reason_codes,
        "quality_score": quality_score,
        "domain_quality_score": int(domain_quality["score"]),
        "domain_quality_label": domain_quality["label"],
        "domain_quality_reason": domain_quality["reason"],
        "distress_quality_score": int(distress_quality["score"]),
        "distress_quality_reason": distress_quality["reason"],
        "icp_fit_score": int(icp_fit["score"]),
        "icp_fit_label": icp_fit["label"],
        "icp_fit_reason": icp_fit["reason"],
        "high_conviction_prospect": bool(icp_fit["high_conviction"]),
        "merchant_domain": merchant_domain,
        "merchant_name": merchant_name,
        "merchant_name_valid": bool(merchant_name_valid),
        "parsed_processor": parsed_processor or "unknown",
        "parsed_distress_type": parsed_distress or "unknown",
        "suppressed_from_blocked_queue": eligibility_class != ELIGIBILITY_OUTREACH,
        "operator_note": _operator_note(
            eligibility_class,
            merchant_domain=merchant_domain,
            domain_quality=domain_quality,
        ),
    }
    return result


def normalize_merchant_domain(domain: str | None) -> str:
    value = str(domain or "").strip().lower()
    if value.startswith("http://"):
        value = value[7:]
    elif value.startswith("https://"):
        value = value[8:]
    value = value.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].strip(".")
    if value.startswith("www."):
        value = value[4:]
    return value


def _looks_like_tracking_subdomain(merchant_domain: str) -> bool:
    labels = [label for label in str(merchant_domain or "").strip().lower().split(".") if label]
    if len(labels) < 3:
        return False
    prefix_labels = labels[:-2]
    return any(label in _TRACKING_SUBDOMAIN_LABELS for label in prefix_labels)


def is_outreach_eligible_quality(quality: dict | None) -> bool:
    return str((quality or {}).get("eligibility_class") or "") == ELIGIBILITY_OUTREACH


def summarize_quality_counts(rows: list[dict]) -> dict:
    counts = Counter(str((row or {}).get("eligibility_class") or "") for row in rows)
    eligible = int(counts.get(ELIGIBILITY_OUTREACH, 0))
    blocked_total = sum(int(counts.get(key, 0)) for key in QUEUE_ELIGIBILITY_CLASSES)
    suppressed = blocked_total - eligible
    return {
        "outreach_eligible_opportunities": eligible,
        "research_only_opportunities": int(counts.get(ELIGIBILITY_RESEARCH_ONLY, 0)),
        "consumer_noise_opportunities": int(counts.get(ELIGIBILITY_CONSUMER_NOISE, 0)),
        "identity_weak_opportunities": int(counts.get(ELIGIBILITY_IDENTITY_WEAK, 0)),
        "domain_unpromotable_opportunities": int(counts.get(ELIGIBILITY_DOMAIN_UNPROMOTABLE, 0)),
        "operator_review_only_opportunities": int(counts.get(ELIGIBILITY_OPERATOR_REVIEW_ONLY, 0)),
        "blocked_queue_quality_ratio": round((eligible / blocked_total), 4) if blocked_total else 0.0,
        "suppressed_total": int(suppressed),
    }


def _distress_quality(
    *,
    text: str,
    parsed_distress: str,
    parsed_processor: str,
    merchant_name: str,
    signal_info: dict,
    entity_info: dict,
) -> dict:
    score = 0
    reasons: list[str] = []
    if signal_info.get("classification") == CLASS_MERCHANT_OPERATOR:
        score += 35
        reasons.append("merchant_operator_language")
    elif signal_info.get("classification") == CLASS_CONSUMER_OR_IRRELEVANT:
        score -= 35
        reasons.append("consumer_language")

    if entity_info.get("classification") == CLASS_MERCHANT_DISTRESS:
        score += 20
        reasons.append("merchant_distress_entity")
    elif entity_info.get("classification") == CLASS_CONSUMER_COMPLAINT:
        score -= 30
        reasons.append("consumer_entity")

    if parsed_distress != "unknown":
        score += 25
        reasons.append("canonical_distress")
    if "[gmail]" in text.lower() and parsed_distress != "unknown":
        score += 15
        reasons.append("gmail_distress_annotation")
    if parsed_processor and parsed_processor != "unknown":
        score += 10
        reasons.append("processor_context")
    if _MERCHANT_DISTRESS_RE.search(text):
        score += 20
        reasons.append("merchant_distress_terms")
    if _MERCHANT_OPERATOR_RE.search(text):
        score += 10
        reasons.append("merchant_operator_terms")
    if "merchant candidate:" in text.lower() or "merchant domain candidate:" in text.lower():
        score += 10
        reasons.append("merchant_candidate_annotation")
    if merchant_name and not _merchant_name_generic(merchant_name):
        score += 5
        reasons.append("named_merchant")
    if _GENERAL_DISCUSSION_RE.search(text):
        score -= 20
        reasons.append("general_discussion")
    if _CONSUMER_LANGUAGE_RE.search(text):
        score -= 25
        reasons.append("consumer_complaint_terms")
    score = max(0, min(100, score))
    return {
        "score": score,
        "reason": _distress_reason(score, reasons, parsed_distress),
        "reasons": reasons,
    }


def _domain_quality(
    *,
    merchant_domain: str,
    merchant_name: str,
    parsed_processor: str,
    text: str,
    domain_confidence: str,
) -> dict:
    if not merchant_domain or not is_valid_domain(merchant_domain):
        return {
            "score": 0,
            "label": "weak",
            "reason": "Merchant identity is too weak to justify contact work.",
            "primary_reason": "merchant_domain_missing_or_invalid",
        }

    score = 40
    reasons: list[str] = ["valid_domain"]
    label = "merchant_like"
    primary_reason = ""
    hosted_storefront = any(
        merchant_domain == suffix or merchant_domain.endswith(f".{suffix}")
        for suffix in _HOSTED_STOREFRONT_SUFFIXES
    )

    if any(merchant_domain == suffix or merchant_domain.endswith(f".{suffix}") for suffix in _EXPLICIT_UNPROMOTABLE_SUFFIXES):
        score -= 60
        label = "unpromotable"
        reasons.append("link_wrapper_or_redirect_domain")
        primary_reason = "link_wrapper_or_redirect_domain"
    if any(merchant_domain == suffix or merchant_domain.endswith(f".{suffix}") for suffix in _EXPLICIT_CONTENT_SUFFIXES):
        score -= 55
        label = "unpromotable"
        reasons.append("content_or_campaign_domain")
        primary_reason = primary_reason or "content_or_campaign_domain"

    if any(pattern.search(merchant_domain) for pattern in _UNPROMOTABLE_DOMAIN_PATTERNS):
        score -= 60
        label = "unpromotable"
        reasons.append("publisher_or_public_domain")
        primary_reason = "publisher_or_public_domain"
    if hosted_storefront:
        reasons.append("hosted_storefront_domain")
    elif any(pattern.search(merchant_domain) for pattern in _PROCESSOR_PLATFORM_DOMAIN_PATTERNS):
        score -= 55
        label = "unpromotable"
        reasons.append("processor_or_platform_domain")
        primary_reason = primary_reason or "processor_or_platform_domain"
    if any(pattern.search(merchant_domain) for pattern in _CONTENT_DOMAIN_PATTERNS):
        score -= 45
        label = "unpromotable"
        reasons.append("content_or_informational_domain")
        primary_reason = primary_reason or "content_or_informational_domain"
    if _looks_like_tracking_subdomain(merchant_domain):
        score -= 60
        label = "unpromotable"
        reasons.append("tracking_or_campaign_subdomain")
        primary_reason = primary_reason or "tracking_or_campaign_subdomain"
    if domain_confidence == "confirmed":
        score += 10
        reasons.append("confirmed_domain")
    elif domain_confidence == "provisional":
        score -= 8
        reasons.append("provisional_domain")
    if _merchant_domain_matches_name(merchant_domain, merchant_name):
        score += 10
        reasons.append("domain_name_alignment")
    if _merchantish_domain(merchant_domain):
        score += 8
        reasons.append("merchant_like_domain")
    if (
        not hosted_storefront
        and parsed_processor
        and parsed_processor != "unknown"
        and parsed_processor in merchant_domain
    ):
        score -= 25
        reasons.append("processor_named_domain")
        primary_reason = primary_reason or "processor_named_domain"
        if label != "unpromotable":
            label = "weak"
    if _GENERAL_DISCUSSION_RE.search(text):
        score -= 8
        reasons.append("context_looks_informational")
    score = max(0, min(100, score))
    if label != "unpromotable":
        label = "merchant_like" if score >= 50 else "weak"
    return {
        "score": score,
        "label": label,
        "reason": _domain_reason(label, reasons),
        "primary_reason": primary_reason,
        "reasons": reasons,
    }


def _merchant_name_valid(merchant_name: str, merchant_domain: str, parsed_processor: str) -> bool:
    if not merchant_name or _merchant_name_generic(merchant_name):
        return False
    return is_valid_merchant_name(merchant_name, domain=merchant_domain or None, processor=parsed_processor or None)


def _merchant_name_generic(merchant_name: str) -> bool:
    value = str(merchant_name or "").strip()
    if not value:
        return True
    if value.lower().startswith("unknown "):
        return True
    return bool(_GENERIC_NAME_RE.search(value))


def _merchant_domain_matches_name(domain: str, merchant_name: str) -> bool:
    if not domain or not merchant_name:
        return False
    domain_root = domain.split(".")[0].replace("-", "").replace("_", "")
    normalized_name = re.sub(r"[^a-z0-9]", "", merchant_name.lower())
    return bool(normalized_name and (normalized_name.startswith(domain_root) or domain_root.startswith(normalized_name[:6])))


def _merchantish_domain(domain: str) -> bool:
    labels = {part for part in str(domain or "").split(".") if part}
    return bool(labels.intersection(_MERCHANTISH_DOMAIN_HINTS))


def _match_annotation(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text or "")
    return str(match.group(1) if match else "").strip().lower()


def _eligibility_reason(
    eligibility_class: str,
    *,
    domain_quality_label: str,
    domain_reason: str,
    distress_reason: str,
    icp_fit_reason: str,
) -> str:
    mapping = {
        ELIGIBILITY_OUTREACH: "This case is clear merchant payment distress and remains outreach-eligible.",
        ELIGIBILITY_RESEARCH_ONLY: "Research-only: monitor the pattern, but do not push to outreach.",
        ELIGIBILITY_CONSUMER_NOISE: "This case is consumer complaint noise, not merchant distress.",
        ELIGIBILITY_IDENTITY_WEAK: "Merchant identity is too weak to justify contact work.",
        ELIGIBILITY_DOMAIN_UNPROMOTABLE: domain_reason or "This domain does not look like a merchant-operated business.",
        ELIGIBILITY_OPERATOR_REVIEW_ONLY: "This case may be merchant-relevant, but it still needs operator review before contact work.",
    }
    if eligibility_class == ELIGIBILITY_RESEARCH_ONLY and domain_quality_label == "weak":
        return "Research-only: the distress may be real, but merchant/domain confidence is too weak for outreach."
    if eligibility_class == ELIGIBILITY_RESEARCH_ONLY and icp_fit_reason:
        return f"{mapping[ELIGIBILITY_RESEARCH_ONLY]} {icp_fit_reason}"
    if eligibility_class == ELIGIBILITY_OPERATOR_REVIEW_ONLY and distress_reason:
        suffix = icp_fit_reason or distress_reason
        return f"{mapping[ELIGIBILITY_OPERATOR_REVIEW_ONLY]} {suffix}"
    return mapping.get(eligibility_class, "Queue quality is unresolved.")


def _operator_note(eligibility_class: str, *, merchant_domain: str, domain_quality: dict) -> str:
    if eligibility_class == ELIGIBILITY_DOMAIN_UNPROMOTABLE:
        return domain_quality.get("reason") or "This domain looks informational rather than merchant-operated."
    if eligibility_class == ELIGIBILITY_IDENTITY_WEAK:
        return "Merchant identity is too weak to justify contact work."
    if eligibility_class == ELIGIBILITY_CONSUMER_NOISE:
        return "This case reads like consumer complaint noise, not merchant distress."
    if eligibility_class == ELIGIBILITY_RESEARCH_ONLY:
        return "Research-only: monitor the pattern, but do not push it into outreach."
    if eligibility_class == ELIGIBILITY_OPERATOR_REVIEW_ONLY:
        return f"The domain {merchant_domain or 'on file'} may be merchant-related, but confidence is still mixed."
    return "This case remains eligible for blocked outreach work."


def _domain_reason(label: str, reasons: list[str]) -> str:
    if label == "unpromotable":
        if "publisher_or_public_domain" in reasons:
            return "This appears to be a publisher or public-information property, not a merchant payments operator."
        if "processor_or_platform_domain" in reasons or "processor_named_domain" in reasons:
            return "This domain looks like a processor, platform, or marketplace property rather than a merchant-operated business."
        return "This domain looks informational rather than merchant-operated."
    if label == "weak":
        if "hosted_storefront_domain" in reasons:
            return "This is a hosted storefront domain, so it may be a real merchant but still needs identity review before outreach."
        return "Domain quality is too weak for contact acquisition."
    return "The domain looks merchant-operated enough for outreach work."


def _distress_reason(score: int, reasons: list[str], parsed_distress: str) -> str:
    if score >= 60 and parsed_distress != "unknown":
        return f"The signal reads like merchant payment distress around {parsed_distress.replace('_', ' ')}."
    if "consumer_language" in reasons or "consumer_entity" in reasons or "consumer_complaint_terms" in reasons:
        return "The signal lacks a credible merchant-operator perspective."
    if "general_discussion" in reasons:
        return "The signal reads like general discussion rather than live merchant distress."
    return "The distress signal is too weak or ambiguous for outreach."


def _icp_fit(
    *,
    text: str,
    parsed_distress: str,
    parsed_processor: str,
    merchant_name: str,
    merchant_domain: str,
    signal_info: dict,
    domain_quality: dict,
    distress_quality: dict,
    contact_email: str,
) -> dict:
    score = 0
    reasons: list[str] = []
    hosted_storefront = "hosted_storefront_domain" in set(domain_quality.get("reasons") or [])

    if distress_quality.get("score", 0) >= 55 and parsed_distress != "unknown":
        score += 30
        reasons.append("credible_processor_distress")
    elif distress_quality.get("score", 0) >= 40:
        score += 15
        reasons.append("plausible_processor_distress")

    if domain_quality.get("label") == "merchant_like" and merchant_name and merchant_domain:
        score += 20
        reasons.append("real_merchant_identity")
    elif hosted_storefront and merchant_name and merchant_domain:
        score += 8
        reasons.append("hosted_storefront_identity")

    if parsed_processor and parsed_processor != "unknown":
        score += 10
        reasons.append("processor_context_present")

    if _LIVE_URGENCY_RE.search(text):
        score += 20
        reasons.append("live_urgency")

    if signal_info.get("classification") == CLASS_MERCHANT_OPERATOR or _BUYER_ROLE_RE.search(text):
        score += 10
        reasons.append("operator_perspective")

    if "merchant candidate:" in text.lower() or "merchant domain candidate:" in text.lower():
        score += 10
        reasons.append("merchant_candidate_annotation")

    if contact_email:
        score += 10
        reasons.append("reachable_contact_path")

    if (
        merchant_name
        and merchant_domain
        and domain_quality.get("label") == "weak"
        and distress_quality.get("score", 0) >= 40
        and parsed_processor
        and parsed_processor != "unknown"
    ):
        score += 5
        reasons.append("named_merchant_with_reviewable_domain")

    if _GENERAL_DISCUSSION_RE.search(text):
        score -= 15
        reasons.append("general_discussion")
    if _HYPOTHETICAL_OR_HISTORICAL_RE.search(text):
        score -= 25
        reasons.append("historical_or_hypothetical")
    if _GENERIC_TOOLING_RE.search(text):
        score -= 30
        reasons.append("generic_tooling_mismatch")
    if not merchant_domain or domain_quality.get("label") != "merchant_like":
        if hosted_storefront and merchant_domain:
            score -= 10
            reasons.append("merchant_identity_needs_hosted_storefront_review")
        else:
            score -= 25
            reasons.append("merchant_identity_weak")

    score = max(0, min(100, score))
    label = "strong" if score >= 70 else "medium" if score >= 50 else "weak"
    high_conviction = bool(
        score >= 70
        and distress_quality.get("score", 0) >= 55
        and domain_quality.get("label") == "merchant_like"
        and merchant_name
        and merchant_domain
    )
    return {
        "score": score,
        "label": label,
        "reason": _icp_fit_reason(label, reasons),
        "reasons": reasons,
        "high_conviction": high_conviction,
    }


def _icp_fit_reason(label: str, reasons: list[str]) -> str:
    if label == "strong":
        return "The case fits the PayFlux ICP: active processor pressure, real merchant identity, and operator-relevant urgency."
    if "generic_tooling_mismatch" in reasons:
        return "This reads more like generic tooling or growth pain than processor distress."
    if "historical_or_hypothetical" in reasons:
        return "The signal reads as historical or hypothetical rather than active processor pressure."
    if "merchant_identity_needs_hosted_storefront_review" in reasons:
        return "This may be a real merchant storefront, but the hosted domain still needs identity review before PayFlux should treat it as frontline outreach."
    if "merchant_identity_weak" in reasons:
        return "The merchant identity is too weak to treat this as a high-conviction PayFlux prospect."
    return "The case is not yet a strong enough PayFlux fit to prioritize for frontline outreach."
