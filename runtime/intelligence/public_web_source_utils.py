from __future__ import annotations

import os
import re
import sys
import time
from collections import Counter
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from runtime.intelligence.distress_normalization import normalize_distress_topic
from runtime.intelligence.merchant_attribution import detect_processor
from runtime.intelligence.merchant_identity import extract_domains, normalize_domain
from runtime.intelligence.merchant_quality import is_valid_domain
from runtime.intelligence.merchant_signal_classifier import (
    CLASS_CONSUMER_OR_IRRELEVANT,
    CLASS_MERCHANT_OPERATOR,
    classify_merchant_signal,
)


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
REQUEST_TIMEOUT_SECONDS = 20
REQUEST_PAUSE_SECONDS = 0.35

BLOCKED_DOMAINS = {
    "community.shopify.com",
    "stackoverflow.com",
    "stackexchange.com",
    "i.sstatic.net",
    "cdn.sstatic.net",
    "canada1.discourse-cdn.com",
    "yyz1.discourse-cdn.com",
    "github.com",
    "stripe.com",
    "paypal.com",
    "shopify.com",
    "shopify.dev",
    "developers.facebook.com",
    "developer.paypal.com",
    "docs.stripe.com",
    "developers.google.com",
}
BLOCKED_ROOT_PATTERNS = (
    re.compile(r"(^|\.)(github|gitlab|medium|substack|wordpress|blogspot|wikipedia)\.", re.IGNORECASE),
    re.compile(r"(^|\.)(reuters|apnews|nytimes|forbes|bloomberg|wsj|medium|substack)\.", re.IGNORECASE),
    re.compile(r"(^|\.)(docs|developers|support|community|forum)\.", re.IGNORECASE),
    re.compile(r"(^|\.)(discourse-cdn|sstatic|cloudfront|googleusercontent|ytimg)\.", re.IGNORECASE),
)
PLATFORM_ROOT_PATTERNS = (
    re.compile(r"(^|\.)(stripe|paypal|squareup|braintreepayments|shopify|woocommerce|bigcommerce|magento)\.", re.IGNORECASE),
)
DISTRESS_PATTERNS = [
    ("payouts_delayed", re.compile(r"\bpayouts?\s+(?:on hold|hold|paused|stopped|delayed|delay|late|missing|stuck)\b", re.IGNORECASE)),
    ("reserve_hold", re.compile(r"\b(?:rolling\s+reserve|reserve\s+(?:hold|increase|requirement)|funds?\s+held)\b", re.IGNORECASE)),
    ("verification_review", re.compile(r"\b(?:under review|account review|payout review|standard review|verification required|verification review|compliance(?:\s+review)?|kyc|kyb)\b", re.IGNORECASE)),
    ("account_frozen", re.compile(r"\b(?:funds?\s+frozen|account\s+restricted|account\s+frozen|payments?\s+disabled|shopify payments disabled|account\s+closed)\b", re.IGNORECASE)),
    ("chargeback_issue", re.compile(r"\b(?:chargebacks?|disputes?|negative balance)\b", re.IGNORECASE)),
    ("onboarding_rejected", re.compile(r"\b(?:payment provider (?:is )?not enabled|provider not enabled|payment gateway error|gateway timeout|transaction declined|payment failed|checkout payment failure|merchant account denied)\b", re.IGNORECASE)),
]
MERCHANT_CONTEXT_PATTERNS = (
    re.compile(r"\b(?:my|our)\s+(?:store|shop|business|company|brand|website)\b", re.IGNORECASE),
    re.compile(r"\b(?:i|we)\s+(?:run|operate|own|manage|sell)\b", re.IGNORECASE),
    re.compile(r"\b(?:orders|checkout|revenue|sales|suppliers|employees|cash flow)\b", re.IGNORECASE),
)
MERCHANTISH_DOMAIN_HINTS = {
    "shop",
    "store",
    "cart",
    "checkout",
    "pay",
    "payments",
    "market",
    "supply",
    "goods",
    "retail",
    "studio",
}
COMMON_MERCHANT_TLDS = {
    "ai",
    "app",
    "biz",
    "co",
    "com",
    "company",
    "dev",
    "eu",
    "in",
    "io",
    "me",
    "net",
    "online",
    "org",
    "shop",
    "store",
    "uk",
    "us",
}
DOMAIN_CONTEXT_RE = re.compile(
    r"\b(?:my|our)\s+(?:store|site|website|domain)\b|\b(?:merchant|store)\s+domain\b|\bmyshopify\b",
    re.IGNORECASE,
)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    time.sleep(REQUEST_PAUSE_SECONDS)
    return response.text


def soup_text(node) -> str:
    if node is None:
        return ""
    return normalize_whitespace(node.get_text(" ", strip=True))


def normalize_whitespace(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def absolute_url(base_url: str, href: str | None) -> str:
    if not href:
        return ""
    return urljoin(base_url, href.strip())


def extract_hrefs(container, *, base_url: str) -> list[str]:
    hrefs: list[str] = []
    seen: set[str] = set()
    if container is None:
        return hrefs
    for link in container.select("a[href]"):
        url = absolute_url(base_url, link.get("href"))
        if not url or url in seen:
            continue
        seen.add(url)
        hrefs.append(url)
    return hrefs


def infer_processor(text: str) -> str:
    lowered = normalize_whitespace(text).lower()
    if "shopify payments" in lowered:
        return "shopify_payments"
    processor = detect_processor(lowered)
    return (processor or "unknown").replace(".", "_")


def infer_distress_type(text: str) -> str:
    lowered = normalize_whitespace(text).lower()
    for canonical, pattern in DISTRESS_PATTERNS:
        if pattern.search(lowered):
            return normalize_distress_topic(canonical)
    return normalize_distress_topic(lowered)


def merchant_operator_signals(text: str) -> dict:
    info = classify_merchant_signal(text or "")
    explicit_context = any(pattern.search(text or "") for pattern in MERCHANT_CONTEXT_PATTERNS)
    return {
        "classification": info.get("classification") or "",
        "merchant_score": int(info.get("merchant_score") or 0),
        "consumer_score": int(info.get("consumer_score") or 0),
        "explicit_context": explicit_context,
        "reasons": info.get("reasons") or [],
    }


def extract_candidate_domains_from_text(text: str, hrefs: Iterable[str] | None = None) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for domain in extract_domains(text or ""):
        if domain and domain not in seen:
            seen.add(domain)
            candidates.append(domain)
    for href in hrefs or []:
        parsed = urlparse(href)
        host = normalize_domain(parsed.netloc or "")
        if not host or host in seen:
            continue
        if parsed.scheme and parsed.scheme not in {"http", "https"}:
            continue
        seen.add(host)
        candidates.append(host)
    return candidates


def select_best_merchant_domain(text: str, hrefs: Iterable[str] | None = None) -> tuple[str, list[dict]]:
    href_hosts = {
        normalize_domain(urlparse(href).netloc or "")
        for href in (hrefs or [])
        if href
    }
    ranked: list[dict] = []
    for domain in extract_candidate_domains_from_text(text, hrefs):
        score = _score_domain_candidate(domain, text, href_hosts=href_hosts)
        if score <= 0:
            continue
        ranked.append({"domain": domain, "score": score})
    ranked.sort(key=lambda item: (-int(item["score"]), item["domain"]))
    return (ranked[0]["domain"] if ranked else "", ranked)


def _score_domain_candidate(domain: str, context_text: str, *, href_hosts: set[str] | None = None) -> int:
    host = normalize_domain(domain or "")
    if not host or not is_valid_domain(host):
        return -100
    href_hosts = href_hosts or set()
    if host.endswith(".myshopify.com"):
        return 95
    if host in BLOCKED_DOMAINS:
        return -100
    if any(pattern.search(host) for pattern in BLOCKED_ROOT_PATTERNS):
        return -60
    if any(pattern.search(host) for pattern in PLATFORM_ROOT_PATTERNS):
        return -35
    tld = host.rsplit(".", 1)[-1]
    has_domain_context = bool(DOMAIN_CONTEXT_RE.search(context_text or ""))
    if host not in href_hosts and tld not in COMMON_MERCHANT_TLDS and not has_domain_context:
        return -50
    score = 20
    labels = set(host.split(".")[0].split("-"))
    if labels & MERCHANTISH_DOMAIN_HINTS:
        score += 15
    if has_domain_context:
        score += 20
    if host in href_hosts:
        score += 20
    if re.search(rf"\b{re.escape(host)}\b", context_text or "", re.IGNORECASE):
        score += 10
    if host.endswith((".gov", ".edu")):
        score -= 80
    return score


def classify_source_candidate(*, title: str, body_text: str, merchant_domain: str = "") -> dict:
    combined = normalize_whitespace(f"{title} {body_text}")
    operator = merchant_operator_signals(combined)
    distress_type = infer_distress_type(combined)
    processor = infer_processor(combined)
    confidence = 0.2
    if distress_type != "unknown":
        confidence += 0.25
    if processor != "unknown":
        confidence += 0.15
    if merchant_domain:
        confidence += 0.25
    if operator["classification"] == CLASS_MERCHANT_OPERATOR:
        confidence += 0.2
    elif operator["classification"] == CLASS_CONSUMER_OR_IRRELEVANT:
        confidence -= 0.2
    elif operator["explicit_context"]:
        confidence += 0.1
    return {
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "merchant_signal_classification": operator["classification"] or "",
        "merchant_operator_clarity": "clear" if operator["classification"] == CLASS_MERCHANT_OPERATOR or operator["explicit_context"] else "weak",
        "confidence": round(max(0.0, min(0.95, confidence)), 2),
        "operator_reasons": operator["reasons"],
    }


def should_emit_signal(candidate: dict, *, allow_without_domain: bool = False) -> tuple[bool, str]:
    distress_type = str(candidate.get("distress_type") or "unknown")
    merchant_domain = str(candidate.get("merchant_domain") or "")
    merchant_signal_classification = str(candidate.get("merchant_signal_classification") or "")
    confidence = float(candidate.get("confidence") or 0.0)
    title = str(candidate.get("title") or "")
    body_text = str(candidate.get("body_text") or "")
    combined = normalize_whitespace(f"{title} {body_text}")

    if distress_type == "unknown":
        return False, "distress_unknown"
    if confidence < 0.45:
        return False, "confidence_below_threshold"
    if merchant_signal_classification == CLASS_CONSUMER_OR_IRRELEVANT:
        return False, "consumer_or_irrelevant"
    if merchant_domain:
        return True, "merchant_domain_resolved"
    if allow_without_domain and merchant_signal_classification == CLASS_MERCHANT_OPERATOR:
        return True, "merchant_operator_without_domain"
    if any(pattern.search(combined) for pattern in MERCHANT_CONTEXT_PATTERNS):
        return True, "merchant_operator_context"
    return False, "merchant_identity_missing"


def format_signal_content(candidate: dict) -> str:
    source_label = str(candidate.get("source_label") or candidate.get("source_name") or "Public Web")
    board = str(candidate.get("source_category") or "")
    processor = str(candidate.get("processor") or "unknown")
    distress_type = str(candidate.get("distress_type") or "unknown")
    merchant_domain = str(candidate.get("merchant_domain") or "")
    title = normalize_whitespace(candidate.get("title"))
    body_text = normalize_whitespace(candidate.get("body_text"))
    evidence = normalize_whitespace(candidate.get("evidence_text"))
    author = normalize_whitespace(candidate.get("author_handle"))
    url = normalize_whitespace(candidate.get("thread_or_question_url"))
    merchant_attribution_state = "domain_resolved" if merchant_domain else str(candidate.get("merchant_attribution_state") or "domainless")
    parts = [
        f"[processor:{processor}]",
        f"[distress:{distress_type}]",
        f"[confidence:{candidate.get('confidence', 0)}]",
        f"[{source_label}]",
    ]
    if board:
        parts.append(f"Board: {board}.")
    if merchant_domain:
        parts.append(f"Merchant domain candidate: {merchant_domain}.")
    else:
        parts.append(f"Merchant attribution state: {merchant_attribution_state}.")
    if author:
        parts.append(f"Author: {author}.")
    if title:
        parts.append(f"Title: {title}.")
    if body_text:
        parts.append(f"Body: {body_text}")
    if evidence:
        parts.append(f"Evidence: {evidence}")
    if url:
        parts.append(f"Source URL: {url}")
    return " ".join(part for part in parts if part).strip()


def summarize_top_distress(candidates: list[dict]) -> str:
    counter = Counter(str(item.get("distress_type") or "unknown") for item in candidates if item.get("distress_type"))
    if not counter:
        return ""
    return counter.most_common(1)[0][0]


def parse_created_at(container, *, selector: str = "time") -> str | None:
    if container is None:
        return None
    node = container.select_one(selector)
    if node is None:
        return None
    return node.get("datetime") or normalize_whitespace(node.get_text(" ", strip=True)) or None


def text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup_text(soup)
