"""
Trustpilot Complaint Monitor
Scrapes Trustpilot review pages for merchant distress signals.
"""
import os
import re
import sys
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.logging_config import get_logger
from memory.structured.db import save_event

logger = get_logger("trustpilot_signals")

try:
    from tools.browser_tool import browser_fetch

    _HAS_BROWSER = True
except ImportError:
    _HAS_BROWSER = False

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

TARGETS = {
    "stripe": "https://www.trustpilot.com/review/stripe.com",
    "paypal": "https://www.trustpilot.com/review/paypal.com",
    "shopify_payments": "https://www.trustpilot.com/review/shopify.com",
    "square": "https://www.trustpilot.com/review/squareup.com",
    "adyen": "https://www.trustpilot.com/review/adyen.com",
}

DISTRESS_PATTERNS = [
    re.compile(r"\bfroz(?:e|en|ing)\b"),
    re.compile(r"\bwithh(?:eld|old(?:ing)?)\b"),
    re.compile(r"\bfunds?\s+(?:held|locked|stuck|frozen)\b"),
    re.compile(r"\bpayouts?\s+(?:delay(?:ed)?|late|paused|held|stuck|missing)\b"),
    re.compile(r"\baccount\s+(?:closed|closure|terminated|suspended|blocked|banned|restricted)\b"),
    re.compile(r"\b(?:reserve|risk review|agreement violation|chargeback)\b"),
]

LIMIT_PER_CYCLE = 20


def _matches_distress(text):
    lowered = text.lower()
    return any(pattern.search(lowered) for pattern in DISTRESS_PATTERNS)


def _fetch_html(url):
    if _HAS_BROWSER:
        logger.info(f"Using browser_fetch for Trustpilot URL: {url}")
        try:
            result = browser_fetch(url)
            if result.get("status") == "ok":
                return result.get("html", "")
            logger.warning(f"Trustpilot browser_fetch returned non-ok status for {url}: {result}")
        except Exception as e:
            logger.warning(f"Trustpilot browser_fetch failed for {url}: {e}")
    else:
        logger.warning("Browser fetch not available for Trustpilot scraping. Returning empty.")
    return ""


def _extract_review_text(review):
    text_parts = []
    for node in review.select("[data-service-review-text-typography], .review-content__text, p"):
        text = node.get_text(" ", strip=True)
        if text and text.lower() != "company replied":
            text_parts.append(text)

    if not text_parts:
        blob = review.get_text(" ", strip=True)
        blob = re.sub(r"\bCompany replied\b", "", blob, flags=re.IGNORECASE)
        blob = re.sub(r"\s+", " ", blob).strip()
        return blob

    deduped = []
    seen = set()
    for part in text_parts:
        if part not in seen:
            seen.add(part)
            deduped.append(part)
    return " ".join(deduped)


def _extract_review_url(review, default_url):
    link_el = review.select_one("a[href*='/reviews/']")
    if not link_el or not link_el.get("href"):
        return default_url
    return urljoin("https://www.trustpilot.com", link_el["href"])


def _extract_author(review):
    author_el = review.select_one("[data-consumer-name-typography], .consumer-information__name")
    return author_el.get_text(" ", strip=True) if author_el else "unknown"


def _extract_detected_at(review):
    time_el = review.select_one("time")
    if time_el:
        return time_el.get("datetime") or time_el.get_text(" ", strip=True)
    return datetime.utcnow().isoformat()


def fetch_signals():
    """Fetch distress reviews from Trustpilot."""
    signals = []
    seen_reviews = set()

    for processor, url in TARGETS.items():
        if len(signals) >= LIMIT_PER_CYCLE:
            break

        html_content = _fetch_html(url)
        if not html_content:
            continue

        soup = BeautifulSoup(html_content, "html.parser")
        reviews = soup.select("[data-service-review-card-paper]") or soup.select("article")
        if not reviews:
            logger.warning(f"No Trustpilot reviews found for {processor}")
            continue

        for review in reviews:
            if len(signals) >= LIMIT_PER_CYCLE:
                break

            content = _extract_review_text(review)
            if not content or not _matches_distress(content):
                continue

            author = _extract_author(review)
            review_url = _extract_review_url(review, url)
            dedupe_key = (processor, review_url, content[:120])
            if dedupe_key in seen_reviews:
                continue
            seen_reviews.add(dedupe_key)

            signals.append(
                normalize_signal(
                    content=content,
                    author=author,
                    url=review_url,
                    processor=processor,
                    stars=0,
                    detected_at=_extract_detected_at(review),
                )
            )

    logger.info(f"Fetched {len(signals)} trustpilot signals")
    return signals


def normalize_signal(content, author, url, processor, stars, detected_at=None):
    """Normalize a Trustpilot review into the standard signal schema."""
    return {
        "source": "trustpilot",
        "entity": author,
        "content": f"[Trustpilot review for {processor}] {content}",
        "url": url,
        "author": author,
        "detected_at": detected_at or datetime.utcnow().isoformat(),
        "metadata": {
            "source_platform": "trustpilot",
            "processor": processor,
            "engagement": stars,
            "language": "en",
        },
    }


def store_signal(signal):
    """Store a signal through the existing ingestion pipeline."""
    from runtime.intelligence.ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_trustpilot_ingestion():
    """Main entry point for scheduled ingestion."""
    signals = fetch_signals()
    stored = 0
    for sig in signals:
        try:
            result = store_signal(sig)
            if result is not None:
                stored += 1
        except Exception as e:
            logger.warning(f"Failed to store trustpilot signal: {e}")

    save_event("trustpilot_ingestion_cycle", {"fetched": len(signals), "stored": stored})
    logger.info(f"Trustpilot ingestion: {stored}/{len(signals)} stored")
    return {"fetched": len(signals), "stored": stored}
