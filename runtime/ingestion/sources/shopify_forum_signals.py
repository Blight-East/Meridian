"""
Shopify Community Forum Scanner
Uses Discourse search and topic JSON to find merchant distress threads.
"""
import os
import re
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.public_web_source_utils import (
    extract_hrefs,
    format_signal_content,
    select_best_merchant_domain,
    should_emit_signal,
)

logger = get_logger("shopify_forum_signals")

BASE_URL = "https://community.shopify.com"
SOURCE_NAME = "shopify_forum"
SOURCE_LABEL = "Shopify Community Forum"
SEARCH_URL = f"{BASE_URL}/search.json"
SEARCH_QUERIES = [
    "payout paused",
    "payout delayed",
    "funds held",
    "account under review",
    "risk review",
    "account closed payouts",
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

DISTRESS_PATTERNS = [
    re.compile(r"\bpayouts?\s+(?:paused|delay(?:ed)?|late|missing|stuck)\b"),
    re.compile(r"\bfunds?\s+(?:held|locked|frozen|withheld)\b"),
    re.compile(r"\baccount\s+(?:closed|under review|suspended|blocked|terminated)\b"),
    re.compile(r"\b(?:risk review|merchant trust team|compliance review|reserve)\b"),
]

LIMIT_PER_CYCLE = 30


def _matches_distress(text):
    lowered = text.lower()
    return any(pattern.search(lowered) for pattern in DISTRESS_PATTERNS)


def _search_posts(query):
    response = requests.get(
        SEARCH_URL,
        params={"q": query},
        headers=REQUEST_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    return response.json().get("posts", [])


def _fetch_topic(topic_id):
    response = requests.get(
        f"{BASE_URL}/t/{topic_id}.json",
        headers=REQUEST_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def _extract_topic_body(topic):
    posts = topic.get("post_stream", {}).get("posts", [])
    if not posts:
        return "", "unknown", []

    first_post = posts[0]
    cooked = first_post.get("cooked", "")
    soup = BeautifulSoup(cooked, "html.parser")
    text = soup.get_text(" ", strip=True)
    hrefs = extract_hrefs(soup, base_url=BASE_URL)
    return text, first_post.get("username") or "unknown", hrefs


def fetch_signals():
    """Fetch distress threads from Shopify Community."""
    signals = []
    seen_topics = set()
    filtered = 0

    for query in SEARCH_QUERIES:
        if len(signals) >= LIMIT_PER_CYCLE:
            break

        try:
            posts = _search_posts(query)
        except Exception as e:
            logger.warning(f"Shopify forum search failed for '{query}': {e}")
            continue

        for post in posts:
            if len(signals) >= LIMIT_PER_CYCLE:
                break

            topic_id = post.get("topic_id")
            if not topic_id or topic_id in seen_topics:
                continue

            blurb = post.get("blurb", "")
            if blurb and not _matches_distress(blurb):
                continue

            try:
                topic = _fetch_topic(topic_id)
            except Exception as e:
                logger.warning(f"Shopify topic fetch failed for {topic_id}: {e}")
                continue

            title = topic.get("title", "").strip()
            content, author, hrefs = _extract_topic_body(topic)
            combined = f"{title} {content}".strip()
            if not combined or not _matches_distress(combined):
                continue

            seen_topics.add(topic_id)
            slug = topic.get("slug") or "-"
            thread_url = f"{BASE_URL}/t/{slug}/{topic_id}"
            replies = topic.get("reply_count", 0)
            merchant_domain, ranked_domains = select_best_merchant_domain(content, hrefs)

            candidate = normalize_signal(
                title=title,
                content=content or blurb or title,
                author=author,
                url=thread_url,
                replies=replies,
                detected_at=topic.get("created_at"),
                merchant_domain=merchant_domain,
                domain_candidates=ranked_domains,
            )
            ok, reason = should_emit_signal(candidate, allow_without_domain=False)
            candidate["filter_reason"] = reason
            if not ok:
                filtered += 1
                continue

            signals.append(candidate)

    logger.info(f"Fetched {len(signals)} shopify forum signals")
    return {"candidates": signals, "filtered": filtered}


def normalize_signal(title, content, author, url, replies, detected_at=None, merchant_domain="", domain_candidates=None):
    """Normalize a Shopify forum thread into the standard signal schema."""
    combined = f"{title} {content}".strip()
    processor = "shopify_payments" if "shopify payments" in combined.lower() else "unknown"
    distress_type = "reserve_hold" if re.search(r"\breserve\b", combined, re.IGNORECASE) else (
        "payouts_delayed" if re.search(r"\bpayouts?\s+(?:paused|delay(?:ed)?|late|missing|stuck)\b", combined, re.IGNORECASE) else "unknown"
    )
    confidence = 0.72 if merchant_domain else 0.52
    return {
        "source": SOURCE_NAME,
        "source_name": SOURCE_NAME,
        "source_label": SOURCE_LABEL,
        "source_category": "Search",
        "entity": author,
        "title": title,
        "body_text": content,
        "thread_or_question_url": url,
        "author_handle": author,
        "merchant_domain": merchant_domain,
        "domain_candidates": domain_candidates or [],
        "merchant_attribution_state": "domain_resolved" if merchant_domain else "domainless",
        "merchant_signal_classification": "merchant_operator" if merchant_domain else "unknown",
        "merchant_operator_clarity": "clear" if merchant_domain else "weak",
        "processor": processor,
        "distress_type": distress_type,
        "confidence": confidence,
        "content": format_signal_content(
            {
                "source_name": SOURCE_NAME,
                "source_label": SOURCE_LABEL,
                "source_category": "Search",
                "processor": processor,
                "distress_type": distress_type,
                "confidence": confidence,
                "merchant_domain": merchant_domain,
                "merchant_attribution_state": "domain_resolved" if merchant_domain else "domainless",
                "title": title,
                "body_text": content,
                "author_handle": author,
                "thread_or_question_url": url,
            }
        ),
        "url": url,
        "author": author,
        "detected_at": detected_at or datetime.utcnow().isoformat(),
        "metadata": {
            "source_platform": SOURCE_NAME,
            "thread_title": title,
            "engagement": replies,
            "language": "en",
            "merchant_domain": merchant_domain,
            "filter_reason": "",
        },
    }


def store_signal(signal):
    """Store signal through the existing ingestion pipeline."""
    from runtime.intelligence.ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_shopify_forum_ingestion():
    """Main entry point for scheduled ingestion."""
    fetch_result = fetch_signals()
    signals = fetch_result["candidates"]
    stored = 0
    for sig in signals:
        try:
            stored_signal = store_signal(sig)
            if stored_signal is not None:
                stored += 1
        except Exception as e:
            logger.warning(f"Failed to store shopify forum signal: {e}")

    save_event(
        "shopify_forum_ingestion_cycle",
        {
            "fetched": len(signals),
            "stored": stored,
            "filtered": int(fetch_result.get("filtered") or 0),
        },
    )
    logger.info(f"Shopify forum ingestion: {stored}/{len(signals)} stored")
    return {"fetched": len(signals), "stored": stored, "filtered": int(fetch_result.get("filtered") or 0)}
