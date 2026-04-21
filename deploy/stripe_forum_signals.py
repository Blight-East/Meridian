"""
Stripe Community / Support Forum Monitor
Collects merchant distress signals from public Stripe support topics.
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

logger = get_logger("stripe_forum_signals")

try:
    from tools.browser_tool import browser_fetch

    _HAS_BROWSER = True
except ImportError:
    _HAS_BROWSER = False

BASE_URL = "https://support.stripe.com"
TOPIC_URLS = {
    "payouts": f"{BASE_URL}/topics/payouts",
    "payments": f"{BASE_URL}/topics/payments",
    "account": f"{BASE_URL}/topics/account",
    "verification": f"{BASE_URL}/topics/verification",
}

CURATED_QUESTION_URLS = [
    ("payouts", f"{BASE_URL}/questions/payout-paused-due-to-pending-withdrawal-for-negative-balance"),
    ("payouts", f"{BASE_URL}/questions/late-or-missing-payouts"),
    ("payouts", f"{BASE_URL}/questions/waiting-on-your-first-stripe-payout-what-you-need-to-know"),
    ("payouts", f"{BASE_URL}/questions/fix-the-negative-balance-on-your-account"),
    ("account", f"{BASE_URL}/questions/close-a-stripe-account"),
    ("verification", f"{BASE_URL}/questions/website-ownership-verification-during-stripe-account-application"),
    ("verification", f"{BASE_URL}/questions/documents-for-business-verification"),
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

DISTRESS_PATTERNS = [
    re.compile(r"\bpayouts?\s+(?:paused|delay(?:ed)?|late|missing|stuck)\b"),
    re.compile(r"\bnegative balance\b"),
    re.compile(r"\baccount\s+(?:closed|closure|under review|suspended|restricted)\b"),
    re.compile(r"\b(?:verification|documents?|website ownership|risk)\b"),
]

LIMIT_PER_CYCLE = 30


def _matches_distress(text):
    lowered = text.lower()
    return any(pattern.search(lowered) for pattern in DISTRESS_PATTERNS)


def _extract_rendered_entries(html_content, topic_name):
    soup = BeautifulSoup(html_content, "html.parser")
    entries = []
    seen = set()

    for link in soup.select("a[href^='/questions/']"):
        href = link.get("href")
        if not href or href in seen:
            continue
        seen.add(href)

        parts = [part.strip() for part in link.stripped_strings if part.strip()]
        if not parts:
            continue

        title = parts[0]
        snippet = " ".join(parts[1:])
        entries.append(
            {
                "topic": topic_name,
                "title": title,
                "snippet": snippet,
                "url": urljoin(BASE_URL, href),
            }
        )

    return entries


def _fetch_question_detail(url):
    response = requests.get(url, timeout=20, headers=REQUEST_HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.get_text(" ", strip=True).replace(" : Stripe: Help & Support", "")
    meta_desc = soup.select_one("meta[name=description]")
    snippet = meta_desc.get("content", "").strip() if meta_desc else ""
    return title, snippet


def fetch_signals():
    """Fetch distress posts from Stripe support topics."""
    signals = []
    seen_urls = set()

    if _HAS_BROWSER:
        for topic_name, topic_url in TOPIC_URLS.items():
            if len(signals) >= LIMIT_PER_CYCLE:
                break

            try:
                result = browser_fetch(topic_url)
                if result.get("status") != "ok":
                    continue
                entries = _extract_rendered_entries(result.get("html", ""), topic_name)
            except Exception as e:
                logger.warning(f"Stripe topic fetch failed for {topic_name}: {e}")
                continue

            for entry in entries:
                if len(signals) >= LIMIT_PER_CYCLE:
                    break
                combined = f"{entry['title']} {entry['snippet']}".strip()
                if not _matches_distress(combined):
                    continue
                if entry["url"] in seen_urls:
                    continue
                seen_urls.add(entry["url"])
                signals.append(
                    normalize_signal(
                        title=entry["title"],
                        content=entry["snippet"] or entry["title"],
                        author="unknown",
                        url=entry["url"],
                        votes=0,
                        topic=entry["topic"],
                    )
                )

    for topic_name, question_url in CURATED_QUESTION_URLS:
        if len(signals) >= LIMIT_PER_CYCLE:
            break
        if question_url in seen_urls:
            continue

        try:
            title, snippet = _fetch_question_detail(question_url)
        except Exception as e:
            logger.warning(f"Stripe question fetch failed for {question_url}: {e}")
            continue

        combined = f"{title} {snippet}".strip()
        if not _matches_distress(combined):
            continue

        seen_urls.add(question_url)
        signals.append(
            normalize_signal(
                title=title,
                content=snippet or title,
                author="unknown",
                url=question_url,
                votes=0,
                topic=topic_name,
            )
        )

    logger.info(f"Fetched {len(signals)} stripe forum signals")
    return signals


def normalize_signal(title, content, author, url, votes, topic):
    """Normalize a Stripe support post into the standard signal schema."""
    return {
        "source": "stripe_forum",
        "entity": author,
        "content": f"[Stripe Support] {title}: {content}",
        "url": url,
        "author": author,
        "detected_at": datetime.utcnow().isoformat(),
        "metadata": {
            "source_platform": "stripe_forum",
            "topic": topic,
            "engagement": votes,
            "language": "en",
        },
    }


def store_signal(signal):
    """Store signal through the existing ingestion pipeline."""
    from ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_stripe_forum_ingestion():
    """Main entry point for scheduled ingestion."""
    signals = fetch_signals()
    stored = 0
    for sig in signals:
        try:
            result = store_signal(sig)
            if result is not None:
                stored += 1
        except Exception as e:
            logger.warning(f"Failed to store stripe forum signal: {e}")

    save_event("stripe_forum_ingestion_cycle", {"fetched": len(signals), "stored": stored})
    logger.info(f"Stripe forum ingestion: {stored}/{len(signals)} stored")
    return {"fetched": len(signals), "stored": stored}
