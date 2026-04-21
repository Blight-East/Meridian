"""
Twitter/X Merchant Distress Signal Scanner
Best-effort scraper for public X mirrors.
"""
import os
import re
import sys
from datetime import datetime
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from config.logging_config import get_logger
from memory.structured.db import save_event

logger = get_logger("twitter_signals")

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

MIRROR_SEARCH_URLS = [
    "https://nitter.net/search?f=tweets&q={query}",
    "https://xcancel.com/search?f=tweets&q={query}",
]

SEARCH_QUERIES = [
    "stripe froze my account",
    "paypal holding funds",
    "shopify payouts delayed",
    "payment processor terminated account",
    "account closed payouts",
]

BLOCKED_MARKERS = (
    "Verifying your browser",
    "Sorry this pages exist",
    "Log in to X / X",
)

LIMIT_PER_CYCLE = 50


def _parse_tweets(html_content, base_url):
    soup = BeautifulSoup(html_content, "html.parser")
    tweet_nodes = soup.select(".timeline-item, article")
    tweets = []

    for tweet in tweet_nodes:
        content_el = tweet.select_one(".tweet-content, .main-tweet .tweet-content, .tweet-body")
        if not content_el:
            continue

        content = content_el.get_text(" ", strip=True)
        if not content:
            continue

        author_el = tweet.select_one(".username, .fullname")
        link_el = tweet.select_one("a[href*='/status/']")
        time_el = tweet.select_one(".tweet-date a, time")
        tweet_url = ""
        if link_el and link_el.get("href"):
            tweet_url = urljoin(base_url, link_el["href"])

        tweets.append(
            {
                "content": content,
                "author": author_el.get_text(" ", strip=True) if author_el else "unknown",
                "url": tweet_url,
                "timestamp": time_el.get("title", "") if time_el else "",
            }
        )

    return tweets


def _fetch_query(query):
    encoded_query = quote(query)
    for search_url in MIRROR_SEARCH_URLS:
        url = search_url.format(query=encoded_query)
        try:
            response = requests.get(url, timeout=20, headers=REQUEST_HEADERS)
            html_content = response.text or ""
            if response.status_code == 200 and html_content and not any(marker in html_content for marker in BLOCKED_MARKERS):
                tweets = _parse_tweets(html_content, url)
                if tweets:
                    return tweets
        except Exception as e:
            logger.warning(f"Twitter mirror request failed for {url}: {e}")

        if _HAS_BROWSER:
            try:
                result = browser_fetch(url)
                if result.get("status") != "ok":
                    continue
                html_content = result.get("html", "")
                if any(marker in html_content for marker in BLOCKED_MARKERS):
                    continue
                tweets = _parse_tweets(html_content, url)
                if tweets:
                    return tweets
            except Exception as e:
                logger.warning(f"Twitter browser fetch failed for {url}: {e}")

    return []


def fetch_signals():
    """Fetch distress tweets from public mirror search pages."""
    signals = []
    seen_urls = set()

    for query in SEARCH_QUERIES:
        if len(signals) >= LIMIT_PER_CYCLE:
            break

        tweets = _fetch_query(query)
        if not tweets:
            logger.warning(f"No public twitter mirror results found for '{query}'")
            continue

        for tweet in tweets:
            if len(signals) >= LIMIT_PER_CYCLE:
                break

            dedupe_key = tweet["url"] or f"{tweet['author']}::{tweet['content'][:120]}"
            if dedupe_key in seen_urls:
                continue
            seen_urls.add(dedupe_key)

            signals.append(
                normalize_signal(
                    content=tweet["content"],
                    author=tweet["author"],
                    url=tweet["url"],
                    timestamp=tweet["timestamp"],
                    query=query,
                )
            )

    logger.info(f"Fetched {len(signals)} twitter signals")
    return signals


def normalize_signal(content, author, url, timestamp, query):
    """Normalize a tweet into the standard signal schema."""
    return {
        "source": "twitter",
        "entity": author,
        "content": content,
        "url": url,
        "author": author,
        "detected_at": timestamp or datetime.utcnow().isoformat(),
        "metadata": {
            "source_platform": "twitter",
            "search_query": query,
            "engagement": 0,
            "language": "en",
        },
    }


def store_signal(signal):
    """Store a signal through the existing ingestion pipeline."""
    from ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_twitter_ingestion():
    """Main entry point for scheduled ingestion."""
    signals = fetch_signals()
    stored = 0
    for sig in signals:
        try:
            result = store_signal(sig)
            if result is not None:
                stored += 1
        except Exception as e:
            logger.warning(f"Failed to store twitter signal: {e}")

    save_event("twitter_ingestion_cycle", {"fetched": len(signals), "stored": stored})
    logger.info(f"Twitter ingestion: {stored}/{len(signals)} stored")
    return {"fetched": len(signals), "stored": stored}
