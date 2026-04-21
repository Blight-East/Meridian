"""
Historical Signal Ingestion via Query Expansion.

Searches Reddit, HackerNews, and forum archives using distress keyword
queries to backfill signals from the past 24 months. Uses existing
dedup infrastructure (signal_hash + semantic) to avoid duplicates.

Rate-limited: 2-3s between queries, max 20 queries per cycle.
Runs once per hour via scheduler.
"""
import sys
import os
import time
import random
import hashlib
import requests
import feedparser

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.ingestion import process_and_store_signal
from runtime.intelligence.distress_queries import DISTRESS_QUERIES, REDDIT_SUBREDDITS

logger = get_logger("historical_ingestion")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

# ── Configuration ────────────────────────────────────────────────────────────
MAX_QUERIES_PER_CYCLE = 20
MAX_RESULTS_PER_QUERY = 200
QUERY_DELAY_MIN = 2.0   # seconds between queries
QUERY_DELAY_MAX = 3.5
HEADERS = {"User-Agent": "AgentFluxHistorical/1.0 (research; respect robots.txt)"}

# Track which queries have been run to rotate through them
_QUERY_STATE_KEY = "historical_ingestion_state"


def _get_next_queries(limit=MAX_QUERIES_PER_CYCLE):
    """
    Select the next batch of queries to run, rotating through the full list.
    Prioritizes queries that haven't been run recently.
    """
    try:
        with engine.connect() as conn:
            # Check which queries were run in the last 24 hours
            recent = conn.execute(text("""
                SELECT DISTINCT data->>'query' as query
                FROM events
                WHERE event_type = 'historical_signal_ingested'
                  AND created_at > NOW() - INTERVAL '24 hours'
            """)).fetchall()
            recent_queries = {r[0] for r in recent if r[0]}
    except Exception:
        recent_queries = set()

    # Prioritize unrun queries
    unrun = [q for q in DISTRESS_QUERIES if q not in recent_queries]
    previously_run = [q for q in DISTRESS_QUERIES if q in recent_queries]

    # Take from unrun first, then cycle back
    selected = unrun[:limit]
    if len(selected) < limit:
        selected.extend(previously_run[:limit - len(selected)])

    # Shuffle to avoid predictable patterns
    random.shuffle(selected)
    return selected[:limit]


def _search_reddit_historical(query, subreddit=None):
    """
    Search Reddit for historical posts matching query.
    Uses Reddit JSON search API with 'all' time range.
    Returns list of signal dicts.
    """
    signals = []
    base = f"https://www.reddit.com"

    if subreddit:
        url = f"{base}/r/{subreddit}/search.json?q={query.replace(' ', '+')}&limit=25&sort=relevance&t=all&restrict_sr=on"
    else:
        url = f"{base}/search.json?q={query.replace(' ', '+')}&limit=25&sort=relevance&t=all"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            for post in data.get("data", {}).get("children", []):
                d = post.get("data", {})
                title = d.get("title", "")
                selftext = (d.get("selftext", "") or "")[:500]
                subreddit_name = d.get("subreddit", "")
                score = d.get("score", 0)
                comments = d.get("num_comments", 0)
                permalink = d.get("permalink", "")

                if not title:
                    continue

                content = f"[reddit/{subreddit_name}] {title}"
                if selftext and len(selftext) > 50:
                    content += f" | {selftext}"

                signals.append({
                    "content": f"{content} [score:{score}|comments:{comments}]",
                    "source": f"https://reddit.com{permalink}",
                    "raw_source": "reddit_historical",
                })
        elif r.status_code == 429:
            logger.warning("Reddit rate limited, backing off")
            time.sleep(10)
    except Exception as e:
        logger.debug(f"Reddit historical search error for '{query}': {e}")

    return signals[:MAX_RESULTS_PER_QUERY]


def _search_hackernews_historical(query):
    """
    Search HackerNews Algolia API for historical posts.
    Algolia search covers full HN history.
    """
    signals = []

    for tag in ["story", "comment"]:
        url = (
            f"https://hn.algolia.com/api/v1/search?"
            f"query={query.replace(' ', '+')}"
            f"&tags={tag}"
            f"&hitsPerPage=20"
        )

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                for hit in r.json().get("hits", []):
                    title = hit.get("title") or hit.get("comment_text", "")
                    if not title:
                        continue

                    # Truncate long comments
                    if len(title) > 500:
                        title = title[:500]

                    points = hit.get("points", 0) or 0
                    num_comments = hit.get("num_comments", 0) or 0
                    object_id = hit.get("objectID", "")
                    story_url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"

                    content = f"[hackernews|pts:{points}] {title}"
                    signals.append({
                        "content": f"{content} [score:{points}|comments:{num_comments}]",
                        "source": story_url,
                        "raw_source": "hn_historical",
                    })
        except Exception as e:
            logger.debug(f"HN historical search error for '{query}': {e}")

        time.sleep(0.5)  # Be gentle with Algolia

    return signals[:MAX_RESULTS_PER_QUERY]


def _search_reddit_subreddit_historical(query, subreddits=None):
    """
    Search specific subreddits for more targeted results.
    Supplements broad Reddit search with subreddit-specific queries.
    """
    if subreddits is None:
        subreddits = random.sample(REDDIT_SUBREDDITS, min(3, len(REDDIT_SUBREDDITS)))

    all_signals = []
    for sub in subreddits:
        signals = _search_reddit_historical(query, subreddit=sub)
        all_signals.extend(signals)
        time.sleep(QUERY_DELAY_MIN)  # Rate limit between subreddit searches

    return all_signals


def run_historical_ingestion():
    """
    Main entry point: run one cycle of historical query expansion.
    Selects next batch of queries, searches Reddit + HN,
    deduplicates via process_and_store_signal(), and logs results.

    Designed to run once per hour.
    """
    cycle_start = time.time()
    queries = _get_next_queries()

    if not queries:
        logger.info("No queries to run for historical ingestion")
        return {"queries_executed": 0, "signals_fetched": 0, "signals_inserted": 0, "duplicates_skipped": 0}

    total_fetched = 0
    total_inserted = 0
    total_duplicates = 0
    query_results = []

    for i, query in enumerate(queries):
        query_start = time.time()
        query_signals = []

        # Alternate between Reddit broad, Reddit subreddit, and HN
        # to spread load across sources
        if i % 3 == 0:
            query_signals.extend(_search_reddit_historical(query))
        elif i % 3 == 1:
            query_signals.extend(_search_hackernews_historical(query))
        else:
            # Targeted subreddit search for every 3rd query
            subs = random.sample(REDDIT_SUBREDDITS, min(2, len(REDDIT_SUBREDDITS)))
            query_signals.extend(_search_reddit_subreddit_historical(query, subs))

        fetched = len(query_signals)
        inserted = 0
        duplicates = 0

        for sig in query_signals:
            try:
                result = process_and_store_signal(sig["source"], sig["content"])
                if result:
                    inserted += 1
                else:
                    duplicates += 1
            except Exception as e:
                logger.debug(f"Historical signal processing error: {e}")
                duplicates += 1

        total_fetched += fetched
        total_inserted += inserted
        total_duplicates += duplicates

        save_event("historical_signal_ingested", {
            "query": query,
            "results_fetched": fetched,
            "new_signals_inserted": inserted,
            "duplicates_skipped": duplicates,
        })

        query_results.append({
            "query": query,
            "fetched": fetched,
            "inserted": inserted,
            "duplicates": duplicates,
        })

        if inserted > 0:
            logger.info(f"Historical query '{query}': {fetched} fetched, {inserted} new, {duplicates} dupes")

        # Rate limiting between queries
        if i < len(queries) - 1:
            delay = random.uniform(QUERY_DELAY_MIN, QUERY_DELAY_MAX)
            time.sleep(delay)

    cycle_duration = time.time() - cycle_start

    result = {
        "queries_executed": len(queries),
        "signals_fetched": total_fetched,
        "signals_inserted": total_inserted,
        "duplicates_skipped": total_duplicates,
        "cycle_duration_seconds": round(cycle_duration, 2),
    }

    save_event("historical_ingestion_cycle", result)
    logger.info(
        f"Historical ingestion cycle complete: "
        f"{result['queries_executed']} queries | "
        f"{result['signals_fetched']} fetched | "
        f"{result['signals_inserted']} inserted | "
        f"{result['duplicates_skipped']} dupes | "
        f"{result['cycle_duration_seconds']}s"
    )

    return result


if __name__ == "__main__":
    import json
    print(json.dumps(run_historical_ingestion(), indent=2))
