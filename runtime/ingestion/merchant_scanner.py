import sys, os, json, random, time, redis
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import requests
import feedparser
from memory.structured.db import save_event, engine
from sqlalchemy import text
from runtime.intelligence.ingestion import process_and_store_signal
from config.logging_config import get_logger

logger = get_logger("scanner")
r_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
HEADERS = {"User-Agent": "AgentFluxScanner/3.0 (reliability-radar)"}

# Per-source timeout for network calls (seconds)
_SOURCE_TIMEOUT = 10
# feedparser has no native timeout; we bound it via ThreadPoolExecutor
_FEEDPARSER_TIMEOUT = 15

EXPLORATORY_QUERIES = [
    "merchant account frozen",
    "payment processor reserve",
    "gateway terminated account",
    "payout delay",
    "chargeback hold",
    "merchant account shutdown"
]

RANDOM_VARIATIONS = [
    "processor held my money",
    "180 day hold stripe",
    "paypal locked funds business",
    "shopify payments disabled store",
    "high risk merchant closed",
    "banned without warning payment gateway"
]


def _feedparser_with_timeout(url, timeout=_FEEDPARSER_TIMEOUT):
    """feedparser.parse() bounded by a thread timeout since the library has no native timeout."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(feedparser.parse, url)
        return future.result(timeout=timeout)


def get_diverse_queries():
    # 40% top (4), 40% exp (4), 20% rand (2) = 10 queries
    queries = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT keyword FROM query_performance ORDER BY success_rate DESC, leads_generated DESC LIMIT 4")).fetchall()
            queries = [r[0] for r in rows]
    except Exception as e:
        logger.error(f"Failed to load DB queries: {e}")
        
    while len(queries) < 4:
        queries.append(random.choice(EXPLORATORY_QUERIES))
        
    queries.extend(random.sample(EXPLORATORY_QUERIES, min(4, len(EXPLORATORY_QUERIES))))
    queries.extend(random.sample(RANDOM_VARIATIONS, min(2, len(RANDOM_VARIATIONS))))
    
    return list(set(queries))[:10]

def get_source_order():
    sources = ["brave", "reddit_json", "reddit_rss", "hackernews"]
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT source, reliability_score FROM source_performance ORDER BY reliability_score DESC")).fetchall()
            if rows:
                ordered_db = [r[0] for r in rows if r[0] in sources]
                for s in ordered_db:
                    sources.remove(s)
                sources = ordered_db + sources
    except Exception:
        pass
    return sources

def update_source_stats(source, signals_generated):
    if signals_generated == 0: return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO source_performance (source, signals_generated, reliability_score)
                VALUES (:s, :c, 0.5)
                ON CONFLICT (source) DO UPDATE SET 
                    signals_generated = source_performance.signals_generated + :c,
                    reliability_score = CAST(source_performance.leads_generated AS FLOAT) / GREATEST(source_performance.signals_generated + :c, 1)
            """), {"s": source, "c": signals_generated})
            conn.commit()
    except Exception as e:
        logger.error(f"Failed source stats: {e}")

def has_search_api():
    return bool(os.getenv("BRAVE_SEARCH_API_KEY") or os.getenv("SEARCH_API_KEY"))

# Domains where real merchants post complaints. Brave results from other
# domains are almost always generic articles that produce garbage merchants.
_BRAVE_FORUM_DOMAINS = {
    "reddit.com", "www.reddit.com",
    "community.shopify.com",
    "trustpilot.com", "www.trustpilot.com",
    "uk.trustpilot.com", "ie.trustpilot.com",
    "news.ycombinator.com",
    "community.stripe.com",
    "stackoverflow.com", "www.stackoverflow.com",
    "quora.com", "www.quora.com",
    "bbb.org", "www.bbb.org",
    "complaintsboard.com", "www.complaintsboard.com",
    "sitejabber.com", "www.sitejabber.com",
    "facebook.com", "www.facebook.com",
}


def _is_forum_url(url):
    """Return True if URL is from a forum/review site where merchants post."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        return host in _BRAVE_FORUM_DOMAINS
    except Exception:
        return False


def search_brave(queries):
    from tools.web_search import web_search
    results = []
    for keyword in queries[:5]:
        try:
            data = web_search(keyword)
            if isinstance(data, dict) and "error" not in data:
                for item in data.get("web", {}).get("results", [])[:5]:
                    url = item.get("url", "")
                    if not _is_forum_url(url):
                        continue
                    results.append({"content": f"{item.get('title', '')} | {item.get('description', '')}", "source": f"{url}#brave", "raw_source": "brave", "score": 0, "comments": 0})
        except Exception as e: logger.error(f"Brave error: {e}")
    return results

def scrape_reddit(queries):
    results = []
    for keyword in queries[:4]:
        url = f"https://www.reddit.com/search.json?q={keyword.replace(' ', '+')}&limit=10&sort=relevance&t=year"
        try:
            r = requests.get(url, headers=HEADERS, timeout=_SOURCE_TIMEOUT)
            if r.status_code == 200:
                for post in r.json().get("data", {}).get("children", []):
                    d = post.get("data", {})
                    if title := d.get("title", ""):
                        results.append({"content": f"[reddit/{d.get('subreddit','')}] {title}", "source": f"https://reddit.com{d.get('permalink','')}", "raw_source": "reddit_json", "score": d.get("score",0), "comments": d.get("num_comments",0)})
        except Exception as e: logger.error(f"Reddit JSON err: {e}")
    return results

def scrape_reddit_rss(queries):
    results = []
    for keyword in queries[:4]:
        url = f"https://www.reddit.com/search.rss?q={keyword.replace(' ', '+')}&sort=relevance&t=year"
        try:
            feed = _feedparser_with_timeout(url, timeout=_FEEDPARSER_TIMEOUT)
            for entry in feed.entries[:5]:
                results.append({"content": f"[reddit/rss] {entry.title}", "source": entry.link, "raw_source": "reddit_rss", "score": 0, "comments": 0})
        except FuturesTimeoutError:
            logger.error(f"Reddit RSS feedparser timed out for query: {keyword}")
        except Exception as e: logger.error(f"Reddit RSS err: {e}")
    return results

def scrape_hackernews(queries):
    results = []
    for keyword in queries[:4]:
        url = f"https://hn.algolia.com/api/v1/search?query={keyword.replace(' ', '+')}&tags=story&hitsPerPage=5"
        try:
            r = requests.get(url, headers=HEADERS, timeout=_SOURCE_TIMEOUT)
            if r.status_code == 200:
                for hit in r.json().get("hits", []):
                    if title := hit.get("title", ""):
                        link = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}"
                        results.append({"content": f"[hackernews|pts:{hit.get('points',0)}] {title}", "source": link, "raw_source": "hackernews", "score": hit.get("points",0), "comments": hit.get("num_comments",0)})
        except Exception as e: logger.error(f"HN err: {e}")
    return results

def scan_merchants():
    signals = []
    breakdown = {"brave": 0, "reddit_json": 0, "reddit_rss": 0, "hackernews": 0}
    inserted = 0

    try:
        queries = get_diverse_queries()
        save_event("query_diversity_applied", {"queries": queries})

        order = get_source_order()
        logger.info(f"Scanning with source priority: {order}")

        scrapers = {
            "brave": (has_search_api, lambda: search_brave(queries)),
            "reddit_json": (lambda: True, lambda: scrape_reddit(queries)),
            "reddit_rss": (lambda: True, lambda: scrape_reddit_rss(queries)),
            "hackernews": (lambda: True, lambda: scrape_hackernews(queries))
        }

        for src in order:
            if scrapers[src][0]():
                t0 = time.time()
                save_event("source_scan_start", {"source": src})
                try:
                    result = scrapers[src][1]()
                    signals += result
                    breakdown[src] = len(result)
                    update_source_stats(src, len(result))
                    elapsed = (time.time() - t0) * 1000
                    save_event("source_scan_success", {"source": src, "elapsed_ms": elapsed, "signals": len(result)})
                except FuturesTimeoutError:
                    elapsed = (time.time() - t0) * 1000
                    logger.error(f"Source {src} timed out after {elapsed:.0f}ms")
                    save_event("source_scan_timeout", {"source": src, "elapsed_ms": elapsed})
                except Exception as e:
                    elapsed = (time.time() - t0) * 1000
                    logger.error(f"Source {src} failed after {elapsed:.0f}ms: {e}")
                    save_event("source_scan_error", {"source": src, "error": str(e), "elapsed_ms": elapsed})

        for sig in signals:
            try:
                enriched = f"{sig.get('content','')} [score:{sig.get('score',0)}|comments:{sig.get('comments',0)}]"
                if process_and_store_signal(sig.get('source'), enriched): inserted += 1
            except Exception as e: logger.error(f"Ingestion err: {e}")

    finally:
        result = {"signals_found": len(signals), "signals_inserted": inserted, "source_breakdown": breakdown}
        try:
            t = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            r_client.set("last_scan_time", t)
        except Exception as e:
            logger.error(f"Failed to update last_scan_time: {e}")
        save_event("merchant_scan_complete", result)
        return result

if __name__ == "__main__":
    print(json.dumps(scan_merchants(), indent=2))
