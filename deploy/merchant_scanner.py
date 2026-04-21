import sys, os, json, random, time, redis
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

def has_search_api(): return bool(os.getenv("SEARCH_API_KEY"))

def search_brave(queries):
    from tools.web_search import web_search
    results = []
    for keyword in queries[:5]:
        try:
            data = web_search(keyword)
            if isinstance(data, dict) and "error" not in data:
                for item in data.get("web", {}).get("results", [])[:3]:
                    results.append({"content": f"{item.get('title', '')} | {item.get('description', '')}", "source": f"{item.get('url', '')}#brave", "raw_source": "brave", "score": 0, "comments": 0})
        except Exception as e: logger.error(f"Brave error: {e}")
    return results

def scrape_reddit(queries):
    results = []
    for keyword in queries[:4]:
        url = f"https://www.reddit.com/search.json?q={keyword.replace(' ', '+')}&limit=10&sort=relevance&t=year"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
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
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                results.append({"content": f"[reddit/rss] {entry.title}", "source": entry.link, "raw_source": "reddit_rss", "score": 0, "comments": 0})
        except Exception as e: logger.error(f"Reddit RSS err: {e}")
    return results

def scrape_hackernews(queries):
    results = []
    for keyword in queries[:4]:
        url = f"https://hn.algolia.com/api/v1/search?query={keyword.replace(' ', '+')}&tags=story&hitsPerPage=5"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
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
        if not signals and scrapers[src][0]():
            result = scrapers[src][1]()
            signals += result
            breakdown[src] = len(result)
            update_source_stats(src, len(result))

    inserted = 0
    for sig in signals:
        enriched = f"{sig.get('content','')} [score:{sig.get('score',0)}|comments:{sig.get('comments',0)}]"
        try:
            if process_and_store_signal(sig.get('source'), enriched): inserted += 1
        except Exception as e: logger.error(f"Ingestion err: {e}")

    result = {"signals_found": len(signals), "signals_inserted": inserted, "source_breakdown": breakdown}
    
    # Update last_scan_time in Redis for reporting persistence
    try:
        t = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        r_client.set("last_scan_time", t)
    except Exception as e:
        logger.error(f"Failed to update last_scan_time: {e}")

    save_event("merchant_scan_complete", result)
    return result

if __name__ == "__main__":
    print(json.dumps(scan_merchants(), indent=2))
