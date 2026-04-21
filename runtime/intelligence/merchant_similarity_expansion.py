"""
Expand the merchant graph by discovering domains similar to high-value seeds.
"""
import base64
import os
import re
import sys
import time
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.merchant_canonicalization import check_and_log_canonical_match
from runtime.intelligence.merchant_graph_store import (
    init_merchant_graph_tables,
    sync_merchant_entity,
)
from runtime.intelligence.merchant_quality import is_valid_domain
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard
from tools.web_search import web_search

try:
    from runtime.intelligence.domain_discovery import normalize_discovered_domain
except ImportError:
    from domain_discovery import normalize_discovered_domain

try:
    from runtime.intelligence.merchant_graph import _init_relationships_table, _store_relationship
except ImportError:
    from merchant_graph import _init_relationships_table, _store_relationship

logger = get_logger("merchant_similarity_expansion")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_NEW_MERCHANTS_PER_RUN = 50
MAX_QUERIES_PER_SEED = 5
RUN_TIME_BUDGET_SECONDS = 45
SEARCH_TIMEOUT = 4
BLOCKED_DOMAINS = {
    "adyen.com",
    "amazon.com",
    "bing.com",
    "ebay.com",
    "etsy.com",
    "facebook.com",
    "instagram.com",
    "paypal.com",
    "reddit.com",
    "shopify.com",
    "squareup.com",
    "stripe.com",
    "trustpilot.com",
    "x.com",
    "youtube.com",
}
GENERIC_DOMAIN_WORDS = {
    "app",
    "brand",
    "company",
    "commerce",
    "health",
    "merchant",
    "naturals",
    "official",
    "online",
    "payments",
    "shop",
    "store",
    "supplement",
    "supplements",
    "vitamin",
    "vitamins",
}
BLOCKED_SEED_WORDS = GENERIC_DOMAIN_WORDS | {
    "account",
    "bank",
    "capital",
    "delay",
    "delays",
    "digital",
    "facial",
    "humans",
    "merchant",
    "offers",
    "platform",
    "platforms",
    "recognition",
    "review",
    "risk",
    "service",
    "services",
    "stripe",
    "unknown",
    "verification",
}
SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}
DOMAIN_PATTERN = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}\b", re.IGNORECASE)


def _init_schema():
    ensure_merchant_slug_guard()
    with engine.connect() as conn:
        init_merchant_graph_tables(conn)
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS confidence_score INTEGER DEFAULT 0
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS detected_from TEXT
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS normalized_domain TEXT
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS domain_confidence TEXT
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT NOW()
        """))
        conn.commit()
        _init_relationships_table(conn)


def _seed_rows(conn, limit):
    rows = conn.execute(text("""
        SELECT id, canonical_name, domain
        FROM merchants
        WHERE COALESCE(opportunity_score, 0) > 50
        ORDER BY COALESCE(opportunity_score, 0) DESC, COALESCE(distress_score, 0) DESC
        LIMIT :limit
    """), {"limit": limit * 4}).fetchall()
    return [row for row in rows if _is_viable_seed(row[1], row[2])][:limit]


def _neighbor_rows(conn, merchant_id):
    return conn.execute(text("""
        SELECT m.id, m.canonical_name, m.domain
        FROM merchant_relationships r
        JOIN merchants m ON m.id = CASE
            WHEN r.merchant_id = :merchant_id THEN r.neighbor_id
            ELSE r.merchant_id
        END
        WHERE r.merchant_id = :merchant_id OR r.neighbor_id = :merchant_id
        LIMIT 10
    """), {"merchant_id": merchant_id}).fetchall()


def _direct_search_urls(query, max_results=10):
    urls = []
    try:
        response = requests.get(
            f"https://www.bing.com/search?q={quote_plus(query)}",
            headers=SEARCH_HEADERS,
            timeout=SEARCH_TIMEOUT,
        )
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            for anchor in soup.select("li.b_algo h2 a"):
                href = _decode_bing_url(anchor.get("href") or "")
                if href.startswith("http"):
                    urls.append(href)
                if len(urls) >= max_results:
                    break
        if urls:
            return urls[:max_results]

        response = requests.get(
            f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
            headers=SEARCH_HEADERS,
            timeout=SEARCH_TIMEOUT,
        )
        if response.status_code != 200:
            return []
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("a.result__a"):
            href = anchor.get("href") or ""
            if "uddg=" in href:
                parsed = urlparse(href)
                href = parse_qs(parsed.query).get("uddg", [href])[0]
            href = unquote(href)
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("http"):
                urls.append(href)
            if len(urls) >= max_results:
                break
    except Exception as exc:
        logger.warning(f"Similarity fallback search failed for '{query}': {exc}")
    return urls


def _decode_bing_url(href):
    if not href:
        return ""
    if href.startswith("http") and "bing.com/ck/" not in href:
        return href

    parsed = urlparse(href)
    encoded = parse_qs(parsed.query).get("u", [""])[0]
    if not encoded:
        return href

    if encoded.startswith("a1"):
        encoded = encoded[2:]
    elif encoded.startswith("a"):
        encoded = encoded[1:]

    for padding in ("", "=", "==", "==="):
        try:
            return base64.b64decode(encoded + padding).decode()
        except Exception:
            pass
        try:
            return base64.urlsafe_b64decode(encoded + padding).decode()
        except Exception:
            continue
    return href


def _search_urls(query, max_results=10):
    urls = []
    try:
        results = web_search(query, max_results=max_results)
        items = []
        if isinstance(results, dict):
            web_results = results.get("web", {})
            if isinstance(web_results, dict):
                items = web_results.get("results", []) or []
            elif isinstance(results.get("results"), list):
                items = results["results"]
        for item in items:
            if isinstance(item, dict) and item.get("url"):
                urls.append(item["url"])
    except Exception as exc:
        logger.warning(f"Configured search failed for '{query}': {exc}")

    if urls:
        return urls[:max_results]
    return _direct_search_urls(query, max_results=max_results)


def _domain_keywords(seed_name, domain):
    tokens = set()
    for source in (seed_name or "", domain or ""):
        for token in re.findall(r"[A-Za-z]{4,}", source.lower()):
            if token not in GENERIC_DOMAIN_WORDS:
                tokens.add(token)
    return sorted(tokens)[:2]


def _is_viable_seed(seed_name, domain):
    if not seed_name:
        return False

    host = (normalize_discovered_domain(domain or "") or "").split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return False
    if any(host == blocked or host.endswith(f".{blocked}") for blocked in BLOCKED_DOMAINS):
        return False

    lowered = seed_name.lower().strip()
    if lowered.startswith("unknown "):
        return False

    tokens = re.findall(r"[a-z]{3,}", lowered)
    if not tokens:
        return False
    if len(tokens) < 2:
        return False
    if any(token in BLOCKED_SEED_WORDS for token in tokens):
        return False

    meaningful = [token for token in tokens if token not in BLOCKED_SEED_WORDS]
    if not meaningful:
        return False

    host_tokens = [
        token for token in re.findall(r"[a-z]{3,}", host.lower())
        if token not in GENERIC_DOMAIN_WORDS and token not in BLOCKED_SEED_WORDS
    ]
    if not host_tokens:
        return False

    return len("".join(meaningful)) >= 6 and len("".join(host_tokens)) >= 5


def _search_queries_for_seed(seed_name, domain, neighbor_names):
    queries = [
        f"\"{seed_name}\" competitors",
        f"\"{seed_name}\" alternatives",
        f"\"{seed_name}\" brands",
    ]
    lower_name = (seed_name or "").lower()
    if any(token in lower_name for token in ("supplement", "vitamin", "health", "nutrition", "natural")):
        queries.append(f"\"{seed_name}\" supplements brand")

    for keyword in _domain_keywords(seed_name, domain):
        queries.extend([
            f"site:.com \"{keyword} store\"",
            f"site:.com \"{keyword} shop\"",
            f"site:.com \"{keyword} brand\"",
        ])

    for neighbor_name in neighbor_names[:1]:
        queries.append(f"\"{neighbor_name}\" competitors")

    deduped = []
    seen = set()
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped[:MAX_QUERIES_PER_SEED]


def _extract_domain_candidates(urls):
    domains = []
    seen = set()
    for url in urls:
        raw_url = (url or "").strip()
        if not raw_url:
            continue

        host = ""
        parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
        if parsed.netloc:
            host = parsed.netloc
        else:
            match = DOMAIN_PATTERN.search(raw_url)
            if match:
                host = match.group(0)

        host = host.lower().strip().strip(".")
        if host.startswith("www."):
            host = host[4:]
        if ":" in host:
            host = host.split(":", 1)[0]
        if not host or "." not in host:
            continue
        if host.endswith((".html", ".htm", ".php", ".aspx", ".jsp")):
            continue
        if not is_valid_domain(host):
            continue
        if any(host == blocked or host.endswith(f".{blocked}") for blocked in BLOCKED_DOMAINS):
            continue

        normalized = normalize_discovered_domain(host)
        candidate = (normalized or "").split("/")[0].lower().strip(".")
        if not candidate or "." not in candidate:
            continue
        if candidate.endswith((".html", ".htm", ".php", ".aspx", ".jsp")):
            continue
        if not is_valid_domain(candidate):
            continue
        if any(candidate == blocked or candidate.endswith(f".{blocked}") for blocked in BLOCKED_DOMAINS):
            continue
        if candidate in seen:
            continue

        seen.add(candidate)
        domains.append(candidate)
    return domains


def _domain_exists(conn, domain):
    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE domain = :domain OR normalized_domain = :domain
        LIMIT 1
    """), {"domain": domain}).fetchone()
    return row[0] if row else None


def _domain_to_name(domain):
    host = (domain or "").split("/")[0]
    label = host.split(".")[0]
    parts = [part for part in re.split(r"[-_]+", label) if part]
    if not parts:
        return label
    return " ".join(part.capitalize() for part in parts)


def _insert_provisional_domain_merchant(conn, domain):
    merchant_name = _domain_to_name(domain)
    merchant_id = conn.execute(text("""
        INSERT INTO merchants (
            canonical_name,
            domain,
            normalized_domain,
            slug,
            status,
            confidence_score,
            detected_from,
            domain_confidence,
            created_at,
            last_seen
        ) VALUES (
            :name,
            :domain,
            :normalized_domain,
            :slug,
            'provisional',
            45,
            'merchant_similarity_expansion',
            'provisional',
            NOW(),
            NOW()
        )
        ON CONFLICT (slug) DO UPDATE SET
            domain = COALESCE(NULLIF(merchants.domain, ''), EXCLUDED.domain),
            normalized_domain = COALESCE(NULLIF(merchants.normalized_domain, ''), EXCLUDED.normalized_domain),
            last_seen = NOW()
        RETURNING id, canonical_name
    """), {
        "name": merchant_name,
        "domain": domain,
        "normalized_domain": normalize_discovered_domain(domain),
        "slug": build_merchant_slug(merchant_name),
    }).fetchone()
    merchant_id, merchant_name = row_to_tuple(merchant_id)
    if merchant_id:
        sync_merchant_entity(conn, merchant_id)
    return merchant_id, merchant_name


def row_to_tuple(row):
    if not row:
        return None, None
    return row[0], row[1]


def expand_similar_merchants(limit=30):
    _init_schema()
    started_at = time.monotonic()

    stats = {
        "seeds_processed": 0,
        "merchants_created": 0,
        "duplicates_skipped": 0,
    }

    with engine.connect() as conn:
        seeds = _seed_rows(conn, limit)

        for seed_id, seed_name, seed_domain in seeds:
            if stats["merchants_created"] >= MAX_NEW_MERCHANTS_PER_RUN:
                break
            if time.monotonic() - started_at >= RUN_TIME_BUDGET_SECONDS:
                logger.info("Merchant similarity expansion stopped after hitting runtime budget")
                break

            stats["seeds_processed"] += 1
            neighbors = _neighbor_rows(conn, seed_id)
            neighbor_names = [row[1] for row in neighbors if row[1]]

            for query in _search_queries_for_seed(seed_name, seed_domain, neighbor_names):
                if stats["merchants_created"] >= MAX_NEW_MERCHANTS_PER_RUN:
                    break
                if time.monotonic() - started_at >= RUN_TIME_BUDGET_SECONDS:
                    break

                domains = _extract_domain_candidates(_search_urls(query))
                for domain in domains:
                    if stats["merchants_created"] >= MAX_NEW_MERCHANTS_PER_RUN:
                        break
                    if time.monotonic() - started_at >= RUN_TIME_BUDGET_SECONDS:
                        break

                    if _domain_exists(conn, domain):
                        stats["duplicates_skipped"] += 1
                        continue

                    candidate_name = _domain_to_name(domain)
                    if check_and_log_canonical_match(conn, candidate_name, domain=domain):
                        stats["duplicates_skipped"] += 1
                        continue

                    merchant_id, merchant_name = _insert_provisional_domain_merchant(conn, domain)
                    if not merchant_id:
                        stats["duplicates_skipped"] += 1
                        continue

                    _store_relationship(conn, seed_id, merchant_id, "similarity", 0.65)
                    sync_merchant_entity(conn, seed_id)
                    sync_merchant_entity(conn, merchant_id)
                    stats["merchants_created"] += 1
                    save_event("merchant_similarity_expanded", {
                        "seed": seed_name,
                        "seed_id": seed_id,
                        "merchant": merchant_name,
                        "merchant_id": merchant_id,
                        "domain": domain,
                    })

        conn.commit()

    save_event("merchant_similarity_expansion_run", stats)
    logger.info(f"Merchant similarity expansion complete: {stats}")
    return stats


if __name__ == "__main__":
    print(expand_similar_merchants())
