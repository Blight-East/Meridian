"""
Controlled merchant graph propagation.

Discovers adjacent merchants from high-quality seed merchants without allowing
runaway graph expansion.
"""
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
from runtime.intelligence.domain_discovery import normalize_discovered_domain
from runtime.intelligence.merchant_graph import _init_relationships_table
from runtime.intelligence.merchant_graph_store import (
    init_merchant_graph_tables,
    sync_merchant_entity,
)
from runtime.intelligence.merchant_quality import invalid_merchant_name_reason, is_valid_domain, is_reverse_dns
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard
from tools.web_search import web_search

logger = get_logger("merchant_propagation")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_SEEDS_PER_CYCLE = 20
MAX_NEW_MERCHANTS_PER_SEED = 5
MAX_TOTAL_NEW_MERCHANTS_PER_RUN = 50
MAX_URLS_PER_QUERY = 8
MAX_PAGES_PER_QUERY = 3
RUN_TIME_BUDGET_SECONDS = 60
SEARCH_TIMEOUT = 4
FETCH_TIMEOUT = 4
SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}
BLOCKED_DOMAINS = {
    "adyen.com",
    "apple.com",
    "answers.com",
    "amazon.com",
    "bbb.org",
    "bing.com",
    "collinsdictionary.com",
    "dictionary.cambridge.org",
    "facebook.com",
    "google.com",
    "inc.com",
    "instagram.com",
    "linkedin.com",
    "merriam-webster.com",
    "medium.com",
    "news.ycombinator.com",
    "paypal.com",
    "pinterest.com",
    "reddit.com",
    "shopify.com",
    "squareup.com",
    "startupschool.org",
    "stripe.com",
    "substack.com",
    "techcrunch.com",
    "thefreedictionary.com",
    "trustpilot.com",
    "twitter.com",
    "wikipedia.org",
    "wordreference.com",
    "x.com",
    "yellowpages.com",
    "youtube.com",
}
DOMAIN_PATTERN = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}\b", re.IGNORECASE)
MIN_DISTRESS_FOR_PROPAGATION = 20


def _init_schema():
    ensure_merchant_slug_guard()
    with engine.connect() as conn:
        init_merchant_graph_tables(conn)
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS detected_from TEXT
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS confidence_score INTEGER DEFAULT 0
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'
        """))
        _init_relationships_table(conn)
        conn.commit()


def _root_domain(domain):
    host = _normalize_host(domain)
    labels = host.split(".")
    if len(labels) < 2:
        return host
    return ".".join(labels[-2:])


def _seed_rows(conn, limit):
    rows = conn.execute(text("""
        SELECT
            id,
            canonical_name,
            COALESCE(NULLIF(normalized_domain, ''), NULLIF(domain, '')) AS domain,
            COALESCE(signal_count, 0) AS signal_count,
            COALESCE(risk_score, 0) AS risk_score,
            COALESCE(opportunity_score, 0) AS opportunity_score
        FROM merchants
        WHERE COALESCE(NULLIF(normalized_domain, ''), NULLIF(domain, '')) IS NOT NULL
          AND COALESCE(signal_count, 0) >= 2
          AND COALESCE(distress_score, 0) >= :min_distress
        ORDER BY COALESCE(opportunity_score, 0) DESC,
                 COALESCE(signal_count, 0) DESC,
                 COALESCE(risk_score, 0) DESC
        LIMIT :limit
    """), {
        "limit": limit * 3,
        "min_distress": MIN_DISTRESS_FOR_PROPAGATION,
    }).fetchall()

    seeds = []
    for row in rows:
        merchant_id, canonical_name, domain, signal_count, risk_score, opportunity_score = row
        if invalid_merchant_name_reason(canonical_name):
            continue
        if not is_valid_domain(domain):
            continue
        host = _normalize_host(domain)
        if not host or _is_blocked_domain(host):
            continue
        seeds.append({
            "merchant_id": merchant_id,
            "canonical_name": canonical_name,
            "domain": host,
            "signal_count": int(signal_count or 0),
            "risk_score": float(risk_score or 0),
            "opportunity_score": float(opportunity_score or 0),
        })
        if len(seeds) >= limit:
            break
    return seeds


def _normalize_host(domain):
    normalized = normalize_discovered_domain(domain or "") or ""
    host = normalized.split("/")[0].lower().strip().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_blocked_domain(domain):
    host = _normalize_host(domain)
    return not host or any(host == blocked or host.endswith(f".{blocked}") for blocked in BLOCKED_DOMAINS)


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
            import base64
            return base64.b64decode(encoded + padding).decode()
        except Exception:
            pass
        try:
            import base64
            return base64.urlsafe_b64decode(encoded + padding).decode()
        except Exception:
            continue
    return href


def _search_urls(query, max_results=MAX_URLS_PER_QUERY):
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
        logger.warning(f"Propagation search failed for '{query}': {exc}")

    if urls:
        return urls[:max_results]

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
    except Exception as exc:
        logger.warning(f"Propagation fallback search failed for '{query}': {exc}")
    return urls[:max_results]


def _extract_domains_from_urls(urls):
    candidates = []
    seen = set()
    for url in urls:
        parsed = urlparse(url if "://" in (url or "") else f"https://{url}")
        host = parsed.netloc or ""
        if not host:
            match = DOMAIN_PATTERN.search(url or "")
            if match:
                host = match.group(0)
        host = _normalize_host(host)
        if not is_valid_domain(host):
            continue
        if _is_blocked_domain(host):
            continue
        if host in seen:
            continue
        seen.add(host)
        candidates.append(host)
    return candidates


def _extract_domains_from_page(url):
    domains = []
    try:
        response = requests.get(url, headers=SEARCH_HEADERS, timeout=FETCH_TIMEOUT)
        if response.status_code != 200 or not response.text:
            return domains
        soup = BeautifulSoup(response.text, "html.parser")
        candidates = []
        for anchor in soup.select("a[href]"):
            href = anchor.get("href") or ""
            if href:
                candidates.append(href)
        text_candidates = DOMAIN_PATTERN.findall(soup.get_text(" ", strip=True))
        candidates.extend(text_candidates)
        domains = _extract_domains_from_urls(candidates)
    except Exception as exc:
        logger.debug(f"Propagation page parse failed for {url}: {exc}")
    return domains


def _strategy_queries(seed_name):
    return [
        ("competitor", f"\"{seed_name}\" competitors"),
        ("competitor", f"companies like \"{seed_name}\""),
        ("competitor", f"alternatives to \"{seed_name}\""),
        ("related", f"\"{seed_name}\" payments"),
        ("related", f"\"{seed_name}\" payment processor"),
        ("adjacent_market", f"\"{seed_name}\" partner brands"),
    ]


def _domain_exists(conn, domain):
    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE normalized_domain = :domain OR domain = :domain
        LIMIT 1
    """), {"domain": domain}).fetchone()
    return row[0] if row else None


def _domain_to_name(domain):
    label = _normalize_host(domain).split(".")[0]
    return " ".join(part.capitalize() for part in re.split(r"[-_]+", label) if part)


def _insert_propagated_merchant(conn, domain):
    canonical_name = _domain_to_name(domain)
    normalized_domain = _normalize_host(domain)
    merchant_slug = _root_domain(domain).split(".")[0].lower() or build_merchant_slug(canonical_name)
    existing = conn.execute(text("""
        SELECT id, canonical_name
        FROM merchants
        WHERE normalized_domain = :normalized_domain
           OR domain = :normalized_domain
           OR slug = :slug
           OR LOWER(canonical_name) = LOWER(:canonical_name)
        LIMIT 1
    """), {
        "normalized_domain": normalized_domain,
        "slug": merchant_slug,
        "canonical_name": canonical_name,
    }).fetchone()
    if existing:
        conn.execute(text("""
            UPDATE merchants
            SET domain = COALESCE(NULLIF(domain, ''), :domain),
                normalized_domain = COALESCE(NULLIF(normalized_domain, ''), :normalized_domain),
                last_seen = NOW()
            WHERE id = :merchant_id
        """), {
            "merchant_id": existing[0],
            "domain": domain,
            "normalized_domain": normalized_domain,
        })
        sync_merchant_entity(conn, existing[0])
        return existing[0], existing[1]

    row = conn.execute(text("""
        INSERT INTO merchants (
            canonical_name,
            domain,
            normalized_domain,
            slug,
            status,
            confidence_score,
            signal_count,
            risk_score,
            detected_from,
            created_at,
            last_seen
        ) VALUES (
            :canonical_name,
            :domain,
            :normalized_domain,
            :slug,
            'provisional',
            40,
            0,
            0,
            'propagation',
            NOW(),
            NOW()
        )
        ON CONFLICT (slug) DO UPDATE SET
            domain = COALESCE(NULLIF(merchants.domain, ''), EXCLUDED.domain),
            normalized_domain = COALESCE(NULLIF(merchants.normalized_domain, ''), EXCLUDED.normalized_domain),
            last_seen = NOW()
        RETURNING id, canonical_name
    """), {
        "canonical_name": canonical_name,
        "domain": domain,
        "normalized_domain": normalized_domain,
        "slug": merchant_slug,
    }).fetchone()
    merchant_id, merchant_name = row[0], row[1]
    sync_merchant_entity(conn, merchant_id)
    return merchant_id, merchant_name


def _store_edge(conn, merchant_id, neighbor_id, relationship_type):
    row = conn.execute(text("""
        INSERT INTO merchant_relationships (
            merchant_id,
            neighbor_id,
            related_merchant_id,
            relationship_type,
            confidence,
            created_at
        ) VALUES (
            :merchant_id,
            :neighbor_id,
            :neighbor_id,
            :relationship_type,
            0.6,
            NOW()
        )
        ON CONFLICT (merchant_id, neighbor_id, relationship_type) DO NOTHING
        RETURNING id
    """), {
        "merchant_id": merchant_id,
        "neighbor_id": neighbor_id,
        "relationship_type": relationship_type,
    }).fetchone()
    return bool(row)


def propagate_merchant_graph():
    _init_schema()
    started_at = time.monotonic()
    stats = {
        "seeds_processed": 0,
        "merchants_created": 0,
        "relationships_created": 0,
    }

    with engine.connect() as conn:
        seeds = _seed_rows(conn, MAX_SEEDS_PER_CYCLE)
        for seed in seeds:
            if stats["merchants_created"] >= MAX_TOTAL_NEW_MERCHANTS_PER_RUN:
                break
            if time.monotonic() - started_at >= RUN_TIME_BUDGET_SECONDS:
                break

            stats["seeds_processed"] += 1
            created_for_seed = 0
            seed_domain = _normalize_host(seed["domain"])
            seed_root = _root_domain(seed_domain)
            seen_domains = {seed_domain}

            for relationship_type, query in _strategy_queries(seed["canonical_name"]):
                if created_for_seed >= MAX_NEW_MERCHANTS_PER_SEED:
                    break
                if stats["merchants_created"] >= MAX_TOTAL_NEW_MERCHANTS_PER_RUN:
                    break
                if time.monotonic() - started_at >= RUN_TIME_BUDGET_SECONDS:
                    break

                urls = _search_urls(query)
                candidate_domains = _extract_domains_from_urls(urls)
                for page_url in urls[:MAX_PAGES_PER_QUERY]:
                    candidate_domains.extend(_extract_domains_from_page(page_url))

                for domain in candidate_domains:
                    normalized_domain = _normalize_host(domain)
                    if not is_valid_domain(normalized_domain):
                        continue
                    if is_reverse_dns(normalized_domain):
                        continue
                    if _is_blocked_domain(normalized_domain):
                        continue
                    if _root_domain(normalized_domain) == seed_root:
                        continue
                    if normalized_domain in seen_domains:
                        continue
                    seen_domains.add(normalized_domain)

                    existing_id = _domain_exists(conn, normalized_domain)
                    if existing_id:
                        if existing_id != seed["merchant_id"] and _store_edge(conn, seed["merchant_id"], existing_id, relationship_type):
                            stats["relationships_created"] += 1
                            save_event("merchant_edge_created", {
                                "seed_id": seed["merchant_id"],
                                "neighbor_id": existing_id,
                                "relationship_type": relationship_type,
                                "domain": normalized_domain,
                            })
                        continue

                    merchant_id, merchant_name = _insert_propagated_merchant(conn, normalized_domain)
                    stats["merchants_created"] += 1
                    created_for_seed += 1
                    save_event("merchant_discovered", {
                        "seed": seed["canonical_name"],
                        "seed_id": seed["merchant_id"],
                        "merchant": merchant_name,
                        "merchant_id": merchant_id,
                        "domain": normalized_domain,
                        "relationship_type": relationship_type,
                    })

                    if _store_edge(conn, seed["merchant_id"], merchant_id, relationship_type):
                        stats["relationships_created"] += 1
                        save_event("merchant_edge_created", {
                            "seed_id": seed["merchant_id"],
                            "neighbor_id": merchant_id,
                            "relationship_type": relationship_type,
                            "domain": normalized_domain,
                        })

                    if created_for_seed >= MAX_NEW_MERCHANTS_PER_SEED:
                        break
                    if stats["merchants_created"] >= MAX_TOTAL_NEW_MERCHANTS_PER_RUN:
                        break

        conn.commit()

    save_event("merchant_propagation_run", stats)
    logger.info(f"Merchant propagation complete: {stats}")
    return stats


if __name__ == "__main__":
    print(propagate_merchant_graph())
