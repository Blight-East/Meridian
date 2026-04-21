"""
Merchant graph expansion engine.

Discovers new merchants deterministically from relationships around already
known merchants. No LLM usage.

Relationship sources:
1. Cluster neighbors: unresolved merchant-style signals that appear inside
   clusters already containing known merchants.
2. Co-mentions: additional brands mentioned alongside a known merchant in a
   merchant distress signal, but only when the related brand resolves to a
   verified domain.
"""
import json
import os
import re
import sys

from sqlalchemy import create_engine, text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard

logger = get_logger("merchant_graph")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

GRAPH_BATCH_SIZE = 40
MIN_SEED_DISTRESS = 5
CO_MENTION_SCORE_FRACTION = 0.35

_BRAND_TOKEN = r"[A-Z][A-Za-z0-9&'\-]{1,29}"
_BRAND_PHRASE = rf"({_BRAND_TOKEN}(?:\s+{_BRAND_TOKEN}){{0,2}})"
_RELATED_BRAND_PATTERNS = [
    re.compile(
        rf"\b(?:and|also|plus|alongside|with|versus|vs\.?|like|similar to)\s+"
        rf"[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?\s+"
        rf"(?:also|too|as well)(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
]
_GRAPH_BRAND_PATTERNS = [
    re.compile(
        rf"(?:my|our|the)\s+(?:store|shop|brand|business|company|site|website)\s+"
        rf"(?:called\s+|named\s+|is\s+)?[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?"
        rf"(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"(?:froze|held|paused|locked|limited|suspended|closed|banned|terminated)\s+"
        rf"(?:my|our)\s+(?:store|shop|brand|business|company)\s+"
        rf"[\"'\u201c]?{_BRAND_PHRASE}[\"'\u201d]?(?:\s|,|\.|!|\?|$)",
        re.IGNORECASE,
    ),
]
_STOP_TOKENS = {
    "account", "accounts", "again", "amazon", "and", "bank", "banks", "brand",
    "business", "businesses", "commerce", "company", "consumer", "customers",
    "delayed", "distress", "etsy", "facebook", "frozen", "funds", "gateway",
    "gumroad", "held", "instagram", "kajabi", "locked", "merchant", "money",
    "online", "paused", "payment", "payments", "paypal", "processor",
    "processors", "reddit", "reserve", "settlement", "shop", "shopify", "site",
    "square", "store", "stripe", "substack", "suspended", "the", "trustpilot",
    "twitter", "website", "woocommerce", "x", "yelp",
}


def normalize_merchant_name(name):
    if not name:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", "", name.lower())
    return normalized


def run_merchant_graph_expansion(limit=GRAPH_BATCH_SIZE):
    """
    Expand the merchant graph from existing merchants to new related merchants.
    """
    ensure_merchant_slug_guard()
    cluster_limit = max(1, int(limit * 0.75))
    co_mention_limit = max(1, limit - cluster_limit)

    with engine.connect() as conn:
        cluster_stats = _expand_from_cluster_neighbors(conn, cluster_limit)
        co_mention_stats = _expand_from_co_mentions(conn, co_mention_limit)
        conn.commit()

    result = {
        "clusters_processed": cluster_stats["clusters_processed"],
        "cluster_candidates": cluster_stats["candidates_seen"],
        "cluster_merchants_expanded": cluster_stats["merchants_expanded"],
        "co_mention_candidates": co_mention_stats["candidates_seen"],
        "co_mention_merchants_expanded": co_mention_stats["merchants_expanded"],
        "merchant_graph_expansions": (
            cluster_stats["merchants_expanded"] + co_mention_stats["merchants_expanded"]
        ),
    }
    save_event("merchant_graph_expansion_run", result)
    logger.info(f"Merchant graph expansion complete: {result}")
    return result


def _expand_from_cluster_neighbors(conn, limit):
    try:
        from runtime.intelligence.brand_extraction import (
            extract_brand_candidates,
            is_valid_brand_candidate,
            normalize_brand,
            should_attempt_brand_extraction,
        )
    except ImportError:
        from brand_extraction import (
            extract_brand_candidates,
            is_valid_brand_candidate,
            normalize_brand,
            should_attempt_brand_extraction,
        )

    try:
        from runtime.intelligence.merchant_identity import resolve_merchant_identity
    except ImportError:
        from merchant_identity import resolve_merchant_identity

    try:
        from runtime.intelligence.merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )
    except ImportError:
        from merchant_signal_classifier import CLASS_MERCHANT_OPERATOR, classify_merchant_signal

    try:
        from runtime.intelligence.merchant_validation import is_valid_merchant_name
    except ImportError:
        from merchant_validation import is_valid_merchant_name

    clusters = conn.execute(text("""
        SELECT id, cluster_topic, signal_ids
        FROM clusters
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND cluster_size >= 2
          AND cluster_topic != 'Consumer Complaints'
        ORDER BY cluster_size DESC, created_at DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    stats = {
        "clusters_processed": 0,
        "candidates_seen": 0,
        "merchants_expanded": 0,
    }

    for cluster_id, cluster_topic, signal_ids_raw in clusters:
        signal_ids = _parse_signal_ids(signal_ids_raw)
        if len(signal_ids) < 2:
            continue

        rows = _load_cluster_signal_rows(conn, signal_ids)
        if not rows:
            continue

        seed_names = {
            normalize_merchant_name(row["canonical_name"] or row["merchant_name"])
            for row in rows
            if row["merchant_id"]
        }
        seed_ids = {row["merchant_id"] for row in rows if row["merchant_id"]}
        if not seed_ids:
            continue

        stats["clusters_processed"] += 1

        for row in rows:
            if row["merchant_id"]:
                continue

            content = row["content"] or ""
            if classify_merchant_signal(content)["classification"] != CLASS_MERCHANT_OPERATOR:
                continue
            if not should_attempt_brand_extraction(content):
                continue

            candidates = extract_brand_candidates(content, merchant_name=row["merchant_name"])
            if not candidates:
                candidates = _fallback_graph_brand_candidates(content)
            for candidate in candidates:
                brand = normalize_brand(candidate["brand"])
                brand_key = normalize_merchant_name(brand)
                if not _is_viable_graph_brand(brand, seed_names, content=content):
                    continue
                if candidate["confidence"] < 0.65:
                    continue

                stats["candidates_seen"] += 1
                existing_id = _find_merchant_by_name(conn, brand)
                if not is_valid_merchant_name(brand):
                    if existing_id is None:
                        save_event("merchant_validation_rejected", {"name": brand})
                    continue

                merchant_id = resolve_merchant_identity(
                    row["signal_id"],
                    content,
                    row["priority_score"] or 0,
                    merchant_name=brand,
                )
                if not merchant_id or merchant_id in seed_ids:
                    continue

                created = existing_id is None
                if created:
                    stats["merchants_expanded"] += 1

                save_event("merchant_graph_expanded", {
                    "relationship_type": "cluster_neighbor",
                    "cluster_id": str(cluster_id),
                    "cluster_topic": cluster_topic,
                    "signal_id": row["signal_id"],
                    "seed_merchant_ids": sorted(str(mid) for mid in seed_ids),
                    "merchant_id": merchant_id,
                    "brand": brand,
                    "created": created,
                })
                break

    return stats


def _expand_from_co_mentions(conn, limit):
    try:
        from runtime.intelligence.brand_extraction import is_valid_brand_candidate, normalize_brand
    except ImportError:
        from brand_extraction import is_valid_brand_candidate, normalize_brand

    try:
        from runtime.intelligence.domain_discovery import (
            discover_domain_for_brand,
            normalize_discovered_domain,
        )
    except ImportError:
        from domain_discovery import discover_domain_for_brand, normalize_discovered_domain

    try:
        from runtime.intelligence.merchant_attribution import detect_platform
    except ImportError:
        from merchant_attribution import detect_platform

    try:
        from runtime.intelligence.merchant_identity import enqueue_contact_discovery
    except ImportError:
        from merchant_identity import enqueue_contact_discovery

    try:
        from runtime.intelligence.merchant_signal_classifier import (
            CLASS_MERCHANT_OPERATOR,
            classify_merchant_signal,
        )
    except ImportError:
        from merchant_signal_classifier import CLASS_MERCHANT_OPERATOR, classify_merchant_signal

    try:
        from runtime.intelligence.merchant_validation import is_valid_merchant_name
    except ImportError:
        from merchant_validation import is_valid_merchant_name

    rows = conn.execute(text("""
        SELECT s.id, s.content, s.source, s.merchant_name,
               m.id, m.canonical_name, m.distress_score
        FROM signals s
        JOIN merchants m ON s.merchant_id = m.id
        WHERE s.detected_at >= NOW() - INTERVAL '30 days'
          AND m.distress_score >= :min_seed_distress
          AND s.classification = 'merchant_distress'
        ORDER BY m.distress_score DESC, s.detected_at DESC
        LIMIT :limit
    """), {"min_seed_distress": MIN_SEED_DISTRESS, "limit": limit}).fetchall()

    stats = {
        "candidates_seen": 0,
        "merchants_expanded": 0,
    }

    for signal_id, content, source, signal_name, seed_merchant_id, seed_name, seed_distress in rows:
        if classify_merchant_signal(content or "")["classification"] != CLASS_MERCHANT_OPERATOR:
            continue

        seed_keys = {
            normalize_merchant_name(seed_name),
            normalize_merchant_name(signal_name),
        }
        for raw_candidate in _extract_related_brand_mentions(content or ""):
            cleaned = normalize_brand(raw_candidate)
            if not _is_viable_graph_brand(cleaned, seed_keys, content=content):
                continue

            stats["candidates_seen"] += 1
            if _find_merchant_by_name(conn, cleaned):
                continue

            if not is_valid_merchant_name(cleaned):
                save_event("merchant_validation_rejected", {"name": cleaned})
                continue

            discovery = discover_domain_for_brand(cleaned, detect_platform(content or ""))
            if not discovery:
                continue

            conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS validation_source TEXT DEFAULT 'manual'"))
            conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS confidence_score INTEGER DEFAULT 0"))

            conf_score = compute_merchant_confidence(
                domain=discovery["domain"],
                processor_mentions=0,
                contact_email=None,
                signal_count=1
            )
            save_event("merchant_confidence_calculated", {"merchant": cleaned, "score": conf_score})

            merchant_id = conn.execute(text("""
                INSERT INTO merchants (
                    canonical_name, slug, domain, normalized_domain, distress_score,
                    detected_from, domain_confidence, validation_source, confidence_score, first_seen, last_seen
                ) VALUES (
                    :name, :slug, :domain, :normalized_domain, :distress_score,
                    :detected_from, :domain_confidence, :val_source, :conf_score, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (slug) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    domain = COALESCE(NULLIF(merchants.domain, ''), EXCLUDED.domain),
                    normalized_domain = COALESCE(NULLIF(merchants.normalized_domain, ''), EXCLUDED.normalized_domain),
                    distress_score = GREATEST(merchants.distress_score, EXCLUDED.distress_score),
                    validation_source = COALESCE(NULLIF(merchants.validation_source, ''), EXCLUDED.validation_source),
                    confidence_score = GREATEST(merchants.confidence_score, EXCLUDED.confidence_score),
                    last_seen = CURRENT_TIMESTAMP
                RETURNING id
            """), {
                "name": cleaned,
                "slug": build_merchant_slug(cleaned),
                "domain": discovery["domain"],
                "normalized_domain": normalize_discovered_domain(discovery["domain"]),
                "distress_score": max(2.0, min((seed_distress or 0) * CO_MENTION_SCORE_FRACTION, 12.0)),
                "detected_from": "merchant_graph_co_mention",
                "domain_confidence": "confirmed" if discovery["confidence"] >= 0.8 else "provisional",
                "val_source": "domain_extraction",
                "conf_score": conf_score,
            }).scalar_one()

            enqueue_contact_discovery(merchant_id)
            stats["merchants_expanded"] += 1
            save_event("merchant_graph_expanded", {
                "relationship_type": "co_mention",
                "signal_id": signal_id,
                "seed_merchant_id": seed_merchant_id,
                "merchant_id": merchant_id,
                "brand": cleaned,
                "domain": discovery["domain"],
                "created": True,
                "source_signal": source,
            })
            break

    return stats


def _parse_signal_ids(signal_ids_raw):
    if not signal_ids_raw:
        return []
    try:
        parsed = json.loads(signal_ids_raw) if isinstance(signal_ids_raw, str) else signal_ids_raw
    except (TypeError, ValueError):
        return []

    signal_ids = []
    for item in parsed or []:
        try:
            signal_ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return signal_ids


def _load_cluster_signal_rows(conn, signal_ids):
    if not signal_ids:
        return []

    placeholders = ",".join(str(int(signal_id)) for signal_id in signal_ids[:200])
    rows = conn.execute(text(f"""
        SELECT s.id, s.content, s.priority_score, s.merchant_name, s.merchant_id,
               m.canonical_name
        FROM signals s
        LEFT JOIN merchants m ON s.merchant_id = m.id
        WHERE s.id IN ({placeholders})
        ORDER BY s.detected_at DESC
    """)).fetchall()

    return [{
        "signal_id": row[0],
        "content": row[1],
        "priority_score": row[2],
        "merchant_name": row[3],
        "merchant_id": row[4],
        "canonical_name": row[5],
    } for row in rows]


def _find_merchant_by_name(conn, brand):
    if not brand:
        return None
    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE LOWER(canonical_name) = LOWER(:brand)
        LIMIT 1
    """), {"brand": brand}).fetchone()
    return row[0] if row else None


def _extract_related_brand_mentions(content):
    candidates = []
    seen = set()

    for pattern in _RELATED_BRAND_PATTERNS:
        for match in pattern.findall(content):
            raw = match[-1] if isinstance(match, tuple) else match
            normalized = raw.strip() if raw else ""
            if normalized and normalized.lower() not in seen:
                seen.add(normalized.lower())
                candidates.append(normalized)

    return candidates


def _fallback_graph_brand_candidates(content):
    try:
        from runtime.intelligence.brand_extraction import is_valid_brand_candidate, normalize_brand
    except ImportError:
        from brand_extraction import is_valid_brand_candidate, normalize_brand

    candidates = []
    seen = set()
    for pattern in _GRAPH_BRAND_PATTERNS:
        for match in pattern.findall(content or ""):
            raw = match[-1] if isinstance(match, tuple) else match
            cleaned = normalize_brand((raw or "").strip())
            key = normalize_merchant_name(cleaned)
            if cleaned and key and key not in seen and is_valid_brand_candidate(cleaned, text=content, source="graph_fallback"):
                seen.add(key)
                candidates.append({
                    "brand": cleaned,
                    "confidence": 0.72,
                    "source": "graph_fallback",
                })
    return candidates


def _is_viable_graph_brand(brand, seed_keys, content=None):
    try:
        from runtime.intelligence.brand_extraction import is_valid_brand_candidate
    except ImportError:
        from brand_extraction import is_valid_brand_candidate

    if not brand:
        return False

    key = normalize_merchant_name(brand)
    if not key or key in seed_keys:
        return False
    if not is_valid_brand_candidate(brand, text=content, source="graph"):
        return False

    tokens = re.findall(r"[a-z0-9]+", brand.lower())
    if not tokens or len("".join(tokens)) < 4:
        return False

    if all(token in _STOP_TOKENS for token in tokens):
        return False
    if tokens[0] in {"my", "our", "the"}:
        return False

    return True


if __name__ == "__main__":
    run_merchant_graph_expansion()
