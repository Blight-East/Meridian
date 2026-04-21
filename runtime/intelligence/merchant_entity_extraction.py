"""
Extract merchant entities directly from raw unattributed signals.

This fills the gap between raw signal ingestion and merchant creation when a
signal references a merchant by name but does not include a domain.
"""
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
from runtime.intelligence.brand_extraction import (
    extract_brand_candidates,
    normalize_brand,
    should_attempt_brand_extraction,
)
from runtime.intelligence.merchant_canonicalization import (
    canonicalize_merchant_name,
    check_and_log_canonical_match,
)
from runtime.intelligence.merchant_graph_store import (
    init_merchant_graph_tables,
    link_signal_to_merchant,
    sync_merchant_entity,
)
from runtime.intelligence.merchant_quality import invalid_merchant_name_reason
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard
from runtime.intelligence.merchant_validation import is_valid_merchant_name

logger = get_logger("merchant_entity_extraction")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

MAX_MERCHANTS_PER_RUN = 50

PROCESSOR_KEYWORDS = {
    "adyen",
    "amazon",
    "bank",
    "braintree",
    "ebay",
    "etsy",
    "gumroad",
    "merchant",
    "paypal",
    "processor",
    "shopify",
    "square",
    "stripe",
    "substack",
    "wix",
    "woocommerce",
    "worldpay",
}
BLOCKED_CANDIDATE_PHRASES = {
    "ask hn",
    "hacker news",
}
BLOCKED_CANDIDATE_TOKENS = {
    "comment",
    "community",
    "forum",
    "forums",
    "hn",
    "news",
    "reddit",
    "support",
    "trustpilot",
    "twitter",
    "user",
}
GENERIC_LEADING_WORDS = {
    "A",
    "An",
    "And",
    "But",
    "For",
    "How",
    "If",
    "My",
    "Our",
    "The",
    "Their",
    "This",
    "What",
    "Why",
}

TITLE_CASE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9&'\-]{1,29}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,29}){1,3})\b"
)
CONTEXT_PATTERNS = [
    re.compile(
        r"\b(?:my|our|the)\s+(?:store|shop|company|brand|merchant|business)\s+"
        r"(?:called\s+|named\s+|is\s+)?"
        r"([A-Z][A-Za-z0-9&'\-]{1,29}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,29}){0,3})\b"
    ),
    re.compile(
        r"\b(?:store|shop|company|brand|merchant|business)\s+"
        r"(?:called\s+|named\s+|is\s+)?"
        r"([A-Z][A-Za-z0-9&'\-]{1,29}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,29}){0,3})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b([A-Z][A-Za-z0-9&'\-]{1,29}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,29}){0,3})\s+"
        r"(?:store|shop|company|brand|business)\b"
    ),
]


def _init_schema():
    ensure_merchant_slug_guard()
    with engine.connect() as conn:
        init_merchant_graph_tables(conn)
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'
        """))
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
            ADD COLUMN IF NOT EXISTS domain_confidence TEXT
        """))
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS normalized_domain TEXT
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


def _find_merchant_by_name(conn, brand):
    try:
        from runtime.intelligence.merchant_graph import _find_merchant_by_name as graph_find
    except ImportError:
        from merchant_graph import _find_merchant_by_name as graph_find
    return graph_find(conn, brand)


def _iter_regex_matches(content):
    for pattern in CONTEXT_PATTERNS:
        for match in pattern.findall(content or ""):
            if match:
                yield match

    if should_attempt_brand_extraction(content or ""):
        for match in TITLE_CASE_PATTERN.findall(content or ""):
            if match:
                yield match


def _candidate_priority(candidate, contextual_matches, extracted_brands):
    if candidate in contextual_matches:
        return 0
    if candidate in extracted_brands:
        return 1
    return 2


def _extract_candidate_names(content):
    contextual_matches = []
    for raw in _iter_regex_matches(content):
        cleaned = normalize_brand(raw)
        if cleaned:
            contextual_matches.append(cleaned)

    extracted_brands = [
        item["brand"]
        for item in extract_brand_candidates(content or "")
        if item.get("brand")
    ]

    ordered = []
    seen = set()
    for candidate in contextual_matches + extracted_brands:
        key = canonicalize_merchant_name(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)

    ordered.sort(key=lambda item: (_candidate_priority(item, contextual_matches, extracted_brands), len(item)))
    return ordered


def _looks_like_processor(candidate):
    tokens = {token.lower() for token in re.findall(r"[A-Za-z0-9]+", candidate or "")}
    return bool(tokens & PROCESSOR_KEYWORDS)


def _is_viable_candidate(candidate, content):
    cleaned = normalize_brand(candidate)
    if not cleaned:
        return False

    lowered_cleaned = cleaned.lower()
    if lowered_cleaned in BLOCKED_CANDIDATE_PHRASES:
        return False
    if ".html" in lowered_cleaned or ".htm" in lowered_cleaned:
        return False
    if invalid_merchant_name_reason(cleaned):
        return False

    words = cleaned.split()
    if not (1 <= len(words) <= 4):
        return False
    if len(cleaned) < 3:
        return False
    if words and words[0] in GENERIC_LEADING_WORDS:
        return False
    if _looks_like_processor(cleaned):
        return False

    canonical_key = canonicalize_merchant_name(cleaned)
    if len(canonical_key) < 3:
        return False
    candidate_tokens = {token.lower() for token in re.findall(r"[A-Za-z0-9]+", cleaned)}
    if candidate_tokens & BLOCKED_CANDIDATE_TOKENS:
        return False

    processor_hint = None
    lowered = f" {content.lower()} "
    for processor in PROCESSOR_KEYWORDS:
        if f" {processor} " in lowered:
            processor_hint = processor
            break
    return is_valid_merchant_name(cleaned, processor=processor_hint)


def _insert_provisional_merchant(conn, candidate_name):
    merchant_id = conn.execute(text("""
        INSERT INTO merchants (
            canonical_name,
            slug,
            status,
            confidence_score,
            detected_from,
            created_at,
            last_seen
        ) VALUES (
            :name,
            :slug,
            'provisional',
            35,
            'merchant_entity_extraction',
            NOW(),
            NOW()
        )
        ON CONFLICT (slug) DO UPDATE SET
            last_seen = NOW()
        RETURNING id
    """), {
        "name": candidate_name,
        "slug": build_merchant_slug(candidate_name),
    }).scalar_one()
    sync_merchant_entity(conn, merchant_id)
    return merchant_id


def extract_merchants_from_signals(limit=100):
    _init_schema()

    stats = {
        "signals_processed": 0,
        "merchants_extracted": 0,
        "duplicates_skipped": 0,
        "signals_linked": 0,
    }

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content
            FROM signals
            WHERE merchant_id IS NULL
            ORDER BY detected_at DESC NULLS LAST
            LIMIT :limit
        """), {"limit": limit}).fetchall()

        for signal_id, content in rows:
            if stats["merchants_extracted"] >= MAX_MERCHANTS_PER_RUN:
                break

            stats["signals_processed"] += 1
            signal_text = content or ""
            linked = False

            for candidate_name in _extract_candidate_names(signal_text):
                if not _is_viable_candidate(candidate_name, signal_text):
                    continue

                merchant_id = _find_merchant_by_name(conn, candidate_name)
                if merchant_id:
                    stats["duplicates_skipped"] += 1
                else:
                    canonical_existing = check_and_log_canonical_match(conn, candidate_name)
                    if canonical_existing:
                        merchant_id = canonical_existing
                        stats["duplicates_skipped"] += 1
                    else:
                        merchant_id = _insert_provisional_merchant(conn, candidate_name)
                        stats["merchants_extracted"] += 1
                        save_event("merchant_entity_extracted", {
                            "merchant": candidate_name,
                            "signal_id": signal_id,
                            "merchant_id": merchant_id,
                        })

                if merchant_id:
                    updated = conn.execute(text("""
                        UPDATE signals
                        SET merchant_id = :merchant_id
                        WHERE id = :signal_id
                          AND merchant_id IS NULL
                    """), {"merchant_id": merchant_id, "signal_id": signal_id}).rowcount
                    if updated:
                        link_signal_to_merchant(
                            conn,
                            merchant_id,
                            signal_id,
                            confidence=0.35,
                            source="merchant_entity_extraction",
                        )
                        stats["signals_linked"] += 1
                        linked = True
                        break

            if not linked:
                continue

        conn.commit()

    save_event("merchant_entity_extraction_run", stats)
    logger.info(f"Merchant entity extraction complete: {stats}")
    return stats


if __name__ == "__main__":
    print(extract_merchants_from_signals())
