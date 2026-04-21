"""
Merchant neighbor discovery engine.

Discovers additional provisional merchants connected to known merchants using
deterministic heuristics only:
1. Shared processor mentions
2. Domain keyword similarity
3. Co-mention signals
4. Cluster co-membership
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
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.merchant_slug import build_merchant_slug, ensure_merchant_slug_guard

try:
    from runtime.intelligence.merchant_canonicalization import check_and_log_canonical_match
except ImportError:
    from merchant_canonicalization import check_and_log_canonical_match

try:
    from runtime.intelligence.brand_extraction import extract_brand, normalize_brand
except ImportError:
    from brand_extraction import extract_brand, normalize_brand

try:
    from runtime.intelligence.merchant_validation import is_valid_merchant_name, compute_merchant_confidence
except ImportError:
    from merchant_validation import is_valid_merchant_name, compute_merchant_confidence

logger = get_logger("merchant_neighbor_discovery")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
MAX_NEIGHBORS_PER_MERCHANT = 10
MAX_NEIGHBORS_PER_RUN = 50

PROCESSOR_PATTERNS = {
    "stripe": re.compile(r"\bstripe\b", re.IGNORECASE),
    "paypal": re.compile(r"\bpaypal\b", re.IGNORECASE),
    "square": re.compile(r"\bsquare\b", re.IGNORECASE),
}
DOMAIN_SUFFIXES = (
    "store", "shop", "online", "official", "app", "site", "co", "inc",
    "llc", "pay", "payments", "merchant",
)
CO_MENTION_PATTERN = re.compile(
    r"\b(?:and|also|plus|with|versus|vs\.?)\s+[\"'\u201c]?([A-Z][A-Za-z0-9&'\-]{1,29}"
    r"(?:\s+[A-Z][A-Za-z0-9&'\-]{1,29}){0,2})[\"'\u201d]?",
    re.IGNORECASE,
)


def _init_neighbor_schema():
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE merchants
            ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'
        """))
        conn.commit()


def _normalize_key(value):
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _parse_signal_ids(raw_signal_ids):
    if not raw_signal_ids:
        return []
    try:
        parsed = json.loads(raw_signal_ids) if isinstance(raw_signal_ids, str) else raw_signal_ids
    except (TypeError, ValueError):
        return []

    signal_ids = []
    for item in parsed or []:
        try:
            signal_ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return signal_ids


def _extract_domain_keyword(domain):
    if not domain:
        return None

    host = domain.split("/")[0].lower()
    base = host.split(".")[0]
    base = re.sub(r"[^a-z0-9]", "", base)
    if len(base) < 4:
        return None

    changed = True
    while changed:
        changed = False
        for suffix in DOMAIN_SUFFIXES:
            if base.endswith(suffix) and len(base) - len(suffix) >= 4:
                base = base[: -len(suffix)]
                changed = True
                break
    return base if len(base) >= 4 else None


def _merchant_exists(conn, candidate_name):
    candidate_slug = build_merchant_slug(candidate_name)
    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE slug = :slug OR LOWER(canonical_name) = LOWER(:name)
        LIMIT 1
    """), {"name": candidate_name, "slug": candidate_slug}).fetchone()
    if row:
        return row[0]

    row = conn.execute(text("""
        SELECT id
        FROM merchants
        WHERE LOWER(canonical_name) LIKE LOWER(:prefix)
        LIMIT 1
    """), {"prefix": f"{candidate_name}%"}).fetchone()
    return row[0] if row else None


def _create_provisional_merchant(conn, candidate_name):
    candidate_slug = build_merchant_slug(candidate_name)
    conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS validation_source TEXT DEFAULT 'manual'"))
    conn.execute(text("ALTER TABLE merchants ADD COLUMN IF NOT EXISTS confidence_score INTEGER DEFAULT 0"))

    domain = None
    processor_mentions = 0
    validation_source = "neighbor_discovery"

    if (
        validation_source == "neighbor_discovery"
        and domain is None
        and processor_mentions == 0
    ):
        save_event(
            "neighbor_discovery_skipped",
            {"candidate": candidate_name, "reason": "no_domain_no_processor"}
        )
        return None

    conf_score = compute_merchant_confidence(domain, processor_mentions, None, 0)
    save_event("merchant_confidence_calculated", {"merchant": candidate_name, "score": conf_score})

    row = conn.execute(text("""
        INSERT INTO merchants (
            canonical_name, slug, domain, status, detected_from, validation_source, confidence_score
        ) VALUES (
            :name, :slug, NULL, 'provisional', 'neighbor_discovery', 'neighbor_discovery', :conf_score
        )
        ON CONFLICT (slug) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
        RETURNING id
    """), {"name": candidate_name, "slug": candidate_slug, "conf_score": conf_score}).fetchone()
    return row[0] if row else None


def _candidate_from_signal(content, merchant_name=None):
    candidate = extract_brand(content or "", merchant_name=merchant_name)
    return normalize_brand(candidate) if candidate else None


def _infer_processors(conn, merchant_id):
    rows = conn.execute(text("""
        SELECT content
        FROM signals
        WHERE merchant_id = :merchant_id
        ORDER BY detected_at DESC
        LIMIT 30
    """), {"merchant_id": merchant_id}).fetchall()

    processors = set()
    for (content,) in rows:
        text_content = content or ""
        for processor, pattern in PROCESSOR_PATTERNS.items():
            if pattern.search(text_content):
                processors.add(processor)
    return processors


def _collect_processor_neighbors(conn, source_key, processors, seen_candidates):
    candidates = []
    for processor in processors:
        rows = conn.execute(text(f"""
            SELECT id, content, merchant_name
            FROM signals
            WHERE merchant_id IS NULL
              AND content ILIKE '%%{processor}%%'
            ORDER BY detected_at DESC
            LIMIT 20
        """)).fetchall()

        for signal_id, content, merchant_name in rows:
            candidate = _candidate_from_signal(content, merchant_name=merchant_name)
            candidate_key = _normalize_key(candidate)
            if not candidate or not candidate_key or candidate_key == source_key or candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            candidates.append(candidate)
    return candidates


def _collect_domain_neighbors(conn, source_key, domain, seen_candidates):
    keyword = _extract_domain_keyword(domain)
    if not keyword:
        return []

    rows = conn.execute(text("""
        SELECT id, content, merchant_name
        FROM signals
        WHERE merchant_id IS NULL
          AND content ILIKE :keyword
        ORDER BY detected_at DESC
        LIMIT 20
    """), {"keyword": f"%{keyword}%"}).fetchall()

    candidates = []
    for signal_id, content, merchant_name in rows:
        candidate = _candidate_from_signal(content, merchant_name=merchant_name)
        candidate_key = _normalize_key(candidate)
        if not candidate or not candidate_key or candidate_key == source_key or candidate_key in seen_candidates:
            continue
        seen_candidates.add(candidate_key)
        candidates.append(candidate)
    return candidates


def _collect_co_mentions(conn, merchant_name, source_key, seen_candidates):
    rows = conn.execute(text("""
        SELECT id, content
        FROM signals
        WHERE merchant_id IS NULL
          AND content ILIKE :merchant_name
        ORDER BY detected_at DESC
        LIMIT 20
    """), {"merchant_name": f"%{merchant_name}%"}).fetchall()

    candidates = []
    for signal_id, content in rows:
        for match in CO_MENTION_PATTERN.findall(content or ""):
            candidate = normalize_brand(match)
            candidate_key = _normalize_key(candidate)
            if not candidate or not candidate_key or candidate_key == source_key or candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            candidates.append(candidate)
    return candidates


def _collect_cluster_neighbors(conn, merchant_id, source_key, seen_candidates):
    signal_rows = conn.execute(text("""
        SELECT id
        FROM signals
        WHERE merchant_id = :merchant_id
        ORDER BY detected_at DESC
        LIMIT 40
    """), {"merchant_id": merchant_id}).fetchall()
    merchant_signal_ids = {int(row[0]) for row in signal_rows}
    if not merchant_signal_ids:
        return []

    cluster_rows = conn.execute(text("""
        SELECT id, signal_ids
        FROM clusters
        ORDER BY created_at DESC
        LIMIT 200
    """)).fetchall()

    cluster_candidates = []
    for cluster_id, signal_ids_raw in cluster_rows:
        signal_ids = _parse_signal_ids(signal_ids_raw)
        if not signal_ids or merchant_signal_ids.isdisjoint(signal_ids):
            continue

        rows = conn.execute(text(f"""
            SELECT id, content, merchant_name
            FROM signals
            WHERE id IN ({",".join(str(int(signal_id)) for signal_id in signal_ids[:200])})
              AND merchant_id IS NULL
        """)).fetchall()

        for signal_id, content, merchant_name in rows:
            candidate = _candidate_from_signal(content, merchant_name=merchant_name)
            candidate_key = _normalize_key(candidate)
            if not candidate or not candidate_key or candidate_key == source_key or candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            cluster_candidates.append(candidate)
    return cluster_candidates


def run_merchant_neighbor_discovery(batch_size=100):
    _init_neighbor_schema()
    ensure_merchant_slug_guard()

    neighbors_scanned = 0
    neighbors_created = 0
    neighbors_skipped_limit = 0

    with engine.connect() as conn:
        merchants = conn.execute(text("""
            SELECT id, canonical_name, domain
            FROM merchants
            ORDER BY distress_score DESC, last_seen DESC
            LIMIT :limit
        """), {"limit": batch_size}).fetchall()

        for merchant_id, merchant_name, domain in merchants:
            if neighbors_created >= MAX_NEIGHBORS_PER_RUN:
                break

            source_key = _normalize_key(merchant_name)
            seen_candidates = set()
            neighbors_created_for_merchant = 0
            processor_candidates = _collect_processor_neighbors(
                conn, source_key, _infer_processors(conn, merchant_id), seen_candidates
            )
            domain_candidates = _collect_domain_neighbors(conn, source_key, domain, seen_candidates)
            co_mention_candidates = _collect_co_mentions(conn, merchant_name, source_key, seen_candidates)
            cluster_candidates = _collect_cluster_neighbors(conn, merchant_id, source_key, seen_candidates)

            for candidate_name in (
                processor_candidates
                + domain_candidates
                + co_mention_candidates
                + cluster_candidates
            ):
                if neighbors_created >= MAX_NEIGHBORS_PER_RUN:
                    neighbors_skipped_limit += 1
                    break
                if neighbors_created_for_merchant >= MAX_NEIGHBORS_PER_MERCHANT:
                    neighbors_skipped_limit += 1
                    break

                neighbors_scanned += 1
                if _merchant_exists(conn, candidate_name):
                    continue

                # Canonical duplicate check
                canonical_existing = check_and_log_canonical_match(conn, candidate_name)
                if canonical_existing:
                    continue

                if not is_valid_merchant_name(candidate_name):
                    save_event("merchant_validation_rejected", {"name": candidate_name})
                    continue

                _create_provisional_merchant(conn, candidate_name)
                neighbors_created += 1
                neighbors_created_for_merchant += 1
                save_event("merchant_neighbor_discovered", {
                    "source_merchant": merchant_name,
                    "neighbor": candidate_name,
                })

        conn.commit()

    result = {
        "merchants_scanned": len(merchants),
        "neighbors_scanned": neighbors_scanned,
        "neighbors_created": neighbors_created,
        "neighbors_skipped_limit": neighbors_skipped_limit,
    }
    save_event("merchant_neighbor_discovery_run", {
        "merchants_scanned": len(merchants),
        "neighbors_created": neighbors_created,
    })
    logger.info(f"Merchant neighbor discovery complete: {result}")
    return result


if __name__ == "__main__":
    print(run_merchant_neighbor_discovery())
