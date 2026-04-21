"""
Merchant name canonicalization for duplicate prevention.

Normalizes merchant names by stripping legal suffixes, punctuation,
and common noise words so that "Acme Store Inc." and "Acme" resolve
to the same canonical key.
"""
import re
import sys
import os

from sqlalchemy import text

_dir = os.path.dirname(os.path.abspath(__file__))
for _ in range(5):
    if os.path.isdir(os.path.join(_dir, "config")):
        break
    _dir = os.path.dirname(_dir)
sys.path.insert(0, _dir)

from config.logging_config import get_logger
from memory.structured.db import save_event

logger = get_logger("merchant_canonicalization")

# Suffixes to strip (order matters — longer first)
_SUFFIXES = [
    "incorporated", "corporation", "company", "official",
    "inc.", "inc", "llc", "ltd", "corp", "co.",
    "co", "store", "shop",
]

# Compiled suffix pattern (word-boundary aware, case-insensitive)
_SUFFIX_PATTERN = re.compile(
    r'\b(?:' + '|'.join(re.escape(s) for s in _SUFFIXES) + r')\b\.?',
    re.IGNORECASE,
)


def canonicalize_merchant_name(name: str) -> str:
    """
    Produce a canonical key from a merchant name.

    Steps:
      1. Lowercase
      2. Remove punctuation (keep alphanumeric + spaces)
      3. Remove known business suffixes
      4. Collapse whitespace
      5. Strip leading/trailing whitespace

    Examples:
        "Acme Store Inc."  → "acme"
        "The Widget Co"    → "widget"
        "Bob's Shop LLC"   → "bobs"
    """
    if not name:
        return ""

    canonical = name.lower()

    # Remove possessives before stripping punctuation
    canonical = re.sub(r"'s\b", "s", canonical)

    # Remove punctuation but keep spaces and alphanumeric
    canonical = re.sub(r"[^a-z0-9\s]", "", canonical)

    # Remove known suffixes
    canonical = _SUFFIX_PATTERN.sub("", canonical)

    # Remove leading articles
    canonical = re.sub(r"^(the|a|an)\s+", "", canonical.strip())

    # Collapse whitespace and strip
    canonical = re.sub(r"\s+", " ", canonical).strip()

    # Final pass: remove all spaces for the key (like slug but suffix-aware)
    canonical = canonical.replace(" ", "")

    return canonical


def find_canonical_match(conn, name: str, domain: str = None) -> int | None:
    """
    Check if a merchant already exists by canonical name or domain.

    Returns merchant_id if found, None otherwise.
    """
    # 1. Domain match (strongest signal)
    if domain:
        row = conn.execute(text("""
            SELECT id FROM merchants
            WHERE domain = :domain OR normalized_domain = :domain
            LIMIT 1
        """), {"domain": domain}).fetchone()
        if row:
            return row[0]

    # 2. Canonical name match
    canonical = canonicalize_merchant_name(name)
    if not canonical or len(canonical) < 3:
        return None

    # Check against canonicalized versions of existing merchant names
    row = conn.execute(text("""
        SELECT id, canonical_name FROM merchants
        WHERE regexp_replace(
            regexp_replace(
                lower(canonical_name),
                '[^a-z0-9 ]', '', 'g'
            ),
            '\\s+', '', 'g'
        ) = :canonical
        LIMIT 1
    """), {"canonical": canonical}).fetchone()

    if row:
        return row[0]

    return None


def check_and_log_canonical_match(conn, name: str, domain: str = None) -> int | None:
    """
    Wrapper around find_canonical_match that logs when a match is found.
    Returns merchant_id if duplicate found, None if safe to insert.
    """
    existing_id = find_canonical_match(conn, name, domain)
    if existing_id:
        save_event("merchant_canonicalized", {
            "new_candidate": name,
            "canonical": canonicalize_merchant_name(name),
            "matched_merchant_id": existing_id,
        })
        logger.info(
            f"Canonical match: '{name}' → merchant_id={existing_id} "
            f"(canonical='{canonicalize_merchant_name(name)}')"
        )
    return existing_id
