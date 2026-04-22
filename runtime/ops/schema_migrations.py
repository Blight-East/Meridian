"""
runtime/ops/schema_migrations.py
================================

Phase 1 of the DB hardening work.

Single source of truth for DDL in Agent Flux.  Replaces the ad-hoc
``_ensure_*_schema()`` / ``_init_*_column()`` pattern spread across 28
modules with:

1. A cluster-wide **advisory lock** (``pg_advisory_lock(<MIGRATION_LOCK_KEY>)``)
   that serializes every schema mutation across the 5 PM2 services
   (api / worker / scheduler / telegram / mcp).  Only one process
   applies DDL at a time; the rest block and then observe the migration
   already-applied.

2. A **``schema_migrations``** tracking table (created on first run) so
   we know exactly which named migration has been applied.

3. A **registry** of ``@register_migration("name_v1")`` entries.
   Modules register their DDL closures at import time; ``run_pending()``
   enumerates the registry and applies anything not in the tracking
   table.

Existing ``_ensure_*_schema`` functions are *not* removed — this phase
is additive.  ``run_pending()`` is wired to run on import from the
scheduler's ``cron_tasks`` (see the Phase 2 change).  Other services
will pick up the same schema lazily when they first call any
registered migration helper.

Feature flag
------------

``MERIDIAN_SCHEMA_MIGRATIONS_ENABLED`` (default ``true``).  When
``false``, ``run_pending()`` is a no-op and registered migrations are
not applied here — legacy ``_ensure_*_schema`` paths continue to run
exactly as before.  The flag exists to give operators a fast rollback
switch; it should normally be left on.

Utility helpers
---------------

``with_advisory_lock(name)`` — context manager that acquires a
txn-scoped advisory lock for a single DDL batch.  Intended for
wrapping legacy ``_ensure_*_schema`` functions without fully
migrating them into the registry.  When the flag is off, it yields
without taking a lock (current behaviour preserved).
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Callable, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger("meridian.schema_migrations")

# Fixed advisory-lock key — shared by every process that calls
# ``run_pending()``.  Chosen as a 32-bit fingerprint of the string
# "meridian:schema_migrations:v1" so it's deterministic and namespaced.
MIGRATION_LOCK_KEY = int.from_bytes(
    hashlib.blake2b(b"meridian:schema_migrations:v1", digest_size=4).digest(),
    "big",
    signed=True,
)

_ENABLED_ENV = "MERIDIAN_SCHEMA_MIGRATIONS_ENABLED"
_MIGRATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    id           TEXT PRIMARY KEY,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by   TEXT NOT NULL DEFAULT current_user,
    checksum     TEXT,
    notes        TEXT
)
"""


# ---------------------------------------------------------------------------
# Flag helpers
# ---------------------------------------------------------------------------


def _flag(name: str, default: str = "true") -> bool:
    raw = os.environ.get(name, default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def is_enabled() -> bool:
    """Return True when the migration runner should execute."""
    return _flag(_ENABLED_ENV, "true")


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Migration:
    id: str
    apply: Callable[[Connection], None]
    notes: str = ""


_REGISTRY: list[Migration] = []
_REGISTERED_IDS: set[str] = set()


def register_migration(
    migration_id: str, *, notes: str = ""
) -> Callable[[Callable[[Connection], None]], Callable[[Connection], None]]:
    """Decorator — register a DDL closure under a stable ID.

    The closure receives a live SQLAlchemy ``Connection`` (inside the
    migration runner's open transaction).  It must be idempotent —
    callers should use ``CREATE TABLE IF NOT EXISTS`` /
    ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` patterns so a manual
    replay is always safe.

    Attempting to re-register the same ID raises ``ValueError``.
    """

    def _decorator(fn: Callable[[Connection], None]) -> Callable[[Connection], None]:
        if migration_id in _REGISTERED_IDS:
            raise ValueError(f"migration already registered: {migration_id}")
        _REGISTERED_IDS.add(migration_id)
        _REGISTRY.append(Migration(id=migration_id, apply=fn, notes=notes))
        return fn

    return _decorator


def registered() -> list[Migration]:
    """Snapshot of registered migrations in registration order."""
    return list(_REGISTRY)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _applied_ids(conn: Connection) -> set[str]:
    rows = conn.execute(text("SELECT id FROM schema_migrations")).fetchall()
    return {r[0] for r in rows}


def _record(conn: Connection, migration: Migration) -> None:
    conn.execute(
        text(
            "INSERT INTO schema_migrations (id, notes) VALUES (:id, :notes) "
            "ON CONFLICT (id) DO NOTHING"
        ),
        {"id": migration.id, "notes": migration.notes or ""},
    )


def run_pending(*, engine=None) -> dict:
    """Apply every registered migration not yet recorded.

    Returns a summary dict ``{"applied": [...], "skipped": [...],
    "enabled": bool}``.  Safe to call repeatedly — idempotent.

    This function acquires ``pg_advisory_lock(MIGRATION_LOCK_KEY)`` for
    its full duration.  Concurrent callers block on the same key and
    then observe the migrations already applied.
    """
    result = {"applied": [], "skipped": [], "enabled": is_enabled()}
    if not result["enabled"]:
        logger.info(
            "[DB_HARDENING] schema_migrations disabled via %s=false; skipping.",
            _ENABLED_ENV,
        )
        return result

    if engine is None:
        # Imported lazily so this module is cheap to import at test time.
        from memory.structured.db import engine as _engine

        engine = _engine

    with engine.connect() as conn:
        # Acquire the advisory lock BEFORE creating the tracking table so
        # two concurrent cold-start PM2 processes don't race on the
        # CREATE itself.
        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": MIGRATION_LOCK_KEY})
        try:
            conn.execute(text(_MIGRATIONS_TABLE_SQL))
            conn.commit()
            applied = _applied_ids(conn)
            for migration in _REGISTRY:
                if migration.id in applied:
                    result["skipped"].append(migration.id)
                    continue
                logger.info(
                    "[DB_HARDENING] applying migration %s (%s)",
                    migration.id,
                    migration.notes or "no notes",
                )
                # Each migration runs in its own transaction with a
                # bounded lock_timeout so a lock hog fails fast rather
                # than blocking the entire fleet.
                with conn.begin():
                    conn.execute(text("SET LOCAL lock_timeout = '60s'"))
                    migration.apply(conn)
                    _record(conn, migration)
                result["applied"].append(migration.id)
        finally:
            conn.execute(
                text("SELECT pg_advisory_unlock(:k)"), {"k": MIGRATION_LOCK_KEY}
            )
            conn.commit()

    logger.info(
        "[DB_HARDENING] run_pending complete: applied=%d skipped=%d",
        len(result["applied"]),
        len(result["skipped"]),
    )
    return result


# ---------------------------------------------------------------------------
# Advisory-lock helper for legacy _ensure_*_schema call sites
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def with_advisory_lock(name: str, *, engine=None):
    """Context manager that serializes a DDL batch across the fleet.

    Usage inside a legacy ``_ensure_*_schema`` function::

        def _ensure_contact_discovery_schema():
            with with_advisory_lock("contact_discovery_schema"):
                with engine.connect() as conn:
                    conn.execute(text("ALTER TABLE ... ADD COLUMN ..."))
                    ...

    ``name`` is hashed to a stable 32-bit key so the same string always
    maps to the same ``pg_advisory_xact_lock`` slot.  When the feature
    flag is OFF the context manager is a pass-through, preserving
    current behaviour.
    """
    if not is_enabled():
        yield
        return

    if engine is None:
        from memory.structured.db import engine as _engine

        engine = _engine

    key = int.from_bytes(
        hashlib.blake2b(
            f"meridian:ensure:{name}".encode("utf-8"), digest_size=4
        ).digest(),
        "big",
        signed=True,
    )

    with engine.connect() as conn:
        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": key})
        try:
            yield
        finally:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
            conn.commit()


# ---------------------------------------------------------------------------
# CLI — used by deploy.sh in Phase 3
# ---------------------------------------------------------------------------


def _main() -> int:  # pragma: no cover
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Run registered schema migrations")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute run_pending() against the configured DATABASE_URL",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List registered migrations without applying",
    )
    args = parser.parse_args()

    # Discovery — import any module that registers migrations.  These
    # imports are explicit so a typo doesn't silently skip a migration.
    _discover_registered_migrations()

    if args.list:
        for m in registered():
            print(f"{m.id}\t{m.notes}")
        return 0

    if args.apply:
        result = run_pending()
        print(json.dumps(result, indent=2, default=str))
        return 0

    parser.print_help()
    return 1


def _discover_registered_migrations() -> None:
    """Side-effect-ful imports that populate the registry.

    Kept in one place so ``--list`` and CI can confirm every
    registration site loads cleanly.
    """
    # Imported for their @register_migration side effects only.
    from runtime.intelligence import contact_discovery  # noqa: F401
    from runtime.intelligence import opportunity_scoring  # noqa: F401


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
