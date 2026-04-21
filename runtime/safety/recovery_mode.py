"""
Dual-lock recovery mode — fail-closed on any disagreement or read failure.

Writers MUST gate on is_recovery_mode(). The check is cached for 2 seconds
to keep the hot path cheap; force_refresh() invalidates the cache.

Both Redis and Postgres must agree on 'normal' for the system to leave
recovery mode. Any disagreement, missing flag, or read failure => RECOVERY.
"""
from __future__ import annotations
import os, time, logging
import redis
from sqlalchemy import text
from memory.structured.db import engine

log = logging.getLogger("recovery_mode")

_REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
_REDIS_KEY  = "agent_flux:system_mode"
_PG_KEY     = "mode"
_CACHE_TTL  = 2.0

_r = redis.Redis(
    host=_REDIS_HOST, port=_REDIS_PORT, decode_responses=True,
    socket_timeout=1.0, socket_connect_timeout=1.0,
)
_cache: dict = {"value": None, "at": 0.0}


def _redis_mode() -> str:
    try:
        v = _r.get(_REDIS_KEY)
        return (v or "").strip().lower()
    except Exception as exc:
        log.warning("recovery_mode: redis read failed (%s) — assuming recovery", exc)
        return "recovery"


def _pg_mode() -> str:
    try:
        with engine.connect() as c:
            row = c.execute(
                text("SELECT value FROM system_mode WHERE key = :k"),
                {"k": _PG_KEY},
            ).fetchone()
            return (row[0] if row else "").strip().lower()
    except Exception as exc:
        log.warning("recovery_mode: postgres read failed (%s) — assuming recovery", exc)
        return "recovery"


def _resolve_mode() -> str:
    rm, pm = _redis_mode(), _pg_mode()
    if rm == "normal" and pm == "normal":
        return "normal"
    if rm != pm:
        log.warning(
            "recovery_mode: dual-lock disagreement redis=%s pg=%s — assuming recovery",
            rm, pm,
        )
    return "recovery"


def is_recovery_mode() -> bool:
    now = time.time()
    if _cache["value"] is not None and (now - _cache["at"]) < _CACHE_TTL:
        return _cache["value"] == "recovery"
    mode = _resolve_mode()
    _cache["value"] = mode
    _cache["at"] = now
    return mode == "recovery"


def force_refresh() -> str:
    """Invalidate cache and return the current resolved mode."""
    _cache["at"] = 0.0
    _cache["value"] = None
    return _resolve_mode()


def current_mode() -> str:
    """Diagnostic accessor; bypasses cache."""
    return _resolve_mode()
