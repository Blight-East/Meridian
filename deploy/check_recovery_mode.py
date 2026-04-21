#!/usr/bin/env python3
"""
check_recovery_mode.py — Show the dual-lock recovery state at a glance.

Reads PG `system_mode` and Redis `agent_flux:system_mode`. Reports whether
they agree, the resolved mode, and whether learning is enabled.

Usage:
    python3 deploy/check_recovery_mode.py
    python3 deploy/check_recovery_mode.py --json
    python3 deploy/check_recovery_mode.py --force-refresh
"""
from __future__ import annotations
import os, sys, json, argparse, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import redis
from sqlalchemy import text

from memory.structured.db import engine
from runtime.safety.recovery_mode import (
    is_recovery_mode,
    current_mode,
    force_refresh,
    _cache,
    _REDIS_KEY,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
_r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                 socket_timeout=1.0, socket_connect_timeout=1.0)


def _read_pg_mode_and_learning() -> tuple[str | None, str | None, str | None]:
    try:
        with engine.connect() as c:
            mode = c.execute(text("SELECT value FROM system_mode WHERE key='mode'")).scalar()
            learning = c.execute(text("SELECT value FROM system_mode WHERE key='learning_enabled'")).scalar()
        return ((mode or "").strip().lower() or None,
                (learning or "").strip().lower() or None,
                None)
    except Exception as e:
        return (None, None, f"{type(e).__name__}: {e}")


def _read_redis_mode() -> tuple[str | None, str | None]:
    try:
        v = _r.get(_REDIS_KEY)
        return ((v or "").strip().lower() or None, None)
    except Exception as e:
        return (None, f"{type(e).__name__}: {e}")


def collect(force_refresh_cache: bool) -> dict:
    if force_refresh_cache:
        force_refresh()
    pg_mode, learning, pg_err = _read_pg_mode_and_learning()
    redis_mode, redis_err = _read_redis_mode()
    resolved = current_mode()  # bypasses cache
    cache_age = None if _cache["at"] == 0 else round(time.time() - _cache["at"], 2)
    return {
        "resolved_mode": resolved,
        "pg_mode": pg_mode,
        "redis_mode": redis_mode,
        "dual_lock_agrees": (pg_mode is not None
                             and redis_mode is not None
                             and pg_mode == redis_mode),
        "is_recovery": resolved == "recovery",
        "learning_enabled": learning,
        "pg_error": pg_err,
        "redis_error": redis_err,
        "cache_age_seconds": cache_age,
    }


def render_human(d: dict) -> str:
    mode = (d["resolved_mode"] or "?").upper()
    pg = d["pg_mode"] or "?"
    rd = d["redis_mode"] or "?"
    agree = "yes" if d["dual_lock_agrees"] else "NO"
    learning = d["learning_enabled"] or "?"
    cache = ("never" if d["cache_age_seconds"] is None
             else f"{d['cache_age_seconds']}s old")
    lines = [
        f"Mode:                {mode} (PG={pg}, Redis={rd})",
        f"Dual-lock agreement: {agree}",
        f"Learning enabled:    {learning}",
        f"Cache state:         {cache}",
    ]
    if d["pg_error"]:
        lines.append(f"PG read error:       {d['pg_error']}")
    if d["redis_error"]:
        lines.append(f"Redis read error:    {d['redis_error']}")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description="Show dual-lock recovery state.")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of human text.")
    p.add_argument("--force-refresh", action="store_true", help="Invalidate the recovery_mode cache before reading.")
    args = p.parse_args()
    d = collect(force_refresh_cache=args.force_refresh)
    if args.json:
        print(json.dumps(d, indent=2))
    else:
        print(render_human(d))
    sys.exit(0 if not d["is_recovery"] else 1)


if __name__ == "__main__":
    main()
