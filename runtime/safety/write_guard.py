"""
Global write enforcement. Every DB and Redis write MUST be wrapped.

Recovery-mode behavior:
  - Writes raise WriteBlocked unless subsystem is in ALLOWLIST.
  - Allowlist intentionally minimal:
      * 'system_recovery' — recovery scripts run by SREs
      * 'queue_runtime'   — worker LMOVE bookkeeping (NOT tool side-effects)
  - 'stripe_webhook' is NOT in the runtime-write allowlist; the webhook handler
     itself returns 503 in recovery mode (see runtime/api/stripe_checkout.py).
"""
from __future__ import annotations
import functools, logging
from contextlib import contextmanager
from typing import Callable
from runtime.safety.recovery_mode import is_recovery_mode

log = logging.getLogger("write_guard")


class WriteBlocked(Exception):
    pass


ALLOWLIST: dict[str, bool] = {
    "system_recovery": True,
    "queue_runtime":   True,
}

_WRITE_VERBS = ("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
                "ALTER", "DROP", "CREATE", "COPY")


def _is_write_sql(sql: str) -> bool:
    s = sql.lstrip().upper()
    return any(s.startswith(v) for v in _WRITE_VERBS)


def assert_write_allowed(subsystem: str) -> None:
    if not is_recovery_mode():
        return
    if ALLOWLIST.get(subsystem) is True:
        return
    raise WriteBlocked(
        f"write blocked: subsystem={subsystem!r} not allowed during recovery mode"
    )


def guarded_write(subsystem: str):
    """Decorator for any function that performs writes."""
    def deco(fn: Callable):
        @functools.wraps(fn)
        def inner(*a, **kw):
            assert_write_allowed(subsystem)
            return fn(*a, **kw)
        return inner
    return deco


@contextmanager
def guarded_conn(eng, subsystem: str):
    """Yields a connection whose .execute() rejects write-class statements
    when recovery mode is active and subsystem is not allowlisted."""
    assert_write_allowed(subsystem)
    with eng.connect() as conn:
        original = conn.execute

        def execute(stmt, *a, **kw):
            sql_text = str(getattr(stmt, "text", stmt))
            if _is_write_sql(sql_text):
                assert_write_allowed(subsystem)
            return original(stmt, *a, **kw)

        conn.execute = execute  # type: ignore[assignment]
        try:
            yield conn
        finally:
            conn.execute = original  # type: ignore[assignment]


# ── Redis wrapper ───────────────────────────────────────────────────────────

_REDIS_WRITE_CMDS = frozenset({
    "set", "setex", "setnx", "mset", "msetnx", "append", "getset",
    "del", "unlink", "expire", "pexpire", "persist", "rename", "renamenx",
    "lpush", "rpush", "lpop", "rpop", "lmove", "blmove", "brpoplpush", "rpoplpush",
    "lrem", "lset", "ltrim",
    "sadd", "srem", "spop", "smove",
    "zadd", "zrem", "zincrby", "zpopmin", "zpopmax",
    "hset", "hsetnx", "hmset", "hdel", "hincrby", "hincrbyfloat",
    "incr", "decr", "incrby", "decrby", "incrbyfloat",
    "publish", "xadd", "xdel", "xtrim",
    "flushdb", "flushall",
})


class GuardedRedis:
    """Wrap any redis.Redis client with subsystem-scoped write enforcement."""
    def __init__(self, client, subsystem: str):
        object.__setattr__(self, "_c", client)
        object.__setattr__(self, "_subsystem", subsystem)

    def __getattr__(self, name: str):
        attr = getattr(self._c, name)
        if not callable(attr) or name.lower() not in _REDIS_WRITE_CMDS:
            return attr
        sub = self._subsystem

        @functools.wraps(attr)
        def wrapped(*a, **kw):
            assert_write_allowed(sub)
            return attr(*a, **kw)
        return wrapped
