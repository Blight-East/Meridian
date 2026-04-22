"""
runtime/ops/db_session_watchdog.py
==================================

Phase 2 of the DB hardening work.

Runs on a 60 s cadence from the scheduler (registered in
``runtime/scheduler/cron_tasks.py``).  Reads ``pg_stat_activity`` and
emits a structured snapshot + Redis state so a stuck session can no
longer hide for 19 hours the way the 2026-04-22 incident did.

Behaviour
---------

Every tick:

1. Query ``pg_stat_activity`` for (a) sessions ``state='idle in
   transaction'`` older than ``IDLE_WARN_SECONDS`` and (b) sessions
   waiting on a lock longer than ``LOCK_WARN_SECONDS``.
2. Classify overall health as ``healthy`` / ``attention`` /
   ``degraded`` / ``critical``.
3. Write the snapshot to Redis under
   ``agent_flux:db_session_watchdog:last_snapshot`` and a compact
   status string under ``agent_flux:db_session_watchdog:last_status``.
4. Emit ``save_event("db_session_watchdog_checked", snapshot)``.
5. On transitions into ``degraded`` / ``critical``, emit an operator
   alert via ``send_operator_alert``.
6. If ``MERIDIAN_DB_WATCHDOG_AUTO_TERMINATE=true`` **and** a session
   has been idle-in-transaction longer than ``IDLE_KILL_SECONDS``,
   call ``pg_terminate_backend(pid)``.  Default is OFF — flag must be
   explicitly enabled.

Feature flags
-------------

``MERIDIAN_DB_WATCHDOG_ENABLED`` (default ``true``)
    When ``false``, ``run_db_session_watchdog()`` short-circuits and
    returns ``{"status": "disabled"}`` — no DB query, no state write.

``MERIDIAN_DB_WATCHDOG_AUTO_TERMINATE`` (default ``false``)
    When ``true`` *and* the enabled flag is also on, will terminate
    sessions exceeding ``IDLE_KILL_SECONDS``.  Until explicitly
    enabled the watchdog is visibility-only.

Thresholds
----------

Overridable via env vars:

- ``MERIDIAN_DB_WATCHDOG_IDLE_WARN_SECONDS`` (default ``300`` — 5 min)
- ``MERIDIAN_DB_WATCHDOG_IDLE_KILL_SECONDS`` (default ``900`` — 15 min)
- ``MERIDIAN_DB_WATCHDOG_LOCK_WARN_SECONDS`` (default ``120`` — 2 min)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import redis
from sqlalchemy import text

from memory.structured.db import engine, save_event

logger = logging.getLogger("meridian.db_session_watchdog")
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

WATCHDOG_STATUS_KEY = "agent_flux:db_session_watchdog:last_status"
WATCHDOG_SNAPSHOT_KEY = "agent_flux:db_session_watchdog:last_snapshot"


def _flag(name: str, default: str = "false") -> bool:
    return str(os.environ.get(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def is_enabled() -> bool:
    return _flag("MERIDIAN_DB_WATCHDOG_ENABLED", "true")


def is_auto_terminate_enabled() -> bool:
    return _flag("MERIDIAN_DB_WATCHDOG_AUTO_TERMINATE", "false")


IDLE_WARN_SECONDS = _int_env("MERIDIAN_DB_WATCHDOG_IDLE_WARN_SECONDS", 300)
IDLE_KILL_SECONDS = _int_env("MERIDIAN_DB_WATCHDOG_IDLE_KILL_SECONDS", 900)
LOCK_WARN_SECONDS = _int_env("MERIDIAN_DB_WATCHDOG_LOCK_WARN_SECONDS", 120)


# ---------------------------------------------------------------------------
# Core query + classification
# ---------------------------------------------------------------------------


_ACTIVITY_SQL = text(
    """
    SELECT
        pid,
        usename,
        application_name,
        client_addr::text AS client_addr,
        state,
        wait_event_type,
        wait_event,
        EXTRACT(EPOCH FROM (NOW() - xact_start))::int AS xact_age_seconds,
        EXTRACT(EPOCH FROM (NOW() - query_start))::int AS query_age_seconds,
        LEFT(query, 400) AS query
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND (
            (state = 'idle in transaction' AND xact_start IS NOT NULL)
         OR (wait_event_type = 'Lock')
      )
    ORDER BY xact_age_seconds DESC NULLS LAST, query_age_seconds DESC NULLS LAST
    """
)


def _classify(rows: list[dict]) -> tuple[str, list[dict], list[dict], list[dict]]:
    """Return (status, idle_warn, idle_kill, lock_warn) tuples."""
    idle_warn = []
    idle_kill = []
    lock_warn = []
    for r in rows:
        state = r.get("state") or ""
        wait_type = r.get("wait_event_type") or ""
        xact_age = int(r.get("xact_age_seconds") or 0)
        query_age = int(r.get("query_age_seconds") or 0)
        if state == "idle in transaction" and xact_age >= IDLE_KILL_SECONDS:
            idle_kill.append(r)
        elif state == "idle in transaction" and xact_age >= IDLE_WARN_SECONDS:
            idle_warn.append(r)
        if wait_type == "Lock" and query_age >= LOCK_WARN_SECONDS:
            lock_warn.append(r)

    if idle_kill or lock_warn:
        status = "critical"
    elif idle_warn:
        status = "degraded"
    elif rows:
        status = "attention"
    else:
        status = "healthy"
    return status, idle_warn, idle_kill, lock_warn


def _terminate(pid: int) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
        ).scalar()
        conn.commit()
        return bool(result)


# ---------------------------------------------------------------------------
# Public entry point — wired into cron_tasks
# ---------------------------------------------------------------------------


def run_db_session_watchdog() -> dict[str, Any]:
    """Scheduler cron entry point.  Returns the snapshot for tests / logs."""
    if not is_enabled():
        return {"status": "disabled"}

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(_ACTIVITY_SQL).fetchall()]

    status, idle_warn, idle_kill, lock_warn = _classify(rows)

    terminated: list[dict] = []
    if is_auto_terminate_enabled():
        for row in idle_kill:
            pid = int(row["pid"])
            try:
                ok = _terminate(pid)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "[DB_HARDENING] pg_terminate_backend(%s) failed: %s", pid, exc
                )
                ok = False
            terminated.append({"pid": pid, "terminated": ok, "query": row.get("query")})

    snapshot: dict[str, Any] = {
        "status": status,
        "total_flagged": len(rows),
        "idle_in_transaction_warn": idle_warn,
        "idle_in_transaction_kill": idle_kill,
        "lock_wait_warn": lock_warn,
        "terminated": terminated,
        "thresholds": {
            "idle_warn_seconds": IDLE_WARN_SECONDS,
            "idle_kill_seconds": IDLE_KILL_SECONDS,
            "lock_warn_seconds": LOCK_WARN_SECONDS,
        },
        "auto_terminate_enabled": is_auto_terminate_enabled(),
    }

    try:
        _redis.set(WATCHDOG_STATUS_KEY, status, ex=3600)
        _redis.set(WATCHDOG_SNAPSHOT_KEY, json.dumps(snapshot, default=str), ex=3600)
    except Exception as exc:  # pragma: no cover - redis optional
        logger.warning("[DB_HARDENING] redis write failed: %s", exc)

    save_event("db_session_watchdog_checked", snapshot)

    if status in {"degraded", "critical"}:
        logger.warning(
            "[DB_HARDENING] db_session_watchdog status=%s total=%d idle_warn=%d idle_kill=%d lock_warn=%d terminated=%d",
            status,
            len(rows),
            len(idle_warn),
            len(idle_kill),
            len(lock_warn),
            len(terminated),
        )
        _emit_alert_if_transitioned(status, snapshot)
    else:
        logger.info(
            "[DB_HARDENING] db_session_watchdog status=%s flagged=%d",
            status,
            len(rows),
        )

    return snapshot


# ---------------------------------------------------------------------------
# Alerting — best-effort, only on status transitions
# ---------------------------------------------------------------------------


def _emit_alert_if_transitioned(status: str, snapshot: dict[str, Any]) -> None:
    """Send an operator alert only when status worsens.

    Best-effort: if the alert sender is unavailable, log and continue.
    """
    prev = None
    try:
        prev = _redis.get(WATCHDOG_STATUS_KEY + ":prev")
    except Exception:  # pragma: no cover
        pass

    severity_order = {"healthy": 0, "attention": 1, "degraded": 2, "critical": 3}
    if prev and severity_order.get(status, 0) <= severity_order.get(prev, 0):
        return

    delivered = False
    try:
        from runtime.ops.operator_alerts import send_operator_alert

        message_text = (
            f"[DB_HARDENING] db_session_watchdog status={status}\n"
            f"{_format_alert_body(snapshot)}"
        )
        # send_operator_alert returns True on successful send, False on
        # dedupe-suppression or internal failure.  Only advance the
        # :prev marker when we actually delivered, otherwise a transient
        # delivery failure would silently prevent future retries.
        delivered = bool(
            send_operator_alert(
                message_text,
                dedupe_key=f"db_session_watchdog:{status}",
                cooldown_seconds=900,
            )
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("[DB_HARDENING] operator alert suppressed: %s", exc)

    if delivered:
        try:
            _redis.set(WATCHDOG_STATUS_KEY + ":prev", status, ex=86400)
        except Exception:  # pragma: no cover
            pass


def _format_alert_body(snapshot: dict[str, Any]) -> str:
    lines: list[str] = []
    idle_kill = snapshot.get("idle_in_transaction_kill") or []
    idle_warn = snapshot.get("idle_in_transaction_warn") or []
    lock_warn = snapshot.get("lock_wait_warn") or []
    if idle_kill:
        lines.append(f"Idle-in-transaction sessions >{IDLE_KILL_SECONDS}s: {len(idle_kill)}")
        for r in idle_kill[:3]:
            lines.append(
                f"  pid={r.get('pid')} age={r.get('xact_age_seconds')}s "
                f"app={r.get('application_name')} query={(r.get('query') or '')[:120]}"
            )
    if idle_warn:
        lines.append(f"Idle-in-transaction sessions >{IDLE_WARN_SECONDS}s: {len(idle_warn)}")
    if lock_warn:
        lines.append(f"Sessions waiting on lock >{LOCK_WARN_SECONDS}s: {len(lock_warn)}")
        for r in lock_warn[:3]:
            lines.append(
                f"  pid={r.get('pid')} lock_age={r.get('query_age_seconds')}s "
                f"query={(r.get('query') or '')[:120]}"
            )
    terminated = snapshot.get("terminated") or []
    if terminated:
        lines.append(f"Auto-terminated: {len(terminated)} session(s)")
    if not lines:
        lines.append("(no flagged sessions)")
    return "\n".join(lines)
