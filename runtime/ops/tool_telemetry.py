"""
runtime/ops/tool_telemetry.py
=============================

Unified tool-invocation telemetry for Meridian.

Problem
-------

Prior to this module we had three independent tool-dispatch surfaces
and no way to answer "which tools is Meridian actually using":

    1. ``runtime/mcp/tools/*.py``        — 16 MCP tools (Claude Desktop / Codex)
    2. ``runtime/conversation/chat_engine.py`` — ~30 handlers routed by the
       chat LLM when the operator talks to Meridian.
    3. ``runtime/worker.py``             — task queue consumer with
       ``web_fetch``, ``web_search``, ``db_query`` etc.

None of the three emitted a ``save_event("tool_invoked", ...)`` with a
consistent shape, so ``SELECT event_type, COUNT(*) FROM events`` showed
zero tool calls for 7 days even though chat_engine and the MCP server
were live.

This module gives all three surfaces a single ``instrument()`` decorator
plus a ``record_tool_call()`` primitive.  The goal is observability only
— when ``MERIDIAN_TOOL_TELEMETRY_ENABLED`` is false everything degrades
to a no-op.

Event shape
-----------

``save_event("tool_invoked", payload)`` where payload is::

    {
        "surface":      "mcp" | "chat" | "worker" | str,
        "tool_name":    "meridian_get_top_opportunities",
        "args_digest":  "sha1-prefix-of-args-json-or-None",
        "ok":           True | False,
        "duration_ms":  12,
        "error":        "KeyError: merchant_id"   # only when ok=False
    }

``args_digest`` is deliberately a short hash (first 12 chars of sha1 of
a best-effort JSON dump) — we do **not** record raw arguments because
some tools accept merchant identifiers, thread ids, email subjects etc.
that we do not want in the events table.  If you need to correlate a
specific invocation, look up the surrounding logs by timestamp.

Feature flag
------------

``MERIDIAN_TOOL_TELEMETRY_ENABLED`` (default: ``true``)
    When ``false``, every public helper becomes a no-op pass-through.
    The decorator returns the wrapped function unchanged; ``record_tool_call``
    returns immediately.  Used to opt out during incident response or
    on a local dev box where ``save_event`` is not wired.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger("meridian.tool_telemetry")

_ENABLED_ENV = "MERIDIAN_TOOL_TELEMETRY_ENABLED"


def is_enabled() -> bool:
    """Return True if tool-invocation telemetry should record events."""
    raw = os.getenv(_ENABLED_ENV, "true").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _args_digest(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str | None:
    """Best-effort short hash of call arguments — never their contents."""
    try:
        payload = json.dumps([args, kwargs], sort_keys=True, default=str)
    except Exception:
        return None
    return hashlib.sha1(payload.encode("utf-8", errors="replace")).hexdigest()[:12]


def record_tool_call(
    surface: str,
    tool_name: str,
    *,
    ok: bool,
    duration_ms: int,
    args_digest: str | None = None,
    error: str | None = None,
) -> None:
    """Emit a single ``tool_invoked`` event.  Safe: never raises."""
    if not is_enabled():
        return
    payload: dict[str, Any] = {
        "surface": str(surface),
        "tool_name": str(tool_name),
        "ok": bool(ok),
        "duration_ms": int(duration_ms),
    }
    if args_digest:
        payload["args_digest"] = args_digest
    if error:
        # Truncate — the events table should not hold long tracebacks.
        payload["error"] = str(error)[:400]
    try:
        from memory.structured.db import save_event

        save_event("tool_invoked", payload)
    except Exception as exc:  # pragma: no cover — telemetry must never crash a call path
        logger.warning("[TOOL_TELEMETRY] save_event suppressed: %s", exc)


F = TypeVar("F", bound=Callable[..., Any])


def instrument(surface: str, tool_name: str | None = None) -> Callable[[F], F]:
    """Decorator that records one ``tool_invoked`` event per call.

    Works on sync and async functions.  The wrapped function's return
    value and exception path are preserved — this is pure measurement.

    Usage::

        @instrument(surface="chat", tool_name="draft_outreach")
        def _draft_outreach(lead_id: int) -> dict: ...

    When ``tool_name`` is None the wrapped function's ``__name__`` is used.
    """

    def decorator(fn: F) -> F:
        resolved_name = tool_name or getattr(fn, "__name__", "anonymous_tool")

        if asyncio.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not is_enabled():
                    return await fn(*args, **kwargs)
                started = time.time()
                digest = _args_digest(args, kwargs)
                try:
                    result = await fn(*args, **kwargs)
                except Exception as exc:
                    record_tool_call(
                        surface,
                        resolved_name,
                        ok=False,
                        duration_ms=int((time.time() - started) * 1000),
                        args_digest=digest,
                        error=f"{exc.__class__.__name__}: {exc}",
                    )
                    raise
                record_tool_call(
                    surface,
                    resolved_name,
                    ok=True,
                    duration_ms=int((time.time() - started) * 1000),
                    args_digest=digest,
                )
                return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            if not is_enabled():
                return fn(*args, **kwargs)
            started = time.time()
            digest = _args_digest(args, kwargs)
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                record_tool_call(
                    surface,
                    resolved_name,
                    ok=False,
                    duration_ms=int((time.time() - started) * 1000),
                    args_digest=digest,
                    error=f"{exc.__class__.__name__}: {exc}",
                )
                raise
            record_tool_call(
                surface,
                resolved_name,
                ok=True,
                duration_ms=int((time.time() - started) * 1000),
                args_digest=digest,
            )
            return result

        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = ["is_enabled", "instrument", "record_tool_call"]
