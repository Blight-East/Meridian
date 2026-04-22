"""
runtime/conversation/slash_commands.py
======================================

Deterministic Telegram slash-command surface for Meridian's MCP tools.

Why
---

Until now the only way for the operator to invoke a Meridian tool from
Telegram was to type a natural-language request and hope the chat LLM
routed it correctly through ``runtime.conversation.chat_engine`` →
``runtime.conversation.intent_router`` → ``runtime.ops.operator_commands``.

That's three layers of ambiguity for commands the operator already
knows the exact name of.  The ``runtime/mcp/tools/*`` module registers
16 tools for Claude Desktop / Codex; nothing exposed them to Telegram.

This module wires thin, deterministic slash-command handlers around
the same ``runtime.ops.operator_commands`` functions the MCP tools
already delegate to, so the operator can type::

    /top 5
    /briefing
    /approve 1234
    /send 1234

…and bypass the LLM classification hop entirely.

Design
------

- Handlers are async wrappers that:
    1. Call ``_require_authorized`` exactly like every other Telegram
       handler.
    2. Parse the command args with a small per-command helper.
    3. Emit a ``tool_invoked`` telemetry event via
       ``runtime.ops.tool_telemetry`` (surface ``telegram``).
    4. Invoke the corresponding ``operator_commands`` function.
    5. Reply using ``_reply_and_record`` and a shared formatter.

- The handlers are purely additive — the existing free-text ``chat``
  handler keeps working for everything that is not a slash command.

- A single feature flag, ``MERIDIAN_SLASH_COMMANDS_ENABLED`` (default
  ``true``), controls whether ``register_slash_commands`` installs
  them at bot startup.  Set it to ``false`` to roll back instantly
  without a code change.

- ``/send`` is NOT an auto-send path.  It still goes through
  ``send_outreach_for_opportunity_command`` which enforces the
  approval-gated Gmail contract; the operator issuing ``/send``
  constitutes explicit approval for a draft that has already passed
  ``approve_outreach_for_opportunity_command``.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Awaitable

logger = logging.getLogger("meridian.slash_commands")

_ENABLED_ENV = "MERIDIAN_SLASH_COMMANDS_ENABLED"


def is_enabled() -> bool:
    raw = os.getenv(_ENABLED_ENV, "true").strip().lower()
    return raw in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Response formatting
# ---------------------------------------------------------------------------

_MAX_REPLY_CHARS = 3800  # Telegram caps messages around 4096; leave headroom.


def _format_response(result: Any) -> str:
    """Render a command return value into Telegram-safe text."""
    if result is None:
        return "(no result)"
    if isinstance(result, str):
        return result[:_MAX_REPLY_CHARS] if result else "(empty)"
    if isinstance(result, (int, float, bool)):
        return str(result)
    # dict / list — dump as pretty JSON, then truncate.
    try:
        body = json.dumps(result, indent=2, sort_keys=True, default=str)
    except Exception:
        body = str(result)
    if len(body) > _MAX_REPLY_CHARS:
        body = body[: _MAX_REPLY_CHARS - 20] + "\n... (truncated)"
    return body


def _parse_int(args: list[str], index: int = 0, default: int | None = None) -> int | None:
    if index >= len(args):
        return default
    try:
        return int(args[index])
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Handler factories
# ---------------------------------------------------------------------------
#
# Each factory closes over the command name + the ``operator_commands`` callable
# and returns an async ``(update, context) -> None`` handler compatible with
# ``telegram.ext.CommandHandler``.
#
# We accept ``require_auth`` and ``reply_and_record`` as parameters so this
# module doesn't have to import from ``runtime.telegram_bot`` (avoids circular
# imports — ``telegram_bot.py`` calls ``register_slash_commands`` in its own
# ``run()``).

AuthCallable = Callable[[Any], Awaitable[Any]]
ReplyCallable = Callable[[Any, str], Awaitable[None]]


def _make_handler(
    tool_name: str,
    run_cmd: Callable[[list[str]], Any],
    usage: str,
    require_auth: AuthCallable,
    reply_and_record: ReplyCallable,
) -> Callable[[Any, Any], Awaitable[None]]:
    """Build an async Telegram handler for a single slash command."""
    from runtime.ops.tool_telemetry import record_tool_call  # local import

    import time as _time

    async def handler(update: Any, context: Any) -> None:
        auth = await require_auth(update)
        if not auth:
            return
        args = list(context.args or [])
        started = _time.time()
        try:
            result = run_cmd(args)
        except TypeError as exc:
            duration_ms = int((_time.time() - started) * 1000)
            try:
                record_tool_call(
                    "telegram", tool_name, ok=False,
                    duration_ms=duration_ms,
                    error=f"TypeError: {exc}",
                )
            except Exception:
                pass
            await reply_and_record(
                update.message,
                f"Usage: {usage}\n\n(Internal: {exc})",
            )
            return
        except Exception as exc:
            duration_ms = int((_time.time() - started) * 1000)
            try:
                record_tool_call(
                    "telegram", tool_name, ok=False,
                    duration_ms=duration_ms,
                    error=f"{exc.__class__.__name__}: {exc}",
                )
            except Exception:
                pass
            logger.exception("[SLASH] %s failed", tool_name)
            await reply_and_record(
                update.message,
                f"/{tool_name} failed: {exc.__class__.__name__}: {exc}",
            )
            return
        duration_ms = int((_time.time() - started) * 1000)
        try:
            record_tool_call(
                "telegram", tool_name, ok=True,
                duration_ms=duration_ms,
            )
        except Exception:
            pass
        await reply_and_record(update.message, _format_response(result))

    return handler


# ---------------------------------------------------------------------------
# Command map
# ---------------------------------------------------------------------------


def build_slash_commands(
    require_auth: AuthCallable,
    reply_and_record: ReplyCallable,
) -> list[tuple[str, Callable[[Any, Any], Awaitable[None]]]]:
    """Return a list of (command_name, handler) pairs ready for
    ``app.add_handler(CommandHandler(name, fn))``.

    The imports happen at call time so that the import of this module
    doesn't pull in the full ``operator_commands`` dependency graph
    unless the Telegram bot is actually starting up.
    """
    from runtime.ops.operator_commands import (
        advance_top_queue_opportunity_command,
        approve_outreach_for_opportunity_command,
        list_sent_outreach_needing_follow_up_command,
        rewrite_outreach_draft_command,
        send_outreach_for_opportunity_command,
        show_daily_operator_briefing_command,
        show_local_outreach_draft_command,
        show_local_outreach_drafts_command,
        show_opportunity_workbench_command,
        show_reply_review_command,
        show_reply_review_queue_command,
        show_top_queue_opportunities_command,
        show_value_heartbeat_command,
    )

    commands: list[tuple[str, Callable[[list[str]], Any]]] = [
        (
            "top",
            lambda args: show_top_queue_opportunities_command(
                limit=_parse_int(args, 0, 3) or 3,
            ),
        ),
        (
            "briefing",
            lambda args: show_daily_operator_briefing_command(),
        ),
        (
            "drafts",
            lambda args: show_local_outreach_drafts_command(
                limit=_parse_int(args, 0, 5) or 5,
            ),
        ),
        (
            "draft",
            lambda args: show_local_outreach_draft_command(
                opportunity_id=_parse_int(args, 0, None),
            ),
        ),
        (
            "workbench",
            lambda args: show_opportunity_workbench_command(
                opportunity_id=_parse_int(args, 0, None),
                merchant_domain=args[1] if len(args) > 1 else "",
            ),
        ),
        (
            "replies",
            lambda args: show_reply_review_queue_command(
                limit=_parse_int(args, 0, 5) or 5,
            ),
        ),
        (
            "reply",
            lambda args: show_reply_review_command(
                opportunity_id=_parse_int(args, 0, 0) or 0,
            ),
        ),
        (
            "follow_ups",
            lambda args: list_sent_outreach_needing_follow_up_command(
                limit=_parse_int(args, 0, 10) or 10,
            ),
        ),
        (
            "approve",
            lambda args: approve_outreach_for_opportunity_command(
                opportunity_id=_parse_int(args, 0, 0) or 0,
            ),
        ),
        (
            "send",
            lambda args: send_outreach_for_opportunity_command(
                opportunity_id=_parse_int(args, 0, 0) or 0,
            ),
        ),
        (
            "rewrite",
            lambda args: rewrite_outreach_draft_command(
                opportunity_id=_parse_int(args, 0, 0) or 0,
                style=(args[1] if len(args) > 1 else "standard"),
                instructions=(" ".join(args[2:]) if len(args) > 2 else ""),
            ),
        ),
        (
            "advance",
            lambda args: advance_top_queue_opportunity_command(),
        ),
        (
            "heartbeat",
            lambda args: show_value_heartbeat_command(refresh=False),
        ),
    ]

    usage_overrides = {
        "top": "/top [limit]",
        "briefing": "/briefing",
        "drafts": "/drafts [limit]",
        "draft": "/draft <opportunity_id>",
        "workbench": "/workbench <opportunity_id> [domain]",
        "replies": "/replies [limit]",
        "reply": "/reply <opportunity_id>",
        "follow_ups": "/follow_ups [limit]",
        "approve": "/approve <opportunity_id>",
        "send": "/send <opportunity_id>",
        "rewrite": "/rewrite <opportunity_id> [style] [instructions...]",
        "advance": "/advance",
        "heartbeat": "/heartbeat",
    }

    return [
        (
            name,
            _make_handler(
                tool_name=name,
                run_cmd=fn,
                usage=usage_overrides.get(name, f"/{name}"),
                require_auth=require_auth,
                reply_and_record=reply_and_record,
            ),
        )
        for name, fn in commands
    ]


def register_slash_commands(
    app,
    require_auth: AuthCallable,
    reply_and_record: ReplyCallable,
) -> int:
    """Register every slash command on ``app`` if the feature flag is on.

    Returns the number of handlers registered (0 when the flag is off).
    """
    if not is_enabled():
        logger.info("[SLASH] MERIDIAN_SLASH_COMMANDS_ENABLED=false — skipping registration")
        return 0
    from telegram.ext import CommandHandler  # local import

    handlers = build_slash_commands(require_auth, reply_and_record)
    count = 0
    for name, fn in handlers:
        try:
            app.add_handler(CommandHandler(name, fn))
            count += 1
        except Exception as exc:  # pragma: no cover
            logger.warning("[SLASH] failed to register /%s: %s", name, exc)
    logger.info("[SLASH] registered %d slash commands", count)
    return count


__all__ = [
    "build_slash_commands",
    "is_enabled",
    "register_slash_commands",
]
